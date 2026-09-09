package responses

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/translator/common"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

var (
	user    = ""
	account = ""
	session = ""
)

// ConvertOpenAIResponsesRequestToClaude transforms an OpenAI Responses API request
// into a Claude Messages API request using only gjson/sjson for JSON handling.
// It supports:
// - instructions -> system message
// - input[].type==message with input_text/output_text -> user/assistant messages
// - function_call -> assistant tool_use
// - function_call_output -> user tool_result
// - tools[].parameters -> tools[].input_schema
// - max_output_tokens -> max_tokens
// - stream passthrough via parameter
func ConvertOpenAIResponsesRequestToClaude(modelName string, inputRawJSON []byte, stream bool) []byte {
	return convertOpenAIResponsesRequestToClaude(modelName, inputRawJSON, stream, false)
}

// ConvertOpenAIResponsesRequestToClaudeWithCompat retains unsigned and opaque
// reasoning for a selected model that explicitly supports compatibility mode.
func ConvertOpenAIResponsesRequestToClaudeWithCompat(modelName string, inputRawJSON []byte, stream bool) []byte {
	return convertOpenAIResponsesRequestToClaude(modelName, inputRawJSON, stream, true)
}

func convertOpenAIResponsesRequestToClaude(modelName string, inputRawJSON []byte, stream, isCompat bool) []byte {
	rawJSON := inputRawJSON

	if account == "" {
		u, _ := uuid.NewRandom()
		account = u.String()
	}
	if session == "" {
		u, _ := uuid.NewRandom()
		session = u.String()
	}
	if user == "" {
		sum := sha256.Sum256([]byte(account + session))
		user = hex.EncodeToString(sum[:])
	}
	userID := fmt.Sprintf("user_%s_account_%s_session_%s", user, account, session)

	// Base Claude message payload
	out := []byte(fmt.Sprintf(`{"model":"","max_tokens":32000,"messages":[],"metadata":{"user_id":"%s"}}`, userID))

	root := gjson.ParseBytes(rawJSON)
	var messages claudeResponsesRequestTurns

	// Convert OpenAI Responses reasoning.effort to Claude thinking config.
	if v := root.Get("reasoning.effort"); v.Exists() {
		effort := strings.ToLower(strings.TrimSpace(v.String()))
		if effort != "" {
			mi := registry.LookupModelInfo(modelName, "claude")
			supportsAdaptive := mi != nil && mi.Thinking != nil && len(mi.Thinking.Levels) > 0
			supportsMax := supportsAdaptive && thinking.HasLevel(mi.Thinking.Levels, string(thinking.LevelMax))

			// Claude 4.6 supports adaptive thinking with output_config.effort.
			// MapToClaudeEffort normalizes levels (e.g. minimal→low, xhigh→high) to avoid
			// validation errors since validate treats same-provider unsupported levels as errors.
			if supportsAdaptive {
				switch effort {
				case "none":
					out, _ = sjson.SetBytes(out, "thinking.type", "disabled")
					out, _ = sjson.DeleteBytes(out, "thinking.budget_tokens")
					out, _ = sjson.DeleteBytes(out, "output_config.effort")
				case "auto":
					out, _ = sjson.SetBytes(out, "thinking.type", "adaptive")
					out, _ = sjson.DeleteBytes(out, "thinking.budget_tokens")
					out, _ = sjson.DeleteBytes(out, "output_config.effort")
				default:
					if mapped, ok := thinking.MapToClaudeEffort(effort, supportsMax); ok {
						effort = mapped
					}
					out, _ = sjson.SetBytes(out, "thinking.type", "adaptive")
					out, _ = sjson.DeleteBytes(out, "thinking.budget_tokens")
					out, _ = sjson.SetBytes(out, "output_config.effort", effort)
				}
			} else {
				// Legacy/manual thinking (budget_tokens).
				budget, ok := thinking.ConvertLevelToBudget(effort)
				if ok {
					switch budget {
					case 0:
						out, _ = sjson.SetBytes(out, "thinking.type", "disabled")
					case -1:
						out, _ = sjson.SetBytes(out, "thinking.type", "enabled")
					default:
						if budget > 0 {
							out, _ = sjson.SetBytes(out, "thinking.type", "enabled")
							out, _ = sjson.SetBytes(out, "thinking.budget_tokens", budget)
						}
					}
				}
			}
		}
	}

	// Helper for generating tool call IDs when missing
	genToolCallID := func() string {
		const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		var b strings.Builder
		for i := 0; i < 24; i++ {
			n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
			b.WriteByte(letters[n.Int64()])
		}
		return "toolu_" + b.String()
	}

	// Model
	out, _ = sjson.SetBytes(out, "model", modelName)

	// Max tokens
	if mot := root.Get("max_output_tokens"); mot.Exists() {
		out, _ = sjson.SetBytes(out, "max_tokens", mot.Int())
	}

	// Stream
	out, _ = sjson.SetBytes(out, "stream", stream)

	// Keep operator authority and source ordering before applying local cloaking.
	var systemBlocks [][]byte
	if instr := root.Get("instructions"); instr.Type == gjson.String {
		systemBlocks = append(systemBlocks, common.ClaudeSystemInputBlocks(instr, gjson.Result{})...)
	}
	if input := root.Get("input"); input.IsArray() {
		input.ForEach(func(_, item gjson.Result) bool {
			if common.IsClaudeSystemInputRole(item.Get("role").String()) {
				systemBlocks = append(systemBlocks, common.ClaudeSystemInputBlocks(item.Get("content"), item)...)
			}
			return true
		})
	}

	// input array processing
	if input := root.Get("input"); input.Exists() && input.IsArray() {
		lastToolResult := make(map[string]gjson.Result)
		input.ForEach(func(_, item gjson.Result) bool {
			switch item.Get("type").String() {
			case "function_call_output", "custom_tool_call_output":
				if id := item.Get("call_id").String(); id != "" {
					lastToolResult[id] = item
				}
			}
			return true
		})
		emittedToolResults := make(map[string]bool)
		input.ForEach(func(_, item gjson.Result) bool {
			if common.IsClaudeSystemInputRole(item.Get("role").String()) {
				return true
			}
			typ := item.Get("type").String()
			if typ == "" && item.Get("role").String() != "" {
				typ = "message"
			}
			switch typ {
			case "message":
				// Determine role and construct Claude-compatible content parts.
				var role string
				var textAggregate strings.Builder
				var partsJSON []string
				if parts := item.Get("content"); parts.Exists() && parts.IsArray() {
					parts.ForEach(func(_, part gjson.Result) bool {
						ptype := part.Get("type").String()
						switch ptype {
						case "input_text", "output_text":
							if t := part.Get("text"); t.Exists() {
								txt := t.String()
								textAggregate.WriteString(txt)
								contentPart := []byte(`{"type":"text","text":""}`)
								contentPart, _ = sjson.SetBytes(contentPart, "text", txt)
								partsJSON = append(partsJSON, string(contentPart))
							}
							if ptype == "input_text" {
								role = "user"
							} else {
								role = "assistant"
							}
						case "input_image":
							url := part.Get("image_url").String()
							if url == "" {
								url = part.Get("url").String()
							}
							if url != "" {
								var contentPart []byte
								if strings.HasPrefix(url, "data:") {
									trimmed := strings.TrimPrefix(url, "data:")
									mediaAndData := strings.SplitN(trimmed, ";base64,", 2)
									mediaType := "application/octet-stream"
									data := ""
									if len(mediaAndData) == 2 {
										if mediaAndData[0] != "" {
											mediaType = mediaAndData[0]
										}
										data = mediaAndData[1]
									}
									if data != "" {
										contentPart = []byte(`{"type":"image","source":{"type":"base64","media_type":"","data":""}}`)
										contentPart, _ = sjson.SetBytes(contentPart, "source.media_type", mediaType)
										contentPart, _ = sjson.SetBytes(contentPart, "source.data", data)
									}
								} else {
									contentPart = []byte(`{"type":"image","source":{"type":"url","url":""}}`)
									contentPart, _ = sjson.SetBytes(contentPart, "source.url", url)
								}
								if len(contentPart) > 0 {
									partsJSON = append(partsJSON, string(contentPart))
									if role == "" {
										role = "user"
									}
								}
							}
						case "input_file":
							fileData := part.Get("file_data").String()
							if fileData != "" {
								mediaType := "application/octet-stream"
								data := fileData
								if strings.HasPrefix(fileData, "data:") {
									trimmed := strings.TrimPrefix(fileData, "data:")
									mediaAndData := strings.SplitN(trimmed, ";base64,", 2)
									if len(mediaAndData) == 2 {
										if mediaAndData[0] != "" {
											mediaType = mediaAndData[0]
										}
										data = mediaAndData[1]
									}
								}
								contentPart := []byte(`{"type":"document","source":{"type":"base64","media_type":"","data":""}}`)
								contentPart, _ = sjson.SetBytes(contentPart, "source.media_type", mediaType)
								contentPart, _ = sjson.SetBytes(contentPart, "source.data", data)
								partsJSON = append(partsJSON, string(contentPart))
								if role == "" {
									role = "user"
								}
							}
						}
						return true
					})
				} else if parts.Type == gjson.String {
					textAggregate.WriteString(parts.String())
				}

				// Fallback to given role if content types not decisive
				if role == "" {
					r := item.Get("role").String()
					switch r {
					case "user", "assistant", "system":
						role = r
					default:
						role = "user"
					}
				}

				if len(partsJSON) > 0 {
					for _, partJSON := range partsJSON {
						messages.appendParts(role, []byte(partJSON))
					}
				} else if textAggregate.Len() > 0 || role == "system" {
					messages.appendParts(role, claudeResponsesTextPart(textAggregate.String()))
				}

			case "reasoning":
				messages.appendReasoning(claudeResponsesReplayReasoning(item, isCompat))

			case "function_call", "custom_tool_call":
				// Map to assistant tool_use
				callID := item.Get("call_id").String()
				if callID == "" {
					callID = genToolCallID()
				}
				name := qualifyClaudeResponsesToolName(strings.TrimSpace(item.Get("namespace").String()), strings.TrimSpace(item.Get("name").String()))
				argsStr := item.Get("arguments").String()

				toolUse := []byte(`{"type":"tool_use","id":"","name":"","input":{}}`)
				toolUse, _ = sjson.SetBytes(toolUse, "id", callID)
				toolUse, _ = sjson.SetBytes(toolUse, "name", name)
				if item.Get("type").String() == "custom_tool_call" {
					toolUse, _ = sjson.SetBytes(toolUse, "input.input", item.Get("input").String())
				} else if argsStr != "" && gjson.Valid(argsStr) {
					argsJSON := gjson.Parse(argsStr)
					if argsJSON.IsObject() {
						toolUse, _ = sjson.SetRawBytes(toolUse, "input", []byte(argsJSON.Raw))
					}
				}

				messages.appendToolUse(toolUse)

			case "function_call_output", "custom_tool_call_output":
				// Map to user tool_result
				callID := item.Get("call_id").String()
				targetItem := item
				if callID != "" {
					if emittedToolResults[callID] {
						return true
					}
					emittedToolResults[callID] = true
					targetItem = lastToolResult[callID]
				}
				outputStr := targetItem.Get("output").String()
				toolResult := []byte(`{"type":"tool_result","tool_use_id":"","content":""}`)
				toolResult, _ = sjson.SetBytes(toolResult, "tool_use_id", callID)
				toolResult, _ = sjson.SetBytes(toolResult, "content", outputStr)
				if cache := targetItem.Get("cache_control"); cache.IsObject() {
					toolResult, _ = sjson.SetRawBytes(toolResult, "cache_control", []byte(cache.Raw))
				}

				messages.appendParts("user", toolResult)
			}
			return true
		})
	} else if input.Type == gjson.String {
		messages.appendParts("user", claudeResponsesTextPart(input.String()))
	}
	out, _ = sjson.SetRawBytes(out, "messages", messages.finish(isCompat))
	if len(systemBlocks) > 0 {
		out, _ = sjson.SetRawBytes(out, "system", claudeResponsesRawArray(systemBlocks))
		if gjson.GetBytes(out, "messages.#").Int() == 0 {
			out, _ = sjson.SetRawBytes(out, "messages", []byte(`[{"role":"user","content":""}]`))
		}
	}

	// tools mapping: parameters -> input_schema
	if tools := claudeResponsesTools(root); len(tools) > 0 {
		toolsJSON := []byte("[]")
		for _, declaration := range tools {
			tool := declaration.Tool
			tJSON := []byte(`{"name":"","description":"","input_schema":{}}`)
			if n := tool.Get("name"); n.Exists() {
				tJSON, _ = sjson.SetBytes(tJSON, "name", n.String())
			}
			if d := tool.Get("description"); d.Exists() {
				tJSON, _ = sjson.SetBytes(tJSON, "description", d.String())
			}

			if tool.Get("type").String() == "custom" {
				tJSON, _ = sjson.SetRawBytes(tJSON, "input_schema", []byte(`{"type":"object","properties":{"input":{"type":"string"}},"required":["input"]}`))
			} else if params := tool.Get("parameters"); params.Exists() {
				tJSON, _ = sjson.SetRawBytes(tJSON, "input_schema", []byte(params.Raw))
			} else if params = tool.Get("parametersJsonSchema"); params.Exists() {
				tJSON, _ = sjson.SetRawBytes(tJSON, "input_schema", []byte(params.Raw))
			}

			toolsJSON, _ = sjson.SetRawBytes(toolsJSON, "-1", tJSON)
		}
		if parsedTools := gjson.ParseBytes(toolsJSON); parsedTools.IsArray() && len(parsedTools.Array()) > 0 {
			out, _ = sjson.SetRawBytes(out, "tools", toolsJSON)
		}
	}

	// Map tool_choice similar to Chat Completions translator (optional in docs, safe to handle)
	if toolChoice := root.Get("tool_choice"); toolChoice.Exists() {
		switch toolChoice.Type {
		case gjson.String:
			switch toolChoice.String() {
			case "auto":
				out, _ = sjson.SetRawBytes(out, "tool_choice", []byte(`{"type":"auto"}`))
			case "none":
				// Leave unset; implies no tools
			case "required":
				out, _ = sjson.SetRawBytes(out, "tool_choice", []byte(`{"type":"any"}`))
			}
		case gjson.JSON:
			if kind := toolChoice.Get("type").String(); kind == "function" || kind == "custom" {
				fn := toolChoice.Get("name").String()
				if fn == "" {
					fn = toolChoice.Get("function.name").String()
				}
				fn = qualifyClaudeResponsesToolName(strings.TrimSpace(toolChoice.Get("namespace").String()), strings.TrimSpace(fn))
				toolChoiceJSON := []byte(`{"name":"","type":"tool"}`)
				toolChoiceJSON, _ = sjson.SetBytes(toolChoiceJSON, "name", fn)
				out, _ = sjson.SetRawBytes(out, "tool_choice", toolChoiceJSON)
			}
		default:

		}
	}

	return out
}

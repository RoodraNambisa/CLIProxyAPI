package responses

import (
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// ConvertOpenAIResponsesRequestToOpenAIChatCompletions converts OpenAI responses format to OpenAI chat completions format.
// It transforms the OpenAI responses API format (with instructions and input array) into the standard
// OpenAI chat completions format (with messages array and system content).
//
// The conversion handles:
// 1. Model name and streaming configuration
// 2. Instructions to system message conversion
// 3. Input array to messages array transformation
// 4. Tool definitions and tool choice conversion
// 5. Function calls and function results handling
// 6. Generation parameters mapping (max_tokens, reasoning, etc.)
//
// Parameters:
//   - modelName: The name of the model to use for the request
//   - rawJSON: The raw JSON request data in OpenAI responses format
//   - stream: A boolean indicating if the request is for a streaming response
//
// Returns:
//   - []byte: The transformed request data in OpenAI chat completions format
func ConvertOpenAIResponsesRequestToOpenAIChatCompletions(modelName string, inputRawJSON []byte, stream bool) []byte {
	rawJSON := inputRawJSON
	// Base OpenAI chat completions template with default values
	out := []byte(`{"model":"","messages":[],"stream":false}`)

	root := gjson.ParseBytes(rawJSON)

	// Set model name
	out, _ = sjson.SetBytes(out, "model", modelName)

	// Set stream configuration
	out, _ = sjson.SetBytes(out, "stream", stream)
	if responseFormat := responsesTextFormatForChat(root.Get("text.format")); len(responseFormat) > 0 {
		out, _ = sjson.SetRawBytes(out, "response_format", responseFormat)
	}

	// Map generation parameters from responses format to chat completions format
	if maxTokens := root.Get("max_output_tokens"); maxTokens.Exists() {
		out, _ = sjson.SetBytes(out, "max_tokens", maxTokens.Int())
	}

	if parallelToolCalls := root.Get("parallel_tool_calls"); parallelToolCalls.Exists() {
		out, _ = sjson.SetBytes(out, "parallel_tool_calls", parallelToolCalls.Bool())
	}

	// Convert instructions to system message
	if instructions := root.Get("instructions"); instructions.Exists() {
		systemMessage := []byte(`{"role":"system","content":""}`)
		systemMessage, _ = sjson.SetBytes(systemMessage, "content", instructions.String())
		out, _ = sjson.SetRawBytes(out, "messages.-1", systemMessage)
	}

	// Convert input array to messages
	if input := root.Get("input"); input.Exists() && input.IsArray() {
		var pendingCalls, deferredMessages [][]byte
		awaitingOutputs := make(map[string]bool)
		messageCount := int(gjson.GetBytes(out, "messages.#").Int())
		mergeableIndex := -1
		var mergeableAssistant []byte
		pendingReasoning := ""
		appendMessage := func(message []byte) int {
			index := messageCount
			out, _ = sjson.SetRawBytes(out, "messages.-1", message)
			messageCount++
			return index
		}
		flushCalls := func() {
			if len(pendingCalls) == 0 {
				return
			}
			merge := mergeableIndex >= 0 && mergeableIndex == messageCount-1 && !gjson.GetBytes(mergeableAssistant, "tool_calls").Exists()
			message := []byte(`{"role":"assistant"}`)
			if merge {
				message = mergeableAssistant
			}
			message, _ = sjson.SetRawBytes(message, "tool_calls", joinResponsesRawArray(pendingCalls))
			if reasoning := combineResponsesInputReasoning(gjson.GetBytes(message, "reasoning_content").String(), pendingReasoning); strings.TrimSpace(reasoning) != "" {
				message, _ = sjson.SetBytes(message, "reasoning_content", reasoning)
			}
			if merge {
				out, _ = sjson.SetRawBytes(out, "messages."+strconv.Itoa(mergeableIndex), message)
			} else {
				appendMessage(message)
			}
			mergeableIndex, mergeableAssistant = -1, nil
			pendingReasoning = ""
			pendingCalls = nil
		}
		flushMessages := func() {
			for _, message := range deferredMessages {
				appendMessage(message)
			}
			deferredMessages = nil
		}
		appendRegularMessage := func(message []byte) int {
			if len(pendingCalls) > 0 || len(awaitingOutputs) > 0 {
				deferredMessages = append(deferredMessages, message)
				return -1
			}
			return appendMessage(message)
		}
		flushReasoning := func() {
			if strings.TrimSpace(pendingReasoning) == "" {
				return
			}
			if len(pendingCalls) > 0 {
				flushCalls()
				return
			}
			message, _ := sjson.SetBytes([]byte(`{"role":"assistant","content":""}`), "reasoning_content", pendingReasoning)
			appendRegularMessage(message)
			pendingReasoning = ""
		}
		input.ForEach(func(_, item gjson.Result) bool {
			itemType := item.Get("type").String()
			if itemType == "" && item.Get("role").String() != "" {
				itemType = "message"
			}

			switch itemType {
			case "message", "":
				// Handle regular message conversion
				role := item.Get("role").String()
				if role == "developer" {
					role = "user"
				}
				if role != "assistant" {
					flushReasoning()
				}
				mergeableIndex, mergeableAssistant = -1, nil
				message := []byte(`{"role":"","content":[]}`)
				message, _ = sjson.SetBytes(message, "role", role)

				if content := item.Get("content"); content.Exists() && content.IsArray() {
					var messageContent string
					var toolCalls []interface{}

					content.ForEach(func(_, contentItem gjson.Result) bool {
						contentType := contentItem.Get("type").String()
						if contentType == "" {
							contentType = "input_text"
						}

						switch contentType {
						case "input_text", "output_text":
							text := contentItem.Get("text").String()
							contentPart := []byte(`{"type":"text","text":""}`)
							contentPart, _ = sjson.SetBytes(contentPart, "text", text)
							message, _ = sjson.SetRawBytes(message, "content.-1", contentPart)
						case "input_image":
							imageURL := contentItem.Get("image_url").String()
							contentPart := []byte(`{"type":"image_url","image_url":{"url":""}}`)
							contentPart, _ = sjson.SetBytes(contentPart, "image_url.url", imageURL)
							message, _ = sjson.SetRawBytes(message, "content.-1", contentPart)
						}
						return true
					})

					if messageContent != "" {
						message, _ = sjson.SetBytes(message, "content", messageContent)
					}

					if len(toolCalls) > 0 {
						message, _ = sjson.SetBytes(message, "tool_calls", toolCalls)
					}
				} else if content.Type == gjson.String {
					message, _ = sjson.SetBytes(message, "content", content.String())
				}

				if role == "assistant" {
					reasoning := combineResponsesInputReasoning(pendingReasoning, responsesInputReasoningString(item.Get("reasoning_content")))
					if strings.TrimSpace(reasoning) != "" {
						message, _ = sjson.SetBytes(message, "reasoning_content", reasoning)
					}
					pendingReasoning = ""
				}
				index := appendRegularMessage(message)
				if role == "assistant" && index >= 0 {
					mergeableIndex, mergeableAssistant = index, message
				}

			case "reasoning":
				pendingReasoning = combineResponsesInputReasoning(pendingReasoning, responsesInputReasoningText(item))

			case "function_call", "custom_tool_call":
				pendingReasoning = combineResponsesInputReasoning(pendingReasoning, responsesInputReasoningString(item.Get("reasoning_content")))
				toolCall := []byte(`{"id":"","type":"function","function":{"name":"","arguments":""}}`)
				callID := item.Get("call_id").String()
				toolCall, _ = sjson.SetBytes(toolCall, "id", callID)
				toolCall, _ = sjson.SetBytes(toolCall, "function.name", responsesHistoryToolName(item))
				arguments := item.Get("arguments").String()
				if itemType == "custom_tool_call" {
					wrapped, _ := sjson.SetBytes([]byte(`{"input":""}`), "input", item.Get("input").String())
					arguments = string(wrapped)
				}
				toolCall, _ = sjson.SetBytes(toolCall, "function.arguments", arguments)
				pendingCalls = append(pendingCalls, toolCall)
				if callID != "" {
					awaitingOutputs[callID] = true
				}

			case "function_call_output", "custom_tool_call_output":
				flushCalls()
				mergeableIndex, mergeableAssistant = -1, nil
				// Handle function call output conversion to tool message
				toolMessage := []byte(`{"role":"tool","tool_call_id":"","content":""}`)

				if callId := item.Get("call_id"); callId.Exists() {
					toolMessage, _ = sjson.SetBytes(toolMessage, "tool_call_id", callId.String())
				}

				if output := item.Get("output"); output.Exists() {
					toolMessage = setResponsesChatToolOutput(toolMessage, output)
				}

				appendMessage(toolMessage)
				delete(awaitingOutputs, item.Get("call_id").String())
				if len(awaitingOutputs) == 0 {
					flushMessages()
				}
			default:
				mergeableIndex, mergeableAssistant = -1, nil
			}

			return true
		})
		flushCalls()
		flushReasoning()
		flushMessages()
	} else if input.Type == gjson.String {
		msg := []byte(`{}`)
		msg, _ = sjson.SetBytes(msg, "role", "user")
		msg, _ = sjson.SetBytes(msg, "content", input.String())
		out, _ = sjson.SetRawBytes(out, "messages.-1", msg)
	}

	if tools := mergeResponsesRequestChatTools(root); len(tools) > 0 {
		out, _ = sjson.SetRawBytes(out, "tools", joinResponsesRawArray(tools))
	}

	if reasoningEffort := root.Get("reasoning.effort"); reasoningEffort.Exists() {
		effort := strings.ToLower(strings.TrimSpace(reasoningEffort.String()))
		if effort != "" {
			out, _ = sjson.SetBytes(out, "reasoning_effort", effort)
		}
	}

	// Convert tool_choice if present
	if toolChoice := root.Get("tool_choice"); toolChoice.Exists() {
		if toolChoice.IsObject() && (toolChoice.Get("type").String() == "function" || toolChoice.Get("type").String() == "custom") {
			choice, _ := sjson.SetBytes([]byte(`{"type":"function","function":{"name":""}}`), "function.name", responsesHistoryToolName(toolChoice))
			out, _ = sjson.SetRawBytes(out, "tool_choice", choice)
		} else {
			out, _ = sjson.SetRawBytes(out, "tool_choice", []byte(toolChoice.Raw))
		}
	}

	return out
}

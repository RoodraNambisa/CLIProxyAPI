package helps

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestGoogleModelCompatibilityThinkingRolesAndText(t *testing.T) {
	for _, target := range []translator.Format{translator.FormatGemini, translator.FormatInteractions} {
		for _, compat := range []bool{false, true} {
			for _, role := range []string{"assistant", "user", "system", "model"} {
				for _, content := range []struct{ raw, text string }{{`""`, ""}, {`"thought"`, "thought"}, {`{"text":"wrapped"}`, "wrapped"}} {
					raw := []byte(fmt.Sprintf(`{"messages":[{"role":%q,"content":[{"type":"text","text":"before"},{"type":"thinking","thinking":%s,"signature":"opaque"},{"type":"text","text":"after"}]}]}`, role, content.raw))
					got := TranslateRequestWithAPIKeyModelCompatibility(translator.FormatClaude, target, "gemini-2.5-pro", raw, false, compat)
					found, text := false, ""
					if target == translator.FormatGemini {
						for _, message := range gjson.GetBytes(got, "contents").Array() {
							for _, part := range message.Get("parts").Array() {
								if part.Get("thought").Bool() {
									found, text = true, part.Get("text").String()
									if part.Get("thoughtSignature").String() != "opaque" {
										t.Fatal("Google compatibility changed the opaque signature")
									}
								}
							}
						}
					} else {
						for _, step := range gjson.GetBytes(got, "input").Array() {
							if step.Get("type").String() == "thought" {
								found, text = true, step.Get("content.0.text").String()
							}
						}
					}
					want := role == "assistant" && (compat || (target == translator.FormatInteractions && content.text != ""))
					if found != want || (found && text != content.text) {
						t.Fatalf("thinking role/text mismatch: target=%s compat=%t role=%s", target, compat, role)
					}
				}
			}
		}
	}
}

func TestOpenAICompatibilityAlreadyPreservesAssistantThinking(t *testing.T) {
	raw := []byte(`{"messages":[{"role":"assistant","content":[{"type":"thinking","thinking":"kept","signature":"opaque"},{"type":"text","text":"answer"},{"type":"tool_use","id":"paired","name":"lookup","input":{"query":"kept"}}]}]}`)
	for _, flag := range []bool{false, true} {
		got := TranslateRequestWithAPIKeyModelCompatibility(translator.FormatClaude, translator.FormatOpenAI, "fixture", raw, false, flag)
		if gjson.GetBytes(got, "messages.0.reasoning_content").String() != "kept" ||
			gjson.GetBytes(got, "messages.0.tool_calls.0.id").String() != "paired" ||
			gjson.GetBytes(got, "messages.0.tool_calls.0.function.name").String() != "lookup" {
			t.Fatal("existing OpenAI thinking/tool support became dependent on a new flag")
		}
	}
}

func TestClaudeCompatibilityPreservesOnlyAssistantReasoningStrings(t *testing.T) {
	for _, role := range []string{"user", "assistant", "system"} {
		for _, value := range []string{`"kept"`, `""`, `"  "`, "false", "2", "null", `{"text":"object"}`} {
			for _, compat := range []bool{false, true} {
				raw := []byte(fmt.Sprintf(`{"messages":[{"role":%q,"content":"answer","reasoning_content":%s}]}`, role, value))
				got := TranslateRequestWithAPIKeyModelCompatibility(translator.FormatOpenAI, translator.FormatClaude, "claude-fixture", raw, false, compat)
				part := gjson.GetBytes(got, "messages.0.content.0")
				want := compat && role == "assistant" && value == `"kept"`
				if (part.Get("type").String() == "thinking") != want {
					t.Fatal("compatibility reinterpreted reasoning role or type")
				}
			}
		}
	}
	raw := []byte(`{"messages":[{"role":"assistant","content":[{"type":"thinking","thinking":"","signature":"opaque-native"}]}]}`)
	for _, compat := range []bool{false, true} {
		got := TranslateRequestWithAPIKeyModelCompatibility(translator.FormatClaude, translator.FormatClaude, "claude-fixture", raw, false, compat)
		if gjson.GetBytes(got, "messages.0.content.0.signature").String() != "opaque-native" {
			t.Fatal("native Claude signature preservation gained an opt-in requirement")
		}
	}
}

func TestModelCompatibilityDoesNotBypassOpaqueHistoryOrCancellation(t *testing.T) {
	cfg := &config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: true}}
	headers := http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}
	raw := []byte(`{"input":[{"type":"agent_message","content":[{"type":"encrypted_content","encrypted_content":"opaque-fixture"}]}]}`)
	for _, target := range []translator.Format{translator.FormatGemini, translator.FormatInteractions, translator.FormatClaude} {
		for _, compat := range []bool{false, true} {
			if _, err := TranslateRequestWithCodexMultiAgentV2(t.Context(), headers, cfg, translator.FormatOpenAIResponse, target, "fixture", raw, false, compat); err == nil {
				t.Fatal("compatibility bypassed the encrypted history guard")
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if _, err := TranslateRequestWithCodexMultiAgentV2(ctx, headers, cfg, translator.FormatOpenAIResponse, target, "fixture", raw, false, compat); !errors.Is(err, context.Canceled) {
				t.Fatal("compatibility lost cancellation precedence")
			}
		}
	}
}

func TestModelCompatibilityCodexSummaryAndUnsignedThinking(t *testing.T) {
	raw := []byte(`{"thinking":{"type":"enabled","display":"omitted","budget_tokens":1024},"messages":[{"role":"assistant","content":[{"type":"thinking","thinking":"","signature":""},{"type":"text","text":"answer"}]}]}`)
	for _, compat := range []bool{false, true} {
		body := TranslateRequestWithAPIKeyModelCompatibility(translator.FormatClaude, translator.FormatCodex, "gpt-5.4", raw, false, compat)
		body = SanitizeCodexReasoningEncryptedContent(t.Context(), "fixture", body, compat)
		items := gjson.GetBytes(body, "input").Array()
		want := 1
		if compat {
			want = 2
		}
		if len(items) != want || gjson.GetBytes(body, "reasoning.summary").Exists() {
			t.Fatal("compatibility lost summary visibility or unsigned reasoning")
		}
		if compat && (items[0].Get("type").String() != "reasoning" || items[0].Get("encrypted_content").Raw != `""`) {
			t.Fatal("compatibility cleanup removed the empty signature field")
		}
	}
	for _, raw := range []string{`""`, `"  "`, `"invalid"`, "1", "null", "{}"} {
		payload := []byte(`{"input":[{"type":"reasoning","encrypted_content":` + raw + `},{"type":"agent_message","content":[{"type":"encrypted_content","encrypted_content":"opaque"}]}]}`)
		for _, compat := range []bool{false, true} {
			before := bytes.Clone(payload)
			got := SanitizeCodexReasoningEncryptedContent(t.Context(), "fixture", payload, compat)
			kept := gjson.GetBytes(got, "input.0.encrypted_content")
			want := compat && (raw == `""` || raw == `"  "`)
			if kept.Exists() != want || (want && kept.Raw != raw) || !bytes.Equal(payload, before) ||
				gjson.GetBytes(got, "input.1").Raw != gjson.GetBytes(payload, "input.1").Raw {
				t.Fatal("empty-signature policy changed another type or opaque agent history")
			}
		}
	}
}

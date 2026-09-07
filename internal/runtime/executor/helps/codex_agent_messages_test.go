package helps

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexAgentMessagesNormalizeOnlyPlaintextEnvelopes(t *testing.T) {
	payload := []byte(`{"prompt_cache_key":"cache-stays","metadata":{"counter":9007199254740993},"input":[{"type":"agent_message","id":"amsg_1","author":"/root","recipient":"/root/worker","content":[{"type":"input_text","text":"task\n\"quoted\" 中文","extension":{"encrypted_content":"keep"}}],"internal_chat_message_metadata_passthrough":{"turn_id":"turn-original"}},{"type":"reasoning","encrypted_content":"opaque-reasoning"},{"type":"function_call_output","call_id":"call-original","output":{"type":"agent_message","content":[{"type":"encrypted_content","encrypted_content":"business"}]}}]}`)
	for _, target := range []sdktranslator.Format{sdktranslator.FormatCodex, sdktranslator.FormatOpenAIResponse, sdktranslator.FormatOpenAI, sdktranslator.FormatClaude, sdktranslator.FormatGemini, sdktranslator.FormatAntigravity, sdktranslator.FormatInteractions} {
		t.Run(string(target), func(t *testing.T) {
			original := bytes.Clone(payload)
			got, err := NormalizeCodexMultiAgentInput(payload, true, target)
			if err != nil || !bytes.Equal(payload, original) {
				t.Fatal("normalization failed or mutated the caller buffer")
			}
			native := target == sdktranslator.FormatCodex || target == sdktranslator.FormatOpenAIResponse
			if native {
				if !bytes.Equal(got, original) {
					t.Fatal("native content was modified")
				}
			} else if gjson.GetBytes(got, "input.0.type").String() != "message" || gjson.GetBytes(got, "input.0.role").String() != "user" {
				t.Fatal("wrong standard envelope")
			}
			for _, path := range []string{"prompt_cache_key", "metadata", "input.0.id", "input.0.author", "input.0.recipient", "input.0.content", "input.0.internal_chat_message_metadata_passthrough", "input.1", "input.2"} {
				if gjson.GetBytes(got, path).Raw != gjson.GetBytes(original, path).Raw {
					t.Fatalf("unrelated field changed: %s", path)
				}
			}
			again, err := NormalizeCodexMultiAgentInput(got, true, target)
			if err != nil || !bytes.Equal(again, got) {
				t.Fatal("normalization is not idempotent")
			}
			disabled, err := NormalizeCodexMultiAgentInput(payload, false, target)
			if err != nil || !bytes.Equal(disabled, payload) {
				t.Fatal("disabled normalization changed a request")
			}
		})
	}
}

func TestCodexAgentMessageCiphertextRemainsOpaque(t *testing.T) {
	for _, value := range []any{"looks like plaintext", "opaque-ciphertext", "", nil, 42} {
		raw, err := json.Marshal(map[string]any{"input": []any{
			map[string]any{"type": "agent_message", "content": []any{map[string]any{"type": "input_text", "text": "earlier"}}},
			map[string]any{"type": "agent_message", "content": []any{map[string]any{"type": "encrypted_content", "encrypted_content": value}}},
		}})
		if err != nil {
			t.Fatal(err)
		}
		for _, target := range []sdktranslator.Format{sdktranslator.FormatCodex, sdktranslator.FormatOpenAIResponse, sdktranslator.FormatClaude} {
			got, err := NormalizeCodexMultiAgentInput(raw, true, target)
			if !bytes.Equal(got, raw) {
				t.Fatal("ciphertext was relabeled or partially modified")
			}
			if target == sdktranslator.FormatClaude {
				var unsupported *codexEncryptedAgentMessageError
				if !errors.As(err, &unsupported) || unsupported.StatusCode() != 400 || !unsupported.SkipAuthResult() || unsupported.RetryOtherAuth() {
					t.Fatal("cross-protocol ciphertext did not return a request-scoped unsupported error")
				}
				if !gjson.Valid(err.Error()) || strings.Contains(err.Error(), "opaque-ciphertext") {
					t.Fatal("error exposes content or is not a public JSON error")
				}
				wrapped := fmt.Errorf("normalize request: %w", err)
				if !errors.As(wrapped, &unsupported) {
					t.Fatal("error chain lost its classification")
				}
			} else if err != nil {
				t.Fatal("native encrypted content was rejected")
			}
			if disabled, err := NormalizeCodexMultiAgentInput(raw, false, target); err != nil || !bytes.Equal(disabled, raw) {
				t.Fatal("disabled mode rejected encrypted content")
			}
		}
	}
}

func TestCodexAgentMessageNormalizationNoopAndUnknownContent(t *testing.T) {
	for _, raw := range []string{"", "not JSON", "{", "null", "{}", `{"input":"text"}`, `{"input":[]}`,
		`{"input":[{"type":"message","role":"user","content":[{"type":"encrypted_content","encrypted_content":"leave"}]}]}`} {
		original := []byte(raw)
		got, err := NormalizeCodexMultiAgentInput(original, true, sdktranslator.FormatClaude)
		if err != nil || !bytes.Equal(got, original) || len(original) > 0 && &got[0] != &original[0] {
			t.Fatal("no-op normalization copied, rejected or changed an unrelated input")
		}
	}
	raw := []byte(`{"input":[{"type":"agent_message","content":[{"type":"unknown","text":"keep"}]},{"type":"agent_message","content":"legacy text"}]}`)
	got, err := NormalizeCodexMultiAgentInput(raw, true, sdktranslator.FormatClaude)
	if err != nil || gjson.GetBytes(got, "input.0.content").Raw != gjson.GetBytes(raw, "input.0.content").Raw || gjson.GetBytes(got, "input.1.content").Raw != gjson.GetBytes(raw, "input.1.content").Raw {
		t.Fatal("normalization interpreted unknown content")
	}
}

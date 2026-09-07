package helps

import (
	"fmt"
	"net/http"
	"strings"

	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type codexEncryptedAgentMessageError struct{}

func (*codexEncryptedAgentMessageError) Error() string {
	return `{"error":{"type":"invalid_request_error","code":"codex_encrypted_agent_message_unsupported","message":"Encrypted agent messages require a Codex-compatible Responses target."}}`
}
func (*codexEncryptedAgentMessageError) StatusCode() int      { return http.StatusBadRequest }
func (*codexEncryptedAgentMessageError) SkipAuthResult() bool { return true }
func (*codexEncryptedAgentMessageError) RetryOtherAuth() bool { return false }

// NormalizeCodexMultiAgentInput converts plaintext collaboration envelopes for
// other protocols. Encrypted content remains opaque and is never relabeled.
func NormalizeCodexMultiAgentInput(payload []byte, enabled bool, target sdktranslator.Format) ([]byte, error) {
	if !enabled || target == sdktranslator.FormatCodex || target == sdktranslator.FormatOpenAIResponse || !gjson.ValidBytes(payload) {
		return payload, nil
	}
	input := gjson.GetBytes(payload, "input")
	if !input.IsArray() {
		return payload, nil
	}
	items := input.Array()
	for _, item := range items {
		if strings.TrimSpace(item.Get("type").String()) != "agent_message" {
			continue
		}
		for _, part := range item.Get("content").Array() {
			if strings.TrimSpace(part.Get("type").String()) == "encrypted_content" {
				return payload, &codexEncryptedAgentMessageError{}
			}
		}
	}
	updated := payload
	for itemIndex, item := range items {
		if strings.TrimSpace(item.Get("type").String()) != "agent_message" {
			continue
		}
		itemPath := fmt.Sprintf("input.%d", itemIndex)
		var errSet error
		updated, errSet = sjson.SetBytes(updated, itemPath+".role", "user")
		if errSet != nil {
			return payload, errSet
		}
		updated, errSet = sjson.SetBytes(updated, itemPath+".type", "message")
		if errSet != nil {
			return payload, errSet
		}
	}
	return updated, nil
}

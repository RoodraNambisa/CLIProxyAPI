package openai

import (
	"errors"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
)

func TestResponsesWebsocketErrorPayloadPreservesStructuredCause(t *testing.T) {
	for _, payload := range []string{
		`{"type":"error","status":403,"error":{"type":"misalignment_policy_violation","message":"request rejected","param":"input","details":{"reason":"fixture"}}}`,
		`{"type":"response.failed","response":{"status_code":403,"error":{"type":"misalignment_policy_violation","message":"request rejected","param":"input"}}}`,
	} {
		buffer := []byte(payload)
		msg := responsesWebsocketErrorMessageFromPayload(buffer)
		if msg == nil || msg.StatusCode != 403 || !coreauth.IsRequestFaultError(msg.Error) {
			t.Fatal("structured request error lost during terminal parsing")
		}
		var status interface{ StatusCode() int }
		if !errors.As(msg.Error, &status) || status.StatusCode() != 403 {
			t.Fatal("error chain lost transport status")
		}
		for i := range buffer {
			buffer[i] = 'x'
		}
		if msg.Error.Error() != payload {
			t.Fatal("error retained a borrowed mutable buffer or lost fields")
		}
		base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{ErrorResponseRewrites: []sdkconfig.ErrorResponseRewriteRule{
			{StatusCode: 403, MessageContains: "response.failed", ResponseStatusCode: 418},
			{StatusCode: 403, MessageContains: "misalignment_policy_violation", ResponseStatusCode: 418},
			{StatusCode: 403, MessageContains: "request rejected", ResponseStatusCode: 422},
		}}, nil)
		projected := base.RewriteExecutionErrorResponse(msg)
		if projected.StatusCode != 422 || handlers.OriginalErrorStatusCode(projected) != 403 {
			t.Fatal("existing diagnostic-only response matching changed")
		}
		rewritten, err := rewriteResponsesWebsocketTerminalErrorPayload([]byte(payload), projected)
		if err != nil {
			t.Fatal(err)
		}
		path := "error"
		if gjson.Get(payload, "type").String() == "response.failed" {
			path = "response.error"
		}
		if gjson.GetBytes(rewritten, path+".type").String() != "misalignment_policy_violation" || gjson.GetBytes(rewritten, path+".param").String() != "input" {
			t.Fatal("status-only projection changed original error fields")
		}
		if msg.StatusCode != 403 || msg.Error.Error() != payload {
			t.Fatal("output projection mutated the classification error")
		}
	}
	if msg := responsesWebsocketErrorMessageFromPayload([]byte(`{"type":"response.completed"}`)); msg != nil {
		t.Fatal("successful terminal became an error")
	}
}

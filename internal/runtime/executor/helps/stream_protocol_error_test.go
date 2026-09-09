package helps

import (
	"bytes"
	"fmt"
	"net/http"
	"testing"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

func TestStreamProtocolErrorKeepsStructuredRefusalAndStatus(t *testing.T) {
	for _, code := range []string{"misalignment_policy_violation", "cyber_policy", "content_policy_violation"} {
		for _, field := range []string{"code", "type"} {
			for _, shape := range []string{"error", "response", "event"} {
				node := fmt.Sprintf(`{"message":"request denied",%q:%q,"detail":"preserved"}`, field, code)
				body := `{"error":` + node + `}`
				if shape == "response" {
					body = `{"type":"response.failed","response":{"error":` + node + `}}`
				}
				if shape == "event" {
					body = fmt.Sprintf(`{"type":"error","message":"request denied","code":%q}`, code)
				}
				raw := []byte(body)
				err := JSONStreamProtocolError("fixture", raw)
				clear(raw)
				if !coreauth.IsPolicyRefusalError(err) {
					t.Fatal("structured refusal code or type was lost")
				}
				if gjson.Get(err.Error(), "error.message").String() != "request denied" {
					t.Fatal("public error message was lost")
				}
				source, ok := err.(interface{ ResponseBody() []byte })
				if !ok || !bytes.Equal(source.ResponseBody(), []byte(body)) {
					t.Fatal("original error body was not available to rules")
				}
				copy := source.ResponseBody()
				clear(copy)
				if !bytes.Equal(source.ResponseBody(), []byte(body)) {
					t.Fatal("caller changed the error snapshot")
				}
			}
		}
	}
	for _, body := range []string{`{"error":{"code":"misalignment_policy_violation","status_code":429,"message":"denied"}}`, `{"response":{"error":{"code":"misalignment_policy_violation","http_status":402,"message":"denied"}}}`, `{"error":{"code":429,"message":"limited","status":"RESOURCE_EXHAUSTED"}}`} {
		err := JSONStreamProtocolError("fixture", []byte(body))
		status := err.(interface{ StatusCode() int }).StatusCode()
		if (status != 402 && status != 429) || coreauth.IsPolicyRefusalError(err) {
			t.Fatal("explicit payment or quota status lost priority")
		}
	}
	for _, body := range []string{`{"error":{"message":"misalignment_policy_violation"}}`, `{"error":{"code":"misalignment_policy_violation","type":"authentication_error","message":"denied"}}}`} {
		if coreauth.IsPolicyRefusalError(JSONStreamProtocolError("fixture", []byte(body))) {
			t.Fatal("message text or authentication error became a policy refusal")
		}
	}
	for _, status := range []string{`"429"`, `429.5`, `700`, `true`, `null`, `{}`} {
		err := JSONStreamProtocolError("fixture", []byte(`{"error":{"message":"failed","status_code":`+status+`}}`))
		if err.(interface{ StatusCode() int }).StatusCode() != http.StatusBadGateway {
			t.Fatal("invalid status changed the fallback")
		}
	}
}

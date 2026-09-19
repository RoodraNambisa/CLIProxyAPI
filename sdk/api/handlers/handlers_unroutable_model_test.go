package handlers

import (
	"net/http"
	"testing"

	"github.com/tidwall/gjson"
)

func TestUnroutableModelKeepsGatewayStatusForCallerFailover(t *testing.T) {
	for _, model := range []string{"unroutable-fixture", "unroutable-fixture(high)", "unroutable-\"quote\"\nline", `unroutable-","code":"injected`} {
		providers, resolved, failure := (&BaseAPIHandler{}).getRequestDetails(nil, model)
		if len(providers) != 0 || resolved != "" || failure == nil || failure.StatusCode != http.StatusBadGateway {
			t.Fatalf("model routing returned %v, want 502", failure)
		}
		body := string(BuildErrorResponseBody(failure.StatusCode, failure.Error.Error()))
		if !gjson.Valid(body) || gjson.Get(body, "error.code").String() != "internal_server_error" || gjson.Get(body, "error.type").String() != "server_error" {
			t.Fatal("original gateway error contract changed or corrupted")
		}
		if gjson.Get(body, "error.message").String() != "unknown provider for model "+model {
			t.Fatal("model escaping changed message")
		}
	}
}

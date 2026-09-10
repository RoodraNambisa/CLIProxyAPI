package handlers

import (
	"net/http"
	"testing"

	"github.com/tidwall/gjson"
)

func TestUnroutableModelIsStructuredClientError(t *testing.T) {
	for _, model := range []string{"unroutable-fixture", "unroutable-fixture(high)", "unroutable-\"quote\"\nline", `unroutable-","code":"injected`} {
		providers, resolved, failure := (&BaseAPIHandler{}).getRequestDetails(model)
		if len(providers) != 0 || resolved != "" || failure == nil || failure.StatusCode != http.StatusBadRequest {
			t.Fatalf("model routing returned %v, want 400", failure)
		}
		body := failure.Error.Error()
		if !gjson.Valid(body) || gjson.Get(body, "error.code").String() != "model_not_found" || gjson.Get(body, "error.type").String() != "invalid_request_error" || gjson.Get(body, "error.param").String() != "model" {
			t.Fatal("structured request error missing or corrupted")
		}
		if gjson.Get(body, "error.message").String() != "unknown provider for model "+model {
			t.Fatal("model escaping changed message")
		}
	}
}

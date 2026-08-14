package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestGetRoutingDiagnosticsRequiresProviderAndModel(t *testing.T) {
	handler := NewHandlerWithoutConfigFilePath(nil, coreauth.NewManager(nil, nil, nil))
	t.Cleanup(func() { _ = handler.Shutdown(t.Context()) })
	for _, target := range []string{
		"/v0/management/routing/diagnostics",
		"/v0/management/routing/diagnostics?provider=chatgpt-web",
		"/v0/management/routing/diagnostics?model=gpt-5",
		"/v0/management/routing/diagnostics?provider=%20&model=gpt-5",
	} {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequest(http.MethodGet, target, nil)
		handler.GetRoutingDiagnostics(ctx)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("GET %s status = %d, want 400: %s", target, recorder.Code, recorder.Body.String())
		}
	}
}

func TestGetRoutingDiagnosticsReturnsRoutingAndExecutionMetrics(t *testing.T) {
	handler := NewHandlerWithoutConfigFilePath(nil, coreauth.NewManager(nil, nil, nil))
	t.Cleanup(func() { _ = handler.Shutdown(t.Context()) })
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/routing/diagnostics?provider=ChatGPT-Web&model=gpt-5", nil)
	handler.GetRoutingDiagnostics(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	var response routingDiagnosticsResponse
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &response); errDecode != nil {
		t.Fatalf("json.Unmarshal() error = %v", errDecode)
	}
	if response.Routing.Provider != "chatgpt-web" || response.Routing.Model != "gpt-5" {
		t.Fatalf("routing = %#v", response.Routing)
	}
	if response.Routing.Priorities == nil {
		t.Fatal("routing priorities = nil, want an empty JSON array")
	}
	var raw map[string]any
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &raw); errDecode != nil {
		t.Fatalf("json.Unmarshal(raw) error = %v", errDecode)
	}
	metrics, ok := raw["request_execution_metrics"].(map[string]any)
	if !ok {
		t.Fatalf("request_execution_metrics = %#v", raw["request_execution_metrics"])
	}
	for _, key := range []string{
		"preflight_rejected",
		"auth_slot_reserved",
		"auth_slot_released",
		"upstream_committed",
		"auth_request_limited",
		"selected_but_not_committed",
	} {
		if _, exists := metrics[key]; !exists {
			t.Errorf("request_execution_metrics missing %q: %s", key, recorder.Body.String())
		}
	}
}

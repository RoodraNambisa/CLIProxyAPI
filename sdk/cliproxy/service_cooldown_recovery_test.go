package cliproxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/api/handlers/management"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

type cooldownRecoveryTransport func(*http.Request) (*http.Response, error)

func (f cooldownRecoveryTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestServiceGrokCooldownClearRestoresRoutingWithoutToggle(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	s := &Service{cfg: &config.Config{}, coreManager: manager}
	s.installAuthMaintenanceHook(t.Context())
	id, model := t.Name(), "grok-cooldown-recovery"
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(id)
		_ = manager.CloseExecutors()
	})
	_, err := manager.Register(t.Context(), &coreauth.Auth{
		ID: id, Provider: "xai", Status: coreauth.StatusActive,
		Attributes: map[string]string{"runtime_only": "true"},
		Metadata: map[string]any{
			"access_token": "fixture-token",
			helps.XAIModelCatalogKey: &helps.XAIModelCatalog{
				Source: "https://cli-chat-proxy.grok.com/v1/models", UpdatedAt: time.Now().UTC(),
				Models: []*registry.ModelInfo{{ID: model, Object: "model", Type: "xai", OwnedBy: "xai"}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	reg := registry.GetGlobalRegistry()
	if reg.GetModelCount(model) != 1 {
		t.Fatal("Grok model not registered")
	}
	manager.MarkResult(t.Context(), coreauth.Result{AuthID: id, Provider: "xai", Model: model, Error: &coreauth.Error{HTTPStatus: 403, Message: "temporary upstream error"}})
	if reg.GetModelCount(model) != 0 {
		t.Fatal("expected blocked model")
	}
	h := management.NewHandlerWithoutConfigFilePath(s.cfg, manager)
	h.SetAuthStatusHook(s.handleManagementAuthStatusChange)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/auth-files/cooldowns/clear-selected", strings.NewReader(`{"names":["`+id+`"]}`))
	h.ClearSelectedAuthCooldowns(c)
	if w.Code != 200 || reg.GetModelCount(model) != 1 || len(reg.GetModelProviders(model)) == 0 {
		t.Fatal("service hooks left model suspended", w.Code, w.Body.String())
	}
	calls := 0
	ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", cooldownRecoveryTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Request: r,
			Body: io.NopCloser(strings.NewReader("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"fixture\",\"status\":\"completed\",\"output\":[]}}\n\n"))}, nil
	}))
	_, err = manager.Execute(ctx, []string{"xai"}, core.Request{Model: model, Payload: []byte(`{"model":"` + model + `","input":"hi"}`)}, core.Options{SourceFormat: translator.FormatOpenAIResponse})
	if err != nil || calls != 1 {
		t.Fatal("cleared credential was not reusable without toggling", calls, err)
	}
}

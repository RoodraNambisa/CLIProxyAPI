package management

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

type xaiResourceRefreshExecutor struct {
	*runtimeexecutor.XAIExecutor
	refreshes atomic.Int32
}

func (e *xaiResourceRefreshExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	e.refreshes.Add(1)
	updated := auth.Clone()
	updated.Metadata["access_token"] = "new-fixture"
	updated.Metadata["refresh_token"] = "rotated-fixture"
	return updated, nil
}

func TestXAIManagementQueriesRecoverRejectedOAuthToken(t *testing.T) {
	for _, operation := range []string{"quota", "models"} {
		for _, scenario := range []struct {
			name       string
			status     int
			errorBody  string
			apiKey     bool
			method     string
			alwaysFail bool
			refresh    bool
			customAuth bool
		}{
			{name: "unauthorized", status: 401, method: "GET", refresh: true},
			{name: "invalid-token", status: 403, errorBody: `{"error":{"code":"bad-credentials"}}`, method: "GET", refresh: true},
			{name: "permission", status: 403, errorBody: `{"error":"permission denied"}`, method: "GET"},
			{name: "api-key", status: 401, apiKey: true, method: "GET"},
			{name: "custom-authorization", status: 401, method: "GET", customAuth: true},
			{name: "single-retry", status: 401, method: "GET", alwaysFail: true, refresh: true},
			{name: "no-write-replay", status: 401, method: "POST"},
		} {
			if operation == "models" && scenario.method != "GET" {
				continue
			}
			t.Run(operation+"/"+scenario.name, func(t *testing.T) {
				var calls atomic.Int32
				upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					if r.Header.Get("X-Keep") != "credential" || r.Header.Get("X-Global") != "inherited" {
						t.Error("query lost configured headers")
					}
					if r.Header.Get("Authorization") != "Bearer new-fixture" || scenario.alwaysFail {
						w.WriteHeader(scenario.status)
						_, _ = w.Write([]byte(scenario.errorBody))
						return
					}
					_, _ = w.Write([]byte(`{"config":{"creditUsagePercent":20},"data":[{"id":"grok-query-fixture"}]}`))
				}))
				defer upstream.Close()
				transport := upstream.Client().Transport.(*http.Transport).Clone()
				transport.TLSClientConfig.ServerName = upstream.Listener.Addr().(*net.TCPAddr).IP.String()
				transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, network, upstream.Listener.Addr().String())
				}
				previous := http.DefaultTransport
				http.DefaultTransport = transport
				defer func() { http.DefaultTransport = previous; transport.CloseIdleConnections() }()
				cfg := &config.Config{AuthDir: t.TempDir(), XAI: config.XAIConfig{Headers: map[string]string{"X-Global": "inherited"}}}
				if scenario.customAuth {
					cfg.XAI.Headers["Authorization"] = "Bearer overridden-fixture"
				}
				exec := &xaiResourceRefreshExecutor{XAIExecutor: runtimeexecutor.NewXAIExecutor(cfg)}
				manager := coreauth.NewManager(nil, nil, nil)
				manager.RegisterExecutor(exec)
				auth := &coreauth.Auth{ID: "query.json", FileName: "query.json", Provider: "xai", Attributes: map[string]string{"header:X-Keep": "credential"}, Metadata: map[string]any{"access_token": "old-fixture", "refresh_token": "refresh-fixture", "xai_identity_seed": "keep-seed"}}
				if scenario.apiKey {
					auth.Attributes["api_key"] = "key-fixture"
				}
				registered, err := manager.Register(t.Context(), auth)
				if err != nil {
					t.Fatal(err)
				}
				h := NewHandlerWithoutConfigFilePath(cfg, manager)
				recorder := httptest.NewRecorder()
				ctx, _ := gin.CreateTestContext(recorder)
				if operation == "quota" {
					body := fmt.Sprintf(`{"authIndex":%q,"provider_headers":true,"method":%q,"url":"https://cli-chat-proxy.grok.com/v1/billing?format=credits","header":{"Authorization":"Bearer $TOKEN$"}}`, registered.EnsureIndex(), scenario.method)
					ctx.Request = httptest.NewRequest(http.MethodPost, "/api-call", strings.NewReader(body))
					h.APICall(ctx)
				} else {
					ctx.Request = httptest.NewRequest(http.MethodPost, "/auth-files/xai/models/refresh", strings.NewReader(`{"name":"query.json"}`))
					h.RefreshXAIModels(ctx)
				}
				wantCalls, wantRefresh := int32(1), int32(0)
				if scenario.refresh {
					wantCalls, wantRefresh = 2, 1
				}
				if calls.Load() != wantCalls || exec.refreshes.Load() != wantRefresh {
					t.Fatalf("requests=%d refreshes=%d, want %d/%d; error=%s", calls.Load(), exec.refreshes.Load(), wantCalls, wantRefresh, gjson.GetBytes(recorder.Body.Bytes(), "error").String())
				}
				if recorder.Code != 200 {
					t.Fatalf("management response=%d %s", recorder.Code, recorder.Body.String())
				}
				if operation == "quota" {
					wantStatus := scenario.status
					if scenario.refresh && !scenario.alwaysFail {
						wantStatus = 200
					}
					if gjson.GetBytes(recorder.Body.Bytes(), "status_code").Int() != int64(wantStatus) {
						t.Fatal("upstream result was not preserved")
					}
				} else if scenario.refresh && !scenario.alwaysFail && gjson.GetBytes(recorder.Body.Bytes(), "models.0.id").String() != "grok-query-fixture" {
					t.Fatal("refreshed catalog was not installed")
				}
				current, _ := manager.GetByID(auth.ID)
				if current.Metadata["xai_identity_seed"] != "keep-seed" || current.Attributes["header:X-Keep"] != "credential" || scenario.refresh && current.Metadata["refresh_token"] != "rotated-fixture" {
					t.Fatal("refresh discarded credential settings or rotation")
				}
			})
		}
	}
}

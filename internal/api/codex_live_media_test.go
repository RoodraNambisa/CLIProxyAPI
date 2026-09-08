package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestServerCodexLiveMediaHotUpdateReachesEveryCreationAlias(t *testing.T) {
	var attempts atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", fmt.Sprintf("/v1/realtime/calls/call_media_route_%d", attempts.Add(1)))
		w.WriteHeader(201)
		_, _ = w.Write([]byte("legacy-answer"))
	}))
	defer upstream.Close()
	s := newTestServerWithConfig(t, func(cfg *config.Config) { cfg.Debug = false; cfg.ProxyURL = "direct"; cfg.Codex.LiveEnabled = true })
	defer s.codexLive.Close()
	manager := s.handlers.AuthManager
	manager.RegisterExecutor(&apiLiveHTTPExecutor{CodexAutoExecutor: executor.NewCodexAutoExecutor(s.currentConfig()), upstream: upstream.URL})
	a := &auth.Auth{ID: "media-routes-" + t.Name(), Provider: "codex", Metadata: map[string]any{"access_token": "fixture-oauth"}}
	if _, err := manager.Register(t.Context(), a); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(a.ID, "codex", registry.GetCodexRealtimeModels())
	defer registry.GetGlobalRegistry().UnregisterClient(a.ID)
	temporary := issueLiveAPISecret(t, s, false)
	paths := []string{"/v1/live", "/v1/realtime", "/v1/realtime/calls"}
	for _, step := range []struct {
		live, media bool
		status      int
		code        string
		calls       int32
	}{
		{true, false, 201, "", 6},
		{true, true, 400, "invalid_realtime_request", 6},
		{false, true, 503, "codex_live_disabled", 6},
		{false, false, 503, "codex_live_disabled", 6},
		{true, false, 201, "", 12},
	} {
		updated, err := config.Clone(s.currentConfig())
		if err != nil {
			t.Fatal(err)
		}
		updated.Codex.LiveEnabled = step.live
		updated.Codex.LiveMediaRelay.Enabled = step.media
		if err := s.UpdateClients(updated); err != nil {
			t.Fatal(err)
		}
		for _, path := range paths {
			for _, token := range []string{"test-key", temporary} {
				w := liveSecretAPIRequest(s, http.MethodPost, path, token, `{"sdp":"v=0"}`)
				if w.Code != step.status || (step.code != "" && !strings.Contains(w.Body.String(), step.code)) {
					t.Fatalf("media route %s live=%v media=%v status=%d", path, step.live, step.media, w.Code)
				}
			}
			if w := liveSecretAPIRequest(s, http.MethodPost, path, "", `{"sdp":"v=0"}`); w.Code != 401 {
				t.Fatal("media mode bypassed route authentication")
			}
		}
		if attempts.Load() != step.calls {
			t.Fatal("media validation or disabled mode contacted upstream")
		}
		// WebSocket validation remains separate from WebRTC media conversion.
		for _, path := range []string{"/v1/realtime", "/v1/live/unknown", "/v1/realtime/calls/unknown"} {
			want := 426
			if !step.live {
				want = 503
			}
			if w := liveSecretAPIRequest(s, http.MethodGet, path, temporary, ""); w.Code != want {
				t.Fatal("media configuration changed WebSocket admission")
			}
		}
	}
}

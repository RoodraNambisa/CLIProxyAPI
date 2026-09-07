package helps

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestCodexMultiAgentPolicyRequiresActualClientIdentity(t *testing.T) {
	for _, tc := range []struct {
		ua, version string
		official    bool
	}{
		{"Codex Desktop/0.153.4 (Mac OS; arm64)", "0.153.4", true},
		{"codex-tui/0.145.0 (Linux)", "0.145.0", true},
		{"codex_cli_rs/0.153.4-alpha.1", "0.153.4-alpha.1", true},
		{"codex_cli_rs", "", true},
		{"curl/8.0", "", false}, {"proxy Codex Desktop/0.153.4", "", false}, {"", "", false},
	} {
		for _, enabled := range []bool{false, true} {
			got := NewCodexMultiAgentPolicy(http.Header{"user-agent": {tc.ua}}, enabled)
			if got.Enabled != (enabled && tc.official) || got.ToolsPrepared || got.ClientVersion != tc.version {
				t.Fatal("client gate or real version changed")
			}
		}
	}
	ambiguous := http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}, "user-agent": {"curl/8.0"}}
	if NewCodexMultiAgentPolicy(ambiguous, true).Enabled {
		t.Fatal("ambiguous caller identity enabled optimization")
	}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
	ctx := context.WithValue(t.Context(), "gin", c)
	if SnapshotCodexMultiAgentPolicy(ctx, http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}, true).Enabled {
		t.Fatal("outbound fallback identity overrode the real incoming request")
	}
}

func TestCodexMultiAgentPolicyUsesManagerSettingAndPreparedBoundary(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	manager.SetConfig(&config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: true}})
	ctx := manager.WithRoutingPolicySnapshot(t.Context())
	manager.SetConfig(&config.Config{})
	headers := http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}
	if !SnapshotCodexMultiAgentPolicy(ctx, headers, false).Enabled {
		t.Fatal("current executor config replaced logical request setting")
	}
	if SnapshotCodexMultiAgentPolicy(manager.WithRoutingPolicySnapshot(t.Context()), headers, true).Enabled {
		t.Fatal("new disabled request adopted executor fallback")
	}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Set(CodexMultiAgentPolicyGinKey, CodexMultiAgentPolicy{})
	if SnapshotCodexMultiAgentPolicy(context.WithValue(ctx, "gin", c), headers, true).Enabled {
		t.Fatal("request boundary's disabled decision was ignored")
	}
}

func TestCodexMultiAgentPolicyContextSurvivesFollowingGinTurns(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	initial := CodexMultiAgentPolicy{Enabled: true, ToolsPrepared: true, ClientVersion: "0.153.4"}
	c.Set(CodexMultiAgentPolicyGinKey, initial)
	base := context.WithValue(t.Context(), "gin", c)
	captured := CaptureCodexMultiAgentPolicyContext(base)
	var readers sync.WaitGroup
	for range 8 {
		readers.Go(func() {
			for range 100 {
				if got := SnapshotCodexMultiAgentPolicy(captured, nil, false); got != initial {
					t.Error("captured turn changed")
					return
				}
			}
		})
	}
	for range 100 {
		c.Set(CodexMultiAgentPolicyGinKey, CodexMultiAgentPolicy{})
	}
	readers.Wait()
	next := CaptureCodexMultiAgentPolicyContext(base)
	if SnapshotCodexMultiAgentPolicy(next, nil, true).Enabled {
		t.Fatal("new turn reused old policy")
	}
	if CaptureCodexMultiAgentPolicyContext(nil) != nil || CaptureCodexMultiAgentPolicyContext(t.Context()) != t.Context() {
		t.Fatal("no-op context capture changed its parent")
	}
}

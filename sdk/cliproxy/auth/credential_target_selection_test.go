package auth

import (
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestCredentialTargetSelectionOverridesAffinityWithoutRebinding(t *testing.T) {
	for _, path := range []string{"single", "mixed", "legacy-single", "legacy-mixed"} {
		t.Run(path, func(t *testing.T) {
			failover := false
			selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &FillFirstSelector{}, Failover: &failover})
			t.Cleanup(selector.Stop)
			m := NewManager(nil, selector, nil)
			m.SetConfig(&config.Config{Routing: config.RoutingConfig{SessionAffinity: true, SessionAffinityFailover: &failover}})
			m.RegisterExecutor(schedulerTestExecutor{})
			for _, id := range []string{"bound", "target"} {
				if _, err := m.Register(WithSkipPersist(t.Context()), &Auth{ID: id, Provider: "test"}); err != nil {
					t.Fatal(err)
				}
				registry.GetGlobalRegistry().RegisterClient(id, "test", []*registry.ModelInfo{{ID: "registered"}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
			}
			ctx := clientPriorityContext(t, "test-key")
			opts := core.Options{Headers: http.Header{"Session-Id": {"target-selection"}}, Metadata: map[string]any{core.PinnedAuthMetadataKey: "target"}}
			selector.BindSession(ctx, "test", "unregistered", opts, "bound")
			ctx.Value("gin").(*gin.Context).Set(sdkaccess.CredentialTargetAuthIDContextKey, "target")
			ctx = m.WithRoutingPolicySnapshot(ctx)
			var selected *Auth
			var err error
			switch path {
			case "single":
				selected, _, err = m.pickNext(ctx, "test", "unregistered", opts, nil)
			case "mixed":
				selected, _, _, err = m.pickNextMixed(ctx, []string{"test"}, "unregistered", opts, nil)
			case "legacy-single":
				selected, _, err = m.pickNextLegacy(ctx, "test", "unregistered", opts, nil)
			case "legacy-mixed":
				selected, _, _, err = m.pickNextMixedLegacy(ctx, []string{"test"}, "unregistered", opts, nil, nil)
			}
			if err != nil || selected == nil || selected.ID != "target" {
				t.Fatalf("explicit target lost to affinity: %v, %v", selected, err)
			}
			m.bindSessionAffinity(ctx, []string{"test"}, "unregistered", opts, selected)
			if selector.cachedAuthID("test", "unregistered", opts, ctx) != "bound" {
				t.Fatal("diagnostic request changed normal session affinity")
			}
			ctx.Value("gin").(*gin.Context).Set(sdkaccess.CredentialTargetAuthIDContextKey, "")
			if selected, _, err := m.pickNext(ctx, "test", "unregistered", opts, nil); err == nil || selected != nil {
				t.Fatal("ordinary SDK pin bypassed catalog")
			}
		})
	}
}

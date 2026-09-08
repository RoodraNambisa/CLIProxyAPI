package auth

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexAlphaSelectionExcludesUnsupportedBeforePriorityAndReservation(t *testing.T) {
	for _, strategy := range []string{"round-robin", "fill-first", "weighted-round-robin", "custom"} {
		for _, mixed := range []bool{false, true} {
			t.Run(strategy+map[bool]string{false: "/single", true: "/mixed"}[mixed], func(t *testing.T) {
				var selector Selector = &RoundRobinSelector{}
				switch strategy {
				case "fill-first":
					selector = &FillFirstSelector{}
				case "weighted-round-robin":
					selector = &WeightedRoundRobinSelector{}
				case "custom":
					selector = &trackingSelector{}
				}
				m := NewManager(nil, selector, nil)
				m.SetConfig(&config.Config{Routing: config.RoutingConfig{PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 1}})
				m.RegisterExecutor(&authFallbackExecutor{id: "codex"})
				m.RegisterExecutor(&authFallbackExecutor{id: "claude"})
				pool := []*Auth{
					{ID: "alpha-unsupported-" + t.Name(), Provider: "codex", Attributes: map[string]string{"api_key": "fixture", "priority": "100"}},
					{ID: "alpha-supported-" + t.Name(), Provider: "codex", Metadata: map[string]any{"access_token": "fixture"}},
					{ID: "alpha-foreign-" + t.Name(), Provider: "claude", Metadata: map[string]any{"access_token": "fixture"}, Attributes: map[string]string{"priority": "200"}},
				}
				for _, a := range pool {
					registerFallbackAuthForModel(t, m, a, "alpha-selection")
				}
				providers := []string{"codex"}
				if mixed {
					providers = append(providers, "claude")
				}
				opts := core.Options{SourceFormat: translator.FormatCodexAlphaSearch, AuthRequestSlot: &core.AuthRequestSlot{}}
				picked, _, _, errPick := m.pickNextMixed(t.Context(), providers, "alpha-selection", opts, nil)
				if errPick != nil || picked == nil || picked.ID != pool[1].ID {
					t.Fatal("search did not select the lower-priority supported credential")
				}
				opts.AuthRequestSlot.Release()
				for _, excluded := range []*Auth{pool[0], pool[2]} {
					if available, _ := m.authRequestLimiter().availableAt(excluded.ID, m.routingAuthRequestLimitPolicyForAuth(excluded), time.Now()); !available {
						t.Fatal("excluded credential consumed capacity")
					}
				}
				ordinary := core.Options{SourceFormat: translator.FormatCodex, AuthRequestSlot: &core.AuthRequestSlot{}}
				picked, _, errPick = m.pickNext(t.Context(), "codex", "alpha-selection", ordinary, nil)
				if errPick != nil || picked == nil || picked.ID != pool[0].ID {
					t.Fatal("search opt-in changed ordinary selection")
				}
				ordinary.AuthRequestSlot.Release()
			})
		}
	}
}

func TestCodexAlphaSelectionCapabilityReloadAndUnavailableScope(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		var selector Selector = &RoundRobinSelector{}
		if legacy {
			selector = &trackingSelector{}
		}
		m := NewManager(nil, selector, nil)
		m.RegisterExecutor(&authFallbackExecutor{id: "codex"})
		a := &Auth{ID: schedulerTestID(t, map[bool]string{false: "fast", true: "legacy"}[legacy]), Provider: "codex", Attributes: map[string]string{"api_key": "fixture"}}
		registerFallbackAuthForModel(t, m, a, "alpha-reload")
		opts := core.Options{SourceFormat: translator.FormatCodexAlphaSearch}
		for _, enabled := range []bool{false, true, false} {
			current, _ := m.GetByID(a.ID)
			current.Attributes[CodexAlphaSearchAttributeKey] = map[bool]string{false: "false", true: "true"}[enabled]
			if _, err := m.Update(t.Context(), current); err != nil {
				t.Fatal(err)
			}
			picked, _, errPick := m.pickNext(t.Context(), "codex", "alpha-reload", opts, nil)
			if enabled && (errPick != nil || picked == nil) {
				t.Fatal("enabled capability not selected")
			}
			if !enabled && (errPick == nil || picked != nil) {
				t.Fatal("disabled capability was selected")
			}
		}
		current, _ := m.GetByID(a.ID)
		current.Unavailable = true
		current.CooldownScope = cooldownScopeAuth
		current.NextRetryAfter = time.Now().Add(time.Minute)
		current.Quota.Exceeded = true
		if _, err := m.Update(t.Context(), current); err != nil {
			t.Fatal(err)
		}
		_, _, _, errPick := m.pickNextMixed(t.Context(), []string{"codex"}, "alpha-reload", opts, nil)
		var cooldown *modelCooldownError
		if errPick == nil || errors.As(errPick, &cooldown) {
			t.Fatal("unsupported credential influenced cooldown waiting")
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, _, _, errPick = m.pickNextMixed(ctx, []string{"codex"}, "alpha-reload", opts, nil)
		if !errors.Is(errPick, context.Canceled) {
			t.Fatal("selection ignored cancellation")
		}
	}
}

func TestCodexAlphaSearchDoesNotUseResponsesImageScheduling(t *testing.T) {
	req := core.Request{Payload: []byte(`{"commands":[{"image":[{"q":"fixture"}]}],"tools":[{"type":"image_generation"}]}`)}
	opts := core.Options{SourceFormat: translator.FormatCodexAlphaSearch}
	if requestHasImageGenerationToolForFallback(req, opts) {
		t.Fatal("search entered Responses image scheduling")
	}
	if withImageGenerationResultState(req, opts).Metadata[core.ImageGenerationResultStateMetadataKey] != nil {
		t.Fatal("search created Responses image state")
	}
}

func TestCodexAlphaSelectionKeepsStrictBindingAndFailoverRules(t *testing.T) {
	for _, failover := range []bool{false, true} {
		selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &FillFirstSelector{}, Failover: &failover, AcrossPriorities: true})
		t.Cleanup(selector.Stop)
		m := NewManager(nil, selector, nil)
		m.RegisterExecutor(&authFallbackExecutor{id: "codex"})
		for _, a := range []*Auth{
			{ID: "alpha-bound", Provider: "codex", Attributes: map[string]string{"api_key": "fixture"}},
			{ID: "alpha-fallback", Provider: "codex", Metadata: map[string]any{"access_token": "fixture"}},
		} {
			registerFallbackAuthForModel(t, m, a, "alpha-affinity")
		}
		opts := core.Options{SourceFormat: translator.FormatCodexAlphaSearch, Headers: http.Header{"Session-Id": {"fixture"}}}
		selector.BindSession(t.Context(), "codex", "alpha-affinity", opts, "alpha-bound")
		picked, _, _, errPick := m.pickNextMixed(t.Context(), []string{"codex"}, "alpha-affinity", opts, nil)
		if failover && (errPick != nil || picked == nil || picked.ID != "alpha-fallback") {
			t.Fatal("unsupported binding prevented allowed failover")
		}
		if !failover && (errPick == nil || picked != nil) {
			t.Fatal("capability filtering bypassed strict binding")
		}
	}
}

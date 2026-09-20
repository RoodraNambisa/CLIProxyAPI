package helps

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

func TestFixedCredentialStateOptionRespectsScopeAndPause(t *testing.T) {
	a := &auth.Auth{ID: "target-state-option-scope", Provider: "codex"}
	c := StateCredential(a, "model")
	for _, scenario := range []string{"paused", "disabled", "excluded", "other-model", "skip"} {
		t.Run(scenario, func(t *testing.T) {
			cfg := &config.Config{Codex: config.CodexConfig{StateOverride: config.CodexStateOverrideConfig{Enabled: true, MissingPolicy: "hide", Acquisition: "manual"}}}
			switch scenario {
			case "disabled":
				cfg.Codex.StateOverride.Enabled = false
			case "excluded":
				cfg.Codex.StateOverride.ExcludedCredentials = []string{a.ID}
			case "other-model":
				cfg.Codex.StateOverride.Models = []string{"other"}
			case "skip":
				cfg.Codex.StateOverride.Rules = &[]config.CodexStateRule{{Action: "skip"}}
			}
			codexstate.Default.Sync(cfg.Codex.StateOverride, []codexstate.Credential{c})
			defer codexstate.Default.Sync(config.CodexStateOverrideConfig{}, nil)
			if scenario == "paused" && !codexstate.Default.Action(c.ID, c.Model, "pause") {
				t.Fatal("could not pause acquisition")
			}
			carrier := &gin.Context{}
			carrier.Set(sdkaccess.CredentialTargetAuthIDContextKey, a.ID)
			carrier.Set(sdkaccess.MetadataCredentialTargetRespectStatePolicy, "true")
			ctx := context.WithValue(t.Context(), "gin", carrier)
			err := ApplyManagedState(ctx, cfg, a, c.Model, http.Header{})
			if scenario == "paused" {
				var status interface{ StatusCode() int }
				if !errors.As(err, &status) || status.StatusCode() != 503 {
					t.Fatalf("paused missing State bypassed policy: %v", err)
				}
			} else if err != nil {
				t.Fatalf("key option applied outside State scope: %v", err)
			}
		})
	}
}

func TestManagedStateScopePriorityAndCatalogIntersection(t *testing.T) {
	a := &auth.Auth{ID: "state-scope", Provider: "codex", Metadata: map[string]any{"account_id": "owner"}}
	r := registry.GetGlobalRegistry()
	r.RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "alias", UpstreamID: "model"}, {ID: "other"}, {ID: "gpt-image-1", SupportedOutputModalities: []string{"image"}}})
	defer r.UnregisterClient(a.ID)
	cfg := &config.Config{Codex: config.CodexConfig{StateOverride: config.CodexStateOverrideConfig{Enabled: true, Priorities: []int{0}}}}
	if got := ManagedStateModels(cfg, a); len(got) != 2 {
		t.Fatalf("all supported text models: %+v", got)
	}
	cfg.Codex.StateOverride.Models = []string{"alias", "unsupported"}
	got := ManagedStateModels(cfg, a)
	if len(got) != 1 || got[0].Model != "model" || got[0].Route != "alias" {
		t.Fatalf("intersection: %+v", got)
	}
	cfg.Codex.StateOverride.Priorities = []int{3}
	if len(ManagedStateModels(cfg, a)) != 0 {
		t.Fatal("ignored priority")
	}
}

func TestDiagnosticStateObservationCannotChangeNormalCache(t *testing.T) {
	a := &auth.Auth{ID: "diagnostic-observation", Provider: "codex"}
	c := StateCredential(a, "model")
	cfg := &config.Config{Codex: config.CodexConfig{StateOverride: config.CodexStateOverrideConfig{Enabled: true, InvalidateOnModelMismatch: true}}}
	codexstate.Default.Sync(cfg.Codex.StateOverride, []codexstate.Credential{c})
	codexstate.Diagnostic.Sync(cfg.Codex.StateOverride, nil, c)
	defer codexstate.Default.Sync(config.CodexStateOverrideConfig{}, nil)
	defer codexstate.Diagnostic.Sync(config.CodexStateOverrideConfig{}, nil)
	for _, manager := range []*codexstate.Manager{codexstate.Default, codexstate.Diagnostic} {
		if manager == codexstate.Diagnostic {
			manager.QueueManual(c)
		} else {
			manager.Action(c.ID, c.Model, "acquire")
		}
		manager.Tick(t.Context(), time.Now(), func(context.Context, codexstate.Credential, config.CodexStateOverrideConfig) (codexstate.Result, error) {
			return codexstate.Result{State: strings.Repeat("s", 292), Model: c.Model, Completed: true}, nil
		})
		manager.Wait()
	}
	ctx := WithCodexStateDiagnostic(core.WithCodexStateSnapshot(t.Context()), "acquired", "", nil)
	headers := http.Header{}
	if err := ApplyManagedState(ctx, cfg, a, c.Model, headers); err != nil {
		t.Fatal(err)
	}
	ObserveManagedStateCompletion(ctx, a, c.Model, headers)
	use := ManagedStateUse(ctx, a, c.Model, headers)
	if !use.Observe(nil, []byte(`{"model":"other"}`)) {
		t.Fatal("diagnostic response validation ignored")
	}
	normal := codexstate.Default.Snapshots(c.ID, time.Now())[0]
	diagnostic := codexstate.Diagnostic.Snapshots(c.ID, time.Now())[0]
	if normal.Status != "valid" || normal.Uses != 0 || normal.Completed != 0 || normal.Invalidations != 0 {
		t.Fatalf("diagnostic changed normal State: %+v", normal)
	}
	if diagnostic.Completed != 1 || diagnostic.Invalidations != 1 || diagnostic.Status == "queued" {
		t.Fatalf("diagnostic observation not isolated: %+v", diagnostic)
	}
}

func TestManagedStateIncludedCredentialScope(t *testing.T) {
	a := &auth.Auth{ID: "state-included", Index: "abc123", FileName: "codex-fixture.json", Provider: "codex"}
	r := registry.GetGlobalRegistry()
	r.RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "model"}})
	defer r.UnregisterClient(a.ID)
	for _, tc := range []struct {
		name       string
		priority   string
		priorities []int
		included   []string
		excluded   []string
		models     []string
		disabled   bool
		want       bool
	}{
		{name: "empty selectors include all", want: true},
		{name: "priority match", priority: "3", priorities: []int{3}, want: true},
		{name: "priority mismatch", priorities: []int{3}},
		{name: "explicit ID adds priority zero", priorities: []int{3}, included: []string{a.ID}, want: true},
		{name: "short ID adds priority zero", priorities: []int{3}, included: []string{a.Index}, want: true},
		{name: "filename adds priority zero", priorities: []int{3}, included: []string{a.FileName}, want: true},
		{name: "other priority zero is not added", priorities: []int{3}, included: []string{"other"}},
		{name: "priority still matches with other explicit ID", priority: "3", priorities: []int{3}, included: []string{"other"}, want: true},
		{name: "explicit only", included: []string{a.ID}, want: true},
		{name: "explicit only excludes all other priorities", priority: "3", included: []string{"other"}},
		{name: "exclusion wins over both selectors", priority: "3", priorities: []int{3}, included: []string{a.ID}, excluded: []string{a.Index}},
		{name: "included still respects models", included: []string{a.ID}, models: []string{"unsupported"}},
		{name: "included does not enable a disabled credential", included: []string{a.ID}, disabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a.Attributes = map[string]string{"priority": tc.priority}
			a.Disabled = tc.disabled
			cfg := &config.Config{Codex: config.CodexConfig{StateOverride: config.CodexStateOverrideConfig{
				Enabled: true, Priorities: tc.priorities, IncludedCredentials: tc.included,
				ExcludedCredentials: tc.excluded, Models: tc.models,
			}}}
			if got := len(ManagedStateModels(cfg, a)) > 0; got != tc.want {
				t.Fatalf("in scope = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestManagedStateAdmissionAndFrozenRetry(t *testing.T) {
	a := &auth.Auth{ID: "state-admission", Provider: "codex", Metadata: map[string]any{"account_id": "owner"}}
	r := registry.GetGlobalRegistry()
	r.RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "model"}})
	defer r.UnregisterClient(a.ID)
	cfg := &config.Config{Codex: config.CodexConfig{StateOverride: config.CodexStateOverrideConfig{Enabled: true, MissingPolicy: "error", Acquisition: "manual"}}}
	c := StateCredential(a, "model")
	codexstate.Default.Sync(cfg.Codex.StateOverride, []codexstate.Credential{c})
	defer codexstate.Default.Sync(config.CodexStateOverrideConfig{}, nil)
	err := ApplyManagedState(t.Context(), cfg, a, "model", http.Header{})
	var status interface {
		StatusCode() int
		SkipAuthResult() bool
		RetryOtherAuth() bool
	}
	if !errors.As(err, &status) || status.StatusCode() != 429 || !status.SkipAuthResult() || !status.RetryOtherAuth() || !core.PreserveErrorResponse(err) || gjson.Get(err.Error(), "error.code").String() != "rate_limit_exceeded" {
		t.Fatalf("bad admission response: %v", err)
	}
	if !strings.Contains(err.Error(), "Rate limit exceeded for image_generation.") {
		t.Fatal("wrong configured default message")
	}
	if codexstate.Default.Snapshots(a.ID, time.Now())[0].Misses != 1 {
		t.Fatal("missing state was not observed")
	}
	acquire := func(value string) {
		codexstate.Default.Action(a.ID, "model", "acquire")
		codexstate.Default.Tick(t.Context(), time.Now(), func(context.Context, codexstate.Credential, config.CodexStateOverrideConfig) (codexstate.Result, error) {
			return codexstate.Result{State: strings.Repeat(value, 292), Model: "model", Completed: true}, nil
		})
		codexstate.Default.Wait()
	}
	acquire("a")
	ctx := core.WithCodexStateSnapshot(t.Context())
	headers := http.Header{"X-Codex-Turn-State": {"client"}}
	if err := ApplyManagedState(ctx, cfg, a, "model", headers); err != nil {
		t.Fatal(err)
	}
	acquire("b")
	if err := ApplyManagedState(ctx, cfg, a, "model", headers); err != nil || headers.Get("X-Codex-Turn-State") != strings.Repeat("a", 292) {
		t.Fatal("retry changed frozen state")
	}
	fresh := http.Header{}
	_ = ApplyManagedState(core.WithCodexStateSnapshot(t.Context()), cfg, a, "model", fresh)
	if fresh.Get("X-Codex-Turn-State") != strings.Repeat("b", 292) {
		t.Fatal("new request ignored refreshed state")
	}
	a.Metadata["plan_type"] = "business"
	c = StateCredential(a, "model")
	codexstate.Default.Sync(cfg.Codex.StateOverride, []codexstate.Credential{c})
	acquire("c")
	if err := ApplyManagedState(ctx, cfg, a, "model", headers); err != nil || headers.Get("X-Codex-Turn-State") != strings.Repeat("c", 292) {
		t.Fatal("retry reused a state frozen for the previous subscription")
	}
}

func TestReviewManagedStateUsesRequestPolicyBeforeRuntimeSync(t *testing.T) {
	a := &auth.Auth{ID: "review-state-policy", Provider: "codex"}
	registry.GetGlobalRegistry().RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "model"}})
	defer registry.GetGlobalRegistry().UnregisterClient(a.ID)
	cfg := &config.Config{Codex: config.CodexConfig{StateOverride: config.CodexStateOverrideConfig{Enabled: true, Acquisition: "manual", MissingPolicy: "error", Lengths: []int{292}}}}
	c := StateCredential(a, "model")
	codexstate.Default.Sync(cfg.Codex.StateOverride, []codexstate.Credential{c})
	defer codexstate.Default.Sync(config.CodexStateOverrideConfig{}, nil)
	codexstate.Default.Action(a.ID, "model", "acquire")
	codexstate.Default.Tick(t.Context(), time.Now(), func(context.Context, codexstate.Credential, config.CodexStateOverrideConfig) (codexstate.Result, error) {
		return codexstate.Result{Completed: true, Model: "model", State: strings.Repeat("s", 292)}, nil
	})
	codexstate.Default.Wait()
	cfg.Codex.StateOverride.Lengths = []int{332}
	headers := http.Header{}
	err := ApplyManagedState(core.WithCodexStateSnapshot(t.Context()), cfg, a, "model", headers)
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != 429 || headers.Get("X-Codex-Turn-State") != "" {
		t.Fatal("new request sent cached State validated against an obsolete policy", err)
	}
	cfg.Codex.StateOverride.MissingPolicy = "continue"
	headers = http.Header{}
	if err := ApplyManagedState(t.Context(), cfg, a, "model", headers); err != nil || headers.Get("X-Codex-Turn-State") != "" {
		t.Fatal("continue policy was not honored while synchronization is pending", err)
	}

	codexstate.Default.Sync(config.CodexStateOverrideConfig{}, nil)
	cfg.Codex.StateOverride.MissingPolicy = "error"
	if err := ApplyManagedState(t.Context(), cfg, a, "model", http.Header{}); !errors.As(err, &status) || status.StatusCode() != 429 {
		t.Fatal("missing runtime entry bypassed request policy", err)
	}
	cfg.Codex.StateOverride.Mode = "missing"
	headers = http.Header{"X-Codex-Turn-State": {"client-value"}}
	if err := ApplyManagedState(t.Context(), cfg, a, "model", headers); err != nil || headers.Get("X-Codex-Turn-State") != "client-value" {
		t.Fatal("missing-only policy did not retain explicit client State", err)
	}
}

func TestReviewLegacyStateScopeKeepsExactRegisteredAlias(t *testing.T) {
	a := &auth.Auth{ID: "review-state-legacy-alias", Provider: "codex"}
	registry.GetGlobalRegistry().RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "alias(high)", UpstreamID: "model"}})
	defer registry.GetGlobalRegistry().UnregisterClient(a.ID)
	cfg := &config.Config{Codex: config.CodexConfig{StateOverride: config.CodexStateOverrideConfig{Enabled: true, Models: []string{"alias(high)"}, Acquisition: "manual", MissingPolicy: "error"}}}
	codexstate.Default.Sync(cfg.Codex.StateOverride, ManagedStateModels(cfg, a))
	defer codexstate.Default.Sync(config.CodexStateOverrideConfig{}, nil)
	codexstate.Default.Action(a.ID, "model", "acquire")
	codexstate.Default.Tick(t.Context(), time.Now(), func(context.Context, codexstate.Credential, config.CodexStateOverrideConfig) (codexstate.Result, error) {
		return codexstate.Result{Completed: true, Model: "model", State: strings.Repeat("s", 292)}, nil
	})
	codexstate.Default.Wait()
	headers := http.Header{}
	if err := ApplyManagedState(t.Context(), cfg, a, "model", headers); err != nil || len(headers.Get("X-Codex-Turn-State")) != 292 {
		t.Fatal("legacy alias scope lost its validated State", err)
	}
}

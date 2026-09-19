package helps

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

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
	if !errors.As(err, &status) || status.StatusCode() != 429 || !status.SkipAuthResult() || status.RetryOtherAuth() || !core.PreserveErrorResponse(err) || gjson.Get(err.Error(), "error.code").String() != "rate_limit_exceeded" {
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

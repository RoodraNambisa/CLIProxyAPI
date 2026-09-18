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
}

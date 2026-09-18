package management

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestModelProbeCodexStateModesAndAccountModelIsolation(t *testing.T) {
	returned := strings.Repeat("r", 292)
	managed := strings.Repeat("m", 292)
	var received atomic.Value
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		received.Store(r.Header.Get("X-Codex-Turn-State"))
		w.Header().Set("X-Codex-Turn-State", returned)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, strings.ReplaceAll(probeResponsesFixture, "grok-4.6", "gpt-fixture"))
	}))
	defer server.Close()
	cfg := &config.Config{Codex: config.CodexConfig{StateOverride: config.CodexStateOverrideConfig{Enabled: true, Acquisition: "manual", MissingPolicy: "error"}}}
	m := auth.NewManager(nil, nil, nil)
	m.SetConfig(cfg)
	m.SetOAuthModelAlias(map[string][]config.OAuthModelAlias{"codex": {{Name: "gpt-fixture", Alias: "friendly"}}})
	r := registry.GetGlobalRegistry()
	var selected *auth.Auth
	for _, id := range []string{"state-probe.json", "other-state-probe.json"} {
		a, err := m.Register(auth.WithSkipPersist(t.Context()), &auth.Auth{ID: id, FileName: id, Provider: "codex", Attributes: map[string]string{"base_url": server.URL}, Metadata: map[string]any{"access_token": "fixture-token", "account_id": id}})
		if err != nil {
			t.Fatal(err)
		}
		r.RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "friendly", UpstreamID: "gpt-fixture"}, {ID: "other-model"}})
		defer r.UnregisterClient(id)
		if selected == nil {
			selected = a
		}
	}
	c := helps.StateCredential(selected, "gpt-fixture")
	codexstate.Default.Sync(cfg.Codex.StateOverride, []codexstate.Credential{c})
	defer codexstate.Default.Sync(config.CodexStateOverrideConfig{}, nil)
	codexstate.Default.Action(c.ID, c.Model, "acquire")
	codexstate.Default.Tick(t.Context(), time.Now(), func(context.Context, codexstate.Credential, config.CodexStateOverrideConfig) (codexstate.Result, error) {
		return codexstate.Result{State: managed, Model: c.Model, Completed: true}, nil
	})
	codexstate.Default.Wait()
	h := &Handler{cfg: cfg, authManager: m}
	for _, stream := range []bool{false, true} {
		for _, mode := range []string{"configured", "managed", "none", "custom"} {
			t.Run(mode+map[bool]string{false: "/http", true: "/sse"}[stream], func(t *testing.T) {
				input := &modelProbeStateInput{Mode: mode}
				want, source := managed, "managed"
				if mode == "none" {
					want, source = "", "none"
				}
				if mode == "custom" {
					input.State = "manual-state"
					want, source = input.State, "custom"
				}
				before := calls.Load()
				status, result := runModelProbe(t, h, t.Context(), modelProbeRequest{Name: selected.FileName, Model: "friendly", Stream: stream, CodexState: input})
				if status != 200 || !result.Success || calls.Load() != before+1 || received.Load() != want {
					t.Fatalf("probe failed: status=%d calls=%d result=%+v", status, calls.Load()-before, result)
				}
				state := result.CodexState
				if state == nil || state.Source != source || state.SentLength != len(want) || state.SentDigest != modelProbeStateDigest(want) || state.State != returned || state.ReturnedLength != 292 {
					t.Fatalf("missing state diagnostics: %+v", state)
				}
			})
		}
	}
	for _, input := range []modelProbeRequest{
		{Name: selected.FileName, Model: "other-model", CodexState: &modelProbeStateInput{Mode: "managed"}},
		{Name: "other-state-probe.json", Model: "friendly", CodexState: &modelProbeStateInput{Mode: "managed"}},
	} {
		before := calls.Load()
		_, result := runModelProbe(t, h, t.Context(), input)
		if result.Success || calls.Load() != before || !strings.Contains(result.Error, "no valid managed State") || result.CodexState == nil || result.CodexState.Source != "unavailable" {
			t.Fatalf("state crossed account/model boundary or silently fell back: %+v", result)
		}
	}
	if snapshots := codexstate.Default.Snapshots(selected.ID, time.Now()); len(snapshots) != 1 || snapshots[0].Acquired != 1 || snapshots[0].Digest != modelProbeStateDigest(managed) || snapshots[0].Uses != 4 {
		t.Fatalf("temporary probe changed managed cache: %+v", snapshots)
	}
}

func TestModelProbeStateInputValidation(t *testing.T) {
	for _, input := range []modelProbeStateInput{
		{Mode: "invalid"}, {Mode: "custom"}, {Mode: "custom", State: "value\r\nInjected:bad"},
		{Mode: "custom", State: strings.Repeat("a", 8193)}, {Mode: "none", State: "unused"},
	} {
		if _, _, err := validateModelProbeState(&input, "codex"); err == nil {
			t.Fatal("invalid State accepted")
		}
	}
	if _, _, err := validateModelProbeState(&modelProbeStateInput{Mode: "custom", State: "valid"}, "xai"); err == nil {
		t.Fatal("Codex-only options accepted for another provider")
	}
}

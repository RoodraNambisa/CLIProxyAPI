package cliproxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCookieSourceModelDoesNotRewriteBusinessRequests(t *testing.T) {
	var acquisitionCalls, businessCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Model string `json:"model"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if r.Header.Get("X-Codex-Turn-State") != "" {
			t.Error("Cookie-only request sent State")
		}
		length := 312
		if body.Model == "base-model" {
			acquisitionCalls.Add(1)
			length = 292
			if r.Header.Get("Cookie") != "" {
				t.Error("acquisition carried Cookie")
			}
		} else {
			businessCalls.Add(1)
			if body.Model != "business-one" && body.Model != "business-two" {
				t.Errorf("business model was changed: %s", body.Model)
			}
			if !strings.Contains(r.Header.Get("Cookie"), "__oailb=from-base") {
				t.Error("business did not use source pool")
			}
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Codex-Turn-State", strings.Repeat("s", length))
		w.Header().Set("Set-Cookie", "__oailb=from-base; Path=/; Max-Age=3600")
		_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"test\",\"status\":\"completed\",\"model\":%q,\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n", body.Model)
	}))
	defer server.Close()
	cfg := &config.Config{Codex: config.CodexConfig{StateOverride: config.CodexStateOverrideConfig{Enabled: true, Strategy: "cookie-only", Acquisition: "all", Rules: &[]config.CodexStateRule{{ID: "business", Models: []string{"business-one", "business-two"}, Settings: config.CodexStateRuleSettings{CodexStateStrategySettings: config.CodexStateStrategySettings{CookieAcquisitionModel: new("base-model")}, Lengths: new([]int{312})}}}}}}
	m := auth.NewManager(nil, nil, nil)
	m.SetConfig(cfg)
	exec := executor.NewCodexExecutor(cfg)
	m.RegisterExecutor(exec)
	a, err := m.Register(auth.WithSkipPersist(t.Context()), &auth.Auth{ID: "source-wire", Provider: "codex", Attributes: map[string]string{"base_url": server.URL, "header:Cookie": "__oailb=old", "header:X-Codex-Turn-State": "old"}, Metadata: map[string]any{"access_token": "fixture", "account_id": "account"}})
	if err != nil {
		t.Fatal(err)
	}
	r := registry.GetGlobalRegistry()
	r.RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "business-one"}, {ID: "business-two"}, {ID: "base-model"}})
	defer r.UnregisterClient(a.ID)
	s := &Service{cfg: cfg, coreManager: m}
	s.syncCodexState(cfg)
	defer func() {
		codexstate.Default.Sync(config.CodexStateOverrideConfig{}, nil)
		codexstate.Diagnostic.Sync(config.CodexStateOverrideConfig{}, nil)
		codexstate.Default.Wait()
		codexstate.Diagnostic.Wait()
	}()
	codexstate.Default.Tick(t.Context(), time.Now(), s.acquireCodexState)
	codexstate.Default.Wait()
	if snap := codexstate.Default.CookieSnapshot(a.ID, time.Now()); snap == nil || snap.Main == nil || snap.AcquisitionModel != "base-model" {
		t.Fatalf("source acquisition failed: %+v", snap)
	}
	for _, model := range []string{"business-one", "business-two"} {
		payload, _ := json.Marshal(map[string]any{"model": model, "input": "OK"})
		_, err := exec.Execute(core.WithCodexStateSnapshot(t.Context()), a, core.Request{Model: model, Payload: payload}, core.Options{SourceFormat: translator.FormatOpenAIResponse})
		if err != nil {
			t.Fatal(err)
		}
	}
	if acquisitionCalls.Load() != 1 || businessCalls.Load() != 2 {
		t.Fatalf("unexpected calls: acquire=%d business=%d", acquisitionCalls.Load(), businessCalls.Load())
	}
}

func TestCodexCookieAcquisitionAndBusinessHeaders(t *testing.T) {
	for _, verify := range []bool{false, true} {
		t.Run(fmt.Sprint(verify), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				call := calls.Add(1)
				if r.Header.Get("X-Codex-Turn-State") != "" {
					t.Error("Cookie request sent State")
				}
				if call == 1 && r.Header.Get("Cookie") != "" {
					t.Errorf("acquisition sent Cookie: %q", r.Header.Get("Cookie"))
				}
				if call > 1 && !strings.Contains(r.Header.Get("Cookie"), "__oailb=sample") {
					t.Error("verification/business did not use acquired Cookie")
				}
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("X-Codex-Turn-State", strings.Repeat("r", 292))
				value := "sample"
				if call > 1 {
					value = "unsolicited"
				}
				w.Header().Set("Set-Cookie", "__oailb="+value+"; Path=/; Max-Age=3600")
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				_, _ = fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"test\",\"status\":\"completed\",\"model\":\"cookie-model\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n")
			}))
			defer server.Close()
			cfg := &config.Config{Codex: config.CodexConfig{StateOverride: config.CodexStateOverrideConfig{Enabled: true, Strategy: "cookie-only", Acquisition: "all", CookieVerifyAfterAcquire: verify}}}
			m := auth.NewManager(nil, nil, nil)
			m.SetConfig(cfg)
			exec := executor.NewCodexExecutor(cfg)
			m.RegisterExecutor(exec)
			a, err := m.Register(auth.WithSkipPersist(t.Context()), &auth.Auth{ID: "cookie-service", Provider: "codex", Attributes: map[string]string{"base_url": server.URL, "header:Cookie": "__oailb=old; custom=value", "header:X-Codex-Turn-State": "old"}, Metadata: map[string]any{"access_token": "fixture", "account_id": "account"}})
			if err != nil {
				t.Fatal(err)
			}
			r := registry.GetGlobalRegistry()
			r.RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "cookie-model"}})
			defer r.UnregisterClient(a.ID)
			s := &Service{cfg: cfg, coreManager: m}
			s.syncCodexState(cfg)
			defer func() {
				codexstate.Default.Sync(config.CodexStateOverrideConfig{}, nil)
				codexstate.Diagnostic.Sync(config.CodexStateOverrideConfig{}, nil)
				codexstate.Default.Wait()
				codexstate.Diagnostic.Wait()
			}()
			codexstate.Default.Tick(t.Context(), time.Now(), s.acquireCodexState)
			codexstate.Default.Wait()
			want := int32(1)
			if verify {
				want = 2
			}
			snapshot := codexstate.Default.CookieSnapshot(a.ID, time.Now())
			if calls.Load() != want || snapshot == nil || snapshot.Main == nil || snapshot.Status != "valid" {
				t.Fatalf("acquisition calls=%d snapshot=%+v", calls.Load(), snapshot)
			}
			ctx := core.WithCodexStateSnapshot(t.Context())
			_, err = exec.Execute(ctx, a, core.Request{Model: "cookie-model", Payload: []byte(`{"model":"cookie-model","input":"OK"}`)}, core.Options{SourceFormat: translator.FormatOpenAIResponse})
			if err != nil {
				t.Fatal(err)
			}
			if calls.Load() != want+1 {
				t.Fatal("unexpected acquisition retry")
			}
			c := helps.StateCredential(a, "cookie-model")
			selected, _, _ := codexstate.Default.PickCookie(c, server.URL+"/responses", time.Now(), cfg.Codex.StateOverride.Resolved())
			if !strings.Contains(selected.Header, "__oailb=sample") {
				t.Fatal("business response replaced frozen routing Cookie")
			}
			stored, _ := m.GetByID(a.ID)
			if stored.Attributes["header:Cookie"] != "__oailb=old; custom=value" {
				t.Fatal("acquisition modified stored credential")
			}
		})
	}
}

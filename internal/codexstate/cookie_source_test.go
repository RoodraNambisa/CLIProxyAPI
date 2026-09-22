package codexstate

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestCookiePoolPriority(t *testing.T) {
	c := Credential{Model: "requested"}
	for _, tc := range []struct{ mode, group, source, want string }{
		{"", "", "", "model:requested"},
		{"auto", "", "base", "source:base"},
		{"auto", "common", "base", "shared:common"},
		{"auto", "common", "", "shared:common"},
		{"model", "common", "base", "model:requested"},
		{"credential", "", "base", ""},
	} {
		p := config.CodexStateOverrideConfig{CookiePoolMode: tc.mode, CookiePoolGroup: tc.group, CookieAcquisitionModel: tc.source}
		if got := CookiePool(c, p); got != tc.want {
			t.Fatalf("%+v: %q", tc, got)
		}
	}
}

func sourceFixture(models ...string) (*Manager, []Credential, config.CodexStateOverrideConfig) {
	p := config.CodexStateOverrideConfig{Enabled: true, Strategy: "cookie-only", Acquisition: "all", Concurrency: 2, MissingPolicy: "hide", InvalidateOnModelMismatch: true, InvalidateOnStateLengthMismatch: true}.Resolved()
	var credentials []Credential
	for _, model := range models {
		credentials = append(credentials, Credential{ID: "source-account", Owner: "owner", Instance: "instance", Model: model, Route: model})
	}
	return New(), credentials, p
}

func acquireSourcePools(t *testing.T, m *Manager, steps int) []string {
	t.Helper()
	var models []string
	for range steps {
		m.Tick(t.Context(), time.Now(), func(_ context.Context, c Credential, p config.CodexStateOverrideConfig) (Result, error) {
			models = append(models, c.Model)
			r := cookieResult(time.Now(), "route-"+c.Model)
			r.Model = c.Model
			if ValidateAcquisition(p, c.Model, r, time.Now()) != "" {
				t.Error("probe used business acceptance instead of source acceptance")
			}
			return r, nil
		})
		m.Wait()
	}
	return models
}

func TestCookieSourceRulesShareLunaAndIsolateOtherModels(t *testing.T) {
	modern := []string{"gpt-5.6-luna", "gpt-5.6-sol", "gpt-5.6-terra", "gpt-6-astra"}
	m, credentials, p := sourceFixture(append(slices.Clone(modern), "gpt-5.5")...)
	p.Rules = &[]config.CodexStateRule{
		{ID: "modern", Models: modern, Settings: config.CodexStateRuleSettings{CodexStateStrategySettings: config.CodexStateStrategySettings{CookieAcquisitionModel: new("gpt-5.6-luna")}, Lengths: new([]int{312})}, ModelOverrides: []config.CodexStateRuleModelOverride{{ID: "source-acceptance", Models: []string{"gpt-5.6-luna"}, Settings: config.CodexStateRuleSettings{Lengths: new([]int{292})}}}},
		{ID: "legacy", Models: []string{"gpt-5.5"}},
	}
	m.Sync(p, credentials)
	models := acquireSourcePools(t, m, 2)
	if len(models) != 2 || !slices.Contains(models, "gpt-5.6-luna") || !slices.Contains(models, "gpt-5.5") {
		t.Fatalf("wrong acquisition models: %v", models)
	}
	var modernVersion uint64
	for _, c := range credentials {
		policy, _, _ := p.PolicyFor(c.Scope())
		picked, _, _ := m.PickCookie(c, "https://chatgpt.com/backend-api/codex/responses", time.Now(), policy)
		if picked.Header == "" {
			t.Fatalf("no Cookie for %s", c.Model)
		}
		if c.Model == "gpt-5.5" {
			if strings.Contains(picked.Header, "luna") || picked.Version == modernVersion {
				t.Fatal("5.5 mixed with the source pool")
			}
		} else {
			if modernVersion != 0 && picked.Version != modernVersion {
				t.Fatal("same source did not share")
			}
			modernVersion = picked.Version
			length := 312
			if c.Model == "gpt-5.6-luna" {
				length = 292
			}
			if m.ObserveCookie(c, picked, policy, http.Header{"X-Codex-Turn-State": {strings.Repeat("s", length)}}, c.Model, true) {
				t.Fatal("business request used source model checks")
			}
		}
	}
	if len(m.CookieSnapshots(credentials[0].ID, time.Now())) != 2 {
		t.Fatal("wrong pool count")
	}
	if ok, _ := m.CookieAction(credentials[0].ID, modern[0], "clear", "model:gpt-5.5"); ok {
		t.Fatal("stale action cleared another pool")
	}
	m.CookieAction(credentials[0].ID, modern[0], "clear")
	if m.CookieSnapshot(credentials[0].ID, time.Now(), "gpt-5.5").Main == nil {
		t.Fatal("clear crossed a pool boundary")
	}
}

func TestCookieDefaultUsesEachModelAndExplicitGroupOverridesSources(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		m, credentials, p := sourceFixture("one", "two")
		if grouped {
			p.CookiePoolGroup = "compatible"
			p.ModelOverrides = []config.CodexStateModelOverride{{Model: "one", CodexStateStrategySettings: config.CodexStateStrategySettings{CookieAcquisitionModel: new("base-one")}}, {Model: "two", CodexStateStrategySettings: config.CodexStateStrategySettings{CookieAcquisitionModel: new("base-two")}}}
		}
		m.Sync(p, credentials)
		models := acquireSourcePools(t, m, 2)
		want := 2
		if grouped {
			want = 1
		}
		if len(models) != want || len(m.CookieSnapshots(credentials[0].ID, time.Now())) != want {
			t.Fatalf("grouped=%v: %v", grouped, models)
		}
		if !grouped && (!slices.Contains(models, "one") || !slices.Contains(models, "two")) {
			t.Fatal("unconfigured model did not acquire for itself")
		}
	}
}

func TestCookieSourceHotReloadAndOneAcquisitionPerCredential(t *testing.T) {
	m, credentials, p := sourceFixture("one", "two")
	m.Sync(p, credentials)
	started, release := make(chan struct{}), make(chan struct{})
	m.Tick(t.Context(), time.Now(), func(_ context.Context, c Credential, _ config.CodexStateOverrideConfig) (Result, error) {
		close(started)
		<-release
		r := cookieResult(time.Now(), "old")
		r.Model = c.Model
		return r, nil
	})
	<-started
	if m.Running() != 1 {
		t.Fatal("parallel pools bypassed credential acquisition coalescing")
	}
	p.CookieAcquisitionModel = "new-source"
	m.Sync(p, credentials)
	close(release)
	m.Wait()
	if s := m.CookieSnapshots(credentials[0].ID, time.Now()); len(s) != 1 || s[0].Main != nil {
		t.Fatal("old acquisition revived a retired pool")
	}
	acquireSourcePools(t, m, 1)
	c := credentials[0]
	policy, _, _ := p.PolicyFor(c.Scope())
	picked, _, _ := m.PickCookie(c, "https://chatgpt.com/backend-api/codex/responses", time.Now(), policy)
	if picked.Pool != "source:new-source" {
		t.Fatal("wrong source after hot reload")
	}
	p.CookiePoolMode = "model"
	m.Sync(p, credentials)
	if m.CookieConnectionValid(c, picked, time.Now(), policy) {
		t.Fatal("old WS handshake reused a pool after reassignment")
	}
}

func TestCookiePoolAcquisitionKeepsRuleOrderAcrossPools(t *testing.T) {
	m, credentials, p := sourceFixture("alpha", "zeta")
	p.Rules = &[]config.CodexStateRule{{ID: "first", Models: []string{"zeta"}}, {ID: "second", Models: []string{"alpha"}}}
	m.Sync(p, credentials)
	got := acquireSourcePools(t, m, 2)
	if !slices.Equal(got, []string{"zeta", "alpha"}) {
		t.Fatalf("pool names overrode rule priority: %v", got)
	}
}

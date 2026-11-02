package auth

import (
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/fillorder"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestManagerFillOrderUsesRegistrationAndManualMoves(t *testing.T) {
	m := NewManager(nil, &FillFirstSelector{}, nil)
	m.RegisterExecutor(schedulerTestExecutor{})
	m.SetConfig(&config.Config{Routing: config.RoutingConfig{Strategy: "fill-first"}})
	t.Cleanup(m.admission.Close)
	if err := m.ConfigureFillOrder(""); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.CloseFillOrder() })
	for _, id := range []string{"z", "b"} {
		if _, err := m.Register(t.Context(), &Auth{ID: id, Provider: "test"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.InitializeFillOrder(); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Register(t.Context(), &Auth{ID: "a", Provider: "test"}); err != nil {
		t.Fatal(err)
	}
	pick := func() string {
		a, _, err := m.pickNext(t.Context(), "test", "", core.Options{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return a.ID
	}
	if got := pick(); got != "b" {
		t.Fatal(got)
	}
	a, _ := m.GetByID("a")
	id := a.fillOrderRecord.ID
	if _, err := m.MoveFillOrder(t.Context(), fillorder.Move{RecordID: id, Revision: m.FillOrderRevision(), Direction: "up", Steps: 2}); err != nil {
		t.Fatal(err)
	}
	if got := pick(); got != "a" {
		t.Fatal(got)
	}
	a, _ = m.GetByID("a")
	a.Attributes = map[string]string{"priority": "5"}
	if _, err := m.Update(t.Context(), a); err != nil {
		t.Fatal(err)
	}
	a, _ = m.GetByID("a")
	if a.fillOrderRecord.ID != id || a.fillOrderRecord.Sequence != 3 {
		t.Fatal(a.fillOrderRecord)
	}
	if err := m.Delete(t.Context(), "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Register(t.Context(), &Auth{ID: "a", Provider: "test"}); err != nil {
		t.Fatal(err)
	}
	if got := pick(); got != "b" {
		t.Fatal(got)
	}
}
func TestFillOrderPrincipalScope(t *testing.T) {
	makeAuth := func(workspace, user, email string) *Auth {
		return &Auth{ID: "a", Provider: "codex", Metadata: map[string]any{"account_id": workspace, "chatgpt_user_id": user, "email": email}}
	}
	a := makeAuth("work-a", "user-a", "one@example.com")
	if fillOrderIdentity(a) == "" {
		t.Fatal("missing identity")
	}
	for _, b := range []*Auth{makeAuth("work-b", "user-a", "one@example.com"), makeAuth("work-a", "user-b", "one@example.com")} {
		if fillOrderIdentity(a) == fillOrderIdentity(b) {
			t.Fatal("different principal conflated")
		}
	}
	b := makeAuth("work-a", "user-a", "renamed@example.com")
	b.Metadata["access_token"] = "rotated"
	if fillOrderIdentity(a) != fillOrderIdentity(b) {
		t.Fatal("token/email rotation changed stable user")
	}
	if fillOrderIdentity(makeAuth("", "", "one@example.com")) != "" {
		t.Fatal("email-only identity")
	}
}

func TestManagerFillOrderMixedProvidersAndGroups(t *testing.T) {
	for _, width := range []int{1, 2} {
		t.Run(string(rune('0'+width)), func(t *testing.T) {
			m := NewManager(nil, &FillFirstSelector{}, nil)
			m.SetConfig(&config.Config{Routing: config.RoutingConfig{Strategy: "fill-first", FillFirstRange: width}})
			t.Cleanup(m.admission.Close)
			if err := m.ConfigureFillOrder(""); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = m.CloseFillOrder() })
			if err := m.InitializeFillOrder(); err != nil {
				t.Fatal(err)
			}
			for i, id := range []string{"z", "y", "a"} {
				provider := "test"
				if i == 1 {
					provider = "other"
				}
				if _, err := m.Register(t.Context(), &Auth{ID: id, Provider: provider}); err != nil {
					t.Fatal(err)
				}
			}
			for range 30 {
				a, _, err := m.scheduler.pickMixed(t.Context(), []string{"test", "other"}, "", core.Options{}, nil)
				if err != nil {
					t.Fatal(err)
				}
				if a.ID == "a" || width == 1 && a.ID != "z" {
					t.Fatal(a.ID)
				}
			}
		})
	}
}

func TestManagerFillOrderConcurrentMovesAndUpdates(t *testing.T) {
	m := NewManager(nil, &FillFirstSelector{}, nil)
	m.RegisterExecutor(schedulerTestExecutor{})
	m.SetConfig(&config.Config{Routing: config.RoutingConfig{Strategy: "fill-first"}})
	t.Cleanup(m.admission.Close)
	if err := m.ConfigureFillOrder(""); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.CloseFillOrder() })
	if err := m.InitializeFillOrder(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b", "c"} {
		if _, err := m.Register(t.Context(), &Auth{ID: id, Provider: "test"}); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	for _, id := range []string{"a", "b", "c"} {
		wg.Go(func() {
			for range 30 {
				a, _ := m.GetByID(id)
				_, err := m.MoveFillOrder(t.Context(), fillorder.Move{RecordID: a.fillOrderRecord.ID, Revision: m.FillOrderRevision(), Direction: "up", Steps: 1})
				if err != nil && !errors.Is(err, fillorder.ErrConflict) {
					t.Error(err)
				}
				a, _ = m.GetByID(id)
				if _, err = m.Update(t.Context(), a); err != nil {
					t.Error(err)
				}
				if _, _, err = m.pickNext(t.Context(), "test", "", core.Options{}, nil); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	seen := map[string]bool{}
	for _, a := range m.List() {
		r, _ := m.fillOrder.Lookup(a.ID)
		if seen[r.ID] || a.fillOrderRecord.ID != r.ID || a.fillOrderRecord.Rank != r.Rank {
			t.Error("stale ordering", a.ID)
		}
		seen[r.ID] = true
	}
}

func TestFillOrderUsesTokenSubjectBeforeMetadata(t *testing.T) {
	token := func(user string) string {
		return "header." + base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"aud":"codex","sub":"%s","https://api.openai.com/auth":{"chatgpt_account_id":"workspace","chatgpt_user_id":"%s"}}`, user, user))) + ".signature"
	}
	a := &Auth{Provider: "codex", Metadata: map[string]any{"account_id": "workspace", "email": "same@example.com", "chatgpt_user_id": "stale", "access_token": token("one")}}
	b := a.Clone()
	b.Metadata["access_token"] = token("two")
	if fillOrderIdentity(a) == fillOrderIdentity(b) || fillOrderRefreshCompatible(a, b) {
		t.Fatal("different token users conflated")
	}
	b.Metadata["access_token"] = token("one")
	if fillOrderIdentity(a) != fillOrderIdentity(b) || !fillOrderRefreshCompatible(a, b) {
		t.Fatal("same user did not retain identity")
	}
}

func TestFillOrderEndpointScopeUsesEffectiveOAuthAddress(t *testing.T) {
	m := NewManager(nil, nil, nil)
	t.Cleanup(m.admission.Close)
	m.SetConfig(&config.Config{Codex: config.CodexConfig{BaseURL: "https://global.example/codex/"}})
	a := &Auth{ID: "a", Provider: "codex", Metadata: map[string]any{"account_id": "workspace", "email": "member@example.com"}}
	global := m.fillOrderInput(a)
	b := a.Clone()
	b.Attributes = map[string]string{"base_url": "https://global.example/codex"}
	explicit := m.fillOrderInput(b)
	if global.Scope != explicit.Scope || global.Identity != explicit.Identity {
		t.Fatal("equivalent endpoint changed identity")
	}
	b.Attributes["base_url"] = "https://different.example/codex"
	other := m.fillOrderInput(b)
	if global.Identity == other.Identity || global.Scope == other.Scope {
		t.Fatal("upstream identity conflated")
	}
}

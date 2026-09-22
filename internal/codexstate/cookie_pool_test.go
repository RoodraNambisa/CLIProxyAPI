package codexstate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func poolFixture(t *testing.T, count int) (*Manager, Credential, config.CodexStateOverrideConfig) {
	m, c, p := cookieFixture(t)
	p.CookieBackupCount, p.MissingPolicy = count, "hide"
	m.Sync(p, []Credential{c})
	return m, c, p
}

func fillPool(t *testing.T, m *Manager, c Credential, now time.Time, count int) {
	t.Helper()
	for i := range count {
		m.Tick(t.Context(), now, func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) {
			return cookieResult(now.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("cookie-%d", i)), nil
		})
		m.Wait()
	}
}

func TestCookiePoolFillsAndPromotesWithoutAcquisition(t *testing.T) {
	m, c, p := poolFixture(t, 2)
	now := time.Now()
	fillPool(t, m, c, now, 3)
	s := m.CookieSnapshot(c.ID, now)
	if s.Main == nil || len(s.Backups) != 2 || s.BackupTarget != 2 || s.Acquired != 3 {
		t.Fatalf("pool not filled: %+v", s)
	}
	m.Tick(t.Context(), now, func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) {
		t.Error("full pool acquired again")
		return Result{}, nil
	})
	m.Wait()
	used, _, _ := m.PickCookie(c, "https://chatgpt.com/backend-api/codex/responses", now, p)
	if !strings.Contains(used.Header, "cookie-0") {
		t.Fatal("filling replaced the active Cookie")
	}
	if !m.ObserveCookie(c, used, p, http.Header{"X-Codex-Turn-State": {strings.Repeat("s", 312)}}, c.Model, true) {
		t.Fatal("primary not invalidated")
	}
	next, _, _ := m.PickCookie(c, used.URL, now, p)
	if next.Version == used.Version || !strings.Contains(next.Header, "cookie-1") {
		t.Fatal("standby not promoted immediately")
	}
	if m.ObserveCookie(c, used, p, nil, "wrong", true) {
		t.Fatal("retired response invalidated new primary")
	}
	if !m.Availability()[c.ID][c.Model].After(now) {
		t.Fatal("hide excluded a credential with a valid standby")
	}
	s = m.CookieSnapshot(c.ID, now)
	if len(s.Backups) != 1 || s.Promotions != 1 || s.Acquired != 3 {
		t.Fatalf("promotion made a request: %+v", s)
	}
	data, _ := json.Marshal(s)
	if strings.Contains(string(data), "cookie-1") || strings.Contains(string(data), "cookie-2") {
		t.Fatal("raw backup leaked")
	}
	m.Tick(t.Context(), now, func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) {
		return cookieResult(now, "replacement"), nil
	})
	m.Wait()
	if len(m.CookieSnapshot(c.ID, now).Backups) != 2 {
		t.Fatal("standby not replenished")
	}
}

func TestCookiePoolIndependentExpiryAndAvailability(t *testing.T) {
	m, c, p := poolFixture(t, 2)
	p.CookieMaxAgeSeconds = 10
	m.Sync(p, []Credential{c})
	now := time.Now()
	installCookie(m, c, cookieResult(now, "primary"), now)
	g := m.cookies[c.ID]
	g.filling = true
	installCookie(m, c, cookieResult(now.Add(5*time.Second), "standby"), now)
	if !m.Availability()[c.ID][c.Model].Equal(now.Add(15 * time.Second)) {
		t.Fatal("availability ignored standby lifetime")
	}
	selected, _, _ := m.PickCookie(c, g.main.Origin, now.Add(11*time.Second), p)
	if !strings.Contains(selected.Header, "standby") {
		t.Fatal("expired primary did not promote valid standby")
	}
	if selected, _, _ = m.PickCookie(c, selected.URL, now.Add(16*time.Second), p); selected.Header != "" {
		t.Fatal("promotion reset Cookie age")
	}
	if len(m.CookieSnapshot(c.ID, now.Add(16*time.Second)).Backups) != 0 {
		t.Fatal("expired backup still counted")
	}
}

func TestCookiePoolDuplicatesUseRetryBudgetAndKeepMain(t *testing.T) {
	m, c, p := poolFixture(t, 2)
	p.MaxAttempts = 2
	m.Sync(p, []Credential{c})
	now := time.Now()
	installCookie(m, c, cookieResult(now, "same"), now)
	for range 3 {
		m.Tick(t.Context(), now.Add(time.Hour/2), func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) {
			return cookieResult(now, "same"), nil
		})
		m.Wait()
	}
	s := m.CookieSnapshot(c.ID, now)
	if s.Main == nil || len(s.Backups) != 0 || s.LastError != "duplicate_cookie" || !s.Exhausted || s.ConsecutiveFailures != 2 {
		t.Fatalf("duplicates bypassed budget: %+v", s)
	}
}

func TestCookiePoolHotReloadPauseAndLateBackupFailure(t *testing.T) {
	m, c, p := poolFixture(t, 3)
	now := time.Now()
	fillPool(t, m, c, now, 2)
	used, _, _ := m.PickCookie(c, "https://chatgpt.com/backend-api/codex/responses", now, p)
	m.CookieAction(c.ID, c.Model, "acquire")
	m.Tick(t.Context(), now, func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) {
		return cookieResult(now.Add(time.Second), "manual-new"), nil
	})
	m.Wait()
	// A refreshed main may remain a standby; its late failure can only remove
	// that exact retained version, never the new main.
	m.mu.Lock()
	g := m.cookies[c.ID]
	currentVersion := g.version
	m.mu.Unlock()
	if !m.ObserveCookie(c, used, p, nil, "wrong", true) {
		t.Fatal("late failure left a known bad backup")
	}
	if m.CookieSnapshot(c.ID, now).Main.Version != currentVersion {
		t.Fatal("late standby failure removed primary")
	}
	c.Instance = "refreshed-token"
	p.CookieBackupCount = 1
	m.Sync(p, []Credential{c})
	if s := m.CookieSnapshot(c.ID, now); s.Main == nil || len(s.Backups) != 1 || s.BackupTarget != 1 {
		t.Fatal("resize lost primary or ignored new size")
	}
	m.CookieAction(c.ID, c.Model, "pause")
	if selection, _, _ := m.PickCookie(c, used.URL, now, p); selection.Header != "" {
		t.Fatal("paused pool served")
	}
	m.CookieAction(c.ID, c.Model, "resume")
	m.CookieAction(c.ID, c.Model, "clear")
	if s := m.CookieSnapshot(c.ID, now); s.Main != nil || len(s.Backups) != 0 {
		t.Fatal("clear retained backups")
	}
}

func TestCookiePoolSharesLargestTargetAndRespectsManualScope(t *testing.T) {
	m, c, p := poolFixture(t, 1)
	other := c
	other.Model = "second"
	p.ModelOverrides = []config.CodexStateModelOverride{{Model: other.Model, CodexStateStrategySettings: config.CodexStateStrategySettings{CookieBackupCount: new(3)}}}
	p.Acquisition = "manual"
	m.Sync(p, []Credential{c, other})
	now := time.Now()
	installCookie(m, c, cookieResult(now, "primary"), now)
	if m.CookieSnapshot(c.ID, now).BackupTarget != 3 {
		t.Fatal("shared target did not include model override")
	}
	m.Tick(t.Context(), now, func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) {
		t.Error("manual scope refilled automatically")
		return Result{}, nil
	})
	m.Wait()
}

func TestCookiePoolReacquisitionCanReplaceExpiredIdenticalCookie(t *testing.T) {
	m, c, p := poolFixture(t, 1)
	p.CookieMaxAgeSeconds = 1
	m.Sync(p, []Credential{c})
	now := time.Now()
	installCookie(m, c, cookieResult(now.Add(-2*time.Second), "same"), now.Add(-2*time.Second))
	installCookie(m, c, cookieResult(now, "same"), now)
	selected, _, _ := m.PickCookie(c, "https://chatgpt.com/backend-api/codex/responses", now, p)
	if selected.Header == "" || m.CookieSnapshot(c.ID, now).LastError != "" {
		t.Fatal("fresh validated acquisition was mistaken for an available duplicate")
	}
}

func TestCookiePoolInFlightRefillPreservesPromotedMainAndHonorsClear(t *testing.T) {
	for _, clearPool := range []bool{false, true} {
		t.Run(fmt.Sprintf("clear=%v", clearPool), func(t *testing.T) {
			m, c, p := poolFixture(t, 2)
			now := time.Now()
			fillPool(t, m, c, now, 2)
			used, _, _ := m.PickCookie(c, "https://chatgpt.com/backend-api/codex/responses", now, p)
			started, release := make(chan struct{}), make(chan struct{})
			m.Tick(t.Context(), now, func(ctx context.Context, _ Credential, _ config.CodexStateOverrideConfig) (Result, error) {
				r := cookieResult(now, "in-flight-backup")
				PublishCookieCandidate(ctx, r.Cookies)
				close(started)
				<-release
				return r, nil
			})
			<-started
			m.ObserveCookie(c, used, p, nil, "wrong", true)
			promoted, _, _ := m.PickCookie(c, used.URL, now, p)
			if clearPool {
				m.CookieAction(c.ID, c.Model, "clear")
			}
			close(release)
			m.Wait()
			s := m.CookieSnapshot(c.ID, now)
			if clearPool {
				if s.Main != nil || s.Candidate != nil || len(s.Backups) != 0 {
					t.Fatal("late candidate revived cleared pool")
				}
			} else if s.Main == nil || s.Main.Version != promoted.Version || len(s.Backups) != 1 || s.Candidate != nil {
				t.Fatal("refill replaced promoted main or lost candidate")
			}
		})
	}
}

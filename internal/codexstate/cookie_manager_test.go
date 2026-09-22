package codexstate

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func cookieFixture(t *testing.T) (*Manager, Credential, config.CodexStateOverrideConfig) {
	t.Helper()
	m, c, p := fixture()
	p.Strategy = "cookie-only"
	p.InvalidateOnStateLengthMismatch = true
	p.InvalidateOnModelMismatch = true
	p = p.Resolved()
	other := c
	other.Model = "second"
	m.Sync(p, []Credential{c, other})
	return m, c, p
}
func cookieResult(now time.Time, value string) Result {
	return Result{State: strings.Repeat("s", 292), Model: "model", Completed: true, Status: 200, ReceivedAt: now, Cookies: CaptureCookies("https://chatgpt.com/backend-api/codex/responses", http.Header{"Set-Cookie": {"__oailb=" + value + "; Path=/; Secure; Max-Age=3600", "__cf_bm=aux; Path=/; Secure; Max-Age=1800"}}, now)}
}
func installCookie(m *Manager, c Credential, r Result, now time.Time) {
	g := m.cookies[c.ID]
	g.work.Model = c.Model
	g.work.credential = c
	m.running++
	g.work.busy = true
	m.finishCookie(c.ID, g, g.work, nil, r, nil, now)
}
func TestCookieCredentialScopeExpiryAndInvalidation(t *testing.T) {
	m, c, p := cookieFixture(t)
	now := time.Now()
	installCookie(m, c, cookieResult(now, "first"), now)
	used, _, _ := m.PickCookie(c, "https://chatgpt.com/backend-api/codex/responses", now, p)
	if !strings.Contains(used.Header, "__oailb=first") {
		t.Fatal("missing cookie")
	}
	other := c
	other.Model = "second"
	if selection, _, _ := m.PickCookie(other, used.URL, now, p); selection.Version != used.Version {
		t.Fatal("model split credential pool")
	}
	other.ID = "another"
	if selection, _, _ := m.PickCookie(other, used.URL, now, p); selection.Header != "" {
		t.Fatal("cross-credential cookie")
	}
	if selection, _, _ := m.PickCookie(c, "https://example.com/v1/responses", now, p); selection.Header != "" {
		t.Fatal("cookie domain ignored")
	}
	if selection, _, _ := m.PickCookie(c, "http://chatgpt.com/backend-api/codex/responses", now, p); selection.Header != "" {
		t.Fatal("secure cookie sent over HTTP")
	}
	if !m.ObserveCookie(c, used, p, http.Header{"X-Codex-Turn-State": {strings.Repeat("b", 312)}}, "model", true) {
		t.Fatal("matching model masked length failure")
	}
	other = c
	other.Model = "second"
	if selection, _, _ := m.PickCookie(other, used.URL, now, p); selection.Header != "" {
		t.Fatal("failure did not invalidate entire credential")
	}
	installCookie(m, c, cookieResult(now, "second"), now)
	if m.ObserveCookie(c, used, p, nil, "wrong", true) {
		t.Fatal("stale response removed replacement")
	}
	p.CookieMaxAgeSeconds = 10
	if selection, _, _ := m.PickCookie(c, used.URL, now.Add(11*time.Second), p); selection.Header != "" {
		t.Fatal("local expiration ignored")
	}
	snapshot, _ := json.Marshal(m.CookieSnapshot(c.ID, now))
	if strings.Contains(string(snapshot), "__oailb=second") || strings.Contains(string(snapshot), "\"Value\"") {
		t.Fatal("raw cookies exposed")
	}
}

func TestCookieTokenRefreshAndPartialUpdates(t *testing.T) {
	m, c, p := cookieFixture(t)
	now := time.Now()
	installCookie(m, c, cookieResult(now, "first"), now)
	used, _, _ := m.PickCookie(c, "https://chatgpt.com/backend-api/codex/responses", now, p)
	updated := http.Header{"Set-Cookie": {"__cf_bm=newaux; Path=/; Secure; Max-Age=1800", "__oailb=unsolicited; Path=/; Secure; Max-Age=3600"}}
	m.ObserveCookie(c, used, p, updated, "", false)
	next, _, _ := m.PickCookie(c, used.URL, now, p)
	if !strings.Contains(next.Header, "__cf_bm=newaux") || !strings.Contains(next.Header, "__oailb=first") {
		t.Fatal("wrong cookie delta handling")
	}
	m.ObserveCookie(c, used, p, http.Header{"Set-Cookie": {"__cf_bm=stale; Path=/; Secure; Max-Age=1800"}}, "", false)
	next, _, _ = m.PickCookie(c, used.URL, now, p)
	if strings.Contains(next.Header, "stale") {
		t.Fatal("stale member overwrote new auxiliary cookie")
	}
	c.Instance = "refreshed"
	m.Sync(p, []Credential{c})
	next, _, _ = m.PickCookie(c, used.URL, now, p)
	if next.Header == "" {
		t.Fatal("ordinary token refresh removed cookies")
	}
	c.Owner = "another-account"
	m.Sync(p, []Credential{c})
	next, _, _ = m.PickCookie(c, used.URL, now, p)
	if next.Header != "" {
		t.Fatal("account replacement retained cookies")
	}
}

func TestCookieAcquisitionValidationAndMissingState(t *testing.T) {
	now := time.Now()
	_, _, p := cookieFixture(t)
	for _, tc := range []struct{ name, model, state, missing, reason string }{
		{"valid", "model", strings.Repeat("s", 292), "ignore", ""},
		{"matching model cannot mask bad length", "model", "bad", "ignore", "state_length_mismatch"},
		{"matching length cannot mask wrong model", "wrong", strings.Repeat("s", 292), "ignore", "response_model_mismatch"},
		{"missing ignored", "model", "", "ignore", ""},
		{"missing rejected", "model", "", "reject", "missing_returned_state"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := cookieResult(now, "route")
			r.Model, r.State = tc.model, tc.state
			p.MissingReturnedState = tc.missing
			if got := ValidateAcquisition(p, "model", r, now); got != tc.reason {
				t.Fatalf("reason %s want %s", got, tc.reason)
			}
		})
	}
	p.Lengths = []int{}
	r := cookieResult(now, "route")
	r.State = ""
	p.MissingReturnedState = "reject"
	if got := ValidateAcquisition(p, "model", r, now); got != "" {
		t.Fatal(got)
	}
	p.Strategy = "state"
	if got := ValidateAcquisition(p, "model", r, now); got != "invalid_or_missing_state" {
		t.Fatal(got)
	}
}

func TestCookieIncompleteObservationAndIndependentFlags(t *testing.T) {
	m, c, p := cookieFixture(t)
	now := time.Now()
	installCookie(m, c, cookieResult(now, "route"), now)
	used, _, _ := m.PickCookie(c, "https://chatgpt.com/backend-api/codex/responses", now, p)
	headers := http.Header{"X-Codex-Turn-State": {"bad"}}
	if m.ObserveCookie(c, used, p, headers, "wrong", false) {
		t.Fatal("incomplete result invalidated cookie")
	}
	p.InvalidateOnModelMismatch = false
	p.InvalidateOnStateLengthMismatch = false
	if m.ObserveCookie(c, used, p, headers, "wrong", true) {
		t.Fatal("disabled flags invalidated cookie")
	}
	snap := m.CookieSnapshot(c.ID, now)
	if snap.Main == nil || snap.Observation != "response_model_mismatch" {
		t.Fatalf("missing observation %+v", snap)
	}
}

func TestCookieGroupDedupCancellationAndReplacement(t *testing.T) {
	m, c, p := cookieFixture(t)
	now := time.Now()
	installCookie(m, c, cookieResult(now, "old"), now)
	before := m.CookieSnapshot(c.ID, now).Main.Version
	m.CookieAction(c.ID, c.Model, "acquire")
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	probe := func(ctx context.Context, c Credential, p config.CodexStateOverrideConfig) (Result, error) {
		calls.Add(1)
		PublishCookieCandidate(ctx, cookieResult(now, "candidate").Cookies)
		close(started)
		<-release
		return cookieResult(now, "late"), nil
	}
	m.Tick(t.Context(), now, probe)
	<-started
	m.CookieAction(c.ID, "second", "acquire")
	m.Tick(t.Context(), now, probe)
	if calls.Load() != 1 {
		t.Fatal("duplicate credential acquisition")
	}
	if m.CookieSnapshot(c.ID, now).Candidate == nil {
		t.Fatal("candidate not shown")
	}
	// Credential refresh cancels old work, retaining the main cookie.
	c.Instance = "new-instance"
	m.Sync(p, []Credential{c})
	close(release)
	m.Wait()
	snap := m.CookieSnapshot(c.ID, now)
	if snap.Main == nil || snap.Main.Version != before || snap.Candidate != nil {
		t.Fatalf("stale work changed main %+v", snap)
	}
	m.Tick(t.Context(), now, func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) {
		return Result{}, errors.New("offline")
	})
	m.Wait()
	if snap = m.CookieSnapshot(c.ID, now); snap.Main == nil || snap.Main.Version != before {
		t.Fatal("failed candidate removed main")
	}
	p.Lengths = []int{12}
	m.Sync(p, []Credential{c})
	if m.CookieSnapshot(c.ID, now).Main != nil {
		t.Fatal("new validation silently inherited old sample")
	}
}

func TestCookieExpirationRefreshAndHide(t *testing.T) {
	m, c, p := cookieFixture(t)
	now := time.Now()
	p.MissingPolicy = "hide"
	p.CookieMaxAgeSeconds = 20
	p.CookieRefreshBeforeSeconds = 5
	m.Sync(p, []Credential{c})
	installCookie(m, c, cookieResult(now, "route"), now)
	end := m.Availability()[c.ID][c.Model]
	if !end.Equal(now.Add(20 * time.Second)) {
		t.Fatalf("hide expiry %v", end)
	}
	var calls atomic.Int32
	probe := func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) {
		calls.Add(1)
		return cookieResult(now.Add(16*time.Second), "new"), nil
	}
	m.Tick(t.Context(), now.Add(14*time.Second), probe)
	m.Wait()
	if calls.Load() != 0 {
		t.Fatal("refresh too early")
	}
	m.Tick(t.Context(), now.Add(16*time.Second), probe)
	m.Wait()
	if calls.Load() != 1 {
		t.Fatal("missing proactive refresh")
	}
	selected, _, _ := m.PickCookie(c, "https://chatgpt.com/backend-api/codex/responses", now.Add(37*time.Second), p)
	if selected.Header != "" {
		t.Fatal("expired cookie used")
	}
	m.CookieAction(c.ID, "", "clear")
	if !m.Availability()[c.ID][c.Model].IsZero() {
		t.Fatal("cleared cookie remains routable")
	}
}

func TestCookieAuxiliaryInsertionDeletionAndScope(t *testing.T) {
	m, c, p := cookieFixture(t)
	now := time.Now()
	r := cookieResult(now, "route")
	r.Cookies.Members = r.Cookies.Members[:1]
	installCookie(m, c, r, now)
	target := "https://chatgpt.com/backend-api/codex/responses"
	old, _, _ := m.PickCookie(c, target, now, p)
	m.ObserveCookie(c, old, p, http.Header{"Set-Cookie": {"__cf_bm=added; Path=/; Secure"}}, "", false)
	next, _, _ := m.PickCookie(c, target, now, p)
	if !strings.Contains(next.Header, "__cf_bm=added") {
		t.Fatal("auxiliary missing")
	}
	m.ObserveCookie(c, old, p, http.Header{"Set-Cookie": {"__cf_bm=stale; Path=/; Secure"}}, "", false)
	if !m.CookieSelectionValid(c, next, now, p) {
		t.Fatal("stale insertion overwrote auxiliary")
	}
	if !m.ObserveCookie(c, next, p, http.Header{"Set-Cookie": {"__oailb=; Path=/; Secure; Max-Age=0"}}, "", false) {
		t.Fatal("explicit route deletion ignored")
	}
	if m.CookieSnapshot(c.ID, now).Main != nil {
		t.Fatal("deleted route reused")
	}
	b := CaptureCookies(target, http.Header{"Set-Cookie": {"__oailb=route; Path=/backend-api; Secure; Max-Age=2", "auth=private; Path=/", "__cf_bm=aux; Domain=unrelated.invalid; Path=/"}}, now)
	if b.Select("https://chatgpt.com/other", now, p).Header != "" || b.Select(target, now.Add(3*time.Second), p).Header != "" {
		t.Fatal("path/declared expiry ignored")
	}
	if got := b.Select(target, now, p).Header; strings.Contains(got, "private") || strings.Contains(got, "aux") {
		t.Fatal("out-of-scope cookie accepted")
	}
}

func TestCookieMembersExpireIndependently(t *testing.T) {
	now := time.Now()
	p := config.CodexStateOverrideConfig{}
	b := CaptureCookies("https://chatgpt.com/responses", http.Header{"Set-Cookie": {"__oailb=first; Path=/; Max-Age=10", "__cflb=second; Path=/; Max-Age=30"}}, now)
	selection := b.Select(b.Origin, now.Add(15*time.Second), p)
	if selection.Header != "__cflb=second" || !selection.ExpiresAt.Equal(now.Add(30*time.Second)) {
		t.Fatalf("wrong remaining member selection: %+v", selection)
	}
	p.CookieRefreshBeforeSeconds = 2
	if !b.refreshDeadline(p, now).Equal(now.Add(8 * time.Second)) {
		t.Fatal("did not refresh before first expiring member")
	}
	b = CaptureCookies(b.Origin, http.Header{"Set-Cookie": {"__oailb=session; Path=/", "__cflb=second; Path=/; Max-Age=30", "__cf_bm=forged; Domain=com; Path=/"}}, now)
	if !b.expiry(config.CodexStateOverrideConfig{}).IsZero() || len(b.Members) != 2 {
		t.Fatal("session Cookie lifetime or domain validation changed")
	}
}

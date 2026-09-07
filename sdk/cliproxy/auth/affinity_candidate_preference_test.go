package auth

import (
	"testing"
	"time"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestAffinityCandidatePreferenceKeepsAvailabilityAndRetryBounds(t *testing.T) {
	for _, tc := range []struct {
		name, preferred, denied, want string
		attempt                       int
		disabled                      bool
	}{
		{name: "default priority", want: "high"},
		{name: "healthy low binding", preferred: "low", want: "low"},
		{name: "binding missing", preferred: "missing", want: "high"},
		{name: "binding capacity rejected", preferred: "low", denied: "low", want: "high"},
		{name: "binding disabled", preferred: "low", disabled: true, want: "high"},
		{name: "higher priority skipped by retry", preferred: "high", attempt: 1, want: "low"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			auths := []*Auth{
				{ID: "high", Provider: "fixture", Attributes: map[string]string{"priority": "10"}},
				{ID: "low", Provider: "fixture", Attributes: map[string]string{"priority": "0"}, Disabled: tc.disabled},
			}
			checks := make(map[string]int)
			got, err := selectAvailableAuthsForAttemptFilteredWithPriority(auths, "fixture", "model", time.Now(), tc.attempt, nil, func(auth *Auth) bool {
				checks[auth.ID]++
				return auth.ID != tc.denied
			}, true, tc.preferred)
			if err != nil || len(got) != 1 || got[0].ID != tc.want {
				t.Fatalf("selected = %v, error = %v, want %s", got, err, tc.want)
			}
			if checks["high"] != 1 || checks["low"] != 1 {
				t.Fatal("availability preference repeated capacity checks")
			}
		})
	}
}

func TestAffinityCandidatePreferenceKeepsTransportAndModelAvailability(t *testing.T) {
	auths := []*Auth{
		{ID: "http", Provider: "codex", Attributes: map[string]string{"priority": "0"}},
		{ID: "websocket", Provider: "codex", Attributes: map[string]string{"priority": "10", "websockets": "true"}},
	}
	ctx := core.WithDownstreamWebsocket(t.Context())
	got, err := getAvailableAuthsForContextWithPreference(ctx, auths, "codex", "model", time.Now(), 0, "http")
	if err != nil || len(got) != 1 || got[0].ID != "websocket" {
		t.Fatal("binding bypassed WebSocket eligibility")
	}
	now := time.Now()
	auths[0].ModelStates = map[string]*ModelState{"model": {Unavailable: true, NextRetryAfter: now.Add(time.Hour)}}
	got, err = getAvailableAuthsForContextWithPreference(t.Context(), auths, "codex", "model", now, 0, "http")
	if err != nil || len(got) != 1 || got[0].ID != "websocket" {
		t.Fatal("binding bypassed model cooldown")
	}
}

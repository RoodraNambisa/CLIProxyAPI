package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"
)

type antigravityUseCreditsContextKey struct{}

// WithAntigravityCredits returns a child context that signals the executor to
// inject enabledCreditTypes into the request payload.
func WithAntigravityCredits(ctx context.Context) context.Context {
	return context.WithValue(ctx, antigravityUseCreditsContextKey{}, true)
}

// AntigravityCreditsRequested reports whether the context carries the credits flag.
func AntigravityCreditsRequested(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(antigravityUseCreditsContextKey{}).(bool)
	return v
}

// AntigravityCreditsHint stores the latest known AI credits state for one auth.
type AntigravityCreditsHint struct {
	Known                  bool
	Available              bool
	CreditAmount           float64
	MinCreditAmount        float64
	PaidTierID             string
	CredentialKey          string
	UnavailableUntil       time.Time
	PermanentlyUnavailable bool
	UpdatedAt              time.Time
}

// AntigravityCreditsHintRefreshInterval controls when a cached credits hint becomes stale.
const AntigravityCreditsHintRefreshInterval = 10 * time.Minute

// IsFresh reports whether the hint can still be used for routing decisions.
func (h AntigravityCreditsHint) IsFresh(now time.Time) bool {
	return h.Known && !h.UpdatedAt.IsZero() && now.Sub(h.UpdatedAt) < AntigravityCreditsHintRefreshInterval
}

// BlocksRouting reports whether a negative hint is still authoritative for routing.
func (h AntigravityCreditsHint) BlocksRouting(now time.Time) bool {
	return h.Known && !h.Available && (h.PermanentlyUnavailable || h.UnavailableUntil.After(now))
}

// MatchesAuth reports whether a hint belongs to the current credential generation.
func (h AntigravityCreditsHint) MatchesAuth(auth *Auth) bool {
	return h.CredentialKey != "" && auth != nil && h.CredentialKey == AntigravityCreditsCredentialKey(auth)
}

// AntigravityCreditsCredentialKey returns an in-memory identity for credits state.
func AntigravityCreditsCredentialKey(auth *Auth) string {
	if auth == nil {
		return ""
	}
	var accessToken, refreshToken string
	if auth.Metadata != nil {
		if value := auth.Metadata["access_token"]; value != nil {
			accessToken = strings.TrimSpace(fmt.Sprint(value))
		}
		if value := auth.Metadata["refresh_token"]; value != nil {
			refreshToken = strings.TrimSpace(fmt.Sprint(value))
		}
	}
	sum := sha256.Sum256([]byte(accessToken + "\x00" + refreshToken))
	return auth.RuntimeInstanceID() + ":" + hex.EncodeToString(sum[:8])
}

var antigravityCreditsHintByAuth sync.Map

// SetAntigravityCreditsHint updates the latest known AI credits state for an auth.
func SetAntigravityCreditsHint(authID string, hint AntigravityCreditsHint) {
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return
	}
	if hint.UpdatedAt.IsZero() {
		hint.UpdatedAt = time.Now()
	}
	antigravityCreditsHintByAuth.Store(authID, hint)
}

// GetAntigravityCreditsHint returns the latest known AI credits state for an auth.
func GetAntigravityCreditsHint(authID string) (AntigravityCreditsHint, bool) {
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return AntigravityCreditsHint{}, false
	}
	value, ok := antigravityCreditsHintByAuth.Load(authID)
	if !ok {
		return AntigravityCreditsHint{}, false
	}
	hint, ok := value.(AntigravityCreditsHint)
	if !ok {
		antigravityCreditsHintByAuth.Delete(authID)
		return AntigravityCreditsHint{}, false
	}
	return hint, true
}

// HasKnownAntigravityCreditsHint reports whether credits state has been discovered for an auth.
func HasKnownAntigravityCreditsHint(authID string) bool {
	hint, ok := GetAntigravityCreditsHint(authID)
	return ok && hint.Known
}

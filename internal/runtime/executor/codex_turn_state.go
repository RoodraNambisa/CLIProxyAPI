package executor

import (
	"crypto/sha256"
	"net/http"
	"strings"
	"sync"
	"time"

	codexauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

const (
	codexTurnStateHeader      = "X-Codex-Turn-State"
	codexTurnStateOriginTTL   = time.Hour
	codexTurnStateOriginLimit = 4096
)

type codexTurnStateOrigin struct {
	owner     string
	expiresAt time.Time
	updatedAt time.Time
}

type codexTurnStateOriginTracker struct {
	mu      sync.Mutex
	origins map[[sha256.Size]byte]codexTurnStateOrigin
	limit   int
	ttl     time.Duration
}

var globalCodexTurnStateOrigins = newCodexTurnStateOriginTracker(codexTurnStateOriginLimit, codexTurnStateOriginTTL)

func newCodexTurnStateOriginTracker(limit int, ttl time.Duration) *codexTurnStateOriginTracker {
	if limit < 1 {
		limit = 1
	}
	if ttl <= 0 {
		ttl = codexTurnStateOriginTTL
	}
	return &codexTurnStateOriginTracker{
		origins: make(map[[sha256.Size]byte]codexTurnStateOrigin),
		limit:   limit,
		ttl:     ttl,
	}
}

func guardCodexTurnStateHeader(auth *cliproxyauth.Auth, headers http.Header) {
	owner := codexTurnStateOwner(auth)
	if owner == "" || headers == nil {
		return
	}
	globalCodexTurnStateOrigins.guard(owner, headers, time.Now())
}

func ensureCodexTurnStateHeader(target, source http.Header) {
	if target == nil || strings.TrimSpace(target.Get(codexTurnStateHeader)) != "" || source == nil {
		return
	}
	if state := strings.TrimSpace(source.Get(codexTurnStateHeader)); state != "" {
		setHeaderCasePreserved(target, codexTurnStateHeader, state)
	}
}

func codexSuccessfulResponseHeaders(auth *cliproxyauth.Auth, headers http.Header) http.Header {
	if headers == nil {
		return nil
	}
	cloned := headers.Clone()
	owner := codexTurnStateOwner(auth)
	if owner != "" {
		globalCodexTurnStateOrigins.note(owner, cloned.Get(codexTurnStateHeader), time.Now())
	}
	return cloned
}

func codexTurnStateOwner(auth *cliproxyauth.Auth) string {
	if auth == nil || codexAuthUsesAPIKey(auth) {
		return ""
	}
	if accountID := strings.TrimSpace(codexauth.EffectiveRequestAccountID(auth.Metadata)); accountID != "" {
		return "account:" + accountID
	}
	if authID := strings.TrimSpace(auth.ID); authID != "" {
		return "auth:" + authID
	}
	return ""
}

func (t *codexTurnStateOriginTracker) note(owner, state string, now time.Time) {
	owner = strings.TrimSpace(owner)
	state = strings.TrimSpace(state)
	if t == nil || owner == "" || state == "" {
		return
	}
	digest := sha256.Sum256([]byte(state))
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.origins[digest]; !exists && len(t.origins) >= t.limit {
		t.evictOneLocked(now)
	}
	t.origins[digest] = codexTurnStateOrigin{
		owner: owner, expiresAt: now.Add(t.ttl), updatedAt: now,
	}
}

func (t *codexTurnStateOriginTracker) guard(owner string, headers http.Header, now time.Time) bool {
	owner = strings.TrimSpace(owner)
	state := strings.TrimSpace(headers.Get(codexTurnStateHeader))
	if t == nil || owner == "" || state == "" {
		return false
	}
	digest := sha256.Sum256([]byte(state))
	t.mu.Lock()
	origin, exists := t.origins[digest]
	if exists && !origin.expiresAt.IsZero() && !now.Before(origin.expiresAt) {
		delete(t.origins, digest)
		exists = false
	}
	t.mu.Unlock()
	if !exists || origin.owner == owner {
		return false
	}
	deleteHeaderCaseInsensitive(headers, codexTurnStateHeader)
	return true
}

func (t *codexTurnStateOriginTracker) evictOneLocked(now time.Time) {
	var oldestKey [sha256.Size]byte
	var oldestTime time.Time
	haveOldest := false
	for key, origin := range t.origins {
		if !origin.expiresAt.IsZero() && !now.Before(origin.expiresAt) {
			delete(t.origins, key)
			continue
		}
		if !haveOldest || origin.updatedAt.Before(oldestTime) {
			oldestKey = key
			oldestTime = origin.updatedAt
			haveOldest = true
		}
	}
	if len(t.origins) >= t.limit && haveOldest {
		delete(t.origins, oldestKey)
	}
}

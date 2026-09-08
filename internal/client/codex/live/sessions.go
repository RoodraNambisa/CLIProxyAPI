package live

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

const (
	callSessionLifetime = time.Hour
	maxStoredCalls      = 1024
)

var callIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

var errCallIDConflict = errors.New("realtime call ID is already registered")

type callOwner [sha256.Size]byte

func requestCallOwner(c *gin.Context) (callOwner, bool) {
	if c == nil || strings.TrimSpace(c.GetString("apiKey")) == "" {
		return callOwner{}, false
	}
	if grant, temporary := clientSecretAuthorization(c); temporary {
		return grant.owner, grant.owner != (callOwner{})
	}
	// Store only an ownership digest, never a caller's key or the Gin context.
	identity, _ := json.Marshal([]string{"codex-live-owner-v1", c.GetString("accessProvider"), c.GetString("apiKey")})
	return sha256.Sum256(identity), true
}

type liveCall struct {
	id              string
	owner           callOwner
	lease           *auth.CodexLiveLease
	secretPrincipal string
	onClose         func()
	once            sync.Once
}

func (s *liveCall) close() {
	s.once.Do(func() {
		if s.onClose != nil {
			s.onClose()
		}
		s.lease.Close()
	})
}

type callEntry struct {
	call    *liveCall
	claimed bool
	expiry  uint64
	timer   *time.Timer
	stop    func() bool
}

// callStore never replaces a call with the same ID. A timer or retirement
// callback may remove only its exact entry, not a later call reusing the ID.
type callStore struct {
	mu       sync.Mutex
	entries  map[string]*callEntry
	closed   bool
	lifetime time.Duration
	capacity int
}

func newCallStore() *callStore {
	return &callStore{entries: make(map[string]*callEntry), lifetime: callSessionLifetime, capacity: maxStoredCalls}
}

func (s *callStore) put(call *liveCall) error {
	if call == nil || !callIDPattern.MatchString(call.id) || call.owner == (callOwner{}) || call.lease == nil {
		return errors.New("invalid realtime call ownership")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return context.Canceled
	}
	if errCtx := context.Cause(call.lease.Context()); errCtx != nil {
		return errCtx
	}
	if s.entries[call.id] != nil {
		return errCallIDConflict
	}
	if len(s.entries) >= s.capacity {
		return errors.New("realtime call registry is at capacity")
	}
	entry := &callEntry{call: call}
	s.entries[call.id] = entry
	s.scheduleExpiry(entry)
	entry.stop = context.AfterFunc(call.lease.Context(), func() { s.remove(call) })
	return nil
}

// find and claim authenticate ownership before revealing busy state. Hangup
// uses find so it remains possible while a sideband connection owns the claim.
func (s *callStore) find(id string, owner callOwner) *liveCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.entries[id]
	if entry == nil || entry.call.owner != owner || entry.call.lease.Context().Err() != nil {
		return nil
	}
	return entry.call
}

func (s *callStore) claim(id string, owner callOwner) (*liveCall, bool) {
	return s.claimForGrant(id, owner, "")
}

func (s *callStore) claimForGrant(id string, owner callOwner, secretPrincipal string) (*liveCall, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.entries[id]
	if entry == nil || entry.call.owner != owner || (secretPrincipal != "" && entry.call.secretPrincipal != secretPrincipal) || entry.call.lease.Context().Err() != nil {
		return nil, false
	}
	if entry.claimed {
		return nil, true
	}
	entry.claimed = true
	entry.expiry++
	entry.timer.Stop()
	return entry.call, false
}

func (s *callStore) release(call *liveCall) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.entries[call.id]
	if entry != nil && entry.call == call && entry.claimed {
		entry.claimed = false
		s.scheduleExpiry(entry)
	}
}

func (s *callStore) scheduleExpiry(entry *callEntry) {
	entry.expiry++
	version := entry.expiry
	entry.timer = time.AfterFunc(s.lifetime, func() { s.expire(entry, version) })
}

func (s *callStore) expire(entry *callEntry, version uint64) {
	s.mu.Lock()
	if s.entries[entry.call.id] != entry || entry.claimed || entry.expiry != version {
		s.mu.Unlock()
		return
	}
	delete(s.entries, entry.call.id)
	s.mu.Unlock()
	closeCallEntry(entry)
}

func (s *callStore) remove(call *liveCall) {
	if s.detach(call) {
		call.close()
	}
}

// detach returns cleanup ownership to the caller, for example when delivery
// failed and an upstream hangup must complete before closing the lease.
func (s *callStore) detach(call *liveCall) bool {
	s.mu.Lock()
	entry := s.entries[call.id]
	if entry == nil || entry.call != call {
		s.mu.Unlock()
		return false
	}
	delete(s.entries, call.id)
	s.mu.Unlock()
	entry.timer.Stop()
	entry.stop()
	return true
}

func (s *callStore) close() {
	s.mu.Lock()
	s.closed = true
	entries := s.entries
	s.entries = make(map[string]*callEntry)
	s.mu.Unlock()
	for _, entry := range entries {
		closeCallEntry(entry)
	}
}

func closeCallEntry(entry *callEntry) {
	entry.timer.Stop()
	entry.stop()
	entry.call.close()
}

func callIDFromLocation(location string) string {
	location = strings.TrimSpace(location)
	if callIDPattern.MatchString(location) {
		return location
	}
	parsed, errURL := url.Parse(location)
	if errURL != nil {
		return ""
	}
	if id := strings.TrimSpace(parsed.Query().Get("call_id")); callIDPattern.MatchString(id) {
		return id
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) < 2 || (parts[len(parts)-2] != "live" && parts[len(parts)-2] != "calls") {
		return ""
	}
	id := parts[len(parts)-1]
	if !callIDPattern.MatchString(id) {
		return ""
	}
	return id
}

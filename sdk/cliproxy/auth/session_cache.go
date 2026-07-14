package auth

import (
	"sync"
	"time"
)

// sessionEntry stores auth binding with expiration.
type sessionEntry struct {
	authID         string
	authGeneration uint64
	expiresAt      time.Time
	version        uint64
}

type sessionMutation struct {
	version     uint64
	previous    sessionEntry
	hadPrevious bool
}

type authGenerationState struct {
	generation uint64
	expiresAt  time.Time
}

// SessionCache provides TTL-based session to auth mapping with automatic cleanup.
type SessionCache struct {
	mu              sync.RWMutex
	entries         map[string]sessionEntry
	ttl             time.Duration
	stopCh          chan struct{}
	version         uint64
	authGenerations map[string]authGenerationState
}

// NewSessionCache creates a cache with the specified TTL.
// A background goroutine periodically cleans expired entries.
func NewSessionCache(ttl time.Duration) *SessionCache {
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}
	c := &SessionCache{
		entries:         make(map[string]sessionEntry),
		authGenerations: make(map[string]authGenerationState),
		ttl:             ttl,
		stopCh:          make(chan struct{}),
	}
	go c.cleanupLoop()
	return c
}

// Get retrieves the auth ID bound to a session, if still valid.
// Does NOT refresh the TTL on access.
func (c *SessionCache) Get(sessionID string) (string, bool) {
	if sessionID == "" {
		return "", false
	}
	c.mu.Lock()
	entry, ok := c.entries[sessionID]
	if !ok {
		c.mu.Unlock()
		return "", false
	}
	if time.Now().After(entry.expiresAt) || entry.authGeneration != c.authGenerations[entry.authID].generation {
		delete(c.entries, sessionID)
		c.mu.Unlock()
		return "", false
	}
	c.mu.Unlock()
	return entry.authID, true
}

// GetAndRefresh retrieves the auth ID bound to a session and refreshes TTL on hit.
// This extends the binding lifetime for active sessions.
func (c *SessionCache) GetAndRefresh(sessionID string) (string, bool) {
	if sessionID == "" {
		return "", false
	}
	now := time.Now()
	c.mu.Lock()
	entry, ok := c.entries[sessionID]
	if !ok {
		c.mu.Unlock()
		return "", false
	}
	if now.After(entry.expiresAt) || entry.authGeneration != c.authGenerations[entry.authID].generation {
		delete(c.entries, sessionID)
		c.mu.Unlock()
		return "", false
	}
	// Refresh TTL on successful access
	entry.expiresAt = now.Add(c.ttl)
	if state := c.authGenerations[entry.authID]; state.generation > 0 && entry.expiresAt.After(state.expiresAt) {
		state.expiresAt = entry.expiresAt
		c.authGenerations[entry.authID] = state
	}
	c.entries[sessionID] = entry
	c.mu.Unlock()
	return entry.authID, true
}

// Set binds a session to an auth ID with TTL refresh.
func (c *SessionCache) Set(sessionID, authID string) {
	c.setWithVersion(sessionID, authID)
}

func (c *SessionCache) setWithVersion(sessionID, authID string) uint64 {
	return c.setWithRollback(sessionID, authID).version
}

func (c *SessionCache) setWithRollback(sessionID, authID string) sessionMutation {
	if sessionID == "" || authID == "" {
		return sessionMutation{}
	}
	c.mu.Lock()
	previous, hadPrevious := c.entries[sessionID]
	c.version++
	version := c.version
	expiresAt := time.Now().Add(c.ttl)
	generationState := c.authGenerations[authID]
	if generationState.generation > 0 && expiresAt.After(generationState.expiresAt) {
		generationState.expiresAt = expiresAt
		c.authGenerations[authID] = generationState
	}
	c.entries[sessionID] = sessionEntry{
		authID:         authID,
		authGeneration: generationState.generation,
		expiresAt:      expiresAt,
		version:        version,
	}
	c.mu.Unlock()
	return sessionMutation{version: version, previous: previous, hadPrevious: hadPrevious}
}

func (c *SessionCache) invalidateIfVersion(sessionID string, version uint64) {
	if sessionID == "" || version == 0 {
		return
	}
	c.mu.Lock()
	if entry, ok := c.entries[sessionID]; ok && entry.version == version {
		delete(c.entries, sessionID)
	}
	c.mu.Unlock()
}

func (c *SessionCache) rollback(sessionID string, mutation sessionMutation) {
	if sessionID == "" || mutation.version == 0 {
		return
	}
	c.mu.Lock()
	if entry, ok := c.entries[sessionID]; ok && entry.version == mutation.version {
		if mutation.hadPrevious && time.Now().Before(mutation.previous.expiresAt) && mutation.previous.authGeneration == c.authGenerations[mutation.previous.authID].generation {
			c.entries[sessionID] = mutation.previous
		} else {
			delete(c.entries, sessionID)
		}
	}
	c.mu.Unlock()
}

// Invalidate removes a specific session binding.
func (c *SessionCache) Invalidate(sessionID string) {
	if sessionID == "" {
		return
	}
	c.mu.Lock()
	delete(c.entries, sessionID)
	c.mu.Unlock()
}

// InvalidateAuth removes all sessions bound to a specific auth ID.
// Used when an auth becomes unavailable.
func (c *SessionCache) InvalidateAuth(authID string) {
	if authID == "" {
		return
	}
	c.mu.Lock()
	state := c.authGenerations[authID]
	state.generation++
	state.expiresAt = time.Now().Add(c.ttl)
	c.authGenerations[authID] = state
	for sid, entry := range c.entries {
		if entry.authID == authID {
			delete(c.entries, sid)
		}
	}
	c.mu.Unlock()
}

// Stop terminates the background cleanup goroutine.
func (c *SessionCache) Stop() {
	select {
	case <-c.stopCh:
	default:
		close(c.stopCh)
	}
}

func (c *SessionCache) cleanupLoop() {
	ticker := time.NewTicker(c.ttl / 2)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.cleanup()
		}
	}
}

func (c *SessionCache) cleanup() {
	now := time.Now()
	c.mu.Lock()
	for sid, entry := range c.entries {
		if now.After(entry.expiresAt) || entry.authGeneration != c.authGenerations[entry.authID].generation {
			delete(c.entries, sid)
		}
	}
	for authID, state := range c.authGenerations {
		if now.After(state.expiresAt) {
			delete(c.authGenerations, authID)
		}
	}
	c.mu.Unlock()
}

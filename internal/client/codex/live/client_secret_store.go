package live

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

const (
	clientSecretPrefix              = "ek_"
	clientSecretDefaultLifetime     = 10 * time.Minute
	clientSecretMinimumLifetime     = 10 * time.Second
	clientSecretMaximumLifetime     = 2 * time.Hour
	clientSecretMaxBodySize         = 64 << 10
	clientSecretMaxEntries          = 1024
	clientSecretMaxEntriesPerIssuer = 64
)

var (
	errInvalidClientSecret   = errors.New("realtime client secret is invalid or expired")
	errClientSecretCapacity  = errors.New("realtime client secret capacity exhausted")
	errClientSecretCollision = errors.New("realtime client secret identifier collision")
)

// ClientSecretAuthorization is an opaque local capability. It contains no
// upstream credential or original caller key, and every returned body is owned.
type ClientSecretAuthorization struct {
	digest    [sha256.Size]byte
	principal string
	owner     callOwner
	model     string
	session   json.RawMessage
	expiresAt time.Time
}

func (a ClientSecretAuthorization) clone() ClientSecretAuthorization {
	a.session = append(json.RawMessage(nil), a.session...)
	return a
}

// prepareClientSecret performs entropy acquisition outside admission/store
// locks. Publication must later recheck both admission and expiration.
func prepareClientSecret(random io.Reader, now time.Time, session json.RawMessage, model string, owner callOwner, lifetime time.Duration) (string, ClientSecretAuthorization, error) {
	if random == nil || owner == (callOwner{}) || strings.TrimSpace(model) == "" || lifetime < clientSecretMinimumLifetime || lifetime > clientSecretMaximumLifetime || len(session) > clientSecretMaxBodySize {
		return "", ClientSecretAuthorization{}, errors.New("invalid realtime client secret scope")
	}
	if _, errObject := callJSONObject(session, "session"); errObject != nil {
		return "", ClientSecretAuthorization{}, errObject
	}
	entropy := make([]byte, 50)
	if _, errRead := io.ReadFull(random, entropy); errRead != nil {
		return "", ClientSecretAuthorization{}, fmt.Errorf("generate realtime client secret: %w", errRead)
	}
	token := clientSecretPrefix + base64.RawURLEncoding.EncodeToString(entropy[:32])
	a := ClientSecretAuthorization{principal: "sess_" + base64.RawURLEncoding.EncodeToString(entropy[32:]), owner: owner, model: model, session: append(json.RawMessage(nil), session...), expiresAt: now.Add(lifetime)}
	return token, a, nil
}

type clientSecretStore struct {
	mu      sync.Mutex
	entries map[[sha256.Size]byte]ClientSecretAuthorization
	closed  bool
	now     func() time.Time
}

func newClientSecretStore() *clientSecretStore {
	return &clientSecretStore{entries: make(map[[sha256.Size]byte]ClientSecretAuthorization), now: time.Now}
}

func (s *clientSecretStore) put(token string, authorization ClientSecretAuthorization) error {
	if !validClientSecretToken(token) || authorization.owner == (callOwner{}) || authorization.principal == "" {
		return errInvalidClientSecret
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if s.closed || !authorization.expiresAt.After(now) {
		return errInvalidClientSecret
	}
	issuerEntries := 0
	for hash, entry := range s.entries {
		if !entry.expiresAt.After(now) {
			delete(s.entries, hash)
			continue
		}
		if entry.principal == authorization.principal {
			return errClientSecretCollision
		}
		if entry.owner == authorization.owner {
			issuerEntries++
		}
	}
	hash := sha256.Sum256([]byte(token))
	if _, exists := s.entries[hash]; exists {
		return errClientSecretCollision
	}
	if len(s.entries) >= clientSecretMaxEntries || issuerEntries >= clientSecretMaxEntriesPerIssuer {
		return errClientSecretCapacity
	}
	authorization.digest = hash
	s.entries[hash] = authorization.clone()
	return nil
}

func (s *clientSecretStore) authenticate(token string) (ClientSecretAuthorization, error) {
	if !validClientSecretToken(token) {
		return ClientSecretAuthorization{}, errInvalidClientSecret
	}
	hash := sha256.Sum256([]byte(token))
	s.mu.Lock()
	defer s.mu.Unlock()
	a, exists := s.entries[hash]
	if s.closed || !exists || !a.expiresAt.After(s.now()) {
		delete(s.entries, hash)
		return ClientSecretAuthorization{}, errInvalidClientSecret
	}
	return a.clone(), nil
}

func (s *clientSecretStore) remove(token, principal string) {
	hash := sha256.Sum256([]byte(token))
	s.mu.Lock()
	defer s.mu.Unlock()
	if a, exists := s.entries[hash]; exists && a.principal == principal {
		delete(s.entries, hash)
	}
}

// valid rechecks the exact grant before a new connection is committed. A
// copied authorization cannot survive expiration, revocation or replacement.
func (s *clientSecretStore) valid(a ClientSecretAuthorization) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.entries[a.digest]
	return !s.closed && exists && current.principal == a.principal && current.owner == a.owner && current.model == a.model && current.expiresAt.Equal(a.expiresAt) && current.expiresAt.After(s.now())
}

func (s *clientSecretStore) close() {
	s.mu.Lock()
	s.closed = true
	clear(s.entries)
	s.mu.Unlock()
}

func validClientSecretToken(token string) bool {
	if !strings.HasPrefix(token, clientSecretPrefix) || len(token) != len(clientSecretPrefix)+43 {
		return false
	}
	raw, errDecode := base64.RawURLEncoding.Strict().DecodeString(strings.TrimPrefix(token, clientSecretPrefix))
	return errDecode == nil && len(raw) == 32
}

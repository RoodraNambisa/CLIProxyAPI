// Package codexcookie implements passive, credential-scoped infrastructure cookies.
package codexcookie

import (
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/publicsuffix"
)

// Default is shared by the Codex HTTP and WebSocket transports, never by credentials.
var Default = NewManager()

type Manager struct {
	mu      sync.Mutex
	enabled bool
	owners  map[string]string
	stores  map[string]*Store
}

func NewManager() *Manager { return &Manager{enabled: true, stores: make(map[string]*Store)} }

// Sync retires stores on shutdown, configuration changes, removal or account replacement.
func (m *Manager) Sync(enabled bool, owners map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.enabled = enabled
	m.owners = make(map[string]string, len(owners))
	for id, owner := range owners {
		m.owners[id] = owner
	}
	for id, store := range m.stores {
		if !enabled || owners[id] != store.owner {
			store.retire()
			delete(m.stores, id)
		}
	}
}

func (m *Manager) Forget(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.owners, id)
	if store := m.stores[id]; store != nil {
		store.retire()
		delete(m.stores, id)
	}
}

// IdentityChanged preserves ordinary token refreshes and proxy changes.
func (m *Manager) IdentityChanged(id, owner string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.owners != nil {
		m.owners[id] = owner
	}
	if store := m.stores[id]; store != nil && store.owner != owner {
		store.retire()
		delete(m.stores, id)
	}
}

func (m *Manager) Acquire(id, owner string) *Store {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.enabled || id == "" || owner == "" || m.owners != nil && m.owners[id] != owner {
		return nil
	}
	store := m.stores[id]
	if store != nil && store.owner != owner {
		store.retire()
		store = nil
	}
	if store == nil {
		store = newStore(owner, time.Now)
		m.stores[id] = store
	}
	return store
}

type member struct {
	origin *url.URL
	cookie http.Cookie
	order  uint64
}

type Store struct {
	mu       sync.Mutex
	owner    string
	retired  bool
	now      func() time.Time
	members  map[string]member
	sequence uint64
}

func newStore(owner string, now func() time.Time) *Store {
	return &Store{owner: owner, now: now, members: make(map[string]member)}
}

func (s *Store) retire() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retired = true
	clear(s.members)
}

// URL follows the official HTTPS/WSS host allowlist.
func URL(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		return nil
	}
	if u.Scheme == "wss" {
		u.Scheme = "https"
	}
	if u.Scheme != "https" {
		return nil
	}
	host := strings.ToLower(u.Hostname())
	if host != "chatgpt.com" && host != "chatgpt-staging.com" && host != "chat.openai.com" && !strings.HasSuffix(host, ".chatgpt.com") && !strings.HasSuffix(host, ".chatgpt-staging.com") {
		return nil
	}
	return u
}

func AllowedName(name string) bool {
	switch name {
	case "__cf_bm", "__cflb", "__cfruid", "__cfseq", "__cfwaitingroom", "__oailb", "_cfuvid", "cf_clearance", "cf_ob_info", "cf_use_ob":
		return true
	default:
		return strings.HasPrefix(name, "cf_chl_")
	}
}

// StoreResponse applies each Set-Cookie in wire order. There is deliberately no
// comparison with the cookies sent by this request: the last writer wins.
func (s *Store) StoreResponse(rawURL string, headers http.Header) {
	if s == nil {
		return
	}
	u := URL(rawURL)
	if u == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retired {
		return
	}
	now := s.now()
	for _, c := range (&http.Response{Header: headers}).Cookies() {
		if !AllowedName(c.Name) || c.Valid() != nil {
			continue
		}
		scope := *c
		scope.Path, scope.Secure, scope.Expires, scope.MaxAge = "/", false, time.Time{}, 0
		jar, _ := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
		jar.SetCookies(u, []*http.Cookie{&scope})
		if len(jar.Cookies(u)) == 0 {
			continue
		}
		if c.Path == "" || c.Path[0] != '/' {
			c.Path = "/"
			if i := strings.LastIndex(u.Path, "/"); i > 0 {
				c.Path = u.Path[:i]
			}
		}
		c.Domain = strings.ToLower(strings.TrimPrefix(c.Domain, "."))
		domain := c.Domain
		if domain == "" {
			domain = strings.ToLower(u.Hostname())
		}
		key := c.Name + "\x00" + domain + "\x00" + c.Path
		if c.MaxAge > 0 {
			if int64(c.MaxAge) > int64((1<<63-1)/int64(time.Second)) {
				c.Expires = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
			} else {
				c.Expires = now.Add(time.Duration(c.MaxAge) * time.Second)
			}
		}
		if c.MaxAge < 0 || !c.Expires.IsZero() && !now.Before(c.Expires) {
			delete(s.members, key)
			continue
		}
		c.MaxAge = 0
		s.sequence++
		s.members[key] = member{origin: u, cookie: *c, order: s.sequence}
	}
}

// Header evaluates expiry immediately before each transport attempt. The jar is
// used for RFC domain/path/host-only selection; our clock controls expiration.
func (s *Store) Header(rawURL string) string {
	if s == nil {
		return ""
	}
	u := URL(rawURL)
	if u == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retired {
		return ""
	}
	now := s.now()
	items := make([]member, 0, len(s.members))
	for key, item := range s.members {
		if !item.cookie.Expires.IsZero() && !now.Before(item.cookie.Expires) {
			delete(s.members, key)
			continue
		}
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].order < items[j].order })
	jar, _ := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
	for _, item := range items {
		c := item.cookie
		c.Expires = time.Time{}
		jar.SetCookies(item.origin, []*http.Cookie{&c})
	}
	req := &http.Request{Header: make(http.Header)}
	for _, c := range jar.Cookies(u) {
		req.AddCookie(c)
	}
	return req.Header.Get("Cookie")
}

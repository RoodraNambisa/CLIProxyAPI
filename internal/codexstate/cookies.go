package codexstate

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"golang.org/x/net/publicsuffix"
)

// CookieBundle owns a frozen acquisition sample. Values are never serialized.
type CookieBundle struct {
	Origin     string         `json:"-"`
	ReceivedAt time.Time      `json:"-"`
	Members    []CookieMember `json:"-"`
}
type CookieMember struct {
	Origin  string      `json:"-"`
	Cookie  http.Cookie `json:"-"`
	Version uint64      `json:"-"`
}
type CookieSelection struct {
	Header    string `json:"-"`
	Version   uint64
	Members   map[string]uint64 `json:"-"`
	URL       string            `json:"-"`
	ExpiresAt time.Time
}
type CookieMemberSnapshot struct {
	Version   uint64    `json:"version"`
	Name      string    `json:"name"`
	Digest    string    `json:"digest"`
	Domain    string    `json:"domain,omitempty"`
	Path      string    `json:"path"`
	Secure    bool      `json:"secure"`
	ExpiresAt time.Time `json:"expires_at,omitzero"`
}
type CookieBundleSnapshot struct {
	Version        uint64                 `json:"version"`
	Digest         string                 `json:"digest"`
	ReceivedAt     time.Time              `json:"received_at"`
	AgeSeconds     int64                  `json:"age_seconds"`
	ExpiresAt      time.Time              `json:"expires_at,omitzero"`
	LocalExpiresAt time.Time              `json:"local_expires_at,omitzero"`
	Members        []CookieMemberSnapshot `json:"members"`
}
type CookieSnapshot struct {
	Snapshot
	Main        *CookieBundleSnapshot `json:"main,omitempty"`
	Candidate   *CookieBundleSnapshot `json:"candidate,omitempty"`
	Observation string                `json:"observation,omitempty"`
}

func RouteCookieName(name string) bool   { return name == "__oailb" || name == "__cflb" }
func ManagedCookieName(name string) bool { return RouteCookieName(name) || name == "__cf_bm" }
func cookieDigest(value string) string {
	d := sha256.Sum256([]byte(value))
	return hex.EncodeToString(d[:8])
}
func cookieURL(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return nil
	}
	if u.Scheme == "wss" {
		u.Scheme = "https"
	}
	if u.Scheme == "ws" {
		u.Scheme = "http"
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return nil
	}
	return u
}

func CaptureCookies(rawURL string, headers http.Header, now time.Time) *CookieBundle {
	u := cookieURL(rawURL)
	if u == nil {
		return nil
	}
	b := &CookieBundle{Origin: u.String(), ReceivedAt: now}
	response := &http.Response{Header: headers}
	for _, c := range response.Cookies() {
		if !ManagedCookieName(c.Name) || len(c.Value) > 8192 || c.Valid() != nil {
			continue
		}
		// Reject foreign/public-suffix domains before a member enters the pool.
		// Path and Secure still participate in each actual selection below.
		scope := *c
		scope.Path, scope.Secure, scope.Expires, scope.MaxAge = "/", false, time.Time{}, 0
		validationJar, _ := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
		validationJar.SetCookies(u, []*http.Cookie{&scope})
		if len(validationJar.Cookies(u)) == 0 {
			continue
		}
		// Normalize Max-Age at receipt, never when selecting a later request.
		if c.MaxAge > 0 {
			if c.MaxAge > 365*24*3600 {
				c.MaxAge = 365 * 24 * 3600
			}
			c.Expires = now.Add(time.Duration(c.MaxAge) * time.Second)
			c.MaxAge = 0
		}
		if c.Path == "" || c.Path[0] != '/' {
			c.Path = "/"
			if i := strings.LastIndex(u.Path, "/"); i > 0 {
				c.Path = u.Path[:i]
			}
		}
		if c.Domain != "" {
			c.Domain = strings.ToLower(strings.TrimPrefix(c.Domain, "."))
		}
		member := CookieMember{Cookie: *c, Version: 1, Origin: u.String()}
		replaced := false
		for i, current := range b.Members {
			if current.key() == member.key() {
				b.Members[i] = member
				replaced = true
				break
			}
		}
		if !replaced {
			b.Members = append(b.Members, member)
		}
	}
	if len(b.Members) == 0 {
		return nil
	}
	return b
}
func cloneCookieBundle(b *CookieBundle) *CookieBundle {
	if b == nil {
		return nil
	}
	copy := *b
	copy.Members = append([]CookieMember(nil), b.Members...)
	return &copy
}
func (b *CookieBundle) localExpiry(p config.CodexStateOverrideConfig) time.Time {
	if b == nil || p.CookieMaxAgeSeconds <= 0 {
		return time.Time{}
	}
	return b.ReceivedAt.Add(time.Duration(p.CookieMaxAgeSeconds) * time.Second)
}

// A group remains available while at least one route Cookie remains valid.
func (b *CookieBundle) expiry(p config.CodexStateOverrideConfig) time.Time {
	if b == nil {
		return time.Time{}
	}
	var declared time.Time
	for _, member := range b.Members {
		c := member.Cookie
		if !RouteCookieName(c.Name) || c.MaxAge < 0 {
			continue
		}
		if c.Expires.IsZero() {
			return b.localExpiry(p)
		}
		if c.Expires.After(declared) {
			declared = c.Expires
		}
	}
	local := b.localExpiry(p)
	if !local.IsZero() && (declared.IsZero() || local.Before(declared)) {
		return local
	}
	return declared
}

func (b *CookieBundle) refreshDeadline(p config.CodexStateOverrideConfig, now time.Time) time.Time {
	if b == nil || p.CookieRefreshBeforeSeconds <= 0 {
		return b.expiry(p)
	}
	end := b.localExpiry(p)
	for _, m := range b.Members {
		c := m.Cookie
		if RouteCookieName(c.Name) && now.Before(c.Expires) && (end.IsZero() || c.Expires.Before(end)) {
			end = c.Expires
		}
	}
	if !end.IsZero() {
		end = end.Add(-time.Duration(p.CookieRefreshBeforeSeconds) * time.Second)
	}
	return end
}
func (b *CookieBundle) Select(rawURL string, now time.Time, p config.CodexStateOverrideConfig) CookieSelection {
	selection := CookieSelection{URL: rawURL, Members: map[string]uint64{}}
	if b == nil {
		return selection
	}
	if end := b.localExpiry(p); !end.IsZero() && !now.Before(end) {
		return selection
	}
	origin, target := cookieURL(b.Origin), cookieURL(rawURL)
	if origin == nil || target == nil {
		return selection
	}
	jar, _ := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
	for _, member := range b.Members {
		c := member.Cookie
		if c.MaxAge < 0 || !c.Expires.IsZero() && !now.Before(c.Expires) {
			continue
		}
		// Selection uses the supplied clock; the jar only handles URL scope.
		c.MaxAge = 0
		c.Expires = time.Time{}
		memberOrigin := cookieURL(member.Origin)
		if memberOrigin == nil {
			memberOrigin = origin
		}
		jar.SetCookies(memberOrigin, []*http.Cookie{&c})
	}
	request := &http.Request{Header: make(http.Header)}
	hasRoute := false
	for _, c := range jar.Cookies(target) {
		request.AddCookie(c)
		hasRoute = hasRoute || RouteCookieName(c.Name)
		for _, member := range b.Members {
			if member.Cookie.Name == c.Name && member.Cookie.Value == c.Value {
				selection.Members[member.key()] = member.Version
			}
		}
	}
	if hasRoute {
		selection.Header = request.Header.Get("Cookie")
		selection.ExpiresAt = b.expiry(p)
	}
	return selection
}
func (m CookieMember) key() string {
	domain := m.Cookie.Domain
	if domain == "" {
		if u := cookieURL(m.Origin); u != nil {
			domain = u.Hostname()
		}
	}
	return m.Cookie.Name + "\x00" + domain + "\x00" + m.Cookie.Path
}
func (b *CookieBundle) snapshot(version uint64, now time.Time, p config.CodexStateOverrideConfig) *CookieBundleSnapshot {
	if b == nil {
		return nil
	}
	s := &CookieBundleSnapshot{Version: version, ReceivedAt: b.ReceivedAt, AgeSeconds: max(0, int64(now.Sub(b.ReceivedAt).Seconds())), ExpiresAt: b.expiry(config.CodexStateOverrideConfig{}), LocalExpiresAt: b.localExpiry(p), Members: []CookieMemberSnapshot{}}
	var digests []string
	for _, m := range b.Members {
		c := m.Cookie
		d := cookieDigest(c.Value)
		digests = append(digests, m.key()+d)
		s.Members = append(s.Members, CookieMemberSnapshot{Version: m.Version, Name: c.Name, Digest: d, Domain: c.Domain, Path: c.Path, Secure: c.Secure, ExpiresAt: c.Expires})
	}
	sort.Strings(digests)
	s.Digest = cookieDigest(strings.Join(digests, "\x00"))
	return s
}

// StripManagedCookies preserves unrelated explicitly configured cookies.
func StripManagedCookies(headers http.Header) {
	request := &http.Request{Header: headers}
	cookies := request.Cookies()
	for key := range headers {
		if strings.EqualFold(key, "Cookie") {
			delete(headers, key)
		}
	}
	for _, c := range cookies {
		if !ManagedCookieName(c.Name) {
			request.AddCookie(c)
		}
	}
}

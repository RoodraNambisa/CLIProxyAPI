package chatgptweb

import (
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

var sessionCookieBaseNames = [...]string{
	"__secure-next-auth.session-token",
	"next-auth.session-token",
	"__secure-authjs.session-token",
	"authjs.session-token",
}

type cookieKey struct {
	name   string
	path   string
	domain string
	host   string
}

type sessionCookieScopeKey struct {
	base string
	path string
	host string
}

type sessionCookieGroup struct {
	directIndexes []int
	chunkIndexes  map[int]int
	maxChunk      int
}

// MergeCookieDelta applies changes made between baseline and next to current.
// Concurrent cookies outside that delta are preserved.
func MergeCookieDelta(current, baseline, next []Cookie) []Cookie {
	currentByKey := cookiesByKey(current)
	baselineByKey := cookiesByKey(baseline)
	nextByKey := cookiesByKey(next)
	for key := range baselineByKey {
		if _, exists := nextByKey[key]; !exists {
			delete(currentByKey, key)
		}
	}
	for key, cookie := range nextByKey {
		if previous, exists := baselineByKey[key]; exists && reflect.DeepEqual(previous, cookie) {
			continue
		}
		currentByKey[key] = cookie
	}
	merged := make([]Cookie, 0, len(currentByKey))
	for _, cookie := range currentByKey {
		merged = append(merged, cookie)
	}
	sort.SliceStable(merged, func(left, right int) bool {
		leftKey := cookieIdentity(merged[left])
		rightKey := cookieIdentity(merged[right])
		if leftKey.host != rightKey.host {
			return leftKey.host < rightKey.host
		}
		if leftKey.domain != rightKey.domain {
			return leftKey.domain < rightKey.domain
		}
		if leftKey.path != rightKey.path {
			return leftKey.path < rightKey.path
		}
		return leftKey.name < rightKey.name
	})
	return merged
}

// normalizeSessionCookies removes an unchunked session cookie when a complete
// chunked representation exists in the same cookie scope. It also returns the
// reconstructed session token without converting arbitrary session_token
// fields into browser cookies.
func normalizeSessionCookies(cookies []Cookie) ([]Cookie, string) {
	groups := make(map[sessionCookieScopeKey]*sessionCookieGroup)
	groupOrder := make([]sessionCookieScopeKey, 0)
	now := time.Now()
	for index, cookie := range cookies {
		base, chunkIndex, chunked, ok := parseSessionCookieName(cookie.Name)
		if !ok || !persistedSessionCookieActive(cookie, now) {
			continue
		}
		path := strings.TrimSpace(cookie.Path)
		if path == "" {
			path = "/"
		}
		key := sessionCookieScopeKey{
			base: base,
			path: path,
			host: sessionCookieHost(cookie),
		}
		group := groups[key]
		if group == nil {
			group = &sessionCookieGroup{
				chunkIndexes: make(map[int]int),
				maxChunk:     -1,
			}
			groups[key] = group
			groupOrder = append(groupOrder, key)
		}
		if !chunked {
			group.directIndexes = append(group.directIndexes, index)
			continue
		}
		group.chunkIndexes[chunkIndex] = index
		if chunkIndex > group.maxChunk {
			group.maxChunk = chunkIndex
		}
	}

	remove := make(map[int]struct{})
	completeTokens := make(map[sessionCookieScopeKey]string)
	for key, group := range groups {
		token, complete := completeSessionCookieToken(cookies, group)
		if !complete {
			continue
		}
		completeTokens[key] = token
		for _, index := range group.directIndexes {
			remove[index] = struct{}{}
		}
	}

	normalized := make([]Cookie, 0, len(cookies)-len(remove))
	for index, cookie := range cookies {
		if _, removed := remove[index]; removed {
			continue
		}
		normalized = append(normalized, cookie)
	}

	for _, base := range sessionCookieBaseNames {
		for _, key := range groupOrder {
			if key.base == base {
				if token := completeTokens[key]; token != "" {
					return normalized, token
				}
			}
		}
	}
	for _, base := range sessionCookieBaseNames {
		for _, cookie := range normalized {
			cookieBase, _, chunked, ok := parseSessionCookieName(cookie.Name)
			if ok && !chunked && cookieBase == base && persistedSessionCookieActive(cookie, now) {
				return normalized, cookie.Value
			}
		}
	}
	return normalized, ""
}

func persistedSessionCookieActive(cookie Cookie, now time.Time) bool {
	if strings.TrimSpace(cookie.Value) == "" || cookie.MaxAge < 0 {
		return false
	}
	if value := strings.TrimSpace(cookie.Expires); value != "" {
		expiresAt, errParse := time.Parse(time.RFC3339Nano, value)
		return errParse == nil && expiresAt.After(now)
	}
	if value := strings.TrimSpace(cookie.RawExpires); value != "" {
		expiresAt, errParse := http.ParseTime(value)
		return errParse == nil && expiresAt.After(now)
	}
	return cookie.MaxAge <= 0
}

func sessionCookieHost(cookie Cookie) string {
	host := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(cookie.Domain)), ".")
	if host == "" {
		host = normalizeChatGPTWebCookieHost(cookie.Host)
	}
	return host
}

func completeSessionCookieToken(cookies []Cookie, group *sessionCookieGroup) (string, bool) {
	if group == nil || group.maxChunk < 1 || len(group.chunkIndexes) != group.maxChunk+1 {
		return "", false
	}
	var token strings.Builder
	for chunkIndex := 0; chunkIndex <= group.maxChunk; chunkIndex++ {
		cookieIndex, exists := group.chunkIndexes[chunkIndex]
		if !exists || cookieIndex < 0 || cookieIndex >= len(cookies) {
			return "", false
		}
		value := strings.TrimSpace(cookies[cookieIndex].Value)
		if value == "" {
			return "", false
		}
		token.WriteString(value)
	}
	return token.String(), token.Len() > 0
}

func parseSessionCookieName(name string) (base string, chunkIndex int, chunked bool, ok bool) {
	name = strings.ToLower(strings.TrimSpace(name))
	for _, candidate := range sessionCookieBaseNames {
		if name == candidate {
			return candidate, -1, false, true
		}
		prefix := candidate + "."
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		suffix := name[len(prefix):]
		if suffix == "" {
			return "", -1, false, false
		}
		index, err := strconv.Atoi(suffix)
		if err != nil || index < 0 || strconv.Itoa(index) != suffix {
			return "", -1, false, false
		}
		return candidate, index, true, true
	}
	return "", -1, false, false
}

func cookiesByKey(cookies []Cookie) map[cookieKey]Cookie {
	byKey := make(map[cookieKey]Cookie, len(cookies))
	for _, cookie := range cookies {
		if strings.TrimSpace(cookie.Name) == "" {
			continue
		}
		byKey[cookieIdentity(cookie)] = cookie
	}
	return byKey
}

func cookieIdentity(cookie Cookie) cookieKey {
	return cookieKey{
		name:   cookie.Name,
		path:   cookie.Path,
		domain: strings.ToLower(strings.TrimSpace(cookie.Domain)),
		host:   strings.ToLower(strings.TrimSpace(cookie.Host)),
	}
}

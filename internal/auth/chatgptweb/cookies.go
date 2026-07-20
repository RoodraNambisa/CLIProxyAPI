package chatgptweb

import (
	"reflect"
	"sort"
	"strings"
)

type cookieKey struct {
	name   string
	path   string
	domain string
	host   string
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

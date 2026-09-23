package codexcookie

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

const target = "https://chatgpt.com/backend-api/codex/responses"

func TestUpdatesExpiryAndDeletion(t *testing.T) {
	now := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)
	s := newStore("account", func() time.Time { return now })
	set := func(values ...string) { s.StoreResponse(target, http.Header{"Set-Cookie": values}) }
	set("__oailb=first; Path=/; Max-Age=10", "__cf_bm=aux; Path=/; Secure", "auth=secret; Path=/")
	if h := s.Header(target); h != "__oailb=first; __cf_bm=aux" {
		t.Fatal(h)
	}
	now = now.Add(8 * time.Second)
	set("__oailb=first; Path=/; Max-Age=20")
	now = now.Add(5 * time.Second)
	if !strings.Contains(s.Header(target), "__oailb=first") {
		t.Fatal("same value did not extend expiry")
	}
	set("__oailb=second; Path=/; Max-Age=2; Expires=Fri, 01 Jan 2099 00:00:00 GMT")
	if !strings.Contains(s.Header(target), "__oailb=second") {
		t.Fatal("value not replaced")
	}
	now = now.Add(2 * time.Second)
	if s.Header(target) != "__cf_bm=aux" {
		t.Fatal("expiry not filtered at send")
	}
	set("__oailb=third; Path=/; Max-Age=30; Expires=Thu, 01 Jan 1970 00:00:00 GMT")
	if !strings.Contains(s.Header(target), "__oailb=third") {
		t.Fatal("Max-Age must override past Expires")
	}
	set("__oailb=third; Path=/; Max-Age=1")
	now = now.Add(time.Second)
	if s.Header(target) != "__cf_bm=aux" {
		t.Fatal("same value shortening ignored")
	}
	set("__oailb=session; Path=/", "__cflb=route; Path=/", "__oailb=; Path=/; Max-Age=0")
	if h := s.Header(target); strings.Contains(h, "__oailb") || !strings.Contains(h, "__cflb=route") {
		t.Fatal(h)
	}
	set("__cflb=; Path=/; Expires=Thu, 01 Jan 1970 00:00:00 GMT")
	now = now.Add(365 * 24 * time.Hour)
	if s.Header(target) != "__cf_bm=aux" {
		t.Fatal("session cookie was lost")
	}
}

func TestAllowlistScopeAndWireOrder(t *testing.T) {
	s := newStore("account", time.Now)
	for _, raw := range []string{"http://chatgpt.com/x", "ws://chatgpt.com/x", "https://api.openai.com/x", "https://foo.chat.openai.com/x", "https://chatgpt.com.evil.test/x", "https://evilchatgpt.com/x"} {
		s.StoreResponse(raw, http.Header{"Set-Cookie": {"__oailb=bad; Path=/"}})
		if s.Header(raw) != "" || s.Header(target) != "" {
			t.Fatal(raw)
		}
	}
	s.StoreResponse(target, http.Header{"Set-Cookie": {"__oailb=host; Path=/", "__cf_bm=domain; Domain=.chatgpt.com; Path=/backend-api; Secure", "__cflb=bad; Domain=com; Path=/", "cf_clearance=bad; Domain=elsewhere.test; Path=/", "account=never; Path=/", "cf_chl_test=allowed; Path=/", "__oailb=new; Path=/"}})
	if h := s.Header(target); strings.Contains(h, "host") || strings.Contains(h, "bad") || strings.Contains(h, "never") || !strings.Contains(h, "__oailb=new") {
		t.Fatal(h)
	}
	if h := s.Header("wss://sub.chatgpt.com/backend-api/x"); h != "__cf_bm=domain" {
		t.Fatal(h)
	}
	if h := s.Header("https://sub.chatgpt.com/backend-api-other"); h != "" {
		t.Fatal(h)
	}
	s.StoreResponse("https://chat.openai.com/a/b", http.Header{"Set-Cookie": {"__cflb=defaultpath"}})
	if s.Header("wss://chat.openai.com/a/c") != "__cflb=defaultpath" || s.Header("https://chat.openai.com/else") != "" {
		t.Fatal("default path mismatch")
	}
	for _, name := range []string{"__cf_bm", "__cflb", "__cfruid", "__cfseq", "__cfwaitingroom", "__oailb", "_cfuvid", "cf_clearance", "cf_ob_info", "cf_use_ob", "cf_chl_any"} {
		if !AllowedName(name) {
			t.Fatal(name)
		}
	}
	for _, name := range []string{"session", "auth", "cf_chl", "__OAILB"} {
		if AllowedName(name) {
			t.Fatal(name)
		}
	}
}

func TestConcurrentLastWriterAndLifecycle(t *testing.T) {
	m := NewManager()
	a := m.Acquire("one", "owner")
	var wg sync.WaitGroup
	for i := range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.StoreResponse(target, http.Header{"Set-Cookie": {fmt.Sprintf("__oailb=%d; Path=/", i)}})
			_ = a.Header(target)
		}()
	}
	wg.Wait()
	// Even a request begun before the concurrent writes can win by responding last.
	a.StoreResponse(target, http.Header{"Set-Cookie": {"__oailb=late; Path=/"}})
	if a.Header(target) != "__oailb=late" {
		t.Fatal("late update rejected")
	}
	if m.Acquire("two", "owner").Header(target) != "" {
		t.Fatal("cross-credential leak")
	}
	m.Sync(true, map[string]string{"one": "owner"})
	if m.Acquire("one", "owner") != a {
		t.Fatal("refresh replaced jar")
	}
	m.IdentityChanged("one", "new-owner")
	b := m.Acquire("one", "new-owner")
	a.StoreResponse(target, http.Header{"Set-Cookie": {"__oailb=retired; Path=/"}})
	if b.Header(target) != "" || a.Header(target) != "" {
		t.Fatal("replaced account reused jar")
	}
	m.Forget("one")
	b.StoreResponse(target, http.Header{"Set-Cookie": {"__oailb=deleted; Path=/"}})
	if m.Acquire("one", "new-owner") != nil || b.Header(target) != "" {
		t.Fatal("deleted jar revived")
	}
	m.Sync(true, map[string]string{"one": "new-owner"})
	c := m.Acquire("one", "new-owner")
	m.Sync(false, nil)
	m.Sync(true, map[string]string{"one": "new-owner"})
	c.StoreResponse(target, http.Header{"Set-Cookie": {"__oailb=old-enable; Path=/"}})
	if m.Acquire("one", "new-owner").Header(target) != "" {
		t.Fatal("disable/re-enable revived jar")
	}
}

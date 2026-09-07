package openai

import (
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestResponsesWebsocketToolCacheScopesTrustedCallerAndPermissions(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	c.Request.Header.Set("Authorization", "Bearer untrusted-header")
	if websocketToolPairScopeKey(c, "same-session") != "" || websocketToolPairScopeKey(nil, "same-session") != "" {
		t.Fatal("unverified identities were allowed to share state")
	}
	privateOne := acquireResponsesWebsocketToolPairState(websocketToolPairScopeKey(c, "same-session"))
	privateTwo := acquireResponsesWebsocketToolPairState(websocketToolPairScopeKey(c, "same-session"))
	if privateOne == privateTwo {
		t.Fatal("anonymous connections shared a cache")
	}
	c.Set("apiKey", "caller-one")
	c.Set("accessProvider", "config-access")
	c.Set("accessMetadata", map[string]string{"allowed_providers": "codex"})
	first := websocketToolPairScopeKey(c, "same-session")
	if first == "" || strings.Contains(first, "caller-one") {
		t.Fatal("scope is missing or exposes principal")
	}
	if websocketToolPairScopeKey(c, "same-session") != first {
		t.Fatal("same caller did not retain session sharing")
	}
	c.Set("apiKey", "caller-two")
	if websocketToolPairScopeKey(c, "same-session") == first {
		t.Fatal("different callers share tools")
	}
	c.Set("apiKey", "caller-one")
	c.Set("accessMetadata", map[string]string{"allowed_providers": "openai"})
	if websocketToolPairScopeKey(c, "same-session") == first {
		t.Fatal("different permissions share tools")
	}
	c.Set("accessMetadata", make(chan int))
	if websocketToolPairScopeKey(c, "same-session") != "" {
		t.Fatal("unrepresentable authorization metadata shared a scope")
	}
}

func TestResponsesWebsocketToolCacheConcurrentLifecycle(t *testing.T) {
	key := t.Name()
	var activeMu sync.Mutex
	active := make(map[*websocketToolPairState]int)
	var work sync.WaitGroup
	for range 8 {
		work.Go(func() {
			for range 200 {
				state := acquireResponsesWebsocketToolPairState(key)
				activeMu.Lock()
				for other, count := range active {
					if count > 0 && other != state {
						t.Error("simultaneous holders received different cache generations")
					}
				}
				active[state]++
				activeMu.Unlock()
				runtime.Gosched()
				activeMu.Lock()
				active[state]--
				if active[state] == 0 {
					delete(active, state)
				}
				activeMu.Unlock()
				releaseResponsesWebsocketToolPairState(key)
			}
		})
	}
	work.Wait()
	defaultWebsocketToolPairStates.mu.Lock()
	_, hasState := defaultWebsocketToolPairStates.states[key]
	defaultWebsocketToolPairStates.mu.Unlock()
	defaultWebsocketToolPairRefs.mu.Lock()
	_, hasRefs := defaultWebsocketToolPairRefs.counts[key]
	defaultWebsocketToolPairRefs.mu.Unlock()
	if hasState || hasRefs {
		t.Fatal("final release retained a state or reference count")
	}
}

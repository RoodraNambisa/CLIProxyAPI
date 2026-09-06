package util

import (
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
)

type cacheLogPolicyTestWriter struct {
	gin.ResponseWriter
	policy *PromptCacheLogRedactor
}

func (w *cacheLogPolicyTestWriter) SetPromptCacheLogRedactor(policy *PromptCacheLogRedactor) {
	w.policy = policy
}

func TestPromptCacheLogPolicyIsPerTurnAndRetainsNoRequestBuffer(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	writer := &cacheLogPolicyTestWriter{ResponseWriter: c.Writer}
	c.Writer = writer
	body := []byte(`{"prompt_cache_key":"first-cache-key","input":"prompt"}`)
	first := RegisterPromptCacheLogPolicy(c, body)
	clear(body)
	if first == nil || !first.ProtectsKey("first-cache-key") || writer.policy != first {
		t.Fatal("request buffer release lost the policy")
	}
	if got := RegisterPromptCacheLogPolicy(c, []byte(`{"prompt_cache_key":"first-cache-key"}`)); got != first {
		t.Fatal("same-turn registration rebuilt the immutable policy")
	}
	second := RegisterPromptCacheLogPolicy(c, []byte(`{"prompt_cache_key":"second-cache-key"}`))
	if second == first || writer.policy != second || first.Redact("first-cache-key") != PromptCacheLogMarker {
		t.Fatal("next turn changed the existing request policy")
	}
	if got := RegisterPromptCacheLogPolicy(c, []byte(`{"input":"next turn"}`)); got != nil || PromptCacheLogForGin(c) != nil || writer.policy != nil {
		t.Fatal("missing key retained the previous turn policy")
	}
	if RegisterPromptCacheLogPolicy(nil, []byte(`{}`)) != nil {
		t.Fatal("nil context changed the missing-key policy")
	}
}

func TestPromptCacheLogRedactorConcurrentDiagnostics(t *testing.T) {
	r := NewPromptCacheLogRedactor("concurrent-key")
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 8 {
				if r.Redact("concurrent-key") != PromptCacheLogMarker {
					t.Error("scalar was not redacted")
				}
				if r.Headers(map[string][]string{"Session-Id": {"concurrent-key"}})["Session-Id"][0] != PromptCacheLogMarker {
					t.Error("header was not redacted")
				}
				stream := r.Stream()
				if string(stream.Write([]byte(`{"prompt_cache_key":"concurrent-key"}`), true)) != `{"prompt_cache_key":"[REDACTED_CACHE_KEY]"}` {
					t.Error("stream redaction changed under concurrent compilation")
				}
			}
		})
	}
	wg.Wait()
}

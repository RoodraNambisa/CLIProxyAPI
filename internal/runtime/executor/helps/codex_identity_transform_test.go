package helps

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/tidwall/gjson"
)

func TestCompleteIdentityProjection(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(fmt.Sprint(force), func(t *testing.T) {
			tr := &CodexIdentityTransform{Scope: "credential-a", Generation: "1", Restore: true}
			raw := []byte(`{"prompt_cache_key":"session-a","input":[{"content":"session-a","arguments":{"session_id":"session-a"}}],"client_metadata":{"session_id":"session-a","thread_id":"session-a","turn_id":"turn-a","x-codex-installation-id":"device-a","x-codex-turn-metadata":{"session_id":"session-a","thread_id":"session-a","root_thread_id":"session-a","parent_thread_id":"parent-a","turn_id":"turn-a","root_turn_id":"turn-a","context_window_id":"context-a","window_id":"session-a:2"}}}`)
			projection := CodexSessionIdentityProjection{ProjectSession: force, Transform: tr}
			if force {
				projection.ForcedIdentity = CodexSessionIdentity{SessionID: "pool-session", ThreadID: "pool-session", TurnID: "pool-turn", WindowID: "pool-session:0"}
			}
			out, identity, _, err := ProjectCodexSessionIdentityWithProjection(raw, CodexSessionIdentityHeaderSource{}, CodexSessionIdentityHeaderSource{}, CodexSessionIdentityHeaderSource{}, CodexSessionIdentity{}, projection)
			if err != nil {
				t.Fatal(err)
			}
			nested := gjson.GetBytes(out, "client_metadata.x-codex-turn-metadata").String()
			if got := gjson.Get(nested, "root_thread_id").String(); got != identity.ThreadID {
				t.Fatalf("root/thread mismatch: %s %s", got, identity.ThreadID)
			}
			if got := gjson.GetBytes(out, "prompt_cache_key").String(); got != identity.SessionID {
				t.Fatalf("cache/session mismatch: %s %s", got, identity.SessionID)
			}
			if gjson.GetBytes(out, "input").Raw != gjson.GetBytes(raw, "input").Raw {
				t.Fatal("business content changed")
			}
			if strings.Contains(gjson.Get(nested, "parent_thread_id").String(), "parent-a") {
				t.Fatal("parent was not mapped")
			}
			response, _ := json.Marshal(map[string]any{"response": map[string]any{"id": "opaque-response", "session_id": identity.SessionID, "thread_id": identity.ThreadID, "output": []any{map[string]string{"call_id": "opaque-call", "text": identity.ThreadID}}}})
			restored := tr.Response(response)
			if gjson.GetBytes(restored, "response.session_id").String() != "session-a" {
				t.Fatalf("restore: %s", restored)
			}
			if gjson.GetBytes(restored, "response.output").Raw != gjson.GetBytes(response, "response.output").Raw {
				t.Fatal("output changed")
			}
		})
	}
}
func TestIdentityMappingRolesAndReturnedAlias(t *testing.T) {
	tr := &CodexIdentityTransform{Scope: "alias-test", Generation: "1"}
	id := "019cd47b-9c84-732e-a18a-fc87682d7fb2"
	mapped := tr.Map("session_id", id)
	if mapped == tr.Map("turn_id", id) || mapped == tr.Map("context_window_id", id) {
		t.Fatal("roles collided")
	}
	if uuid.MustParse(mapped).Version() != 7 || mapped[:13] != id[:13] {
		t.Fatal("UUID lifecycle changed")
	}
	if tr.Map("window_id", id+":9") != mapped+":9" {
		t.Fatal("window counter changed")
	}
	payload := []byte(`event: response.created\r\ndata: {"response":{"session_id":"` + mapped + `"}}\r\n\r\n`)
	payload = bytes.ReplaceAll(payload, []byte(`\r\n`), []byte("\r\n"))
	if !bytes.Equal(tr.Response(payload), payload) {
		t.Fatal("disabled restoration changed response")
	}
	tr.ObserveReturned(payload)
	next := &CodexIdentityTransform{Scope: "alias-test", Generation: "1"}
	if next.Map("session_id", mapped) != mapped {
		t.Fatal("returned identity mapped twice")
	}
	other := &CodexIdentityTransform{Scope: "alias-other", Generation: "1"}
	if got := other.Map("session_id", mapped); got != StableCodexIdentity("alias-other", "conversation", id) {
		t.Fatalf("failover did not use original: %s", got)
	}
}
func TestIdentityRewriteMultilineSSE(t *testing.T) {
	tr := &CodexIdentityTransform{Scope: "sse", Restore: true}
	mapped := tr.Map("thread_id", "thread-original")
	raw := []byte("event: response.metadata\r\ndata: {\"response\":{\r\ndata: \"thread_id\":\"" + mapped + "\"}}\r\n\r\n")
	got := tr.Response(raw)
	if bytes.Contains(got, []byte(mapped)) || !bytes.Contains(got, []byte("thread-original")) {
		t.Fatalf("multiline: %s", got)
	}
}
func BenchmarkIdentityEnvelope(b *testing.B) {
	for _, size := range []int{4096, 1 << 20, 16 << 20} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			raw := []byte(`{"input":"` + strings.Repeat("x", size) + `","client_metadata":{"thread_id":"t","turn_id":"u","context_window_id":"c"}}`)
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			for b.Loop() {
				tr := &CodexIdentityTransform{Scope: "credential"}
				_ = RewriteCodexIdentityEnvelope(raw, tr.Map)
			}
		})
	}
}
func BenchmarkIdentityResponseDisabled(b *testing.B) {
	raw := []byte(`{"type":"response.output_text.delta","delta":"hello"}`)
	tr := &CodexIdentityTransform{}
	b.ReportAllocs()
	for b.Loop() {
		_ = tr.Response(raw)
	}
}

func TestIdentityRestoreUsesCurrentClientAlias(t *testing.T) {
	first := &CodexIdentityTransform{Scope: "current-alias", Generation: "1"}
	mapped := first.Map("session_id", "original-client")
	first.ObserveReturned([]byte(`{"session_id":"` + mapped + `"}`))
	second := &CodexIdentityTransform{Scope: "current-alias", Generation: "1", Restore: true}
	if second.Map("session_id", mapped) != mapped {
		t.Fatal("double mapping")
	}
	response := []byte(`{"session_id":"` + mapped + `"}`)
	if !bytes.Equal(second.Response(response), response) {
		t.Fatalf("restored an earlier caller identity: %s", second.Response(response))
	}
	first.Converge("session_id", mapped, "shared-slot")
	first.ObserveReturned([]byte(`{"session_id":"shared-slot"}`))
	third := &CodexIdentityTransform{Scope: "current-alias", Generation: "1", Restore: true}
	if third.Map("session_id", "shared-slot") != "shared-slot" {
		t.Fatal("converged alias mapped again")
	}
	if got := third.Response([]byte(`{"session_id":"shared-slot"}`)); string(got) != `{"session_id":"shared-slot"}` {
		t.Fatalf("ambiguous restore: %s", got)
	}
}
func TestIdentityExplicitCacheWhitespaceIsIndependent(t *testing.T) {
	tr := &CodexIdentityTransform{Scope: "exact-cache"}
	raw := []byte(`{"prompt_cache_key":" session ","client_metadata":{"session_id":"session"}}`)
	out, identity, _, err := ProjectCodexSessionIdentityWithProjection(raw, CodexSessionIdentityHeaderSource{}, CodexSessionIdentityHeaderSource{}, CodexSessionIdentityHeaderSource{}, CodexSessionIdentity{}, CodexSessionIdentityProjection{Transform: tr})
	if err != nil {
		t.Fatal(err)
	}
	if key := gjson.GetBytes(out, "prompt_cache_key").String(); key == identity.SessionID || key != StableCodexIdentity("exact-cache", "cache", " session ") {
		t.Fatalf("lost explicit key identity: %s", out)
	}
}

func TestIdentityCacheSessionAliasLifecycle(t *testing.T) {
	tr := &CodexIdentityTransform{Scope: "cache-session", Generation: "1"}
	id := uuid.NewString()
	mapped := tr.Map("prompt_cache_key", id)
	if tr.Map("session_id", id) != mapped {
		t.Fatal("default UUID cache/session relation lost")
	}
	opaque := tr.Map("prompt_cache_key", "opaque-key")
	tr.ObserveReturned([]byte(`{"prompt_cache_key":"` + opaque + `"}`))
	next := &CodexIdentityTransform{Scope: "cache-session", Generation: "1"}
	if value := next.Map("session_id", opaque); value != opaque {
		t.Fatal("returned cache alias mapped twice as session")
	}
}

func TestIdentityWindowBuiltFromReturnedThread(t *testing.T) {
	first := &CodexIdentityTransform{Scope: "window-alias", Generation: "1"}
	mapped := first.Map("thread_id", "client-thread")
	first.ObserveReturned([]byte(`{"thread_id":"` + mapped + `"}`))
	next := &CodexIdentityTransform{Scope: "window-alias", Generation: "1"}
	if got := next.Map("window_id", mapped+":4"); got != mapped+":4" {
		t.Fatalf("double-mapped returned window: %s", got)
	}
	other := &CodexIdentityTransform{Scope: "other-window", Generation: "1"}
	if got := other.Map("window_id", mapped+":4"); got != StableCodexIdentity("other-window", "conversation", "client-thread")+":4" {
		t.Fatal("window failover lost original thread")
	}
}

func TestIdentityEnvelopeNestedSpansPreserveFormatting(t *testing.T) {
	tr := &CodexIdentityTransform{Scope: "spans", Restore: true}
	a, b := tr.Map("session_id", "a"), tr.Map("turn_id", "b")
	raw := []byte(" \n{ \"response\" : {\"session_id\":\"" + a + "\",\"error\":{\"turn_id\":\"" + b + "\"},\"output\": [ 1, 2 ] }, \"turn_id\": \"" + b + "\" }\n ")
	expected := bytes.ReplaceAll(bytes.ReplaceAll(raw, []byte(a), []byte("a")), []byte(b), []byte("b"))
	if got := tr.Response(raw); !bytes.Equal(got, expected) {
		t.Fatalf("nested offsets changed formatting: %q", got)
	}
}
func BenchmarkIdentityResponsePlainEvent(b *testing.B) {
	tr := &CodexIdentityTransform{Scope: "plain", Restore: true}
	tr.Map("session_id", "client")
	raw := []byte(`{"type":"response.output_text.delta","delta":"hello","item_id":"item"}`)
	b.ReportAllocs()
	for b.Loop() {
		_ = tr.Response(raw)
	}
}

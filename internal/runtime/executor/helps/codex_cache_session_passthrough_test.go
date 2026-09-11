package helps

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexCacheSessionSnapshotSources(t *testing.T) {
	for _, tc := range []struct {
		name, body, key, session string
		headers                  []http.Header
	}{
		{name: "absent", body: `{}`},
		{name: "cache_only", body: `{"prompt_cache_key":"non-uuid-cache"}`, key: "non-uuid-cache", session: "non-uuid-cache"},
		{name: "uuid", body: `{"prompt_cache_key":"019e417b-e000-7000-8000-000000000001"}`, key: "019e417b-e000-7000-8000-000000000001", session: "019e417b-e000-7000-8000-000000000001"},
		{name: "header_without_key", body: `{}`, headers: []http.Header{{"session_id": {"client-session"}}}, session: "client-session"},
		{name: "headers_precedence", body: `{"prompt_cache_key":"cache"}`, headers: []http.Header{{"session-id": {"options"}}, {"Session-Id": {"incoming"}}}, key: "cache", session: "options"},
		{name: "body_precedes_header", body: `{"prompt_cache_key":"cache","client_metadata":{"session_id":"body-session"}}`, headers: []http.Header{{"Session-Id": {"incoming"}}}, key: "cache", session: "body-session"},
		{name: "frame_turn", body: `{"prompt_cache_key":"cache","client_metadata":{"session_id":"flat","x-codex-turn-metadata":"{\"session_id\":\"frame\"}"}}`, key: "cache", session: "frame"},
		{name: "object_turn", body: `{"client_metadata":{"x-codex-turn-metadata":{"session_id":"frame"}}}`, session: "frame"},
		{name: "header_turn", body: `{}`, headers: []http.Header{{"X-Codex-Turn-Metadata": {`{"session_id":"metadata-session"}`}}}, session: "metadata-session"},
		{name: "top_level", body: `{"session_id":"root-session"}`, session: "root-session"},
		{name: "empty_session", body: `{"prompt_cache_key":"cache","client_metadata":{"session_id":""}}`, key: "cache", session: "cache"},
		{name: "other_ids_are_not_session", body: `{"prompt_cache_key":"cache","client_metadata":{"thread_id":"thread","turn_id":"turn"}}`, headers: []http.Header{{"Thread-Id": {"thread"}}}, key: "cache", session: "cache"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := []byte(tc.body)
			snapshot := SnapshotCodexPromptCacheKey(payload, true, tc.headers...)
			clear(payload)
			for _, headers := range tc.headers {
				clear(headers)
			}
			if snapshot.Key != tc.key || snapshot.SessionID != tc.session || snapshot.Validate() != nil {
				t.Fatal("snapshot lost explicit routing identifiers or retained mutable input")
			}
			if got := SnapshotCodexPromptCacheKey([]byte(tc.body), false, tc.headers...); got != (CodexPromptCacheKeySnapshot{}) {
				t.Fatal("disabled passthrough changed legacy identity behavior")
			}
		})
	}
}

func TestCodexCacheSessionFinalProjection(t *testing.T) {
	snapshot := CodexPromptCacheKeySnapshot{Key: "client-cache", SessionID: "client-session"}
	payload := []byte(`{"prompt_cache_key":"converged-cache","session_id":"converged-session","input":{"session_id":"business","prompt_cache_key":"business"},"client_metadata":{"session_id":"converged-session","thread_id":"converged-thread","x-codex-turn-metadata":"{\"session_id\":\"converged-session\",\"prompt_cache_key\":\"converged-cache\",\"turn_id\":\"converged-turn\"}"}}`)
	headers := http.Header{"Session-Id": {"admin"}, "sEsSiOn_Id": {"converged"}, "Thread-Id": {"converged-thread"}, "X-Codex-Turn-Metadata": {`{"session_id":"converged","prompt_cache_key":"converged-cache","turn_id":"converged-turn"}`}}
	got, err := snapshot.ApplyFinal(payload, headers)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"session_id", "client_metadata.session_id"} {
		if gjson.GetBytes(got, field).Str != snapshot.SessionID {
			t.Fatal("session lost final precedence")
		}
	}
	if headers.Get("Session-Id") != snapshot.SessionID || headers.Get("Session_id") != snapshot.SessionID || len(headers.Values("Session_id")) != 1 {
		t.Fatal("header aliases did not converge on the explicit session")
	}
	for _, raw := range []string{gjson.GetBytes(got, "client_metadata.x-codex-turn-metadata").Str, headers.Get("X-Codex-Turn-Metadata")} {
		if gjson.Get(raw, "session_id").Str != snapshot.SessionID || gjson.Get(raw, "prompt_cache_key").Str != snapshot.Key || gjson.Get(raw, "turn_id").Str != "converged-turn" {
			t.Fatal("protocol mirror or independent turn identity changed")
		}
	}
	if gjson.GetBytes(got, "input.session_id").Str != "business" || gjson.GetBytes(got, "input.prompt_cache_key").Str != "business" || gjson.GetBytes(got, "client_metadata.thread_id").Str != "converged-thread" || headers.Get("Thread-Id") != "converged-thread" {
		t.Fatal("final routing override changed another identity or business data")
	}
	unchanged, err := snapshot.ApplyFinal(got, headers)
	if err != nil || &unchanged[0] != &got[0] {
		t.Fatal("idempotent projection copied the large request buffer")
	}
}

func TestCodexCacheSessionValidation(t *testing.T) {
	for _, key := range []string{" leading", "trailing ", "line\nbreak", "line\rbreak", "control\x00", "delete\x7f"} {
		payload, _ := json.Marshal(map[string]string{"prompt_cache_key": key})
		if SnapshotCodexPromptCacheKey(payload, true).Validate() == nil {
			t.Fatal("invalid fallback session accepted")
		}
		if SnapshotCodexPromptCacheKey(payload, false).Validate() != nil {
			t.Fatal("disabled passthrough rejected an existing cache value")
		}
		snapshot := SnapshotCodexPromptCacheKey(payload, true, http.Header{"Session_id": {"explicit-valid-session"}})
		got, err := snapshot.ApplyFinal([]byte(`{"prompt_cache_key":"generated"}`), make(http.Header))
		if err != nil || !json.Valid(got) || gjson.GetBytes(got, "prompt_cache_key").Str != key {
			t.Fatal("valid independent session changed or rejected the cache string")
		}
	}
	large := []byte(`{"prompt_cache_key":"same","input":"` + string(bytes.Repeat([]byte{'x'}, 1<<20)) + `"}`)
	snapshot := CodexPromptCacheKeySnapshot{Key: "same", SessionID: "same"}
	if got, err := snapshot.ApplyFinal(large, nil); err != nil || &got[0] != &large[0] {
		t.Fatal("unchanged envelope was copied")
	}
}

func TestCodexProtectedSessionSurvivesEqualIdentityRoles(t *testing.T) {
	payload := []byte(`{"session_id":"same","prompt_cache_key":"same","thread_id":"same","window_id":"same:0","turn_id":"same","output":[{"arguments":"same"}]}`)
	got := ReplaceCodexResponseIdentityFields(payload, "same", "mapped", CodexResponseThreadAndWindowIdentity)
	got = ReplaceCodexResponseIdentityFields(got, "same", "turn", CodexResponseTurnIdentity)
	if gjson.GetBytes(got, "session_id").Str != "same" || gjson.GetBytes(got, "prompt_cache_key").Str != "same" || gjson.GetBytes(got, "output.0.arguments").Str != "same" {
		t.Fatal("equal strings crossed identity roles")
	}
	if gjson.GetBytes(got, "thread_id").Str != "mapped" || gjson.GetBytes(got, "window_id").Str != "mapped:0" || gjson.GetBytes(got, "turn_id").Str != "turn" {
		t.Fatal("protecting session disabled other identity mappings")
	}
}

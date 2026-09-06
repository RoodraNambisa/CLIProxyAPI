package helps

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexResponseIdentitySSELineEndings(t *testing.T) {
	for _, ending := range []string{"\n", "\r\n", "\r"} {
		payload := []byte("event: response.completed" + ending + `data: {"session_id":"original","prompt_cache_key":"original"}` + ending + ending + ": ping" + ending)
		got := ReplaceCodexResponseIdentityFields(payload, "original", "projected", CodexResponseSessionIdentity)
		want := bytes.Replace(payload, []byte(`"session_id":"original"`), []byte(`"session_id":"projected"`), 1)
		if !bytes.Equal(got, want) {
			t.Errorf("SSE line ending %q lost identity projection or changed framing", ending)
		}
	}
}

func TestCodexPromptCacheProtectionRetainsSessionProjection(t *testing.T) {
	for _, protected := range []string{"", "same"} {
		body := []byte(`{"prompt_cache_key":"same","client_metadata":{"session_id":"same","x-codex-turn-metadata":"{\"prompt_cache_key\":\"same\"}"}}`)
		projection := CodexSessionIdentityProjection{
			ProjectSession: true, ForcedIdentity: CodexSessionIdentity{SessionID: "projected", ThreadID: "thread", TurnID: "turn", WindowID: "window"},
			PromptCacheKeyAlias: "same", ProtectedPromptCacheKey: protected,
		}
		got, identity, turn, err := ProjectCodexSessionIdentityWithProjection(body, CodexSessionIdentityHeaderSource{}, CodexSessionIdentityHeaderSource{}, CodexSessionIdentityHeaderSource{}, CodexSessionIdentity{}, projection)
		if err != nil {
			t.Fatal(err)
		}
		want := "projected"
		if protected != "" {
			want = protected
		}
		if gjson.GetBytes(got, "prompt_cache_key").String() != want || gjson.Get(turn, "prompt_cache_key").String() != want {
			t.Fatal("cache and mirror roles do not follow the protection policy")
		}
		if identity.SessionID != "projected" || gjson.GetBytes(got, "client_metadata.session_id").String() != "projected" {
			t.Fatal("cache protection disabled session convergence")
		}
	}
}

func TestCodexPromptCacheSnapshotPreservesOnlyExplicitStrings(t *testing.T) {
	for _, raw := range []string{`{}`, `{"prompt_cache_key":null}`, `{"prompt_cache_key":42}`, `{"prompt_cache_key":true}`, `{"prompt_cache_key":{}}`, `{"prompt_cache_key":""}`, `{"prompt_cache_key":"  "}`} {
		if got := SnapshotCodexPromptCacheKey([]byte(raw), true); got.Key != "" {
			t.Fatalf("unexpected protected key for %s", raw)
		}
	}
	payload := []byte(`{"prompt_cache_key":" Case\tKey ","input":"large body"}`)
	if got := SnapshotCodexPromptCacheKey(payload, false); got.Key != "" {
		t.Fatal("disabled policy captured a key")
	}
	snapshot := SnapshotCodexPromptCacheKey(payload, true)
	clear(payload)
	if snapshot.Key != " Case\tKey " {
		t.Fatal("snapshot changed after releasing the source buffer")
	}
	got := snapshot.Apply([]byte(`{"prompt_cache_key":"generated","input":{"prompt_cache_key":"business"}}`))
	if gjson.GetBytes(got, "prompt_cache_key").Str != snapshot.Key || gjson.GetBytes(got, "input.prompt_cache_key").Str != "business" {
		t.Fatal("cache role was not preserved independently of business JSON")
	}
}

func TestCodexResponseIdentityFieldsPreserveCacheAndBusinessRoles(t *testing.T) {
	for _, escaped := range []bool{false, true} {
		key := "same-key"
		if escaped {
			key = "same\"key"
		}
		body := []byte(fmt.Sprintf(`{"session_id":%q,"prompt_cache_key":%q,"id":%q,"output":[{"text":%q,"arguments":%q}],"response":{"client_metadata":{"session_id":%q,"x-codex-turn-metadata":%q}}}`, key, key, key, key, key, key, fmt.Sprintf(`{"turn_id":%q,"prompt_cache_key":%q}`, key, key)))
		for _, sse := range []bool{false, true} {
			payload := body
			if sse {
				payload = append([]byte("event: response.completed\r\ndata: "), body...)
				payload = append(payload, []byte("\r\n\r\n: ping\n\ndata: [DONE]\n\n")...)
			}
			updated := ReplaceCodexResponseIdentityFields(payload, key, "upstream-id", CodexResponseSessionIdentity)
			updated = ReplaceCodexResponseIdentityFields(updated, key, "upstream-turn", CodexResponseTurnIdentity)
			jsonBody := updated
			if sse {
				jsonBody = bytes.Split(bytes.SplitN(updated, []byte("data: "), 2)[1], []byte("\r\n"))[0]
			}
			for _, path := range []string{"prompt_cache_key", "id", "output.0.text", "output.0.arguments"} {
				if gjson.GetBytes(jsonBody, path).Str != key {
					t.Fatalf("rewrote non-identity role %s", path)
				}
			}
			if gjson.GetBytes(jsonBody, "session_id").Str != "upstream-id" {
				t.Fatal("session identity was not rewritten")
			}
			turn := gjson.GetBytes(jsonBody, "response.client_metadata.x-codex-turn-metadata").Str
			if gjson.Get(turn, "prompt_cache_key").Str != key || gjson.Get(turn, "turn_id").Str != "upstream-turn" {
				t.Fatal("embedded metadata confused cache and identity roles")
			}
			restored := ReplaceCodexResponseIdentityFields(updated, "upstream-id", key, CodexResponseSessionIdentity)
			restored = ReplaceCodexResponseIdentityFields(restored, "upstream-turn", key, CodexResponseTurnIdentity)
			if !bytes.Equal(restored, payload) {
				t.Fatal("identity round trip changed business data or SSE framing")
			}
		}
	}
}

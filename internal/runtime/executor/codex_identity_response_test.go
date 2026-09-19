package executor

import (
	"bytes"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexIdentityResponseShortKeysPreserveBusinessContent(t *testing.T) {
	for _, protected := range []bool{false, true} {
		state := codexIdentityConfuseState{enabled: true, originalPromptCacheKey: "a", promptCacheKey: "mapped-cache", turnIDs: []codexIdentityReplacement{{original: "id", confused: "mapped-turn"}}}
		if protected {
			state.protectedPromptCacheKey = "a"
		}
		body := []byte(`{"type":"response.completed","response":{"id":"a","prompt_cache_key":"a","turn_id":"id","session_id":"a","output":[{"type":"message","text":"a id mapped-cache mapped-turn","arguments":"{\"a\":\"id\"}","call_id":"id"}],"metadata":{"a":"id","context_window_id":"a"},"a":9007199254740993}}`)
		for _, framed := range []bool{false, true} {
			payload := body
			if framed {
				payload = append(append([]byte("event: response.completed\r\ndata: "), body...), []byte("\r\n\r\ndata: [DONE]\r\n\r\n")...)
			}
			mapped := applyCodexIdentityConfuseResponsePayload(payload, state)
			jsonBody := mapped
			if framed {
				jsonBody = bytes.Split(bytes.SplitN(mapped, []byte("data: "), 2)[1], []byte("\r\n"))[0]
			}
			for _, path := range []string{"response.id", "response.output", "response.metadata", "response.a"} {
				if gjson.GetBytes(body, path).Raw != gjson.GetBytes(jsonBody, path).Raw {
					t.Fatalf("changed business path %s: %s", path, mapped)
				}
			}
			if gjson.GetBytes(jsonBody, "response.turn_id").Str != "mapped-turn" {
				t.Fatal("turn identity was not mapped")
			}
			wantCache := "mapped-cache"
			if protected {
				wantCache = "a"
			}
			if gjson.GetBytes(jsonBody, "response.prompt_cache_key").Str != wantCache {
				t.Fatal("cache protection was not respected")
			}
			if restored := applyCodexIdentityExposeResponsePayload(mapped, state); !bytes.Equal(restored, payload) {
				t.Fatalf("round trip changed content: %s", restored)
			}
		}
	}
}

func TestCodexIdentityResponseReplacementsDoNotCascade(t *testing.T) {
	state := codexIdentityConfuseState{turnIDs: []codexIdentityReplacement{{original: "a", confused: "b"}, {original: "b", confused: "c"}}}
	payload := []byte(`{"turn_id":"a","response":{"turn_id":"b"}}`)
	mapped := applyCodexIdentityConfuseResponsePayload(payload, state)
	if string(mapped) != `{"turn_id":"b","response":{"turn_id":"c"}}` {
		t.Fatalf("cascading replacement: %s", mapped)
	}
	if restored := applyCodexIdentityExposeResponsePayload(mapped, state); !bytes.Equal(restored, payload) {
		t.Fatalf("reverse mapping cascaded: %s", restored)
	}
}

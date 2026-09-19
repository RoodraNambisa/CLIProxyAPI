package helps

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/tidwall/gjson"
)

func TestCodexContextWindowMappingPreservesUUIDLifecycle(t *testing.T) {
	for _, original := range []uuid.UUID{uuid.New(), uuid.Must(uuid.NewV7())} {
		mapped := MapCodexContextWindow("account", original.String())
		parsed, err := uuid.Parse(mapped)
		if err != nil || parsed.Version() != original.Version() || mapped == original.String() {
			t.Fatalf("invalid mapped context window: %q", mapped)
		}
		if original.Version() == 7 && !bytes.Equal(parsed[:6], original[:6]) {
			t.Fatal("UUIDv7 timestamp changed")
		}
		if MapCodexContextWindow("account", original.String()) != mapped || MapCodexContextWindow("other", original.String()) == mapped || MapCodexContextWindow("account", uuid.NewString()) == mapped {
			t.Fatal("mapping is unstable or merges distinct identities")
		}
	}
	for _, value := range []string{"", "opaque", uuid.Nil.String()} {
		if MapCodexContextWindow("account", value) != value {
			t.Fatal("missing or invalid UUID was synthesized")
		}
	}
}

func TestCodexRelatedProjectionKeepsReferencesAndZeroWindowConsistent(t *testing.T) {
	contextID := uuid.Must(uuid.NewV7()).String()
	turn, _ := json.Marshal(map[string]any{
		"session_id": "session", "thread_id": "thread", "turn_id": "turn",
		"root_turn_id": "turn", "parent_turn_id": "other-turn", "root_thread_id": "thread",
		"parent_thread_id": "thread", "forked_from_thread_id": "unknown-thread",
		"window_id": "thread:7", "window_number": 7, "context_window_id": contextID,
		"extra": map[string]any{"context_window_id": contextID, "window_number": 7},
	})
	payload, _ := json.Marshal(map[string]any{"client_metadata": map[string]any{
		"session_id": "session", "thread_id": "thread", "turn_id": "turn", "root_turn_id": "turn",
		"window_number": "7", "context_window_id": contextID, "x-codex-turn-metadata": string(turn),
	}, "input": []any{map[string]any{"context_window_id": contextID, "window_number": 7}}})
	for _, converge := range []bool{false, true} {
		projection := CodexSessionIdentityProjection{ProjectSession: true}
		if converge {
			projection.ContextWindowScope = "account"
			projection.ForcedIdentity = CodexSessionIdentity{SessionID: "new-session", ThreadID: "new-thread", TurnID: "new-turn", WindowID: "new-thread:0"}
		}
		var mappings []CodexResponseIdentityReplacement
		projection.ContextReplacements = &mappings
		out, identity, encoded, err := ProjectCodexSessionIdentityWithProjection(payload, CodexSessionIdentityHeaderSource{}, CodexSessionIdentityHeaderSource{}, CodexSessionIdentityHeaderSource{}, CodexSessionIdentity{RequestKind: "turn"}, projection)
		if err != nil {
			t.Fatal(err)
		}
		meta := gjson.Parse(encoded)
		if meta.Get("root_turn_id").Str != identity.TurnID || gjson.GetBytes(out, "client_metadata.root_turn_id").Str != identity.TurnID || meta.Get("root_thread_id").Str != identity.ThreadID || identity.ParentThreadID != identity.ThreadID {
			t.Fatalf("broken identity relationship: %s", out)
		}
		if meta.Get("parent_turn_id").Str != "other-turn" || meta.Get("forked_from_thread_id").Str != "unknown-thread" || meta.Get("extra.window_number").Int() != 7 || meta.Get("extra.context_window_id").Str != contextID || gjson.GetBytes(out, "input.0.context_window_id").Str != contextID {
			t.Fatal("unknown references or business content were changed")
		}
		wantContext, wantNumber := contextID, int64(7)
		if converge {
			wantContext, wantNumber = MapCodexContextWindow("account", contextID), 0
		}
		if meta.Get("context_window_id").Str != wantContext || gjson.GetBytes(out, "client_metadata.context_window_id").Str != wantContext || meta.Get("window_number").Int() != wantNumber || gjson.GetBytes(out, "client_metadata.window_number").Int() != wantNumber {
			t.Fatalf("inconsistent window fields: %s", out)
		}
		if converge && len(mappings) != 1 {
			t.Fatal("context mapping was not recorded once")
		}
	}
}

func TestCodexRelatedProjectionDoesNotAddMissingWindowFields(t *testing.T) {
	projection := CodexSessionIdentityProjection{ProjectSession: true, ContextWindowScope: "account", ForcedIdentity: CodexSessionIdentity{SessionID: "s", ThreadID: "t", TurnID: "u", WindowID: "t:0"}}
	out, _, turn, err := ProjectCodexSessionIdentityWithProjection([]byte(`{}`), CodexSessionIdentityHeaderSource{}, CodexSessionIdentityHeaderSource{}, CodexSessionIdentityHeaderSource{}, CodexSessionIdentity{}, projection)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"window_number", "context_window_id", "root_turn_id", "parent_thread_id"} {
		if gjson.Get(turn, key).Exists() || gjson.GetBytes(out, "client_metadata."+key).Exists() {
			t.Fatalf("synthesized %s", key)
		}
	}
}

func TestCodexRelatedReferencesDoNotCollapseUnrelatedTurns(t *testing.T) {
	body := []byte(`{"client_metadata":{"turn_id":"mapped-main","parent_turn_id":"other","x-codex-turn-metadata":"{\"turn_id\":\"mapped-main\",\"root_turn_id\":\"main\",\"parent_turn_id\":\"other\"}"}}`)
	projection := CodexSessionIdentityProjection{ProjectSession: true, ForcedIdentity: CodexSessionIdentity{SessionID: "s", ThreadID: "t", TurnID: "final", WindowID: "t:0"}, KnownReplacements: []CodexResponseIdentityReplacement{{From: "main", To: "mapped-main", Role: CodexResponseTurnIdentity}, {From: "other", To: "mapped-other", Role: CodexResponseTurnIdentity}}}
	_, _, turn, err := ProjectCodexSessionIdentityWithProjection(body, CodexSessionIdentityHeaderSource{}, CodexSessionIdentityHeaderSource{}, CodexSessionIdentityHeaderSource{}, CodexSessionIdentity{}, projection)
	if err != nil {
		t.Fatal(err)
	}
	if gjson.Get(turn, "root_turn_id").Str != "final" || gjson.Get(turn, "parent_turn_id").Str != "mapped-other" {
		t.Fatalf("unrelated turn collapsed: %s", turn)
	}
}

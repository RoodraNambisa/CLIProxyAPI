package auth

import (
	"reflect"
	"testing"
)

func TestFileProjectionWeightDoesNotKeepStaleAttributes(t *testing.T) {
	for _, tc := range []struct {
		metadata map[string]any
		want     string
		present  bool
	}{
		{map[string]any{"type": "codex"}, "", false},
		{map[string]any{"type": "codex", "weight": 0}, "0", true},
		{map[string]any{"type": "codex", "weight": -3}, "0", true},
		{map[string]any{"type": "codex", "weight": "9"}, "9", true},
	} {
		credential := &Auth{ID: "weight.json", Provider: "codex", Attributes: map[string]string{"weight": "8"}, Metadata: tc.metadata}
		if err := ApplyFileAuthProjection(credential, FileAuthProjectionOptions{}); err != nil {
			t.Fatal(err)
		}
		value, present := credential.Attributes[AttributeWeight]
		if present != tc.present || value != tc.want {
			t.Fatal("file projection retained a stale weight or lost zero")
		}
	}
	credential := &Auth{ID: "unchanged", Provider: "codex", Attributes: map[string]string{"weight": "8"}, Metadata: map[string]any{"type": "claude", "weight": true}}
	before := credential.Clone()
	if err := ApplyFileAuthProjection(credential, FileAuthProjectionOptions{}); err == nil {
		t.Fatal("invalid file weight accepted")
	}
	if credential.ID != before.ID || credential.Provider != before.Provider || !reflect.DeepEqual(credential.Attributes, before.Attributes) {
		t.Fatal("failed projection partially changed credential fields")
	}
}

package auth

import "testing"

func TestCredentialTargetResolutionIsUnambiguous(t *testing.T) {
	m := NewManager(nil, nil, nil)
	a, err := m.Register(t.Context(), &Auth{ID: "target-a", FileName: "target-a.json", Provider: "codex", Metadata: map[string]any{RoutingAliasMetadataKey: "test"}})
	if err != nil {
		t.Fatal(err)
	}
	b, err := m.Register(t.Context(), &Auth{ID: "target-b", FileName: "target-b.json", Provider: "xai", Metadata: map[string]any{RoutingAliasMetadataKey: "TEST"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = m.ResolveCredentialTarget("test"); err == nil {
		t.Fatal("ambiguous alias was accepted")
	}
	selected, err := m.ResolveCredentialTarget(a.Index)
	if err != nil || selected.ID != a.ID {
		t.Fatalf("ID resolution: %v", err)
	}
	b.Disabled = true
	if _, err = m.Update(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	if _, err = m.ResolveCredentialTarget(b.Index); err == nil {
		t.Fatal("disabled credential was accepted")
	}
	if _, err = m.ResolveCredentialTarget("missing"); err == nil {
		t.Fatal("unknown credential was accepted")
	}
}

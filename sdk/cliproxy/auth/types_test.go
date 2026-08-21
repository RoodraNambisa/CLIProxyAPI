package auth

import "testing"

func TestRuntimeInstallationID(t *testing.T) {
	var nilAuth *Auth
	if got := nilAuth.RuntimeInstallationID(); got != "" {
		t.Fatalf("nil RuntimeInstallationID() = %q, want empty", got)
	}

	auth := &Auth{installationID: "installation-1"}
	if got := auth.RuntimeInstallationID(); got != "installation-1" {
		t.Fatalf("RuntimeInstallationID() = %q, want installation-1", got)
	}
	if got := auth.Clone().RuntimeInstallationID(); got != "installation-1" {
		t.Fatalf("clone RuntimeInstallationID() = %q, want installation-1", got)
	}
	if got := auth.CloneWithoutRuntimeInstance().RuntimeInstallationID(); got != "" {
		t.Fatalf("runtime-free clone RuntimeInstallationID() = %q, want empty", got)
	}
}

func TestCloneWithoutRuntimeInstanceClearsCredentialGeneration(t *testing.T) {
	auth := &Auth{
		RuntimeProxyURL:                "http://proxy.example",
		RuntimeProxyBindingID:          "binding",
		RuntimeProxyAuthID:             "auth",
		runtimeProxyResolved:           true,
		chatGPTWebCredentialGeneration: "runtime-generation",
	}
	clone := auth.CloneWithoutRuntimeInstance()
	if clone.chatGPTWebCredentialGeneration != "" {
		t.Fatalf("runtime-free clone generation = %q, want empty", clone.chatGPTWebCredentialGeneration)
	}
	if clone.RuntimeProxyURL != "" || clone.RuntimeProxyBindingID != "" || clone.RuntimeProxyAuthID != "" || clone.runtimeProxyResolved {
		t.Fatalf("runtime-free clone retained proxy identity: %#v", clone)
	}
	if auth.chatGPTWebCredentialGeneration != "runtime-generation" {
		t.Fatal("CloneWithoutRuntimeInstance() mutated the source auth")
	}
}

func TestToolPrefixDisabled(t *testing.T) {
	var a *Auth
	if a.ToolPrefixDisabled() {
		t.Error("nil auth should return false")
	}

	a = &Auth{}
	if a.ToolPrefixDisabled() {
		t.Error("empty auth should return false")
	}

	a = &Auth{Metadata: map[string]any{"tool_prefix_disabled": true}}
	if !a.ToolPrefixDisabled() {
		t.Error("should return true when set to true")
	}

	a = &Auth{Metadata: map[string]any{"tool_prefix_disabled": "true"}}
	if !a.ToolPrefixDisabled() {
		t.Error("should return true when set to string 'true'")
	}

	a = &Auth{Metadata: map[string]any{"tool-prefix-disabled": true}}
	if !a.ToolPrefixDisabled() {
		t.Error("should return true with kebab-case key")
	}

	a = &Auth{Metadata: map[string]any{"tool_prefix_disabled": false}}
	if a.ToolPrefixDisabled() {
		t.Error("should return false when set to false")
	}
}

func TestEnsureIndexUsesCredentialIdentity(t *testing.T) {
	t.Parallel()

	geminiAuth := &Auth{
		Provider: "gemini",
		Attributes: map[string]string{
			"api_key": "shared-key",
			"source":  "config:gemini[abc123]",
		},
	}
	compatAuth := &Auth{
		Provider: "bohe",
		Attributes: map[string]string{
			"api_key":      "shared-key",
			"compat_name":  "bohe",
			"provider_key": "bohe",
			"source":       "config:bohe[def456]",
		},
	}
	geminiAltBase := &Auth{
		Provider: "gemini",
		Attributes: map[string]string{
			"api_key":  "shared-key",
			"base_url": "https://alt.example.com",
			"source":   "config:gemini[ghi789]",
		},
	}
	geminiDuplicate := &Auth{
		Provider: "gemini",
		Attributes: map[string]string{
			"api_key": "shared-key",
			"source":  "config:gemini[abc123-1]",
		},
	}

	geminiIndex := geminiAuth.EnsureIndex()
	compatIndex := compatAuth.EnsureIndex()
	altBaseIndex := geminiAltBase.EnsureIndex()
	duplicateIndex := geminiDuplicate.EnsureIndex()

	if geminiIndex == "" {
		t.Fatal("gemini index should not be empty")
	}
	if compatIndex == "" {
		t.Fatal("compat index should not be empty")
	}
	if altBaseIndex == "" {
		t.Fatal("alt base index should not be empty")
	}
	if duplicateIndex == "" {
		t.Fatal("duplicate index should not be empty")
	}
	if geminiIndex == compatIndex {
		t.Fatalf("shared api key produced duplicate auth_index %q", geminiIndex)
	}
	if geminiIndex == altBaseIndex {
		t.Fatalf("same provider/key with different base_url produced duplicate auth_index %q", geminiIndex)
	}
	if geminiIndex == duplicateIndex {
		t.Fatalf("duplicate config entries should be separated by source-derived seed, got %q", geminiIndex)
	}
}

func TestMetadataWithDisabledClonesAndInjectsDisabled(t *testing.T) {
	t.Parallel()

	auth := &Auth{
		Disabled: true,
		Metadata: map[string]any{
			"type":  "vertex",
			"label": "vertex-label",
		},
	}

	got := MetadataWithDisabled(auth)
	if got["disabled"] != true {
		t.Fatalf("disabled = %#v, want true", got["disabled"])
	}
	if got["label"] != "vertex-label" {
		t.Fatalf("label = %#v, want %q", got["label"], "vertex-label")
	}

	got["label"] = "mutated"
	if auth.Metadata["label"] != "vertex-label" {
		t.Fatalf("original metadata mutated = %#v, want %q", auth.Metadata["label"], "vertex-label")
	}
}

func TestCanonicalSourceHashTreatsMissingDisabledAsFalse(t *testing.T) {
	t.Parallel()

	withoutDisabled := []byte(`{"type":"chatgpt-web","access_token":"token"}`)
	withDisabledFalse := []byte(`{"access_token":"token","disabled":false,"type":"chatgpt-web"}`)
	expectedHash, errHash := CanonicalSourceHashFromBytes(withoutDisabled)
	if errHash != nil {
		t.Fatal(errHash)
	}
	if !SourceHashMatchesBytes(expectedHash, withDisabledFalse) {
		t.Fatal("canonical source hash did not treat missing disabled as false")
	}

	auth := &Auth{}
	if errSync := SyncPersistedMetadataAndSourceHash(auth, withoutDisabled); errSync != nil {
		t.Fatal(errSync)
	}
	if !SourceHashMatchesBytes(auth.Attributes[SourceHashAttributeKey], withoutDisabled) {
		t.Fatal("runtime source hash no longer matches the original payload")
	}
}

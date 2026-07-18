package chatgptweb

import "testing"

func TestCredentialFileNameNormalizesEmail(t *testing.T) {
	first := CredentialFileName(" Person@Example.com ")
	second := CredentialFileName("person@example.com")
	if first != second {
		t.Fatalf("CredentialFileName() = %q and %q, want the same name", first, second)
	}
	if first != "chatgpt-web-542d240129883c01.json" {
		t.Fatalf("CredentialFileName() = %q", first)
	}
}

package util

import (
	"strings"
	"testing"
)

func TestMaskSensitiveHeaderValueMasksManagementKey(t *testing.T) {
	secret := "super-secret-management-key"
	got := MaskSensitiveHeaderValue("X-Management-Key", secret)
	if got == secret {
		t.Fatalf("management key was not masked")
	}
	if !strings.Contains(got, "...") {
		t.Fatalf("masked management key %q does not look masked", got)
	}
}

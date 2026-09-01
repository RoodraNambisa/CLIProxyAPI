package executor

import (
	"errors"
	"testing"
)

func TestErrorResponseSourcePreservesOriginalError(t *testing.T) {
	original := errors.New("boom")
	wrapped := WithErrorResponseSource(original, CredentialErrorResponseSource(" ChatGPT-Web ", -2))
	source, ok := ErrorResponseSourceOf(wrapped)
	if !ok || source.Provider != "chatgpt-web" || source.AuthPriority != -2 || !source.HasAuthPriority {
		t.Fatalf("source = %#v, ok = %v", source, ok)
	}
	if got := WithoutErrorResponseSource(wrapped); got != original {
		t.Fatalf("restored error = %T %v, want original", got, got)
	}
}

func TestLocalErrorResponseSourceHasNoPriority(t *testing.T) {
	wrapped := WithErrorResponseSource(errors.New("local"), ErrorResponseSourceSnapshot{
		Provider:        "LOCAL",
		AuthPriority:    7,
		HasAuthPriority: true,
	})
	source, ok := ErrorResponseSourceOf(wrapped)
	if !ok || source.Provider != "local" || source.AuthPriority != 0 || source.HasAuthPriority {
		t.Fatalf("source = %#v, ok = %v", source, ok)
	}
}

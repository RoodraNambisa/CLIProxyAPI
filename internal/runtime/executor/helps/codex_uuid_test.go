package helps

import (
	"testing"

	"github.com/google/uuid"
)

func TestNewCodexUUIDv7(t *testing.T) {
	value, err := NewCodexUUIDv7()
	if err != nil {
		t.Fatalf("NewCodexUUIDv7() error = %v", err)
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		t.Fatalf("NewCodexUUIDv7() = %q: %v", value, err)
	}
	if parsed.Version() != 7 {
		t.Fatalf("NewCodexUUIDv7() version = %d, want 7", parsed.Version())
	}
}

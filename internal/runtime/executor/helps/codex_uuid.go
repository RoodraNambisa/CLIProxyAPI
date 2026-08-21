package helps

import (
	"fmt"

	"github.com/google/uuid"
)

// NewCodexUUIDv7 creates an upstream-visible Codex session identifier.
func NewCodexUUIDv7() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("generate Codex UUIDv7: %w", err)
	}
	return id.String(), nil
}

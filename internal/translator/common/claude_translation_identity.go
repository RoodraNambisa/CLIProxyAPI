package common

import (
	"crypto/sha256"
	"fmt"

	"github.com/google/uuid"
)

// NewClaudeTranslationIdentity retains the legacy process-scoped identity shape.
// Each converter caches its own value with sync.OnceValue.
func NewClaudeTranslationIdentity() string {
	account, _ := uuid.NewRandom()
	session, _ := uuid.NewRandom()
	sum := sha256.Sum256([]byte(account.String() + session.String()))
	return fmt.Sprintf("user_%x_account_%s_session_%s", sum, account.String(), session.String())
}

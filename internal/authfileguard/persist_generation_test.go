package authfileguard

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
)

func TestValidatePersistSnapshot(t *testing.T) {
	data := []byte("replacement")
	sum := sha256.Sum256(data)
	ctx := WithExpectedPersistHash(context.Background(), hex.EncodeToString(sum[:]))
	if errValidate := ValidatePersistSnapshot(ctx, data, true); errValidate != nil {
		t.Fatalf("ValidatePersistSnapshot() error = %v", errValidate)
	}
	if errValidate := ValidatePersistSnapshot(ctx, []byte("original"), true); !errors.Is(errValidate, ErrPersistGenerationStale) {
		t.Fatalf("changed snapshot error = %v, want ErrPersistGenerationStale", errValidate)
	}
	if errValidate := ValidatePersistSnapshot(ctx, nil, false); !errors.Is(errValidate, ErrPersistGenerationStale) {
		t.Fatalf("missing snapshot error = %v, want ErrPersistGenerationStale", errValidate)
	}
}

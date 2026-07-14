package authfileguard

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
)

type expectedPersistHashKey struct{}

// ErrPersistGenerationStale prevents a changed local file from being written by an older persistence attempt.
var ErrPersistGenerationStale = errors.New("auth persist generation is stale")

// WithExpectedPersistHash binds a persistence attempt to one immutable local file content hash.
func WithExpectedPersistHash(ctx context.Context, hash string) context.Context {
	hash = strings.ToLower(strings.TrimSpace(hash))
	if hash == "" {
		return ctx
	}
	return context.WithValue(ctx, expectedPersistHashKey{}, hash)
}

// ExpectedPersistHash returns the local content hash bound to a persistence attempt.
func ExpectedPersistHash(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	hash, _ := ctx.Value(expectedPersistHashKey{}).(string)
	return strings.ToLower(strings.TrimSpace(hash))
}

// ValidatePersistSnapshot rejects a missing or changed local snapshot before any remote mutation.
func ValidatePersistSnapshot(ctx context.Context, data []byte, exists bool) error {
	expectedHash := ExpectedPersistHash(ctx)
	if expectedHash == "" {
		return nil
	}
	if !exists {
		return ErrPersistGenerationStale
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != expectedHash {
		return ErrPersistGenerationStale
	}
	return nil
}

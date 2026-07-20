package auth

import (
	"context"
	"errors"
	"strings"
)

// DeleteOutcome describes the durable result of a delete operation that
// returned an error after touching an external store.
type DeleteOutcome uint8

const (
	DeleteOutcomeUncertain DeleteOutcome = iota
	DeleteOutcomeCommitted
	DeleteOutcomeRolledBack
)

// DeleteOutcomeError preserves the original error while telling the runtime
// whether the credential is durably deleted, durably retained, or unknown.
type DeleteOutcomeError struct {
	Outcome DeleteOutcome
	Err     error
}

func (e *DeleteOutcomeError) Error() string {
	if e == nil || e.Err == nil {
		return "auth delete outcome error"
	}
	return e.Err.Error()
}

func (e *DeleteOutcomeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// NewDeleteOutcomeError attaches a durable delete outcome to err.
func NewDeleteOutcomeError(outcome DeleteOutcome, err error) error {
	if err == nil {
		return nil
	}
	return &DeleteOutcomeError{Outcome: outcome, Err: err}
}

// DeleteOutcomeFromError returns the durable delete outcome carried by err.
func DeleteOutcomeFromError(err error) (DeleteOutcome, bool) {
	var outcomeErr *DeleteOutcomeError
	if !errors.As(err, &outcomeErr) || outcomeErr == nil {
		return DeleteOutcomeUncertain, false
	}
	return outcomeErr.Outcome, true
}

// SaveOutcome describes the durable result of a save operation that returned
// an error after touching local or external storage.
type SaveOutcome uint8

const (
	SaveOutcomeUncertain SaveOutcome = iota
	SaveOutcomeCommitted
	SaveOutcomeRolledBack
)

// SaveOutcomeError preserves the original error while telling the runtime
// whether the credential is durably saved, durably rolled back, or unknown.
type SaveOutcomeError struct {
	Outcome SaveOutcome
	Err     error
}

func (e *SaveOutcomeError) Error() string {
	if e == nil || e.Err == nil {
		return "auth save outcome error"
	}
	return e.Err.Error()
}

func (e *SaveOutcomeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// NewSaveOutcomeError attaches a durable save outcome to err.
func NewSaveOutcomeError(outcome SaveOutcome, err error) error {
	if err == nil {
		return nil
	}
	return &SaveOutcomeError{Outcome: outcome, Err: err}
}

// SaveOutcomeFromError returns the durable save outcome carried by err.
func SaveOutcomeFromError(err error) (SaveOutcome, bool) {
	var outcomeErr *SaveOutcomeError
	if !errors.As(err, &outcomeErr) || outcomeErr == nil {
		return SaveOutcomeUncertain, false
	}
	return outcomeErr.Outcome, true
}

// Store abstracts persistence of Auth state across restarts.
type Store interface {
	// List returns all auth records stored in the backend.
	List(ctx context.Context) ([]*Auth, error)
	// Save persists the provided auth record, replacing any existing one with same ID.
	Save(ctx context.Context, auth *Auth) (string, error)
	// Delete removes the auth record identified by id.
	Delete(ctx context.Context, id string) error
}

// ConditionalCreateStore atomically persists a record only when its ID is
// absent. Errors after a write may have reached durable storage should carry a
// SaveOutcomeError.
type ConditionalCreateStore interface {
	SaveIfAbsent(ctx context.Context, auth *Auth) (string, error)
}

type sourceHashSavePreconditionContextKey struct{}

// WithSourceHashSavePrecondition requires capable stores to replace an auth
// only when its current persisted generation matches expectedSourceHash.
func WithSourceHashSavePrecondition(ctx context.Context, expectedSourceHash string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, sourceHashSavePreconditionContextKey{}, strings.TrimSpace(expectedSourceHash))
}

// SourceHashSavePrecondition returns the source generation required by ctx.
func SourceHashSavePrecondition(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	expectedSourceHash, _ := ctx.Value(sourceHashSavePreconditionContextKey{}).(string)
	expectedSourceHash = strings.TrimSpace(expectedSourceHash)
	return expectedSourceHash, expectedSourceHash != ""
}

// SourceConditionalSaveStore atomically replaces a record only when its
// current source generation matches expectedSourceHash. Implementations must
// also honor WithSourceHashSavePrecondition when Save is called by Manager.
type SourceConditionalSaveStore interface {
	SaveIfSourceHashMatches(ctx context.Context, auth *Auth, expectedSourceHash string) (string, error)
}

// SourceConditionalDeleteStore atomically deletes a record only when its
// current source generation matches expectedSourceHash. The expected value is
// the hash of the exact source bytes; stores that normalize JSON should compare
// it with SourceHashMatchesBytes. Implementations must return a rolled-back
// delete outcome when the generation no longer matches.
type SourceConditionalDeleteStore interface {
	DeleteIfSourceHashMatches(ctx context.Context, id, expectedSourceHash string) error
}

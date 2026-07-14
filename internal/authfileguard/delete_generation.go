package authfileguard

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/google/uuid"
)

type deleteGenerationKey struct{}
type deleteAttemptKey struct{}
type deleteIdentityBindingKey struct{}

// DeleteGeneration binds one watcher deletion generation to backend identities.
type DeleteGeneration struct {
	generationID string
	expectedHash string
	mu           sync.Mutex
	identities   map[string]DeleteBackendIdentity
	persist      func(DeleteGenerationSnapshot) error
}

// DeleteBackendIdentity is the durable backend generation bound to a delete.
type DeleteBackendIdentity struct {
	Value     string `json:"value"`
	RetrySafe bool   `json:"retry_safe"`
}

// DeleteGenerationSnapshot is the durable state needed to resume a delete.
type DeleteGenerationSnapshot struct {
	Generation   string                           `json:"generation,omitempty"`
	ExpectedHash string                           `json:"expected_hash,omitempty"`
	Identities   map[string]DeleteBackendIdentity `json:"identities,omitempty"`
}

// DeleteIdentityResult classifies a backend identity check for a delete attempt.
type DeleteIdentityResult uint8

const (
	DeleteIdentityUncertain DeleteIdentityResult = iota
	DeleteIdentityMatched
)

// ErrDeleteGenerationUncertain keeps a path quarantined when deletion cannot be proven safe.
var ErrDeleteGenerationUncertain = errors.New("auth delete generation is uncertain")

// NewDeleteGeneration creates a deletion generation for one removed file.
func NewDeleteGeneration(expectedHash string) *DeleteGeneration {
	return &DeleteGeneration{
		generationID: uuid.NewString(),
		expectedHash: strings.TrimSpace(expectedHash),
		identities:   make(map[string]DeleteBackendIdentity),
	}
}

// NewDeleteGenerationFromSnapshot restores a durable delete generation.
func NewDeleteGenerationFromSnapshot(snapshot DeleteGenerationSnapshot) *DeleteGeneration {
	generation := NewDeleteGeneration(snapshot.ExpectedHash)
	generation.generationID = strings.TrimSpace(snapshot.Generation)
	for key, identity := range snapshot.Identities {
		key = strings.TrimSpace(key)
		identity.Value = strings.TrimSpace(identity.Value)
		if key == "" || identity.Value == "" {
			continue
		}
		generation.identities[key] = identity
	}
	return generation
}

// WithDeleteGeneration attaches a deletion generation to a persistence context.
func WithDeleteGeneration(ctx context.Context, generation *DeleteGeneration) context.Context {
	if generation == nil {
		return ctx
	}
	return context.WithValue(ctx, deleteGenerationKey{}, generation)
}

// DeleteGenerationFromContext returns the deletion generation attached to ctx.
func DeleteGenerationFromContext(ctx context.Context) *DeleteGeneration {
	if ctx == nil {
		return nil
	}
	generation, _ := ctx.Value(deleteGenerationKey{}).(*DeleteGeneration)
	return generation
}

// WithDeleteAttempt attaches the zero-based persistence attempt to ctx.
func WithDeleteAttempt(ctx context.Context, attempt int) context.Context {
	if attempt < 0 {
		attempt = 0
	}
	return context.WithValue(ctx, deleteAttemptKey{}, attempt)
}

// DeleteAttempt returns the zero-based persistence attempt attached to ctx.
func DeleteAttempt(ctx context.Context) int {
	if ctx == nil {
		return 0
	}
	attempt, _ := ctx.Value(deleteAttemptKey{}).(int)
	if attempt < 0 {
		return 0
	}
	return attempt
}

// WithDeleteIdentityBinding permits one live deletion attempt to bind the
// backend generation it inspected before mutating that backend.
func WithDeleteIdentityBinding(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, deleteIdentityBindingKey{}, true)
}

// DeleteIdentityBindingAllowed reports whether this live attempt may bind an
// as-yet unseen backend generation.
func DeleteIdentityBindingAllowed(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	allowed, _ := ctx.Value(deleteIdentityBindingKey{}).(bool)
	return allowed
}

// WithExpectedDeleteHash binds a watcher deletion attempt to the removed file content.
func WithExpectedDeleteHash(ctx context.Context, hash string) context.Context {
	return WithDeleteGeneration(ctx, NewDeleteGeneration(hash))
}

// ExpectedDeleteHash returns the removed file content hash bound to the deletion attempt.
func ExpectedDeleteHash(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	generation := DeleteGenerationFromContext(ctx)
	if generation == nil {
		return ""
	}
	return generation.ExpectedHash()
}

// ExpectedHash returns the local content hash removed by this generation.
func (g *DeleteGeneration) ExpectedHash() string {
	if g == nil {
		return ""
	}
	return g.expectedHash
}

// GenerationID identifies one durable delete operation across retries and
// process restarts.
func (g *DeleteGeneration) GenerationID() string {
	if g == nil {
		return ""
	}
	return g.generationID
}

// Snapshot returns a copy suitable for durable persistence.
func (g *DeleteGeneration) Snapshot() DeleteGenerationSnapshot {
	if g == nil {
		return DeleteGenerationSnapshot{}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.snapshotLocked()
}

// SetPersistHook installs a synchronous hook used before a newly observed
// backend identity is accepted for deletion.
func (g *DeleteGeneration) SetPersistHook(persist func(DeleteGenerationSnapshot) error) {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.persist = persist
	g.mu.Unlock()
}

func (g *DeleteGeneration) snapshotLocked() DeleteGenerationSnapshot {
	identities := make(map[string]DeleteBackendIdentity, len(g.identities))
	for key, identity := range g.identities {
		identities[key] = identity
	}
	return DeleteGenerationSnapshot{Generation: g.generationID, ExpectedHash: g.expectedHash, Identities: identities}
}

// BindBackendIdentity records the first backend identity and checks later attempts against it.
func (g *DeleteGeneration) BindBackendIdentity(key, identity string) bool {
	return g.BindBackendIdentityForRetry(key, identity, true)
}

// BindBackendIdentityForRetry rejects repeated attempts when the backend identity is not retry-safe.
func (g *DeleteGeneration) BindBackendIdentityForRetry(key, identity string, retrySafe bool) bool {
	return g.MatchBackendIdentity(key, identity, retrySafe, true)
}

// MatchBackendIdentity optionally binds the first identity and validates later attempts.
func (g *DeleteGeneration) MatchBackendIdentity(key, identity string, retrySafe, allowBind bool) bool {
	return g.CheckBackendIdentity(key, identity, retrySafe, allowBind) == DeleteIdentityMatched
}

// CheckBackendIdentity binds an initial identity or validates a retry against it.
func (g *DeleteGeneration) CheckBackendIdentity(key, identity string, retrySafe, allowBind bool) DeleteIdentityResult {
	if g == nil {
		return DeleteIdentityMatched
	}
	key = strings.TrimSpace(key)
	identity = strings.TrimSpace(identity)
	if key == "" || identity == "" {
		return DeleteIdentityUncertain
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	bound, ok := g.identities[key]
	if !ok {
		if !allowBind {
			return DeleteIdentityUncertain
		}
		g.identities[key] = DeleteBackendIdentity{Value: identity, RetrySafe: retrySafe}
		if g.persist != nil {
			if errPersist := g.persist(g.snapshotLocked()); errPersist != nil {
				delete(g.identities, key)
				return DeleteIdentityUncertain
			}
		}
		return DeleteIdentityMatched
	}
	if !bound.RetrySafe || !retrySafe || bound.Value != identity {
		return DeleteIdentityUncertain
	}
	return DeleteIdentityMatched
}

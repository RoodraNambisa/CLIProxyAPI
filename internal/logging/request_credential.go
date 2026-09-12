package logging

import (
	"context"
	"sync"
)

// CredentialIdentity contains only the public identity of a selected credential.
type CredentialIdentity struct {
	Provider string
	Index    string
	Name     string
}

type requestCredentialKey struct{}

type requestCredential struct {
	mu       sync.Mutex
	identity CredentialIdentity
}

func withRequestCredential(ctx context.Context) context.Context {
	return context.WithValue(ctx, requestCredentialKey{}, &requestCredential{})
}

// WithRequestCredentialFrom preserves request log ownership when an executor uses a separate parent context.
func WithRequestCredentialFrom(ctx, source context.Context) context.Context {
	if source != nil {
		if state, ok := source.Value(requestCredentialKey{}).(*requestCredential); ok {
			return context.WithValue(ctx, requestCredentialKey{}, state)
		}
	}
	return ctx
}

// SetRequestCredential records the last selected credential for the access log.
// Attempt logs must keep their own immutable identity instead of reading this value.
func SetRequestCredential(ctx context.Context, identity CredentialIdentity) {
	if ctx == nil {
		return
	}
	if state, ok := ctx.Value(requestCredentialKey{}).(*requestCredential); ok {
		state.mu.Lock()
		state.identity = identity
		state.mu.Unlock()
	}
}

func requestCredentialIdentity(ctx context.Context) CredentialIdentity {
	if state, ok := ctx.Value(requestCredentialKey{}).(*requestCredential); ok {
		state.mu.Lock()
		defer state.mu.Unlock()
		return state.identity
	}
	return CredentialIdentity{}
}

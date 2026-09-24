package logging

import (
	"context"
	"sync"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/managementdiag"
	log "github.com/sirupsen/logrus"
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
	upstream log.Fields
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
		state.upstream = nil
		state.mu.Unlock()
	}
}

// SetRequestUpstreamDiagnostic keeps bounded internal evidence out of the public
// error body. A credential switch discards it, including late prior attempts.
func SetRequestUpstreamDiagnostic(ctx context.Context, authIndex string, fields log.Fields) {
	if ctx == nil || authIndex == "" {
		return
	}
	state, ok := ctx.Value(requestCredentialKey{}).(*requestCredential)
	if !ok {
		return
	}
	snapshot := log.Fields{}
	for _, key := range []string{"stage", "code", "status", "response_type", "content_type", "target_host", "target_path", "cf_ray", "response_body_truncated"} {
		if value, exists := fields[key]; exists {
			snapshot[key] = value
		}
	}
	if value, ok := fields["response_body"].(managementdiag.ManagementOnlyValue); ok {
		snapshot["response_body"] = value
	}
	if value, ok := fields["response_text"].(managementdiag.ManagementOnlyValue); ok {
		snapshot["response_text"] = value
	}
	state.mu.Lock()
	if state.identity.Index == authIndex {
		state.upstream = snapshot
	}
	state.mu.Unlock()
}

func requestUpstreamDiagnostic(ctx context.Context) log.Fields {
	if state, ok := ctx.Value(requestCredentialKey{}).(*requestCredential); ok {
		state.mu.Lock()
		defer state.mu.Unlock()
		fields := make(log.Fields, len(state.upstream))
		for key, value := range state.upstream {
			fields[key] = value
		}
		return fields
	}
	return nil
}

func requestCredentialIdentity(ctx context.Context) CredentialIdentity {
	if state, ok := ctx.Value(requestCredentialKey{}).(*requestCredential); ok {
		state.mu.Lock()
		defer state.mu.Unlock()
		return state.identity
	}
	return CredentialIdentity{}
}

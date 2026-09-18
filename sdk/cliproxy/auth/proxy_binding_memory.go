package auth

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"strings"
)

const ProxyBindingMemoryKey = "proxy_binding"

// ProxyBindingMemory refers to a configured node without storing its address,
// password or observed exit IP. Placeholder values are generated session IDs.
type ProxyBindingMemory struct {
	Version      int      `json:"version"`
	NodeID       string   `json:"node_id"`
	Port         int      `json:"port"`
	Placeholders []string `json:"placeholders,omitempty"`
}

type proxyBindingRecorder interface {
	RememberCredentialBinding(context.Context, *Auth, string) (*Auth, error)
}

// LockProxyBindingMutation lets a resolver serialize binding selection and its
// metadata write under one outer credential lock. Nested metadata writes use
// the token's own gate; acquiring that gate here again would deadlock them.
func (m *Manager) LockProxyBindingMutation(ctx context.Context, auth *Auth) (context.Context, func(), error) {
	if token := authMutationTokenForManager(ctx, m); token != nil {
		if auth == nil || token.id != auth.ID {
			return ctx, nil, ErrAuthMutationIdentityChanged
		}
		return ctx, func() {}, nil
	}
	return m.LockAuthMutation(ctx, auth)
}

// ReadProxyBindingMemory accepts only a small versioned, non-secret reference.
func ReadProxyBindingMemory(auth *Auth) *ProxyBindingMemory {
	if auth == nil {
		return nil
	}
	value, ok := auth.Metadata[ProxyBindingMemoryKey].(map[string]any)
	if !ok || len(value) > 4 {
		return nil
	}
	raw, err := json.Marshal(value)
	if err != nil || len(raw) > 4096 {
		return nil
	}
	var memory ProxyBindingMemory
	if json.Unmarshal(raw, &memory) != nil || memory.Version != 1 || memory.Port < 1 || memory.Port > 65535 || len(memory.NodeID) != 64 || len(memory.Placeholders) > 32 {
		return nil
	}
	if _, err := hex.DecodeString(memory.NodeID); err != nil {
		return nil
	}
	for _, value := range memory.Placeholders {
		if len(value) < 1 || len(value) > 128 {
			return nil
		}
		for _, char := range value {
			if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || strings.ContainsRune("-._~", char)) {
				return nil
			}
		}
	}
	return &memory
}

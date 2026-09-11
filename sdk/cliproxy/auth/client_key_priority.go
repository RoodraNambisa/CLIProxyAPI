package auth

import (
	"context"
	"crypto/sha256"
	"slices"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
)

// ClientAPIKeyScopeContextKey carries an authenticated issuer digest for local grants.
// Only server authentication code may set this Gin value; request fields are never read.
const ClientAPIKeyScopeContextKey = "clientAPIKeyPriorityScope"

type clientKeyPriorityPolicy struct {
	allowed, excluded []int
	denyAll           bool
}

type clientKeyPrioritySnapshot struct {
	manager *Manager
	policy  clientKeyPriorityPolicy
}

type clientKeyPrioritySnapshotKey struct{}

// ClientAPIKeyPriorityChoices lists configured credential tiers without cloning secrets.
func (m *Manager) ClientAPIKeyPriorityChoices() []int {
	priorities := []int{0}
	if m != nil {
		m.mu.RLock()
		for _, credential := range m.auths {
			priority := authPriority(credential)
			if int64(priority) >= -config.APIKeyPriorityLimit && int64(priority) <= config.APIKeyPriorityLimit {
				priorities = append(priorities, priority)
			}
		}
		m.mu.RUnlock()
	}
	slices.Sort(priorities)
	return slices.Compact(priorities)
}

func compileClientKeyPriorities(cfg *config.Config) map[[sha256.Size]byte]clientKeyPriorityPolicy {
	if cfg == nil || len(cfg.APIKeys) == 0 {
		return nil
	}
	result := make(map[[sha256.Size]byte]clientKeyPriorityPolicy, len(cfg.APIKeys))
	for _, key := range cfg.APIKeys {
		result[sha256.Sum256([]byte(strings.TrimSpace(key)))] = clientKeyPriorityPolicy{}
	}
	for _, group := range cfg.APIKeyGroups {
		digest := sha256.Sum256([]byte(strings.TrimSpace(group.APIKey)))
		if _, exists := result[digest]; exists {
			result[digest] = clientKeyPriorityPolicy{allowed: slices.Clone(group.AllowedPriorities), excluded: slices.Clone(group.ExcludedPriorities)}
		}
	}
	return result
}

func clientKeyScope(ctx context.Context) (digest [sha256.Size]byte, required bool) {
	if ctx == nil {
		return
	}
	c, _ := ctx.Value("gin").(*gin.Context)
	if c == nil {
		return
	}
	if value, exists := c.Get(ClientAPIKeyScopeContextKey); exists {
		if scope, ok := value.([sha256.Size]byte); ok && scope != digest {
			return scope, true
		}
	}
	if key := c.GetString("apiKey"); key != "" {
		digest = sha256.Sum256([]byte(key))
		required = c.GetString("accessProvider") == sdkaccess.DefaultAccessProviderName
	}
	return digest, required
}

// ClientAPIKeyPriorityScope retains a configured issuer's identity without its key.
func (m *Manager) ClientAPIKeyPriorityScope(ctx context.Context) [sha256.Size]byte {
	digest, required := clientKeyScope(ctx)
	if required {
		return digest
	}
	if policy := m.selectionPolicy(ctx); policy != nil {
		if _, configured := policy.clientKeyPriorities[digest]; configured {
			return digest
		}
	}
	return [sha256.Size]byte{}
}

// ClientAPIKeyAllowsCredential checks a new connection to an already leased credential.
// Established sessions and authenticated cleanup do not call this admission check.
func (m *Manager) ClientAPIKeyAllowsCredential(ctx context.Context, credential *Auth) bool {
	if credential == nil {
		return false
	}
	ctx = m.WithRoutingPolicySnapshot(ctx)
	current, exists := m.GetByID(credential.ID)
	return exists && clientKeyPriorityAllowed(ctx, current)
}

func (m *Manager) withClientKeyPrioritySnapshot(ctx context.Context, routing *routingRequestPolicy) context.Context {
	if snapshot, _ := ctx.Value(clientKeyPrioritySnapshotKey{}).(*clientKeyPrioritySnapshot); snapshot != nil && snapshot.manager == m {
		return ctx
	}
	digest, required := clientKeyScope(ctx)
	policy, exists := routing.clientKeyPriorities[digest]
	// Removed configured keys and their grants cannot become unrestricted.
	if required && !exists {
		policy.denyAll = true
	}
	return context.WithValue(ctx, clientKeyPrioritySnapshotKey{}, &clientKeyPrioritySnapshot{manager: m, policy: policy})
}

func clientKeyPriorityAllowed(ctx context.Context, credential *Auth) bool {
	if credential == nil {
		return false
	}
	if ctx == nil {
		return true
	}
	snapshot, _ := ctx.Value(clientKeyPrioritySnapshotKey{}).(*clientKeyPrioritySnapshot)
	if snapshot == nil {
		return true
	}
	priority := authPriority(credential)
	policy := snapshot.policy
	return !policy.denyAll && !slices.Contains(policy.excluded, priority) && (len(policy.allowed) == 0 || slices.Contains(policy.allowed, priority))
}

func clientKeyPriorityFilter(ctx context.Context, allowed func(*Auth) bool) func(*Auth) bool {
	return func(credential *Auth) bool {
		return clientKeyPriorityAllowed(ctx, credential) && (allowed == nil || allowed(credential))
	}
}

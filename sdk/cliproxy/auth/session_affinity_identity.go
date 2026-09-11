package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/session"
)

const affinityIdentityMetadataKey = "session_affinity_identity_snapshot"
const basicAffinityIdentityMetadataKey = "basic_session_affinity_identity_snapshot"

type basicAffinityIdentity struct {
	selector          *SessionAffinitySelector
	primary, fallback string
}

// Capture basic identities before provider translation and automatic cache keys.
// Keep cross-client-key binding semantics and retain only detached identifiers.
func (s *SessionAffinitySelector) withBasicAffinityIdentity(ctx context.Context, req core.Request, opts core.Options) core.Options {
	if captured, ok := opts.Metadata[basicAffinityIdentityMetadataKey].(basicAffinityIdentity); ok && captured.selector == s {
		return opts
	}
	headers := opts.Headers
	if headers == nil && ctx != nil {
		if c, _ := ctx.Value("gin").(*gin.Context); c != nil && c.Request != nil {
			headers = c.Request.Header
		}
	}
	payload := opts.OriginalRequest
	if len(payload) == 0 {
		payload = req.Payload
	}
	primary, fallback := extractSessionIDs(headers, payload, opts.Metadata)
	metadata := make(map[string]any, len(opts.Metadata)+1)
	for key, value := range opts.Metadata {
		metadata[key] = value
	}
	metadata[basicAffinityIdentityMetadataKey] = basicAffinityIdentity{selector: s, primary: strings.Clone(primary), fallback: strings.Clone(fallback)}
	opts.Metadata = metadata
	return opts
}

type affinityRequestIdentity struct {
	scope          string
	identity       session.Identity
	legacyPrimary  string
	legacyFallback string
	history        *session.History
}

func captureAffinityIdentity(ctx context.Context, req core.Request, opts core.Options, includeHistory ...bool) affinityRequestIdentity {
	if captured, ok := opts.Metadata[affinityIdentityMetadataKey].(affinityRequestIdentity); ok {
		return captured
	}
	scope := util.AuthenticatedSessionScope(ctx)
	if scope == "" {
		if provided, ok := opts.Metadata[core.CallerScopeMetadataKey].(string); ok && strings.TrimSpace(provided) != "" {
			digest := sha256.Sum256([]byte("sdk-affinity-caller-v1:" + provided))
			scope = hex.EncodeToString(digest[:])
		}
	}
	if scope == "" {
		return affinityRequestIdentity{}
	}
	headers := opts.Headers
	if headers == nil && ctx != nil {
		if c, _ := ctx.Value("gin").(*gin.Context); c != nil && c.Request != nil {
			headers = c.Request.Header
		}
	}
	payload := opts.OriginalRequest
	if len(payload) == 0 {
		payload = req.Payload
	}
	executionID, _ := opts.Metadata[core.ExecutionSessionMetadataKey].(string)
	identity, _ := session.ExtractExplicitIdentity(headers, payload, executionID)
	captured := affinityRequestIdentity{scope: scope, identity: identity}
	if len(includeHistory) > 0 && includeHistory[0] && (identity.SessionID == "" || strings.HasPrefix(identity.SessionID, "execution:")) {
		history := session.FingerprintHistory(opts.SourceFormat, payload)
		captured.history = &history
		if history.Usable() {
			// The execution ID is a transport fallback, not a client-provided
			// conversation identity. It must not mask complete history evidence.
			captured.identity = session.Identity{}
		}
	} else if identity.SessionID == "" {
		captured.legacyPrimary, captured.legacyFallback = extractMessageHashIDs(payload)
	}
	return captured
}

// withAffinityIdentity detaches optional inference inputs before provider
// preparation, retries, and request-body release. It never mutates caller maps.
func withAffinityIdentity(ctx context.Context, req core.Request, opts core.Options, includeHistory ...bool) core.Options {
	if _, ok := opts.Metadata[affinityIdentityMetadataKey].(affinityRequestIdentity); ok {
		return opts
	}
	captured := captureAffinityIdentity(ctx, req, opts, includeHistory...)
	metadata := make(map[string]any, len(opts.Metadata)+1)
	for key, value := range opts.Metadata {
		metadata[key] = value
	}
	metadata[affinityIdentityMetadataKey] = captured
	opts.Metadata = metadata
	return opts
}

func scopedAffinityID(scope, id string) string {
	if scope == "" || id == "" {
		return ""
	}
	raw, _ := json.Marshal([]string{"session-affinity-v1", scope, id})
	digest := sha256.Sum256(raw)
	return "scoped:" + hex.EncodeToString(digest[:])
}

func (s *SessionAffinitySelector) sessionIDs(ctx context.Context, opts core.Options) (string, string) {
	if captured, ok := opts.Metadata[basicAffinityIdentityMetadataKey].(basicAffinityIdentity); ok && captured.selector == s {
		return captured.primary, captured.fallback
	}
	if s != nil && (s.subagents || s.lcp) {
		captured := captureAffinityIdentity(ctx, core.Request{}, opts, s.lcp)
		if captured.scope != "" {
			if captured.identity.SessionID == "" {
				if s.lcp {
					return "", ""
				}
				return scopedAffinityID(captured.scope, captured.legacyPrimary), scopedAffinityID(captured.scope, captured.legacyFallback)
			}
			primary := scopedAffinityID(captured.scope, captured.identity.SessionID)
			parent := ""
			if s.subagents && (captured.identity.IsSubagent || captured.identity.IsFork) {
				parent = scopedAffinityID(captured.scope, captured.identity.ParentSessionID)
			}
			return primary, parent
		}
	}
	return extractSessionIDs(opts.Headers, opts.OriginalRequest, opts.Metadata)
}

// An explicit parent is only a preference until the child has a binding of its
// own. A parent's exhausted capacity must not turn that preference into a lock.
func (s *SessionAffinitySelector) cachedStrictAuthID(ctx context.Context, provider, model string, opts core.Options) string {
	if s != nil && (s.subagents || s.lcp) {
		captured := captureAffinityIdentity(ctx, core.Request{}, opts, s.lcp)
		if captured.scope != "" && s.lcp && captured.identity.SessionID == "" {
			return ""
		}
		if captured.scope != "" && captured.identity.ParentSessionID != "" {
			primary, _ := s.sessionIDs(ctx, opts)
			authID, _ := s.cache.GetAndRefresh(provider + "::" + primary + "::" + canonicalModelKey(model))
			return authID
		}
	}
	return s.cachedAuthID(provider, model, opts, ctx)
}

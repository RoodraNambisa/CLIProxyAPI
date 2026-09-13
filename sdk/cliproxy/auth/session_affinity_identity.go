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
const grokSessionIdentityMetadataKey = "grok_session_identity_policy"

// ProviderSessionFingerprint reuses captured routing identity even when empty.
// Without routing affinity, enabled provider identity policies may resolve one
// identity themselves; inferred histories retain authenticated caller scope.
func ProviderSessionFingerprint(ctx context.Context, req core.Request, opts core.Options, useHistory bool, explicit ...session.Identity) (string, string) {
	if _, ok := opts.Metadata[basicAffinityIdentityMetadataKey]; ok {
		return SessionAffinityFingerprint(opts), "routing"
	}
	if _, ok := opts.Metadata[affinityIdentityMetadataKey]; ok {
		return SessionAffinityFingerprint(opts), "routing"
	}
	payload := opts.OriginalRequest
	if len(payload) == 0 {
		payload = req.Payload
	}
	executionID, _ := opts.Metadata[core.ExecutionSessionMetadataKey].(string)
	var identity session.Identity
	if len(explicit) > 0 {
		identity = explicit[0]
	} else {
		identity, _ = session.ExtractExplicitIdentity(opts.Headers, payload, "")
	}
	if identity.SessionID != "" {
		digest := messageTextDigest(identity.SessionID)
		return hex.EncodeToString(digest[:]), "explicit"
	}
	if useHistory {
		primary, fallback := extractMessageHashIDs(payload)
		if fallback != "" {
			primary = fallback
		}
		scope := util.AuthenticatedSessionScope(ctx)
		if scope == "" {
			scope, _ = opts.Metadata[core.CallerScopeMetadataKey].(string)
		}
		if primary != "" && scope != "" {
			return scopedAffinityID(scope, primary), "history"
		}
	}
	if executionID != "" {
		digest := messageTextDigest("execution:" + executionID)
		return hex.EncodeToString(digest[:]), "execution"
	}
	return "", "request"
}

type basicAffinityIdentity struct {
	selector          *SessionAffinitySelector
	primary, fallback string
}

// SessionAffinityFingerprint reuses the immutable routing snapshot for provider
// identity pools. It never parses request bodies or establishes an auth binding.
// Explicit sessions share pool affinity across callers; inferred LCP histories
// retain their authenticated scope. An empty result leaves provider fallback
// behavior unchanged when routing affinity is disabled or has no usable identity.
func SessionAffinityFingerprint(opts core.Options) string {
	var id string
	if captured, ok := opts.Metadata[basicAffinityIdentityMetadataKey].(basicAffinityIdentity); ok {
		id = captured.primary
		if captured.fallback != "" {
			// The first-user anchor survives the addition of the first assistant.
			id = captured.fallback
		}
	} else if captured, ok := opts.Metadata[affinityIdentityMetadataKey].(affinityRequestIdentity); ok && captured.scope != "" {
		id = captured.identity.SessionID
		if id == "" && captured.history != nil {
			if digest, ok := captured.history.InitialUserPrefixDigest(); ok {
				id = scopedAffinityID(captured.scope, "history:"+hex.EncodeToString(digest[:]))
			}
		} else if id == "" {
			id = captured.legacyPrimary
			if captured.legacyFallback != "" {
				id = captured.legacyFallback
			}
		}
	}
	if id == "" {
		return ""
	}
	digest := messageTextDigest(id)
	return hex.EncodeToString(digest[:])
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
	primary, fallback := extractSessionIDsWithHistory(headers, payload, opts.Metadata, !s.disableHistory)
	if grok, _ := opts.Metadata[grokSessionIdentityMetadataKey].(bool); grok && strings.HasPrefix(primary, "msg:") {
		scope := util.AuthenticatedSessionScope(ctx)
		if scope == "" {
			scope, _ = opts.Metadata[core.CallerScopeMetadataKey].(string)
		}
		if scope == "" {
			primary, fallback = "", ""
		} else {
			primary = scopedAffinityID(scope, primary)
			if fallback != "" {
				fallback = scopedAffinityID(scope, fallback)
			}
		}
	}
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
	return captureAffinityIdentityWithHistoryPolicy(ctx, req, opts, len(includeHistory) > 0 && includeHistory[0], true)
}

func captureAffinityIdentityWithHistoryPolicy(ctx context.Context, req core.Request, opts core.Options, includeHistory, useHistory bool) affinityRequestIdentity {
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
	var identity session.Identity
	if grok, _ := opts.Metadata[grokSessionIdentityMetadataKey].(bool); grok {
		identity, _ = session.ExtractGrokExplicitIdentity(headers, payload, executionID)
	} else {
		identity, _ = session.ExtractExplicitIdentity(headers, payload, executionID)
	}
	captured := affinityRequestIdentity{scope: scope, identity: identity}
	if !useHistory {
		return captured
	}
	if includeHistory && (identity.SessionID == "" || strings.HasPrefix(identity.SessionID, "execution:")) {
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
	return withCapturedAffinityIdentity(opts, captureAffinityIdentity(ctx, req, opts, includeHistory...))
}

func withCapturedAffinityIdentity(opts core.Options, captured affinityRequestIdentity) core.Options {
	if _, ok := opts.Metadata[affinityIdentityMetadataKey].(affinityRequestIdentity); ok {
		return opts
	}
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
		captured := captureAffinityIdentityWithHistoryPolicy(ctx, core.Request{}, opts, s.lcp, !s.disableHistory)
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
	return extractSessionIDsWithHistory(opts.Headers, opts.OriginalRequest, opts.Metadata, s == nil || !s.disableHistory)
}

// An explicit parent is only a preference until the child has a binding of its
// own. A parent's exhausted capacity must not turn that preference into a lock.
func (s *SessionAffinitySelector) cachedStrictAuthID(ctx context.Context, provider, model string, opts core.Options) string {
	if s != nil && (s.subagents || s.lcp) {
		captured := captureAffinityIdentityWithHistoryPolicy(ctx, core.Request{}, opts, s.lcp, !s.disableHistory)
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

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

type affinityRequestIdentity struct {
	scope    string
	identity session.Identity
}

func captureAffinityIdentity(ctx context.Context, req core.Request, opts core.Options) affinityRequestIdentity {
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
	identity, _ := session.ExtractCodexIdentity(headers, payload)
	return affinityRequestIdentity{scope: scope, identity: identity}
}

// withAffinityIdentity detaches optional inference inputs before provider
// preparation, retries, and request-body release. It never mutates caller maps.
func withAffinityIdentity(ctx context.Context, req core.Request, opts core.Options) core.Options {
	if _, ok := opts.Metadata[affinityIdentityMetadataKey].(affinityRequestIdentity); ok {
		return opts
	}
	captured := captureAffinityIdentity(ctx, req, opts)
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

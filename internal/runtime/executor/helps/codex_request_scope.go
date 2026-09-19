package helps

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/tidwall/gjson"
)

// CodexRequestScope describes original client identity, before outbound mapping.
type CodexRequestScope struct {
	ThreadID  string
	SessionID string
	Kind      string
}

func SnapshotCodexRequestScope(payload []byte, headers ...http.Header) CodexRequestScope {
	metadata := gjson.GetBytes(payload, "client_metadata")
	turn := codexRoutingTurnObject(metadata.Get("x-codex-turn-metadata"))
	scope := CodexRequestScope{
		ThreadID:  firstExplicitCodexRoutingString(turn.Get("thread_id"), metadata.Get("thread_id")),
		SessionID: firstExplicitCodexRoutingString(turn.Get("session_id"), metadata.Get("session_id")),
		Kind:      firstExplicitCodexRoutingString(turn.Get("request_kind"), metadata.Get("request_kind")),
	}
	for _, header := range headers {
		embedded := gjson.Parse(codexRoutingHeader(header, "X-Codex-Turn-Metadata"))
		if scope.ThreadID == "" {
			scope.ThreadID = firstCodexIdentityValue(codexRoutingHeader(header, "Thread-Id"), embedded.Get("thread_id").String(), codexRoutingHeader(header, "X-Client-Request-Id"))
		}
		if scope.SessionID == "" {
			scope.SessionID = firstCodexIdentityValue(embedded.Get("session_id").String(), codexRoutingHeader(header, "Session-Id"), codexRoutingHeader(header, "session_id"))
		}
		if scope.Kind == "" {
			scope.Kind = firstExplicitCodexRoutingString(embedded.Get("request_kind"))
		}
	}
	if scope.Kind == "" {
		scope.Kind = "turn"
	}
	if generate := gjson.GetBytes(payload, "generate"); generate.Exists() && !generate.Bool() {
		scope.Kind = "prewarm"
	}
	return scope
}

// Key excludes mutable outbound slots and provider credentials. Unidentified
// callers must still be isolated by a connection-local owner.
func (scope CodexRequestScope) Key() string {
	encoded, _ := json.Marshal(scope)
	return string(encoded)
}

type codexRequestScopeContextKey struct{}

func WithCodexRequestScope(ctx context.Context, scope CodexRequestScope, model string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if scope.ThreadID == "" && scope.SessionID == "" && (scope.Kind == "" || scope.Kind == "turn") {
		return ctx
	}
	encoded, _ := json.Marshal([]string{scope.ThreadID, scope.SessionID, scope.Kind, strings.TrimSpace(model)})
	return context.WithValue(ctx, codexRequestScopeContextKey{}, sha256.Sum256(encoded))
}

func CodexRequestScopeDigest(ctx context.Context) [sha256.Size]byte {
	if ctx != nil {
		if digest, ok := ctx.Value(codexRequestScopeContextKey{}).([sha256.Size]byte); ok {
			return digest
		}
	}
	return [sha256.Size]byte{}
}

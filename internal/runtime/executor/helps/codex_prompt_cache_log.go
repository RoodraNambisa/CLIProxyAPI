package helps

import (
	"context"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

type codexPromptCacheLogKey struct{}

// SnapshotCodexPromptCacheLog protects explicit keys independently of the wire
// passthrough flag and never retains the source request buffer.
func SnapshotCodexPromptCacheLog(ctx context.Context, payload []byte) *util.PromptCacheLogRedactor {
	key := gjson.GetBytes(payload, "prompt_cache_key")
	if key.Type != gjson.String || key.Str == "" {
		return nil
	}
	if ctx != nil {
		redactor := util.PromptCacheLogForGin(ginContextFrom(ctx))
		if redactor.ProtectsKey(key.Str) {
			return redactor
		}
	}
	return util.NewPromptCacheLogRedactor(key.Str)
}

// WithCodexPromptCacheLogRedaction fixes the diagnostic policy for this attempt.
// A typed nil policy also prevents inheriting another turn's Gin policy.
func WithCodexPromptCacheLogRedaction(ctx context.Context, redactor *util.PromptCacheLogRedactor) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, codexPromptCacheLogKey{}, redactor)
}

func redactCodexPromptCacheLog(ctx context.Context, text string) string {
	return CodexPromptCacheLogRedactor(ctx).Redact(text)
}

// RedactCodexPromptCacheLog changes a diagnostic copy, not the public error chain.
func RedactCodexPromptCacheLog(ctx context.Context, text string) string {
	return redactCodexPromptCacheLog(ctx, text)
}

func CodexPromptCacheLogRedactor(ctx context.Context) *util.PromptCacheLogRedactor {
	if ctx == nil {
		return nil
	}
	if redactor, ok := ctx.Value(codexPromptCacheLogKey{}).(*util.PromptCacheLogRedactor); ok {
		return redactor
	}
	return util.PromptCacheLogForGin(ginContextFrom(ctx))
}

func writeHeadersWithCodexCacheRedaction(ctx context.Context, builder *strings.Builder, headers http.Header) {
	writeHeaders(builder, CodexPromptCacheLogRedactor(ctx).Headers(headers))
}

// DebugCodexResponseError avoids parsing and compiling a matcher when debug
// logging is disabled. Redact the scalar detail before adding the log prefix.
func DebugCodexResponseError(ctx context.Context, status int, contentType string, body []byte) {
	if !log.IsLevelEnabled(log.DebugLevel) {
		return
	}
	detail := redactCodexPromptCacheLog(ctx, SummarizeErrorBody(contentType, body))
	LogWithRequestID(ctx).Debugf("request error, error status: %d, error message: %s", status, detail)
}

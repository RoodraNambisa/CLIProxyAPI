package helps

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"golang.org/x/net/http/httpguts"
)

type codexCacheSessionDigestKey struct{}

// WithCodexCacheSession freezes the connection reuse identity for this turn.
// A zero digest also clears a previous turn's opt-in policy without retaining IDs.
func WithCodexCacheSession(ctx context.Context, snapshot CodexPromptCacheKeySnapshot) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	var digest [sha256.Size]byte
	if snapshot.SessionID != "" {
		digest = sha256.Sum256([]byte(snapshot.SessionID))
	}
	if CodexCacheSessionDigest(ctx) == digest {
		return ctx
	}
	return context.WithValue(ctx, codexCacheSessionDigestKey{}, digest)
}

func CodexCacheSessionDigest(ctx context.Context) [sha256.Size]byte {
	if ctx == nil {
		return [sha256.Size]byte{}
	}
	digest, _ := ctx.Value(codexCacheSessionDigestKey{}).([sha256.Size]byte)
	return digest
}

func explicitCodexRoutingString(value gjson.Result) string {
	if value.Type != gjson.String || strings.TrimSpace(value.Str) == "" {
		return ""
	}
	return strings.Clone(value.Str)
}

func codexRoutingJSONString(value string) []byte {
	encoded, _ := json.Marshal(value)
	return encoded
}

func firstExplicitCodexRoutingString(values ...gjson.Result) string {
	for _, value := range values {
		if result := explicitCodexRoutingString(value); result != "" {
			return result
		}
	}
	return ""
}

func codexRoutingHeader(headers http.Header, name string) string {
	if values := headers[http.CanonicalHeaderKey(name)]; len(values) > 0 {
		return values[0]
	}
	for key, values := range headers {
		if strings.EqualFold(key, name) && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}

func setCodexRoutingHeader(headers http.Header, name, value string) {
	for key := range headers {
		if strings.EqualFold(key, name) {
			delete(headers, key)
		}
	}
	headers.Set(name, value)
}

func codexRoutingTurnObject(value gjson.Result) gjson.Result {
	if value.IsObject() {
		return value
	}
	if value.Type == gjson.String && gjson.Valid(value.Str) {
		return gjson.Parse(value.Str)
	}
	return gjson.Result{}
}

// Validate rejects values that HTTP would reject or silently trim before routing.
func (snapshot CodexPromptCacheKeySnapshot) Validate() error {
	if snapshot.SessionID != "" && (!httpguts.ValidHeaderFieldValue(snapshot.SessionID) || strings.TrimSpace(snapshot.SessionID) != snapshot.SessionID) {
		return errors.New("session_id must be an HTTP header value without surrounding whitespace; when absent, prompt_cache_key must satisfy the same requirement")
	}
	return nil
}

// ApplyFinal restores routing fields after all identity projections. Only the
// request envelope and existing protocol metadata are touched, never tool data.
// Large payloads are rebuilt at most once; unchanged payloads retain their buffer.
func (snapshot CodexPromptCacheKeySnapshot) ApplyFinal(payload []byte, headers http.Header) ([]byte, error) {
	if snapshot.Key == "" && snapshot.SessionID == "" {
		return payload, nil
	}
	if err := snapshot.Validate(); err != nil {
		return nil, err
	}
	fields := make([]codexSessionIdentityRawField, 0, 16)
	changed, hasKey := false, false
	err := visitCodexTopLevelFields(payload, func(key, value []byte) {
		updated := value
		switch {
		case codexJSONKeyEquals(key, "prompt_cache_key"):
			hasKey = true
			if snapshot.Key != "" && gjson.ParseBytes(value).Str != snapshot.Key {
				updated = codexRoutingJSONString(snapshot.Key)
			}
		case codexJSONKeyEquals(key, "session_id"):
			if snapshot.SessionID != "" && gjson.ParseBytes(value).Str != snapshot.SessionID {
				updated = codexRoutingJSONString(snapshot.SessionID)
			}
		case codexJSONKeyEquals(key, "client_metadata"):
			if gjson.ParseBytes(value).IsObject() {
				updated = snapshot.applyMetadata(value, true)
			}
		}
		changed = changed || !bytes.Equal(value, updated)
		fields = append(fields, codexSessionIdentityRawField{key: key, value: updated})
	})
	if err != nil {
		return nil, err
	}
	if headers != nil {
		if snapshot.SessionID != "" {
			setCodexRoutingHeader(headers, "Session-Id", snapshot.SessionID)
			setCodexRoutingHeader(headers, "session_id", snapshot.SessionID)
		}
		if raw := codexRoutingHeader(headers, "X-Codex-Turn-Metadata"); gjson.Valid(raw) && gjson.Parse(raw).IsObject() {
			setCodexRoutingHeader(headers, "X-Codex-Turn-Metadata", string(snapshot.applyMetadata([]byte(raw), false)))
		}
	}
	if !changed && (hasKey || snapshot.Key == "") {
		return payload, nil
	}
	out := make([]byte, 0, len(payload)+len(snapshot.Key)+256)
	out = append(out, '{')
	count := 0
	for _, field := range fields {
		out = appendCodexRawJSONField(out, &count, field.key, field.value)
	}
	if !hasKey && snapshot.Key != "" {
		out = appendCodexNamedJSONField(out, &count, "prompt_cache_key", string(codexRoutingJSONString(snapshot.Key)))
	}
	return append(out, '}'), nil
}

func (snapshot CodexPromptCacheKeySnapshot) applyMetadata(payload []byte, client bool) []byte {
	if snapshot.SessionID != "" {
		payload, _ = SetStringIfDifferent(payload, "session_id", snapshot.SessionID)
	}
	if snapshot.Key != "" && gjson.GetBytes(payload, "prompt_cache_key").Exists() {
		payload, _ = SetStringIfDifferent(payload, "prompt_cache_key", snapshot.Key)
	}
	if !client {
		return payload
	}
	value := gjson.GetBytes(payload, "x-codex-turn-metadata")
	turn := codexRoutingTurnObject(value)
	if !turn.IsObject() {
		return payload
	}
	updated := snapshot.applyMetadata([]byte(turn.Raw), false)
	if bytes.Equal(updated, []byte(turn.Raw)) {
		return payload
	}
	if value.Type == gjson.String {
		payload, _ = sjson.SetBytes(payload, "x-codex-turn-metadata", string(updated))
	} else {
		payload, _ = sjson.SetRawBytes(payload, "x-codex-turn-metadata", updated)
	}
	return payload
}

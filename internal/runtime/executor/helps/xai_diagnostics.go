package helps

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"golang.org/x/net/http/httpguts"
)

type XAIHeaderDiagnostic struct {
	Name     string `json:"name"`
	Value    string `json:"value,omitempty"`
	Source   string `json:"source"`
	Redacted bool   `json:"redacted,omitempty"`
	Missing  bool   `json:"missing,omitempty"`
	Example  bool   `json:"example,omitempty"`
}
type XAICompressionDiagnostic struct {
	Enabled       bool   `json:"enabled"`
	Reason        string `json:"reason"`
	SettingsURL   string `json:"settings_url,omitempty"`
	Supported     *bool  `json:"supported,omitempty"`
	OriginalBytes *int64 `json:"original_bytes,omitempty"`
	SentBytes     *int64 `json:"sent_bytes,omitempty"`
}
type XAITransmission struct {
	ID           uint64                   `json:"id"`
	Stage        string                   `json:"stage"`
	URL          string                   `json:"url"`
	URLSource    string                   `json:"url_source,omitempty"`
	Headers      []XAIHeaderDiagnostic    `json:"headers"`
	Compression  XAICompressionDiagnostic `json:"compression"`
	Capabilities *XAIRequestCapabilities  `json:"capabilities,omitempty"`
	Status       int                      `json:"status,omitempty"`
	Preview      bool                     `json:"preview,omitempty"`
}
type xaiDiagnosticKey struct{}
type xaiDiagnosticState struct {
	sync.Mutex
	sources      map[string]string
	compression  XAICompressionDiagnostic
	capabilities *XAIRequestCapabilities
	sequence     uint64
	preview      bool
	observe      func(XAITransmission)
}

func WithXAIDiagnostics(ctx context.Context, preview bool, observe func(XAITransmission)) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, xaiDiagnosticKey{}, &xaiDiagnosticState{sources: map[string]string{}, preview: preview, observe: observe})
}
func xaiDiagnostics(ctx context.Context) *xaiDiagnosticState {
	if ctx == nil {
		return nil
	}
	state, _ := ctx.Value(xaiDiagnosticKey{}).(*xaiDiagnosticState)
	return state
}
func NoteXAIHeaderSources(ctx context.Context, source string, names ...string) {
	if state := xaiDiagnostics(ctx); state != nil {
		state.Lock()
		defer state.Unlock()
		for _, name := range names {
			state.sources[http.CanonicalHeaderKey(name)] = source
		}
	}
}
func NoteXAIHeaderMap(ctx context.Context, source string, headers http.Header) {
	for key := range headers {
		NoteXAIHeaderSources(ctx, source, key)
	}
}
func NoteXAICapabilities(ctx context.Context, caps XAIRequestCapabilities) {
	if state := xaiDiagnostics(ctx); state != nil {
		state.Lock()
		defer state.Unlock()
		state.capabilities = &caps
	}
}
func NoteXAICompression(ctx context.Context, value XAICompressionDiagnostic) {
	if state := xaiDiagnostics(ctx); state != nil {
		state.Lock()
		defer state.Unlock()
		state.compression = value
	}
}

func SafeXAIHeaders(headers http.Header, sources map[string]string, auth *coreauth.Auth, preview bool) []XAIHeaderDiagnostic {
	safe := map[string]bool{"Accept": true, "Content-Type": true, "Content-Encoding": true, "Content-Length": true, "Connection": true, "User-Agent": true, "X-Grok-Client-Version": true, "X-Grok-Client-Identifier": true, "X-Grok-Client-Mode": true, "X-Grok-Model-Override": true, "X-Xai-Token-Auth": true, "X-Authenticateresponse": true, "X-Grok-Client-Surface": true}
	secrets := []string{}
	authorization := strings.TrimPrefix(headers.Get("Authorization"), "Bearer ")
	if len(authorization) > 3 {
		secrets = append(secrets, authorization)
	}
	if auth != nil {
		for _, key := range []string{"api_key", "access_token", "refresh_token", "sub", "user_id", "email"} {
			if value := auth.Attributes[key]; len(value) > 3 {
				secrets = append(secrets, value)
			}
			if value, ok := auth.Metadata[key].(string); ok && len(value) > 3 {
				secrets = append(secrets, value)
			}
		}
	}
	names := map[string]bool{}
	for name := range headers {
		names[http.CanonicalHeaderKey(name)] = true
	}
	for name, source := range sources {
		if source == "dynamic-template" {
			names[name] = true
		}
	}
	result := make([]XAIHeaderDiagnostic, 0, len(names))
	for name := range names {
		values, exists := headers[name]
		if !exists {
			for key, v := range headers {
				if strings.EqualFold(key, name) {
					values = v
					exists = true
					break
				}
			}
		}
		value := strings.Join(values, ", ")
		source := sources[name]
		if source == "" {
			source = "transport"
		}
		entry := XAIHeaderDiagnostic{Name: name, Source: source, Missing: !exists}
		if exists {
			redacted := !safe[name] || source == "global-config" || source == "credential-config" || source == "dynamic-template"
			for _, secret := range secrets {
				if strings.Contains(value, secret) {
					redacted = true
				}
			}
			if redacted {
				entry.Value = "[redacted sha256:" + XAIIdentityDigest(value) + "]"
				entry.Redacted = true
			} else {
				entry.Value = value
			}
			entry.Example = preview && (strings.Contains(strings.ToLower(name), "session") || strings.Contains(strings.ToLower(name), "conv") || strings.Contains(strings.ToLower(name), "req-id") || strings.Contains(strings.ToLower(name), "agent-id"))
		}
		result = append(result, entry)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	if len(result) > 128 {
		result = result[:128]
	}
	return result
}
func safeXAIDiagnosticURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	u.User = nil
	query := u.Query()
	for key := range query {
		if key != "format" {
			query.Set(key, "[redacted]")
		}
	}
	u.RawQuery = query.Encode()
	return u.String()
}

func EmitXAITransmission(ctx context.Context, req *http.Request, auth *coreauth.Auth, stage string) *XAITransmission {
	state := xaiDiagnostics(ctx)
	if state == nil || req == nil {
		return nil
	}
	state.Lock()
	state.sequence++
	id := state.sequence
	source := map[string]string{}
	for k, v := range state.sources {
		source[k] = v
	}
	compression := state.compression
	caps := state.capabilities
	preview := state.preview
	state.Unlock()
	headers := req.Header.Clone()
	if headers.Get("User-Agent") == "" {
		headers.Set("User-Agent", "Go-http-client/1.1")
		source["User-Agent"] = "transport"
	}
	if req.ContentLength >= 0 && req.Body != nil {
		headers.Set("Content-Length", strconv.FormatInt(req.ContentLength, 10))
		source["Content-Length"] = "transport"
		size := req.ContentLength
		compression.SentBytes = &size
	}
	if stage == "settings" {
		compression = XAICompressionDiagnostic{Reason: "settings-query"}
		caps = nil
	}
	if stage == "websocket_handshake" {
		compression = XAICompressionDiagnostic{Enabled: compression.Enabled, Reason: "websocket"}
		headers.Del("Content-Length")
	}
	if compression.Reason == "compressed" && req.Header.Get("Content-Encoding") != "zstd" {
		compression.Reason = "redirect_plain"
	}
	if headers.Get("Content-Encoding") == "zstd" {
		source["Content-Encoding"] = "compression-negotiation"
	}
	value := &XAITransmission{ID: id, Stage: stage, URL: safeXAIDiagnosticURL(req.URL.String()), Headers: SafeXAIHeaders(headers, source, auth, preview), Compression: compression, Capabilities: caps, Preview: preview}
	emitXAIDiagnostic(ctx, *value)
	return value
}
func emitXAIDiagnostic(ctx context.Context, value XAITransmission) {
	if state := xaiDiagnostics(ctx); state != nil && state.observe != nil {
		state.observe(value)
	}
}
func CompleteXAITransmission(ctx context.Context, value *XAITransmission, status int) {
	if value != nil {
		copy := *value
		copy.Status = status
		emitXAIDiagnostic(ctx, copy)
	}
}
func EmitXAIWebsocketReuse(ctx context.Context, value *XAITransmission) {
	if value == nil {
		return
	}
	copy := *value
	copy.Stage = "websocket_reuse"
	if state := xaiDiagnostics(ctx); state != nil {
		state.Lock()
		state.sequence++
		copy.ID = state.sequence
		copy.Capabilities = state.capabilities
		state.Unlock()
	}
	emitXAIDiagnostic(ctx, copy)
}

// Capture even without an observer so a later diagnostic request can describe
// the handshake of a connection established by an earlier ordinary request.
func CaptureXAIWebsocketHandshake(ctx context.Context, address string, headers http.Header, auth *coreauth.Auth, configurations ...*config.Config) *XAITransmission {
	if xaiDiagnostics(ctx) == nil {
		ctx = WithXAIDiagnostics(ctx, false, nil)
		NoteXAIHeaderMap(ctx, "builtin", headers)
		var cfg *config.Config
		if len(configurations) > 0 {
			cfg = configurations[0]
		}
		NoteXAIConfiguredHeaderSources(ctx, cfg, auth, headers)
		if headers.Get("x-grok-model-override") != "" {
			NoteXAIHeaderSources(ctx, "model-route", "x-grok-model-override")
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return nil
	}
	req.Header = headers.Clone()
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	NoteXAIHeaderSources(ctx, "transport-generated", "Sec-WebSocket-Key")
	// Gorilla negotiates permessage-deflate when EnableCompression is true.
	req.Header.Set("Sec-WebSocket-Extensions", "permessage-deflate; server_no_context_takeover; client_no_context_takeover")
	return EmitXAITransmission(ctx, req, auth, "websocket_handshake")
}

type xaiDiagnosticTransport struct {
	base http.RoundTripper
	auth *coreauth.Auth
}

func (t *xaiDiagnosticTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	value := EmitXAITransmission(req.Context(), req, t.auth, "http")
	resp, err := t.base.RoundTrip(req)
	if resp != nil {
		CompleteXAITransmission(req.Context(), value, resp.StatusCode)
	}
	return resp, err
}
func (t *xaiDiagnosticTransport) CloseIdleConnections() {
	if c, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}
func (t *xaiDiagnosticTransport) CancelRequest(req *http.Request) {
	if c, ok := t.base.(interface{ CancelRequest(*http.Request) }); ok {
		c.CancelRequest(req)
	}
}
func XAIObserveHTTPClient(ctx context.Context, client *http.Client, auth *coreauth.Auth) *http.Client {
	if xaiDiagnostics(ctx) == nil {
		return client
	}
	copy := *client
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	copy.Transport = &xaiDiagnosticTransport{base: base, auth: auth}
	return &copy
}

func NoteXAIConfiguredHeaderSources(ctx context.Context, cfg *config.Config, auth *coreauth.Auth, headers http.Header) {
	if cfg != nil {
		for name, value := range cfg.XAI.Headers {
			source := "global-config"
			if cfg.XAI.DynamicHeaders && strings.Contains(value, "$") {
				source = "dynamic-template"
			}
			NoteXAIHeaderSources(ctx, source, name)
		}
	}
	if auth != nil {
		for key, value := range auth.Attributes {
			if name, ok := strings.CutPrefix(key, "header:"); ok {
				source := "credential-config"
				if cfg != nil && cfg.XAI.DynamicHeaders && strings.Contains(value, "$") {
					source = "dynamic-template"
				}
				NoteXAIHeaderSources(ctx, source, name)
			}
		}
	}
	_ = headers
}

func ValidateXAIClientHeaders(headers http.Header) error {
	if len(headers) > 64 {
		return fmt.Errorf("too many preview headers")
	}
	for key, values := range headers {
		if !httpguts.ValidHeaderFieldName(key) || len(key) > 256 || len(values) > 16 {
			return fmt.Errorf("invalid preview headers")
		}
		for _, value := range values {
			if len(value) > 8192 || !httpguts.ValidHeaderFieldValue(value) {
				return fmt.Errorf("invalid preview header value")
			}
		}
	}
	return nil
}

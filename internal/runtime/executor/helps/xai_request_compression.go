package helps

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/singleflight"
)

const XAICompressionMinimum = 64 * 1024

var xaiCompressionCache = struct {
	sync.Mutex
	entries map[string]xaiCompressionCapability
}{entries: map[string]xaiCompressionCapability{}}
var xaiCompressionQueries singleflight.Group

type xaiCompressionCapability struct {
	enabled bool
	until   time.Time
}

// PrepareXAICompressedRequest negotiates only with the exact selected base URL.
// Failure preserves the caller's original request body and headers.
func PrepareXAICompressedRequest(ctx context.Context, client *http.Client, req *http.Request, auth *coreauth.Auth, cfg *config.Config) *http.Request {
	info := XAICompressionDiagnostic{Reason: "disabled"}
	if req != nil && req.ContentLength >= 0 {
		size := req.ContentLength
		info.OriginalBytes = &size
		info.SentBytes = &size
	}
	if cfg != nil && cfg.XAI.RequestCompression == "auto" {
		info.Enabled = true
		info.Reason = "ineligible"
		if req != nil && req.ContentLength < XAICompressionMinimum {
			info.Reason = "below_threshold"
		}
	}
	NoteXAICompression(ctx, info)
	if cfg == nil || cfg.XAI.RequestCompression != "auto" || req == nil || req.URL == nil || req.Method != http.MethodPost || req.ContentLength < XAICompressionMinimum || req.GetBody == nil || req.Header.Get("Content-Encoding") != "" || !strings.HasPrefix(req.Header.Get("Content-Type"), "application/json") {
		return req
	}
	if req.Host != "" && !strings.EqualFold(req.Host, req.URL.Host) {
		return req
	}
	source := requestBodyForCompression(req)
	if len(source) < XAICompressionMinimum {
		return req
	}
	base, err := ResolveXAIModelUpstream(auth, cfg, gjson.GetBytes(source, "model").String())
	if err != nil || !strings.HasPrefix(req.URL.String(), strings.TrimRight(base.BaseURL, "/")+"/") {
		return req
	}
	query, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(base.BaseURL, "/")+"/settings", nil)
	if err != nil {
		return req
	}
	query.Header = req.Header.Clone()
	query.Header.Del("Content-Type")
	query.Header.Del("Content-Length")
	query.Header.Del("Content-Encoding")
	query.Header.Set("Accept", "application/json")
	applyXAIResourceIdentity(query, auth)
	key := xaiCompressionCacheKey(base.BaseURL, query.Header, auth, cfg)
	info.SettingsURL = query.URL.String()
	info.Reason = "unknown_support"
	NoteXAICompression(ctx, info)
	if !xaiSupportsCompression(ctx, client, query, key, auth) {
		return req
	}
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(3)), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return req
	}
	info.Reason = "compressed"
	support := true
	info.Supported = &support
	compressed := encoder.EncodeAll(source, nil)
	encoder.Close()
	clone := req.Clone(req.Context())
	clone.Body = io.NopCloser(bytes.NewReader(compressed))
	clone.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(compressed)), nil }
	clone.ContentLength = int64(len(compressed))
	clone.Header.Set("Content-Encoding", "zstd")
	clone.Header.Del("Content-Length")
	size := int64(len(compressed))
	info.SentBytes = &size
	NoteXAICompression(ctx, info)
	log.WithFields(log.Fields{"original_bytes": len(source), "compressed_bytes": len(compressed)}).Debug("Grok HTTP request compressed")
	return clone
}

func xaiCompressionCacheKey(base string, headers http.Header, auth *coreauth.Auth, cfg *config.Config) string {
	identity := base + "\x00" + cfg.ProxyURL + "\x00" + headers.Get("Authorization")
	if auth != nil {
		identity += "\x00" + auth.ID + "\x00" + auth.RuntimeInstallationID() + "\x00" + auth.ProxyURL + "\x00" + auth.EffectiveProxyBindingID()
	}
	profile, _ := jsonCompressionProfile(cfg)
	sum := sha256.Sum256([]byte(identity + profile))
	return fmt.Sprintf("%x", sum)
}

// Preview uses only a previously negotiated capability; it never queries settings.
func NoteXAIPreviewCompression(ctx context.Context, req *http.Request, auth *coreauth.Auth, cfg *config.Config) {
	info := XAICompressionDiagnostic{Reason: "disabled", Enabled: cfg.XAI.RequestCompression == "auto"}
	if req.Body != nil {
		n := req.ContentLength
		info.OriginalBytes = &n
		info.SentBytes = &n
	}
	if info.Enabled {
		info.Reason = "ineligible"
		if req.Method == http.MethodPost && strings.HasPrefix(req.Header.Get("Content-Type"), "application/json") && req.Header.Get("Content-Encoding") == "" {
			info.Reason = "below_threshold"
			if req.ContentLength >= XAICompressionMinimum {
				source := requestBodyForCompression(req)
				route, err := ResolveXAIModelUpstream(auth, cfg, gjson.GetBytes(source, "model").String())
				if err == nil {
					info.SettingsURL = strings.TrimRight(route.BaseURL, "/") + "/settings"
					info.Reason = "preview_support_unknown"
					xaiCompressionCache.Lock()
					cached, ok := xaiCompressionCache.entries[xaiCompressionCacheKey(route.BaseURL, req.Header, auth, cfg)]
					xaiCompressionCache.Unlock()
					if ok && time.Now().Before(cached.until) {
						info.Supported = &cached.enabled
						info.Reason = "unsupported"
						if cached.enabled {
							encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(3)), zstd.WithEncoderConcurrency(1))
							if err == nil {
								compressed := encoder.EncodeAll(source, nil)
								encoder.Close()
								req.Body = io.NopCloser(bytes.NewReader(compressed))
								req.ContentLength = int64(len(compressed))
								req.Header.Set("Content-Encoding", "zstd")
								info.Reason = "compressed"
								n := req.ContentLength
								info.SentBytes = &n
							} else {
								info.Reason = "compression_failed"
							}
						}
					}
				}
			}
		}
	}
	NoteXAICompression(ctx, info)
}

func requestBodyForCompression(req *http.Request) []byte {
	body, err := req.GetBody()
	if err != nil {
		return nil
	}
	defer func() {
		if err := body.Close(); err != nil {
			log.WithError(err).Debug("close Grok compression body clone")
		}
	}()
	data, err := io.ReadAll(body)
	if err != nil {
		return nil
	}
	return data
}

func jsonCompressionProfile(cfg *config.Config) (string, error) {
	data, err := json.Marshal(struct {
		Headers  map[string]string
		Defaults config.XAIHeaderDefaults
	}{cfg.XAI.Headers, cfg.XAI.HeaderDefaults})
	return string(data), err
}

func xaiSupportsCompression(ctx context.Context, client *http.Client, query *http.Request, key string, auth *coreauth.Auth) bool {
	cached := func() (bool, bool) {
		xaiCompressionCache.Lock()
		defer xaiCompressionCache.Unlock()
		entry, ok := xaiCompressionCache.entries[key]
		return entry.enabled, ok && time.Now().Before(entry.until)
	}
	if enabled, ok := cached(); ok {
		noteXAICompressionSupport(ctx, enabled)
		return enabled
	}
	result := xaiCompressionQueries.DoChan(key, func() (any, error) {
		if enabled, ok := cached(); ok {
			return enabled, nil
		}
		// Do not follow settings redirects: support is scoped to the advertised origin.
		isolated := *client
		if timed, ok := isolated.Transport.(*usageTimingTransport); ok {
			isolated.Transport = timed.base
		}
		isolated.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		queryTrace := EmitXAITransmission(ctx, query, auth, "settings")
		resp, err := isolated.Do(query)
		if resp != nil {
			CompleteXAITransmission(ctx, queryTrace, resp.StatusCode)
		}
		if err != nil {
			return false, err
		}
		defer func() {
			if err := resp.Body.Close(); err != nil {
				log.WithError(err).Debug("close Grok compression settings response")
			}
		}()
		if resp.StatusCode != http.StatusOK {
			return false, fmt.Errorf("settings query returned HTTP %d", resp.StatusCode)
		}
		data, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
		if err != nil {
			return false, err
		}
		if len(data) > 1<<20 || !gjson.ValidBytes(data) || !gjson.GetBytes(data, "accept_request_encodings").IsArray() {
			return false, fmt.Errorf("invalid compression settings")
		}
		enabled := false
		for _, value := range gjson.GetBytes(data, "accept_request_encodings").Array() {
			if value.String() == "zstd" {
				enabled = true
			}
		}
		if ctx.Err() != nil || (auth != nil && auth.RuntimeInstanceRetired()) {
			return false, nil
		}
		xaiCompressionCache.Lock()
		if len(xaiCompressionCache.entries) >= 1024 {
			for k, entry := range xaiCompressionCache.entries {
				if time.Now().After(entry.until) {
					delete(xaiCompressionCache.entries, k)
				}
			}
			if len(xaiCompressionCache.entries) >= 1024 {
				for k := range xaiCompressionCache.entries {
					delete(xaiCompressionCache.entries, k)
					break
				}
			}
		}
		xaiCompressionCache.entries[key] = xaiCompressionCapability{enabled: enabled, until: time.Now().Add(3 * time.Hour)}
		xaiCompressionCache.Unlock()
		return enabled, nil
	})
	select {
	case <-ctx.Done():
		noteXAICompressionFailure(ctx, "canceled")
		return false
	case value := <-result:
		enabled, _ := value.Val.(bool)
		if value.Err == nil {
			noteXAICompressionSupport(ctx, enabled)
		} else {
			noteXAICompressionFailure(ctx, "negotiation_failed")
		}
		return value.Err == nil && enabled
	}
}

func noteXAICompressionFailure(ctx context.Context, reason string) {
	if state := xaiDiagnostics(ctx); state != nil {
		state.Lock()
		state.compression.Reason = reason
		state.Unlock()
	}
}

// XAICompressedHTTPClient avoids forwarding negotiated encodings to a redirect
// target that has not advertised support. Redirects retain the logical JSON body.
func XAICompressedHTTPClient(client *http.Client, original *http.Request) *http.Client {
	owned := *client
	owned.CheckRedirect = func(next *http.Request, via []*http.Request) error {
		if client.CheckRedirect != nil {
			if err := client.CheckRedirect(next, via); err != nil {
				return err
			}
		} else if len(via) >= 10 {
			return fmt.Errorf("stopped after 10 redirects")
		}
		if next.Method != http.MethodGet && next.Method != http.MethodHead && next.URL.String() != original.URL.String() {
			body, err := original.GetBody()
			if err != nil {
				return err
			}
			if next.Body != nil {
				if errClose := next.Body.Close(); errClose != nil {
					log.WithError(errClose).Debug("close Grok redirected compressed body")
				}
			}
			next.Body = body
			next.GetBody = original.GetBody
			next.ContentLength = original.ContentLength
		}
		if next.URL.String() != original.URL.String() {
			next.Header.Del("Content-Encoding")
			next.Header.Del("Content-Length")
		}
		return nil
	}
	return &owned
}

func noteXAICompressionSupport(ctx context.Context, supported bool) {
	if state := xaiDiagnostics(ctx); state != nil {
		state.Lock()
		defer state.Unlock()
		state.compression.Supported = &supported
		if !supported {
			state.compression.Reason = "unsupported"
		}
	}
}

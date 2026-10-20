package helps

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	log "github.com/sirupsen/logrus"
)

var codexRequestZstd struct {
	once    sync.Once
	encoder *zstd.Encoder
	err     error
}

// PrepareCodexRequestCompression runs after all body/header projection and logging.
// It never replays a request or changes a WebSocket frame.
func PrepareCodexRequestCompression(req *http.Request, body []byte, auth *coreauth.Auth, cfg *config.Config) bool {
	if cfg == nil || !strings.EqualFold(strings.TrimSpace(cfg.Codex.UpstreamRequestCompression), "zstd") ||
		auth == nil || codexUsesAPIKeyAuth(auth) || !codexCompressionTarget(req) || len(body) < 1024 ||
		(req.Header.Get("Content-Encoding") != "" && !strings.EqualFold(req.Header.Get("Content-Encoding"), "identity")) {
		return false
	}
	codexRequestZstd.once.Do(func() {
		codexRequestZstd.encoder, codexRequestZstd.err = zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(3)), zstd.WithEncoderConcurrency(1))
	})
	if codexRequestZstd.err != nil {
		log.WithError(codexRequestZstd.err).Warn("Codex request compression unavailable")
		return false
	}
	encoded := codexRequestZstd.encoder.EncodeAll(body, nil)
	if len(encoded) >= len(body) {
		return false
	}
	if req.Body != nil {
		if errClose := req.Body.Close(); errClose != nil {
			log.WithError(errClose).Debug("close uncompressed Codex request body")
		}
	}
	req.Body = coreexecutor.NewReleasableReadCloser(encoded, nil)
	// Preserve existing replay ownership. Native Codex bodies are non-replayable
	// and release their bytes at EOF rather than retaining them through the SSE.
	if req.GetBody != nil {
		req.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(encoded)), nil }
	}
	req.ContentLength = int64(len(encoded))
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	req.Header.Del("Content-Length")
	req.Header.Del("Content-MD5")
	req.Header.Set("Content-Encoding", "zstd")
	log.WithFields(log.Fields{"original_bytes": len(body), "compressed_bytes": len(encoded)}).Debug("Codex HTTP request compressed")
	return true
}

func codexCompressionTarget(req *http.Request) bool {
	if req == nil || req.URL == nil || req.Method != http.MethodPost ||
		req.URL.Scheme != "https" || !strings.EqualFold(req.URL.Hostname(), "chatgpt.com") ||
		(req.URL.Port() != "" && req.URL.Port() != "443") || req.URL.User != nil ||
		(req.Host != "" && !strings.EqualFold(req.Host, req.URL.Host)) {
		return false
	}
	switch req.URL.EscapedPath() {
	case "/backend-api/codex/responses", "/backend-api/codex/responses/compact":
		return true
	default:
		return false
	}
}

// CodexCompressedHTTPClient prevents redirecting encoded credentials/body to a
// destination whose request-compression support has not been established.
func CodexCompressedHTTPClient(client *http.Client, compressed bool) *http.Client {
	if !compressed {
		return client
	}
	owned := *client
	owned.CheckRedirect = func(next *http.Request, via []*http.Request) error {
		if !codexCompressionTarget(next) {
			return http.ErrUseLastResponse
		}
		if client.CheckRedirect != nil {
			if err := client.CheckRedirect(next, via); err != nil {
				return err
			}
			if !codexCompressionTarget(next) {
				return http.ErrUseLastResponse
			}
		}
		if len(via) >= 10 {
			return http.ErrUseLastResponse
		}
		return nil
	}
	return &owned
}

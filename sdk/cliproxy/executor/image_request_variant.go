package executor

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"

	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

const CodexNativeImageRequestMetadataKey = "codex_native_image_request"

type codexNativeImageRequestContextKey struct{}

// CodexNativeImageRequest pins one native request alternative. The ordinary
// Responses payload remains available to Web without applying native rules.
type CodexNativeImageRequest struct {
	mu             sync.RWMutex
	payload        []byte
	alt            string
	nativeResponse atomic.Bool
}

func NewCodexNativeImageRequest(payload []byte, alt string) *CodexNativeImageRequest {
	return &CodexNativeImageRequest{payload: payload, alt: alt}
}

func WithCodexNativeImageRequest(ctx context.Context, request *CodexNativeImageRequest) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, codexNativeImageRequestContextKey{}, request)
}

func CodexNativeImageRequestFromContext(ctx context.Context) *CodexNativeImageRequest {
	if ctx == nil {
		return nil
	}
	request, _ := ctx.Value(codexNativeImageRequestContextKey{}).(*CodexNativeImageRequest)
	return request
}

func CodexNativeImageRequestFromOptions(opts Options) *CodexNativeImageRequest {
	request, _ := opts.Metadata[CodexNativeImageRequestMetadataKey].(*CodexNativeImageRequest)
	return request
}

func (request *CodexNativeImageRequest) NativeResponse() bool {
	return request != nil && request.nativeResponse.Load()
}

func (request *CodexNativeImageRequest) Release() {
	if request != nil {
		request.mu.Lock()
		request.payload = nil
		request.mu.Unlock()
	}
}

// PrepareImageRequestForProvider is pure with respect to provider selection;
// preflight may prepare both providers before either credential is chosen.
func PrepareImageRequestForProvider(provider string, req Request, opts Options) (Request, Options) {
	request := CodexNativeImageRequestFromOptions(opts)
	if request == nil || !strings.EqualFold(strings.TrimSpace(provider), "codex") {
		return req, opts
	}
	request.mu.RLock()
	req.Payload = request.payload
	opts.OriginalRequest = request.payload
	opts.Alt = request.alt
	request.mu.RUnlock()
	opts.SourceFormat = sdktranslator.FromString("openai-image")
	return req, opts
}

// ObserveImageResponseProvider is called only at the actual execution boundary,
// so fallback cannot leave the handler using a previous credential's format.
func ObserveImageResponseProvider(provider string, opts Options) {
	if request := CodexNativeImageRequestFromOptions(opts); request != nil {
		request.nativeResponse.Store(strings.EqualFold(strings.TrimSpace(provider), "codex"))
	}
}

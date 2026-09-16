package helps

import (
	"context"
	"net/http"
)

type requestTraceContextKey struct{}

// WithRequestTrace observes one diagnostic execution without enabling request
// logging. Callbacks are synchronous and must copy any retained request bytes.
// Request headers and credential fields are deliberately not exposed.
func WithRequestTrace(ctx context.Context, request func(string, []byte), response func(int, http.Header)) context.Context {
	return context.WithValue(ctx, requestTraceContextKey{}, &requestTrace{request, response})
}

type requestTrace struct {
	request  func(string, []byte)
	response func(int, http.Header)
}

func traceRequest(ctx context.Context, url string, body []byte) {
	if ctx != nil {
		if trace, ok := ctx.Value(requestTraceContextKey{}).(*requestTrace); ok && trace.request != nil {
			trace.request(url, body)
		}
	}
}

func traceResponse(ctx context.Context, status int, headers http.Header) {
	if ctx != nil {
		if trace, ok := ctx.Value(requestTraceContextKey{}).(*requestTrace); ok && trace.response != nil {
			trace.response(status, headers)
		}
	}
}

package usage

import "context"

// RequestMetadata freezes the request identity needed by asynchronous usage
// consumers. It contains no request body, credential, headers or response writer.
type RequestMetadata struct {
	APIIdentifier string
	ClientIP      string
	Method        string
	Path          string
}

type requestMetadataContextKey struct{}

func WithRequestMetadata(ctx context.Context, metadata RequestMetadata) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, requestMetadataContextKey{}, metadata)
}

func RequestMetadataFromContext(ctx context.Context) (RequestMetadata, bool) {
	if ctx == nil {
		return RequestMetadata{}, false
	}
	metadata, ok := ctx.Value(requestMetadataContextKey{}).(RequestMetadata)
	return metadata, ok
}

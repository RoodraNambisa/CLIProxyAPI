package usage

import "context"

type generateContextKey struct{}

// WithGenerate stores whether this request asks the upstream to generate output.
func WithGenerate(ctx context.Context, generate bool) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, generateContextKey{}, generate)
}

// GenerateFromContext defaults to true for callers predating this field.
func GenerateFromContext(ctx context.Context) bool {
	if ctx != nil {
		if generate, ok := ctx.Value(generateContextKey{}).(bool); ok {
			return generate
		}
	}
	return true
}

// GenerateFlag returns an independently owned value for a usage record.
func GenerateFlag(generate bool) *bool { return &generate }

// GenerateEnabled treats omitted legacy fields as generation requests.
func GenerateEnabled(generate *bool) bool { return generate == nil || *generate }

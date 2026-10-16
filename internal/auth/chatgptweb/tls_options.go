package chatgptweb

import "context"

// TLSOptions is copied into a client once and inherited by its child transports.
// Omitted constructor options use the original Chrome 146 template.
type TLSOptions struct {
	TrustAnchorsEnabled bool
}

func resolvedTLSOptions(options []TLSOptions) TLSOptions {
	if len(options) > 0 {
		return options[0]
	}
	return TLSOptions{TrustAnchorsEnabled: true}
}

type tlsOptionsKey struct{}

// WithTLSOptions freezes TLS settings for a flow, preserving an earlier snapshot.
func WithTLSOptions(ctx context.Context, options TLSOptions) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Value(tlsOptionsKey{}).(TLSOptions); ok {
		return ctx
	}
	return context.WithValue(ctx, tlsOptionsKey{}, options)
}

// TLSOptionsFromContext returns the flow snapshot, or the supplied/default settings.
func TLSOptionsFromContext(ctx context.Context, fallback ...TLSOptions) TLSOptions {
	if ctx != nil {
		if options, ok := ctx.Value(tlsOptionsKey{}).(TLSOptions); ok {
			return options
		}
	}
	return resolvedTLSOptions(fallback)
}

// TLSOptions returns a value copy; changing it cannot change an existing client.
func (client *Client) TLSOptions() TLSOptions {
	if client == nil {
		return resolvedTLSOptions(nil)
	}
	return client.tlsOptions
}

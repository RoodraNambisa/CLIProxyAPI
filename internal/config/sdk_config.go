// Package config provides configuration management for the CLI Proxy API server.
// It handles loading and parsing YAML configuration files, and provides structured
// access to application settings including server port, authentication directory,
// debug settings, proxy configuration, and API keys.
package config

// SDKConfig represents the application's configuration, loaded from a YAML file.
type SDKConfig struct {
	// ProxyURL is the URL of an optional proxy server to use for outbound requests.
	ProxyURL string `yaml:"proxy-url" json:"proxy-url"`

	// EnableGeminiCLIEndpoint is retained for v6 source compatibility and has no effect.
	// Gemini CLI routes and execution support have been removed.
	EnableGeminiCLIEndpoint bool `yaml:"-" json:"-"`

	// ForceModelPrefix requires explicit model prefixes (e.g., "teamA/gemini-3-pro-preview")
	// to target prefixed credentials. When false, unprefixed model requests may use prefixed
	// credentials as well.
	ForceModelPrefix bool `yaml:"force-model-prefix" json:"force-model-prefix"`

	// RequestLog enables or disables detailed request logging functionality.
	RequestLog bool `yaml:"request-log" json:"request-log"`

	// RequestBodyRelease controls timed release of retained request body copies.
	RequestBodyRelease RequestBodyReleaseConfig `yaml:"request-body-release" json:"request-body-release"`

	// APIKeys is a list of keys for authenticating clients to this proxy server.
	APIKeys []string `yaml:"api-keys" json:"api-keys"`

	// PassthroughHeaders controls whether upstream response headers are forwarded to downstream clients.
	// Default is false (disabled).
	PassthroughHeaders bool `yaml:"passthrough-headers" json:"passthrough-headers"`

	// Streaming configures server-side streaming behavior (keep-alives and safe bootstrap retries).
	Streaming StreamingConfig `yaml:"streaming" json:"streaming"`

	// NonStreamKeepAliveInterval controls how often blank lines are emitted for non-streaming responses.
	// <= 0 disables keep-alives. Value is in seconds.
	NonStreamKeepAliveInterval int `yaml:"nonstream-keepalive-interval,omitempty" json:"nonstream-keepalive-interval,omitempty"`

	// Images configures OpenAI Images compatibility backed by Codex Responses.
	Images ImagesConfig `yaml:"images,omitempty" json:"images,omitempty"`
}

// StreamingConfig holds server streaming behavior configuration.
type StreamingConfig struct {
	// KeepAliveSeconds controls how often the server emits SSE heartbeats (": keep-alive\n\n").
	// <= 0 disables keep-alives. Default is 0.
	KeepAliveSeconds int `yaml:"keepalive-seconds,omitempty" json:"keepalive-seconds,omitempty"`

	// BootstrapRetries controls how many times the server may retry a streaming request before any bytes are sent,
	// to allow auth rotation / transient recovery.
	// <= 0 disables bootstrap retries. Default is 0.
	BootstrapRetries int `yaml:"bootstrap-retries,omitempty" json:"bootstrap-retries,omitempty"`

	// EnableStreamFlush enables flush batching for regular streaming responses.
	// Default is false to preserve token-by-token latency.
	EnableStreamFlush bool `yaml:"enable-stream-flush,omitempty" json:"enable-stream-flush,omitempty"`

	// StreamFlushIntervalMS batches regular streaming flushes for up to this many milliseconds.
	StreamFlushIntervalMS int `yaml:"stream-flush-interval-ms,omitempty" json:"stream-flush-interval-ms,omitempty"`

	// StreamFlushMinBytes flushes regular streaming output once this many bytes are pending.
	StreamFlushMinBytes int `yaml:"stream-flush-min-bytes,omitempty" json:"stream-flush-min-bytes,omitempty"`

	// TrustUpstreamSSE forwards OpenAI Responses SSE without repair/validation.
	// Default is false for compatibility with split or incomplete upstream SSE frames.
	TrustUpstreamSSE bool `yaml:"trust-upstream-sse,omitempty" json:"trust-upstream-sse,omitempty"`
}

// ImagesConfig holds OpenAI Images compatibility configuration.
type ImagesConfig struct {
	// CodexModel is the outer Responses model used to invoke the Codex image_generation tool.
	CodexModel string `yaml:"codex-model,omitempty" json:"codex-model,omitempty"`
	// ImageModel is the image_generation tool model exposed through the OpenAI Images API.
	ImageModel string `yaml:"image-model,omitempty" json:"image-model,omitempty"`
	// EnableFreePlanImageModel controls whether Codex free-plan auths register the configured image model.
	EnableFreePlanImageModel bool `yaml:"enable-free-plan-image-model,omitempty" json:"enable-free-plan-image-model,omitempty"`
	// EnableNAggregation enables multi-call aggregation for Images API n > 1 requests.
	EnableNAggregation *bool `yaml:"enable-n-aggregation,omitempty" json:"enable-n-aggregation,omitempty"`
	// UnsupportedStatusCode is used for unsupported Images API options.
	UnsupportedStatusCode int `yaml:"unsupported-status-code,omitempty" json:"unsupported-status-code,omitempty"`
	// OverrideUnsupportedParams is a legacy shortcut for enabling all supported option overrides.
	OverrideUnsupportedParams bool `yaml:"override-unsupported-params,omitempty" json:"override-unsupported-params,omitempty"`
	// OverrideResponseFormatURL coerces response_format=url to b64_json when set.
	OverrideResponseFormatURL *bool `yaml:"override-response-format-url,omitempty" json:"override-response-format-url,omitempty"`
	// ResponseFormatURLDataURL returns data: URLs for response_format=url when set.
	ResponseFormatURLDataURL *bool `yaml:"response-format-url-data-url,omitempty" json:"response-format-url-data-url,omitempty"`
	// OverrideTransparentBackground coerces background=transparent to auto when set.
	OverrideTransparentBackground *bool `yaml:"override-transparent-background,omitempty" json:"override-transparent-background,omitempty"`
	// OverrideInputFidelity omits input_fidelity instead of forwarding it when set.
	OverrideInputFidelity *bool `yaml:"override-input-fidelity,omitempty" json:"override-input-fidelity,omitempty"`
	// EnableStreamFlush enables flush batching for image streaming responses. Default is true.
	EnableStreamFlush *bool `yaml:"enable-stream-flush,omitempty" json:"enable-stream-flush,omitempty"`
	// StreamFlushIntervalMS batches image streaming flushes for up to this many milliseconds.
	StreamFlushIntervalMS int `yaml:"stream-flush-interval-ms,omitempty" json:"stream-flush-interval-ms,omitempty"`
	// StreamFlushMinBytes flushes image streaming output once this many bytes are pending.
	StreamFlushMinBytes int `yaml:"stream-flush-min-bytes,omitempty" json:"stream-flush-min-bytes,omitempty"`
	// Native configures direct Codex Images API proxying.
	Native NativeImagesConfig `yaml:"native,omitempty" json:"native,omitempty"`
}

// NativeImagesConfig holds direct Codex Images API configuration.
type NativeImagesConfig struct {
	// Generations configures POST /v1/images/generations native proxying.
	Generations NativeImageEndpointConfig `yaml:"generations,omitempty" json:"generations,omitempty"`
	// Edits configures POST /v1/images/edits native proxying.
	Edits NativeImageEndpointConfig `yaml:"edits,omitempty" json:"edits,omitempty"`
}

// NativeImageEndpointConfig holds per-endpoint native Images API options.
type NativeImageEndpointConfig struct {
	// Enabled controls whether this endpoint uses the native Images API path.
	Enabled bool `yaml:"enabled,omitempty" json:"enabled,omitempty"`
	// Models lists image models allowed on the native path.
	Models []string `yaml:"models,omitempty" json:"models,omitempty"`
	// ParamRules deletes or overrides request parameters before forwarding.
	ParamRules []string `yaml:"param-rules,omitempty" json:"param-rules,omitempty"`
	// UnsupportedModelStatusCode is returned when native is enabled but the model is not allowed.
	UnsupportedModelStatusCode int `yaml:"unsupported-model-status-code,omitempty" json:"unsupported-model-status-code,omitempty"`
	// UnsupportedModelMessage is returned when native is enabled but the model is not allowed.
	UnsupportedModelMessage string `yaml:"unsupported-model-message,omitempty" json:"unsupported-model-message,omitempty"`
}

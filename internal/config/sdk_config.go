// Package config provides configuration management for the CLI Proxy API server.
// It handles loading and parsing YAML configuration files, and provides structured
// access to application settings including server port, authentication directory,
// debug settings, proxy configuration, and API keys.
package config

import (
	"fmt"
	"math"
	"strings"
)

// SDKConfig represents the application's configuration, loaded from a YAML file.
type SDKConfig struct {
	// ProxyURL is the URL of an optional proxy server to use for outbound requests.
	ProxyURL string `yaml:"proxy-url" json:"proxy-url"`

	// ProxyPools defines reusable structured proxy sources.
	ProxyPools []ProxyPoolConfig `yaml:"proxy-pools" json:"proxy-pools,omitempty"`

	// ProxyRules selects proxy pools by runtime provider and credential priority.
	ProxyRules []ProxyRuleConfig `yaml:"proxy-rules" json:"proxy-rules,omitempty"`

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

	// APIKeyGroups optionally restricts API keys to selected runtime providers.
	APIKeyGroups []APIKeyGroup `yaml:"api-key-groups" json:"api-key-groups"`

	// PassthroughHeaders controls whether upstream response headers are forwarded to downstream clients.
	// Default is false (disabled).
	PassthroughHeaders bool `yaml:"passthrough-headers" json:"passthrough-headers"`

	// ErrorResponseRewrites changes selected final runtime and local-conversion error responses.
	ErrorResponseRewrites []ErrorResponseRewriteRule `yaml:"error-response-rewrites,omitempty" json:"error-response-rewrites,omitempty"`

	// Streaming configures server-side streaming behavior (keep-alives and safe bootstrap retries).
	Streaming StreamingConfig `yaml:"streaming" json:"streaming"`

	// NonStreamKeepAliveInterval controls how often blank lines are emitted for non-streaming responses.
	// <= 0 disables keep-alives. Value is in seconds.
	NonStreamKeepAliveInterval int `yaml:"nonstream-keepalive-interval,omitempty" json:"nonstream-keepalive-interval,omitempty"`

	// Images configures OpenAI Images compatibility backed by Codex Responses.
	Images ImagesConfig `yaml:"images,omitempty" json:"images,omitempty"`
}

// ProxyPoolConfig defines one named proxy pool.
type ProxyPoolConfig struct {
	Name                 string                 `yaml:"name" json:"name"`
	PlaceholderCharset   string                 `yaml:"placeholder-charset,omitempty" json:"placeholder-charset,omitempty"`
	CheckIntervalSeconds int                    `yaml:"check-interval-seconds,omitempty" json:"check-interval-seconds,omitempty"`
	BindAttempts         int                    `yaml:"bind-attempts,omitempty" json:"bind-attempts,omitempty"`
	SpreadBindings       bool                   `yaml:"spread-bindings,omitempty" json:"spread-bindings,omitempty"`
	Entries              []ProxyPoolEntryConfig `yaml:"entries" json:"entries"`
}

// ProxyPoolEntryConfig defines a URL template and an optional compact port set.
type ProxyPoolEntryConfig struct {
	ID          string `yaml:"id" json:"id"`
	URLTemplate string `yaml:"url-template" json:"url-template"`
	Ports       string `yaml:"ports,omitempty" json:"ports,omitempty"`
}

// ProxyRuleConfig routes matching credentials through a named proxy pool.
type ProxyRuleConfig struct {
	Name       string   `yaml:"name" json:"name"`
	Pool       string   `yaml:"pool" json:"pool"`
	Providers  []string `yaml:"providers,omitempty" json:"providers,omitempty"`
	Priorities []int    `yaml:"priorities,omitempty" json:"priorities,omitempty"`
}

// APIKeyGroup restricts one configured API key to a set of runtime provider IDs.
type APIKeyGroup struct {
	APIKey    string   `yaml:"api-key" json:"api-key"`
	Providers []string `yaml:"providers" json:"providers"`
}

// ErrorResponseRewriteRule projects a matching final error to a client-facing status or JSON body.
type ErrorResponseRewriteRule struct {
	// StatusCode optionally restricts the rule to one original HTTP status code.
	StatusCode int `yaml:"status-code,omitempty" json:"status-code,omitempty"`
	// MessageContains optionally matches the original error text case-insensitively.
	MessageContains string `yaml:"message-contains,omitempty" json:"message-contains,omitempty"`
	// ResponseStatusCode optionally replaces the downstream HTTP or event status.
	ResponseStatusCode int `yaml:"response-status-code,omitempty" json:"response-status-code,omitempty"`
	// ResponseBody optionally replaces the downstream JSON object. A non-nil empty map means {}.
	ResponseBody *map[string]any `yaml:"response-body,omitempty" json:"response-body,omitempty"`
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
	// ChatGPTWeb configures the ChatGPT Web image compatibility path.
	ChatGPTWeb ChatGPTWebImageConfig `yaml:"chatgpt-web,omitempty" json:"chatgpt-web,omitempty"`
	// Native configures direct Codex Images API proxying.
	Native NativeImagesConfig `yaml:"native,omitempty" json:"native,omitempty"`
}

// ChatGPTWebImageConfig controls the ChatGPT Web image compatibility path.
type ChatGPTWebImageConfig struct {
	// UpstreamModel is the ChatGPT Web conversation model that invokes picture_v2.
	UpstreamModel string `yaml:"upstream-model,omitempty" json:"upstream-model,omitempty"`
	// IgnoreUnsupportedParams allows Web routing after dropping options that Web cannot express.
	IgnoreUnsupportedParams bool `yaml:"ignore-unsupported-params,omitempty" json:"ignore-unsupported-params,omitempty"`
	// AdaptSizeToAspectRatio maps compatible explicit image sizes to an upstream canvas prompt.
	AdaptSizeToAspectRatio bool `yaml:"adapt-size-to-aspect-ratio,omitempty" json:"adapt-size-to-aspect-ratio,omitempty"`
	// StrictSize excludes ChatGPT Web when an explicit image size cannot be adapted.
	StrictSize bool `yaml:"strict-size,omitempty" json:"strict-size,omitempty"`
	// AspectRatioMaxErrorPercent is the maximum relative error accepted for generated output.
	AspectRatioMaxErrorPercent *float64 `yaml:"aspect-ratio-max-error-percent,omitempty" json:"aspect-ratio-max-error-percent,omitempty"`
	// MaxResizeEdgePixels limits explicit size adaptation targets.
	MaxResizeEdgePixels *int `yaml:"max-resize-edge-pixels,omitempty" json:"max-resize-edge-pixels,omitempty"`
	// ResizeToRequestedSize resizes matched Web image results to the explicit target.
	ResizeToRequestedSize bool `yaml:"resize-to-requested-size,omitempty" json:"resize-to-requested-size,omitempty"`
	// ResizeFilter selects the local image resampling filter.
	ResizeFilter string `yaml:"resize-filter,omitempty" json:"resize-filter,omitempty"`
	// MaxImageResponseMegabytes limits final compressed image bytes per request.
	MaxImageResponseMegabytes *int `yaml:"max-image-response-megabytes,omitempty" json:"max-image-response-megabytes,omitempty"`
	// MaxN limits the number of images requested from ChatGPT Web in one logical request.
	MaxN *int `yaml:"max-n,omitempty" json:"max-n,omitempty"`
	// MaxInFlight bounds complete ChatGPT Web image attempts, including sleeping polls.
	MaxInFlight *int `yaml:"max-in-flight,omitempty" json:"max-in-flight,omitempty"`
	// AdmissionQueueSize bounds requests waiting to enter the Web image lifecycle.
	AdmissionQueueSize *int `yaml:"admission-queue-size,omitempty" json:"admission-queue-size,omitempty"`
	// AdmissionWaitMilliseconds limits pre-upstream lifecycle admission waiting.
	AdmissionWaitMilliseconds *int `yaml:"admission-wait-milliseconds,omitempty" json:"admission-wait-milliseconds,omitempty"`
	// MaxFinalizers bounds settled requests admitted to finalizer staging. The
	// memory-owning download/decode/encode critical section is serialized.
	MaxFinalizers *int `yaml:"max-finalizers,omitempty" json:"max-finalizers,omitempty"`
	// CompletionReserveMegabytes reserves a small per-attempt completion allowance before upstream work.
	CompletionReserveMegabytes *int `yaml:"completion-reserve-megabytes,omitempty" json:"completion-reserve-megabytes,omitempty"`
	// MemoryCapacityMegabytes bounds the shared process-wide ChatGPT Web image working set.
	MemoryCapacityMegabytes *int `yaml:"memory-capacity-megabytes,omitempty" json:"memory-capacity-megabytes,omitempty"`
}

const (
	DefaultChatGPTWebAspectRatioMaxErrorPercent = 1.0
	MaxChatGPTWebAspectRatioMaxErrorPercent     = 10.0
	DefaultChatGPTWebMaxResizeEdgePixels        = 3840
	MaxChatGPTWebMaxResizeEdgePixels            = 3840
	ChatGPTWebResizeFilterCatmullRom            = "catmull-rom"
	ChatGPTWebResizeFilterApproxBiLinear        = "approx-bilinear"
	DefaultChatGPTWebResizeFilter               = ChatGPTWebResizeFilterCatmullRom
	DefaultChatGPTWebMaxImageResponseMegabytes  = 128
	MinChatGPTWebMaxImageResponseMegabytes      = 1
	MaxChatGPTWebMaxImageResponseMegabytes      = 256
	DefaultChatGPTWebMaxN                       = 1
	MinChatGPTWebMaxN                           = 1
	MaxChatGPTWebMaxN                           = 10
	DefaultChatGPTWebImageMaxInFlight           = 64
	MinChatGPTWebImageMaxInFlight               = 1
	MaxChatGPTWebImageMaxInFlight               = 512
	DefaultChatGPTWebImageAdmissionQueueSize    = 64
	MinChatGPTWebImageAdmissionQueueSize        = 0
	MaxChatGPTWebImageAdmissionQueueSize        = 512
	DefaultChatGPTWebImageAdmissionWaitMS       = 1000
	MinChatGPTWebImageAdmissionWaitMS           = 0
	MaxChatGPTWebImageAdmissionWaitMS           = 30000
	DefaultChatGPTWebImageMaxFinalizers         = 8
	MinChatGPTWebImageMaxFinalizers             = 1
	MaxChatGPTWebImageMaxFinalizers             = 64
	DefaultChatGPTWebImageCompletionReserveMB   = 1
	MinChatGPTWebImageCompletionReserveMB       = 0
	MaxChatGPTWebImageCompletionReserveMB       = 32
	DefaultChatGPTWebImageMemoryCapacityMB      = 512
	MinChatGPTWebImageMemoryCapacityMB          = 64
	MaxChatGPTWebImageMemoryCapacityMB          = 8192
)

// ResolvedChatGPTWebImageConfig contains effective ChatGPT Web image compatibility values.
type ResolvedChatGPTWebImageConfig struct {
	UpstreamModel              string
	IgnoreUnsupportedParams    bool
	AdaptSizeToAspectRatio     bool
	StrictSize                 bool
	AspectRatioMaxErrorPercent float64
	MaxResizeEdgePixels        int
	ResizeToRequestedSize      bool
	ResizeFilter               string
	MaxImageResponseMegabytes  int
	MaxN                       int
	MaxInFlight                int
	AdmissionQueueSize         int
	AdmissionWaitMilliseconds  int
	MaxFinalizers              int
	CompletionReserveMegabytes int
	MemoryCapacityMegabytes    int
}

// ResolvedUpstreamModel returns the effective ChatGPT Web image conversation model.
func (cfg ChatGPTWebImageConfig) ResolvedUpstreamModel() string {
	if model := strings.TrimSpace(cfg.UpstreamModel); model != "" {
		return model
	}
	return DefaultChatGPTWebImageUpstreamModel
}

// Resolved returns the effective ChatGPT Web image compatibility configuration.
func (cfg ChatGPTWebImageConfig) Resolved() ResolvedChatGPTWebImageConfig {
	resolved := ResolvedChatGPTWebImageConfig{
		UpstreamModel:              cfg.ResolvedUpstreamModel(),
		IgnoreUnsupportedParams:    cfg.IgnoreUnsupportedParams,
		AdaptSizeToAspectRatio:     cfg.AdaptSizeToAspectRatio,
		StrictSize:                 cfg.StrictSize,
		AspectRatioMaxErrorPercent: DefaultChatGPTWebAspectRatioMaxErrorPercent,
		MaxResizeEdgePixels:        DefaultChatGPTWebMaxResizeEdgePixels,
		ResizeToRequestedSize:      cfg.ResizeToRequestedSize,
		ResizeFilter:               DefaultChatGPTWebResizeFilter,
		MaxImageResponseMegabytes:  DefaultChatGPTWebMaxImageResponseMegabytes,
		MaxN:                       DefaultChatGPTWebMaxN,
		MaxInFlight:                DefaultChatGPTWebImageMaxInFlight,
		AdmissionQueueSize:         DefaultChatGPTWebImageAdmissionQueueSize,
		AdmissionWaitMilliseconds:  DefaultChatGPTWebImageAdmissionWaitMS,
		MaxFinalizers:              DefaultChatGPTWebImageMaxFinalizers,
		CompletionReserveMegabytes: DefaultChatGPTWebImageCompletionReserveMB,
		MemoryCapacityMegabytes:    DefaultChatGPTWebImageMemoryCapacityMB,
	}
	if cfg.AspectRatioMaxErrorPercent != nil {
		resolved.AspectRatioMaxErrorPercent = *cfg.AspectRatioMaxErrorPercent
	}
	if cfg.MaxResizeEdgePixels != nil {
		resolved.MaxResizeEdgePixels = *cfg.MaxResizeEdgePixels
	}
	if filter := strings.ToLower(strings.TrimSpace(cfg.ResizeFilter)); filter != "" {
		resolved.ResizeFilter = filter
	}
	if cfg.MaxImageResponseMegabytes != nil {
		resolved.MaxImageResponseMegabytes = *cfg.MaxImageResponseMegabytes
	}
	if cfg.MaxN != nil {
		resolved.MaxN = *cfg.MaxN
	}
	if cfg.MaxInFlight != nil {
		resolved.MaxInFlight = *cfg.MaxInFlight
	}
	if cfg.AdmissionQueueSize != nil {
		resolved.AdmissionQueueSize = *cfg.AdmissionQueueSize
	}
	if cfg.AdmissionWaitMilliseconds != nil {
		resolved.AdmissionWaitMilliseconds = *cfg.AdmissionWaitMilliseconds
	}
	if cfg.MaxFinalizers != nil {
		resolved.MaxFinalizers = *cfg.MaxFinalizers
	}
	if cfg.CompletionReserveMegabytes != nil {
		resolved.CompletionReserveMegabytes = *cfg.CompletionReserveMegabytes
	}
	if cfg.MemoryCapacityMegabytes != nil {
		resolved.MemoryCapacityMegabytes = *cfg.MemoryCapacityMegabytes
	}
	return resolved
}

// Validate rejects invalid ChatGPT Web image compatibility settings.
func (cfg ChatGPTWebImageConfig) Validate() error {
	resolved := cfg.Resolved()
	if math.IsNaN(resolved.AspectRatioMaxErrorPercent) || math.IsInf(resolved.AspectRatioMaxErrorPercent, 0) ||
		resolved.AspectRatioMaxErrorPercent < 0 ||
		resolved.AspectRatioMaxErrorPercent > MaxChatGPTWebAspectRatioMaxErrorPercent {
		return fmt.Errorf("images.chatgpt-web.aspect-ratio-max-error-percent must be between 0 and %.0f", MaxChatGPTWebAspectRatioMaxErrorPercent)
	}
	if resolved.MaxResizeEdgePixels < 1 || resolved.MaxResizeEdgePixels > MaxChatGPTWebMaxResizeEdgePixels {
		return fmt.Errorf("images.chatgpt-web.max-resize-edge-pixels must be between 1 and %d", MaxChatGPTWebMaxResizeEdgePixels)
	}
	if resolved.ResizeToRequestedSize && !resolved.AdaptSizeToAspectRatio {
		return fmt.Errorf("images.chatgpt-web.resize-to-requested-size requires adapt-size-to-aspect-ratio")
	}
	if resolved.StrictSize && !resolved.AdaptSizeToAspectRatio {
		return fmt.Errorf("images.chatgpt-web.strict-size requires adapt-size-to-aspect-ratio")
	}
	if resolved.ResizeFilter != ChatGPTWebResizeFilterCatmullRom && resolved.ResizeFilter != ChatGPTWebResizeFilterApproxBiLinear {
		return fmt.Errorf("images.chatgpt-web.resize-filter must be %s or %s", ChatGPTWebResizeFilterCatmullRom, ChatGPTWebResizeFilterApproxBiLinear)
	}
	if resolved.MaxImageResponseMegabytes < MinChatGPTWebMaxImageResponseMegabytes ||
		resolved.MaxImageResponseMegabytes > MaxChatGPTWebMaxImageResponseMegabytes {
		return fmt.Errorf("images.chatgpt-web.max-image-response-megabytes must be between %d and %d", MinChatGPTWebMaxImageResponseMegabytes, MaxChatGPTWebMaxImageResponseMegabytes)
	}
	if resolved.MaxN < MinChatGPTWebMaxN || resolved.MaxN > MaxChatGPTWebMaxN {
		return fmt.Errorf("images.chatgpt-web.max-n must be between %d and %d", MinChatGPTWebMaxN, MaxChatGPTWebMaxN)
	}
	if resolved.MaxInFlight < MinChatGPTWebImageMaxInFlight || resolved.MaxInFlight > MaxChatGPTWebImageMaxInFlight {
		return fmt.Errorf("images.chatgpt-web.max-in-flight must be between %d and %d", MinChatGPTWebImageMaxInFlight, MaxChatGPTWebImageMaxInFlight)
	}
	if resolved.AdmissionQueueSize < MinChatGPTWebImageAdmissionQueueSize || resolved.AdmissionQueueSize > MaxChatGPTWebImageAdmissionQueueSize {
		return fmt.Errorf("images.chatgpt-web.admission-queue-size must be between %d and %d", MinChatGPTWebImageAdmissionQueueSize, MaxChatGPTWebImageAdmissionQueueSize)
	}
	if resolved.AdmissionWaitMilliseconds < MinChatGPTWebImageAdmissionWaitMS || resolved.AdmissionWaitMilliseconds > MaxChatGPTWebImageAdmissionWaitMS {
		return fmt.Errorf("images.chatgpt-web.admission-wait-milliseconds must be between %d and %d", MinChatGPTWebImageAdmissionWaitMS, MaxChatGPTWebImageAdmissionWaitMS)
	}
	if resolved.MaxFinalizers < MinChatGPTWebImageMaxFinalizers || resolved.MaxFinalizers > MaxChatGPTWebImageMaxFinalizers {
		return fmt.Errorf("images.chatgpt-web.max-finalizers must be between %d and %d", MinChatGPTWebImageMaxFinalizers, MaxChatGPTWebImageMaxFinalizers)
	}
	if resolved.CompletionReserveMegabytes < MinChatGPTWebImageCompletionReserveMB || resolved.CompletionReserveMegabytes > MaxChatGPTWebImageCompletionReserveMB {
		return fmt.Errorf("images.chatgpt-web.completion-reserve-megabytes must be between %d and %d", MinChatGPTWebImageCompletionReserveMB, MaxChatGPTWebImageCompletionReserveMB)
	}
	if resolved.MemoryCapacityMegabytes < MinChatGPTWebImageMemoryCapacityMB || resolved.MemoryCapacityMegabytes > MaxChatGPTWebImageMemoryCapacityMB {
		return fmt.Errorf("images.chatgpt-web.memory-capacity-megabytes must be between %d and %d", MinChatGPTWebImageMemoryCapacityMB, MaxChatGPTWebImageMemoryCapacityMB)
	}
	return nil
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

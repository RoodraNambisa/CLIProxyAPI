package executor

import (
	"net/http"
	"net/url"
	"sync/atomic"

	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

// RequestedModelMetadataKey stores the client-requested model name in Options.Metadata.
const RequestedModelMetadataKey = "requested_model"

// RequestPathMetadataKey stores the inbound HTTP request path in Options.Metadata.
const RequestPathMetadataKey = "request_path"

// ExecutionModelOverrideMetadataKey overrides the upstream execution model while
// keeping auth selection bound to Request.Model.
const ExecutionModelOverrideMetadataKey = "execution_model_override"

// InteractionsAPIVersionMetadataKey stores the trusted Interactions route version.
const InteractionsAPIVersionMetadataKey = "interactions_api_version"

// InteractionsAPIRevisionMetadataKey stores the client-supplied Interactions revision.
const InteractionsAPIRevisionMetadataKey = "interactions_api_revision"

const (
	// StreamBufferSize bounds per-stream buffering between executor layers.
	StreamBufferSize = 16

	// PinnedAuthMetadataKey locks execution to a specific auth ID.
	PinnedAuthMetadataKey = "pinned_auth_id"
	// ImageGenerationStreamPassthroughMetadataKey requests low-overhead passthrough for
	// streaming Responses image_generation events.
	ImageGenerationStreamPassthroughMetadataKey = "image_generation_stream_passthrough"
	// ImageGenerationStreamPassthroughStateMetadataKey carries the effective passthrough
	// state after provider policy has transformed the request.
	ImageGenerationStreamPassthroughStateMetadataKey = "image_generation_stream_passthrough_state"
	// ImageGenerationMaxResultsMetadataKey limits provider-side image result
	// materialization for compatibility image endpoints.
	ImageGenerationMaxResultsMetadataKey = "image_generation_max_results"
	// ChatGPTWebIgnoreUnsupportedImageParamsMetadataKey pins the compatibility
	// endpoint's unsupported image parameter policy for one execution.
	ChatGPTWebIgnoreUnsupportedImageParamsMetadataKey = "chatgpt_web_ignore_unsupported_image_params"
	// ChatGPTWebImageConfigSnapshotMetadataKey pins Web image adaptation settings
	// after compatibility routing has accepted a request.
	ChatGPTWebImageConfigSnapshotMetadataKey = "chatgpt_web_image_config_snapshot"
	// ImageGenerationResultStateMetadataKey carries provider-confirmed image
	// generation success back to the auth result recorder.
	ImageGenerationResultStateMetadataKey = "image_generation_result_state"
	// TrustUpstreamSSEMetadataKey requests direct forwarding of trusted upstream SSE frames.
	TrustUpstreamSSEMetadataKey = "trust_upstream_sse"
	// SelectionAttemptMetadataKey stores the outer retry attempt index for auth selection.
	SelectionAttemptMetadataKey = "selection_attempt"
	// SelectedAuthMetadataKey stores the auth ID selected by the scheduler.
	SelectedAuthMetadataKey = "selected_auth_id"
	// SelectedAuthInstanceMetadataKey stores the immutable runtime instance selected by the scheduler.
	SelectedAuthInstanceMetadataKey = "selected_auth_instance_id"
	// SelectedAuthInstanceRetirementMetadataKey carries the selected instance retirement state.
	SelectedAuthInstanceRetirementMetadataKey = "selected_auth_instance_retirement"
	// StreamTerminalMarkerMetadataKey asks compatible executors to emit an internal
	// zero-payload completion marker before closing a successful stream.
	StreamTerminalMarkerMetadataKey = "stream_terminal_marker"
	// SelectedAuthCallbackMetadataKey carries an optional callback invoked with the selected auth ID.
	SelectedAuthCallbackMetadataKey = "selected_auth_callback"
	// ExecutionSessionMetadataKey identifies a long-lived downstream execution session.
	ExecutionSessionMetadataKey = "execution_session_id"
)

// ChatGPTWebImageConfigSnapshot contains request-scoped Web image adaptation settings.
type ChatGPTWebImageConfigSnapshot struct {
	AdaptSizeToAspectRatio     bool
	StrictSize                 bool
	AspectRatioMaxErrorPercent float64
	MaxResizeEdgePixels        int
	ResizeToRequestedSize      bool
	ResizeFilter               string
	MaxImageResponseBytes      int
	MaxN                       int
}

// ImageGenerationStreamPassthroughState reports whether the selected upstream request
// still contains an image generation tool after provider policy is applied.
type ImageGenerationStreamPassthroughState struct {
	enabled atomic.Bool
}

// SetEnabled updates the effective image stream passthrough state.
func (s *ImageGenerationStreamPassthroughState) SetEnabled(enabled bool) {
	if s != nil {
		s.enabled.Store(enabled)
	}
}

// Enabled returns the effective image stream passthrough state.
func (s *ImageGenerationStreamPassthroughState) Enabled() bool {
	return s != nil && s.enabled.Load()
}

// ImageGenerationResultState records how many completed images an upstream
// provider returned. Merely accepting a request with an image tool is not success.
type ImageGenerationResultState struct {
	succeededCount atomic.Int64
	producedCount  atomic.Int64
}

// MarkSucceeded records provider-confirmed image output.
func (s *ImageGenerationResultState) MarkSucceeded() {
	s.AddSucceeded(1)
}

// AddSucceeded records provider-confirmed image output.
func (s *ImageGenerationResultState) AddSucceeded(count int) {
	if s != nil && count > 0 {
		s.succeededCount.Add(int64(count))
	}
}

// Succeeded reports whether provider-confirmed image output was produced.
func (s *ImageGenerationResultState) Succeeded() bool {
	return s.SucceededCount() > 0
}

// SucceededCount returns the number of provider-confirmed image outputs.
func (s *ImageGenerationResultState) SucceededCount() int64 {
	if s == nil {
		return 0
	}
	return s.succeededCount.Load()
}

// AddProduced records images that were generated and downloaded even if local
// post-processing later prevents them from reaching the client.
func (s *ImageGenerationResultState) AddProduced(count int) {
	if s != nil && count > 0 {
		s.producedCount.Add(int64(count))
	}
}

// ProducedCount returns the number of generated and downloaded images.
func (s *ImageGenerationResultState) ProducedCount() int64 {
	if s == nil {
		return 0
	}
	return s.producedCount.Load()
}

// TakeProducedCount atomically consumes generated images for quota projection.
func (s *ImageGenerationResultState) TakeProducedCount() int64 {
	if s == nil {
		return 0
	}
	return s.producedCount.Swap(0)
}

// AuthInstanceRetirement reports whether a selected runtime auth instance was retired.
type AuthInstanceRetirement interface {
	Retired() bool
}

// Request encapsulates the translated payload that will be sent to a provider executor.
type Request struct {
	// Model is the upstream model identifier after translation.
	Model string
	// Payload is the provider specific JSON payload.
	Payload []byte
	// Format represents the provider payload schema.
	Format sdktranslator.Format
	// Metadata carries optional provider specific execution hints.
	Metadata map[string]any
}

// Options controls execution behavior for both streaming and non-streaming calls.
type Options struct {
	// Stream toggles streaming mode.
	Stream bool
	// Alt carries optional alternate format hint (e.g. SSE JSON key).
	Alt string
	// Headers are forwarded to the provider request builder.
	Headers http.Header
	// Query contains optional query string parameters.
	Query url.Values
	// OriginalRequest preserves the inbound request bytes prior to translation.
	OriginalRequest []byte
	// SourceFormat identifies the inbound schema.
	SourceFormat sdktranslator.Format
	// ResponseFormat identifies the downstream response schema. Empty preserves
	// the historical behavior of using SourceFormat.
	ResponseFormat sdktranslator.Format
	// Metadata carries extra execution hints shared across selection and executors.
	Metadata map[string]any
}

// ResponseFormatOrSource returns the explicit downstream format when present.
func ResponseFormatOrSource(opts Options) sdktranslator.Format {
	if opts.ResponseFormat != "" {
		return opts.ResponseFormat
	}
	return opts.SourceFormat
}

// Response wraps either a full provider response or metadata for streaming flows.
type Response struct {
	// Payload is the provider response in the executor format.
	Payload []byte
	// Metadata exposes optional structured data for translators.
	Metadata map[string]any
	// Headers carries upstream HTTP response headers for passthrough to clients.
	Headers http.Header
}

// StreamChunk represents a single streaming payload unit emitted by provider executors.
type StreamChunk struct {
	// Payload is the raw provider chunk payload.
	Payload []byte
	// Err reports any terminal error encountered while producing chunks.
	Err error
}

// StreamResult wraps the streaming response, providing both the chunk channel
// and the upstream HTTP response headers captured before streaming begins.
type StreamResult struct {
	// Headers carries upstream HTTP response headers from the initial connection.
	Headers http.Header
	// Chunks is the channel of streaming payload units.
	Chunks <-chan StreamChunk
}

// StatusError represents an error that carries an HTTP-like status code.
// Provider executors should implement this when possible to enable
// better auth state updates on failures (e.g., 401/402/429).
type StatusError interface {
	error
	StatusCode() int
}

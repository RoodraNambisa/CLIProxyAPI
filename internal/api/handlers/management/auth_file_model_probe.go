package management

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

const modelProbeDefaultPrompt = "Reply with exactly OK."

type modelProbeRequest struct {
	Name            string                `json:"name"`
	Model           string                `json:"model"`
	Protocol        string                `json:"protocol"`
	Stream          bool                  `json:"stream"`
	Upstream        string                `json:"upstream"`
	Prompt          string                `json:"prompt,omitempty"`
	MaxOutputTokens int                   `json:"max_output_tokens,omitempty"`
	RequestBody     json.RawMessage       `json:"request_body,omitempty"`
	CodexState      *modelProbeStateInput `json:"codex_state,omitempty"`
}

type modelProbeResult struct {
	Success             bool                   `json:"success"`
	Name                string                 `json:"name"`
	Provider            string                 `json:"provider,omitempty"`
	Model               string                 `json:"model"`
	UpstreamModel       string                 `json:"upstream_model,omitempty"`
	ReturnedModel       string                 `json:"returned_model,omitempty"`
	RequestPath         string                 `json:"request_path"`
	UpstreamURL         string                 `json:"upstream_url,omitempty"`
	Stream              bool                   `json:"stream"`
	LatencyMS           int64                  `json:"latency_ms"`
	StatusCode          int                    `json:"status_code,omitempty"`
	RequestID           string                 `json:"request_id,omitempty"`
	ResponseID          string                 `json:"response_id,omitempty"`
	FinishReason        string                 `json:"finish_reason,omitempty"`
	Response            string                 `json:"response,omitempty"`
	Error               string                 `json:"error,omitempty"`
	Usage               *modelProbeUsage       `json:"usage,omitempty"`
	RequestBody         string                 `json:"request_body,omitempty"`
	UpstreamRequestBody string                 `json:"upstream_request_body,omitempty"`
	ResponseBody        string                 `json:"response_body,omitempty"`
	DetailsTruncated    bool                   `json:"details_truncated,omitempty"`
	CodexState          *modelProbeStateResult `json:"codex_state,omitempty"`
}

// ProbeAuthFileModel uses the credential's provider implementation with temporary
// request options. No account selection, route persistence or replay is involved.
func (h *Handler) ProbeAuthFileModel(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 128*1024)
	var input modelProbeRequest
	if c.ShouldBindJSON(&input) != nil {
		c.JSON(400, gin.H{"error": "invalid model probe request"})
		return
	}
	input.Name, input.Model = strings.TrimSpace(input.Name), strings.TrimSpace(input.Model)
	if input.Name == "" || len(input.Name) > 512 || input.Model == "" || len(input.Model) > 256 || strings.ContainsAny(input.Model, "\r\n\x00") {
		c.JSON(400, gin.H{"error": "credential name and model are required"})
		return
	}
	if len(input.Prompt) > 16*1024 || len(input.RequestBody) > 64*1024 || input.MaxOutputTokens < 0 || input.MaxOutputTokens > 32768 {
		c.JSON(400, gin.H{"error": "probe prompt/body is too large or output token limit is outside 1–32768"})
		return
	}
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	if manager == nil {
		c.JSON(503, gin.H{"error": "credential manager unavailable"})
		return
	}
	auth := h.findManagedAuthWithManager(input.Name, manager)
	if auth == nil {
		c.JSON(404, gin.H{"error": "credential not found"})
		return
	}
	provider := auth.ExecutionProvider()
	stateMode, stateValue, errState := validateModelProbeState(input.CodexState, provider)
	if errState != nil {
		c.JSON(400, gin.H{"error": errState.Error()})
		return
	}
	if coreauth.IsRetiredGeminiCLIAuth(auth) || provider == "qwen" || provider == "iflow" {
		c.JSON(400, gin.H{"error": "credential provider is no longer supported"})
		return
	}
	cfg, err := config.Clone(h.currentConfig())
	if err != nil {
		c.JSON(500, gin.H{"error": "configuration unavailable"})
		return
	}
	if cfg == nil {
		cfg = &config.Config{}
	}
	if input.Protocol == "" {
		input.Protocol = "responses"
	}
	format, requestPath := sdktranslator.FormatOpenAIResponse, "/v1/responses"
	switch input.Protocol {
	case "responses":
	case "chat", "chat-direct", "chat-responses":
		format, requestPath = sdktranslator.FormatOpenAI, "/v1/chat/completions"
		if input.Protocol == "chat-direct" {
			if provider != "xai" {
				c.JSON(400, gin.H{"error": "explicit Chat routing is available only for Grok"})
				return
			}
			cfg.XAI.ChatCompletionsMode = "direct"
		} else if input.Protocol == "chat-responses" {
			cfg.XAI.ChatCompletionsMode = "responses"
		}
	default:
		c.JSON(400, gin.H{"error": "unsupported probe protocol"})
		return
	}
	input.Upstream = strings.TrimSpace(input.Upstream)
	if input.Upstream != "" && input.Upstream != "configured" {
		if provider != "xai" {
			c.JSON(400, gin.H{"error": "upstream override is available only for Grok"})
			return
		}
		if base, ok := config.XAIBaseURLForMode(input.Upstream); ok {
			input.Upstream = base
		}
		input.Upstream, err = config.NormalizeXAIBaseURL(input.Upstream)
		if err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
	}
	executor, _ := manager.Executor(provider)
	// These HTTP adapters have no long-lived resources and permit a Grok config
	// override without mutating the registered executor. Other providers reuse
	// their registered lifecycle, browser relay and login dependencies.
	switch provider {
	case "codex":
		executor = runtimeexecutor.NewCodexExecutor(cfg)
	case "xai":
		executor = runtimeexecutor.NewXAIExecutor(cfg)
	}
	if executor == nil {
		c.JSON(400, gin.H{"error": "credential provider executor is unavailable"})
		return
	}
	payload, err := buildModelProbePayload(input, format)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	probeID := uuid.NewString()
	opts := core.Options{Stream: input.Stream, SourceFormat: format, OriginalRequest: payload, Headers: http.Header{"X-Session-Id": {probeID}},
		Metadata: map[string]any{core.RequestPathMetadataKey: requestPath, core.ExecutionSessionMetadataKey: probeID, core.StreamTerminalMarkerMetadataKey: true}}
	result := modelProbeResult{Name: input.Name, Provider: provider, Model: input.Model, RequestPath: requestPath, Stream: input.Stream, RequestBody: string(payload)}
	observer := modelProbeObserver{}
	trace := modelProbeTrace{}
	if provider == "codex" {
		trace.codexState = &modelProbeStateResult{Mode: stateMode, Source: "none"}
	}
	started := time.Now()
	ctx, cancel := context.WithCancel(c.Request.Context())
	defer cancel()
	err = manager.ProbeCredential(ctx, auth, executor, core.Request{Model: input.Model, Payload: payload}, opts, func(execCtx context.Context, selected *coreauth.Auth, req core.Request, options core.Options) error {
		selected = selected.Clone()
		if provider == "xai" && input.Upstream != "" && input.Upstream != "configured" {
			if selected.Metadata == nil {
				selected.Metadata = make(map[string]any)
			}
			selected.Metadata[helps.XAIModelRoutesKey] = []config.XAIModelRoute{{Models: []string{"*"}, Upstream: input.Upstream}}
		}
		auth = selected
		result.UpstreamModel = thinking.ParseSuffix(req.Model).ModelName
		if strings.Contains(strings.ToLower(result.UpstreamModel), "image") || strings.Contains(strings.ToLower(result.UpstreamModel), "video") {
			return fmt.Errorf("this probe supports text models; image and video generation are not tested")
		}
		execCtx = helps.WithRequestTrace(execCtx, trace.request, trace.response)
		if provider == "codex" {
			execCtx = helps.WithCodexStateDiagnostic(execCtx, stateMode, stateValue, trace.stateApplied)
		}
		if !input.Stream {
			response, errExecute := executor.Execute(execCtx, selected, req, options)
			if errExecute != nil {
				return errExecute
			}
			result.StatusCode = response.StatusCode
			if result.StatusCode == 0 {
				result.StatusCode = 200
			}
			result.RequestID = probeRequestID(response.Headers)
			if errObserve := observer.observe(response.Payload, false); errObserve != nil {
				return errObserve
			}
		} else {
			stream, errExecute := executor.ExecuteStream(execCtx, selected, req, options)
			if errExecute != nil {
				return errExecute
			}
			if stream == nil || stream.Chunks == nil {
				return fmt.Errorf("upstream returned no stream")
			}
			result.StatusCode, result.RequestID = 200, probeRequestID(stream.Headers)
		consume:
			for {
				select {
				case <-execCtx.Done():
					return execCtx.Err()
				case chunk, ok := <-stream.Chunks:
					if !ok {
						break consume
					}
					if core.IsSuccessfulStreamTerminalChunk(chunk) {
						observer.finished = true
						continue
					}
					if chunk.Err != nil {
						return chunk.Err
					}
					if errObserve := observer.observe(chunk.Payload, true); errObserve != nil {
						return errObserve
					}
				}
			}
		}
		if !observer.finished || !observer.hasOutput {
			return fmt.Errorf("upstream did not return a completed response")
		}
		return nil
	})
	observer.fill(&result)
	trace.fill(&result)
	result.LatencyMS = time.Since(started).Milliseconds()
	result.Success = err == nil
	if err != nil {
		var status core.StatusError
		if result.StatusCode == 0 && errors.As(err, &status) {
			result.StatusCode = status.StatusCode()
		}
		result.Error = probeSafeText(err.Error(), auth, cfg, 4000)
		if result.ResponseBody == "" {
			result.ResponseBody = result.Error
		}
	}
	for _, field := range []*string{&result.Response, &result.RequestBody, &result.UpstreamRequestBody, &result.ResponseBody} {
		*field = probeSafeText(*field, auth, cfg, modelProbeDetailLimit)
	}
	for _, field := range []*string{&result.RequestID, &result.ResponseID, &result.ReturnedModel, &result.UpstreamModel, &result.FinishReason} {
		*field = probeSafeText(*field, auth, cfg, 256)
	}
	result.UpstreamURL = probeSafeText(probeSafeURL(result.UpstreamURL), auth, cfg, 2048)
	// Full state is exposed only in the dedicated, authenticated diagnostic field.
	for _, secret := range []string{stateValue, trace.sentState(), trace.returnedState()} {
		if len(secret) < 4 {
			continue
		}
		for _, field := range []*string{&result.Error, &result.Response, &result.RequestBody, &result.UpstreamRequestBody, &result.ResponseBody} {
			*field = strings.ReplaceAll(*field, secret, "[redacted-state]")
			if encoded, errEncode := json.Marshal(secret); errEncode == nil {
				*field = strings.ReplaceAll(*field, string(encoded[1:len(encoded)-1]), "[redacted-state]")
			}
		}
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, result)
}

func buildModelProbePayload(input modelProbeRequest, format sdktranslator.Format) ([]byte, error) {
	prompt := input.Prompt
	if strings.TrimSpace(prompt) == "" {
		prompt = modelProbeDefaultPrompt
	}
	body := map[string]any{}
	if format == sdktranslator.FormatOpenAI {
		body["messages"] = []any{map[string]any{"role": "user", "content": prompt}}
	} else {
		body["input"] = []any{map[string]any{"role": "user", "content": prompt}}
	}
	if len(input.RequestBody) > 0 {
		var custom map[string]any
		decoder := json.NewDecoder(bytes.NewReader(input.RequestBody))
		decoder.UseNumber()
		if !json.Valid(input.RequestBody) || decoder.Decode(&custom) != nil || custom == nil {
			return nil, fmt.Errorf("temporary request body must be a JSON object")
		}
		for key, value := range custom {
			body[key] = value
		}
	}
	body["model"], body["stream"] = input.Model, input.Stream
	budget := input.MaxOutputTokens
	if budget == 0 {
		budget = 1024
	}
	tokenField := "max_output_tokens"
	if format == sdktranslator.FormatOpenAI {
		tokenField = "max_tokens"
	}
	_, hasBudget := body[tokenField]
	_, hasCompletionBudget := body["max_completion_tokens"]
	if !hasBudget && !(format == sdktranslator.FormatOpenAI && hasCompletionBudget) {
		body[tokenField] = budget
	}
	if format == sdktranslator.FormatOpenAI && input.Stream {
		options, ok := body["stream_options"].(map[string]any)
		if body["stream_options"] == nil || ok {
			if options == nil {
				options = map[string]any{}
				body["stream_options"] = options
			}
			if _, exists := options["include_usage"]; !exists {
				options["include_usage"] = true
			}
		}
	}
	if tools, ok := body["tools"].([]any); ok {
		for _, raw := range tools {
			if tool, ok := raw.(map[string]any); ok && (tool["type"] == "image_generation" || tool["type"] == "video_generation") {
				return nil, fmt.Errorf("media generation tools are not supported by text probes")
			}
		}
	}
	return json.Marshal(body)
}

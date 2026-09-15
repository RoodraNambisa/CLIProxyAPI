package management

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
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
	"github.com/tidwall/gjson"
)

type modelProbeRequest struct {
	Name     string `json:"name"`
	Model    string `json:"model"`
	Protocol string `json:"protocol"`
	Stream   bool   `json:"stream"`
	Upstream string `json:"upstream"`
}

type modelProbeResult struct {
	Success       bool   `json:"success"`
	Name          string `json:"name"`
	Model         string `json:"model"`
	UpstreamModel string `json:"upstream_model,omitempty"`
	RequestPath   string `json:"request_path"`
	UpstreamURL   string `json:"upstream_url,omitempty"`
	Stream        bool   `json:"stream"`
	LatencyMS     int64  `json:"latency_ms"`
	StatusCode    int    `json:"status_code,omitempty"`
	RequestID     string `json:"request_id,omitempty"`
	Response      string `json:"response,omitempty"`
	Error         string `json:"error,omitempty"`
}

// ProbeAuthFileModel exercises the regular HTTP executor with request-local
// overrides. It does not select accounts, retry a different node, or save routes.
func (h *Handler) ProbeAuthFileModel(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 8*1024)
	var input modelProbeRequest
	if c.ShouldBindJSON(&input) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid model probe request"})
		return
	}
	input.Name, input.Model = strings.TrimSpace(input.Name), strings.TrimSpace(input.Model)
	if input.Name == "" || len(input.Name) > 512 || input.Model == "" || len(input.Model) > 256 || strings.ContainsAny(input.Model, "\r\n\x00") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "credential name and model are required"})
		return
	}
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	if manager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "credential manager unavailable"})
		return
	}
	auth := h.findManagedAuthWithManager(input.Name, manager)
	if auth == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "credential not found"})
		return
	}
	provider := strings.ToLower(auth.Provider)
	if provider != "xai" && provider != "codex" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "model probe supports only Codex and Grok"})
		return
	}
	cfg, err := config.Clone(h.currentConfig())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "configuration unavailable"})
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
				c.JSON(http.StatusBadRequest, gin.H{"error": "Codex Chat requests require Responses conversion"})
				return
			}
			cfg.XAI.ChatCompletionsMode = "direct"
		} else if input.Protocol == "chat-responses" {
			cfg.XAI.ChatCompletionsMode = "responses"
		}
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported probe protocol"})
		return
	}
	input.Upstream = strings.TrimSpace(input.Upstream)
	if input.Upstream != "" && input.Upstream != "configured" {
		if provider != "xai" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "upstream override is available only for Grok"})
			return
		}
		if baseURL, ok := config.XAIBaseURLForMode(input.Upstream); ok {
			input.Upstream = baseURL
		}
		input.Upstream, err = config.NormalizeXAIBaseURL(input.Upstream)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	}
	var executor coreauth.ProviderExecutor = runtimeexecutor.NewCodexExecutor(cfg)
	if provider == "xai" {
		executor = runtimeexecutor.NewXAIExecutor(cfg)
	}
	probeID := uuid.NewString()
	body := map[string]any{"model": input.Model, "stream": input.Stream, "tool_choice": "none"}
	if format == sdktranslator.FormatOpenAI {
		body["messages"] = []any{map[string]any{"role": "user", "content": "Reply with exactly OK."}}
		body["max_tokens"] = 1024
	} else {
		body["input"] = []any{map[string]any{"role": "user", "content": "Reply with exactly OK."}}
		body["max_output_tokens"] = 1024
	}
	payload, _ := json.Marshal(body)
	opts := core.Options{Stream: input.Stream, SourceFormat: format, OriginalRequest: payload,
		Headers:  http.Header{"X-Session-Id": []string{probeID}},
		Metadata: map[string]any{core.RequestPathMetadataKey: requestPath, core.ExecutionSessionMetadataKey: probeID, core.StreamTerminalMarkerMetadataKey: true},
	}
	result := modelProbeResult{Name: input.Name, Model: input.Model, RequestPath: requestPath, Stream: input.Stream}
	started := time.Now()
	ctx, cancel := context.WithCancel(c.Request.Context())
	defer cancel()
	err = manager.ProbeCredential(ctx, auth, executor, core.Request{Model: input.Model, Payload: payload}, opts,
		func(execCtx context.Context, selected *coreauth.Auth, req core.Request, options core.Options) error {
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
			baseURL := selected.Attributes["base_url"]
			path := "/responses"
			if provider == "xai" {
				upstream, errRoute := helps.ResolveXAIModelUpstream(selected, cfg, req.Model)
				if errRoute != nil {
					return errRoute
				}
				baseURL = upstream.BaseURL
				if format == sdktranslator.FormatOpenAI && cfg.XAI.ChatCompletionsMode == "direct" {
					path = "/chat/completions"
				}
			} else if baseURL == "" {
				baseURL = "https://chatgpt.com/backend-api/codex"
			}
			result.UpstreamURL = probeSafeURL(strings.TrimRight(baseURL, "/") + path)
			observer := modelProbeObserver{}
			if !input.Stream {
				response, errExecute := executor.Execute(execCtx, selected, req, options)
				if errExecute != nil {
					return errExecute
				}
				result.StatusCode = response.StatusCode
				if result.StatusCode == 0 {
					result.StatusCode = http.StatusOK
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
				result.StatusCode, result.RequestID = http.StatusOK, probeRequestID(stream.Headers)
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
			result.Response = observer.text
			if !observer.finished || observer.text == "" {
				return fmt.Errorf("upstream did not return a completed text response")
			}
			return nil
		})
	result.LatencyMS = time.Since(started).Milliseconds()
	result.Success = err == nil
	if err != nil {
		var status core.StatusError
		if result.StatusCode == 0 && errors.As(err, &status) {
			result.StatusCode = status.StatusCode()
		}
		result.Error = probeSafeText(err.Error(), auth, cfg, 2000)
	}
	result.Response = probeSafeText(result.Response, auth, cfg, 240)
	result.RequestID = probeSafeText(result.RequestID, auth, cfg, 128)
	c.JSON(http.StatusOK, result)
}

type modelProbeObserver struct {
	bytes    int
	finished bool
	text     string
}

func (o *modelProbeObserver) observe(payload []byte, stream bool) error {
	o.bytes += len(payload)
	if o.bytes > 2*1024*1024 {
		return fmt.Errorf("probe response exceeded the 2 MiB limit")
	}
	if !stream {
		return o.observeJSON(payload, false)
	}
	// Chat translators return decoded JSON chunks; Responses can return SSE.
	if gjson.ValidBytes(payload) {
		return o.observeJSON(payload, true)
	}
	if strings.TrimSpace(string(payload)) == "[DONE]" {
		o.finished = true
		return nil
	}
	for _, event := range helps.ParseOpenAIStreamFrame(payload) {
		if event.Err != nil {
			return event.Err
		}
		if string(event.Data) == "[DONE]" {
			o.finished = true
		} else if err := o.observeJSON(event.Data, true); err != nil {
			return err
		}
	}
	return nil
}

func (o *modelProbeObserver) observeJSON(payload []byte, stream bool) error {
	if !gjson.ValidBytes(payload) {
		return fmt.Errorf("upstream response is not valid JSON")
	}
	root := gjson.ParseBytes(payload)
	if value := root.Get("error"); value.Exists() && value.Type != gjson.Null {
		return fmt.Errorf("upstream error: %s", value.Raw)
	}
	if root.Get("response").IsObject() {
		root = root.Get("response")
	}
	status := root.Get("status").String()
	kind := gjson.GetBytes(payload, "type").String()
	if status == "failed" || status == "incomplete" || kind == "error" || kind == "response.failed" || kind == "response.incomplete" {
		return fmt.Errorf("upstream did not complete: %s", string(payload))
	}
	if status == "completed" || kind == "response.completed" || kind == "response.done" {
		o.finished = true
	}
	text := root.Get("choices.0.message.content").String()
	if stream {
		text = root.Get("choices.0.delta.content").String()
		if kind == "response.output_text.delta" {
			text = root.Get("delta").String()
		}
	}
	if reason := root.Get("choices.0.finish_reason"); reason.Exists() && reason.Type == gjson.String && reason.Str != "" {
		if reason.Str != "stop" {
			return fmt.Errorf("upstream stopped with finish_reason=%s", reason.Str)
		}
		o.finished = true
	}
	if o.text == "" && text == "" {
		for _, item := range root.Get("output").Array() {
			for _, content := range item.Get("content").Array() {
				if content.Get("type").String() == "output_text" {
					text += content.Get("text").String()
				}
			}
		}
	}
	o.text += text
	return nil
}

func probeRequestID(headers http.Header) string {
	for _, key := range []string{"x-request-id", "x-oai-request-id", "x-grok-req-id", "request-id"} {
		if value := headers.Get(key); value != "" {
			return value
		}
	}
	return ""
}

func probeSafeURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	u.User, u.RawQuery, u.Fragment = nil, "", ""
	return u.String()
}

func probeTruncate(value string, length int) string {
	runes := []rune(value)
	if len(runes) > length {
		return string(runes[:length]) + "…"
	}
	return value
}

func probeSafeText(value string, auth *coreauth.Auth, cfg *config.Config, limit int) string {
	redact := func(secret string) {
		if len(secret) >= 4 {
			value = strings.ReplaceAll(value, secret, "[redacted]")
		}
	}
	if auth != nil {
		for _, key := range []string{"access_token", "refresh_token", "id_token", "api_key", "token", "cookie", "password", helps.XAIIdentitySeedKey} {
			if secret, ok := auth.Metadata[key].(string); ok {
				redact(secret)
			}
			redact(auth.Attributes[key])
		}
		for key, secret := range auth.Attributes {
			if strings.HasPrefix(key, "header:") {
				redact(secret)
			}
		}
		redact(auth.ProxyURL)
	}
	if cfg != nil {
		redact(cfg.ProxyURL)
		for _, secret := range cfg.XAI.Headers {
			redact(secret)
		}
	}
	return probeTruncate(value, limit)
}

package openai

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	. "github.com/router-for-me/CLIProxyAPI/v6/internal/constant"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	executorhelps "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	defaultImagesCodexModel = "gpt-5.4"
	defaultImagesImageModel = "gpt-image-2"
	maxImageUploadBytes     = 50 << 20
	maxImageMultipartBytes  = 220 << 20
	maxImageSSEPendingBytes = ((maxImageUploadBytes + 2) / 3 * 4) + (8 << 20)
	maxImageRequestCount    = 10
	nativeImagesHandlerType = "openai-image"
	nativeImagesGenerations = "images/generations"
	nativeImagesEdits       = "images/edits"
)

type imageOperation struct {
	action         string
	partialEvent   string
	completedEvent string
}

var (
	imageGenerationOperation = imageOperation{
		action:         "generate",
		partialEvent:   "image_generation.partial_image",
		completedEvent: "image_generation.completed",
	}
	imageEditOperation = imageOperation{
		action:         "edit",
		partialEvent:   "image_edit.partial_image",
		completedEvent: "image_edit.completed",
	}
)

// OpenAIImagesAPIHandler adapts OpenAI Images requests to Codex Responses image tools.
type OpenAIImagesAPIHandler struct {
	*handlers.BaseAPIHandler
}

// NewOpenAIImagesAPIHandler creates a new OpenAI Images compatibility handler.
func NewOpenAIImagesAPIHandler(apiHandlers *handlers.BaseAPIHandler) *OpenAIImagesAPIHandler {
	return &OpenAIImagesAPIHandler{
		BaseAPIHandler: apiHandlers,
	}
}

// HandlerType returns the upstream schema used for the Codex-backed request.
func (h *OpenAIImagesAPIHandler) HandlerType() string {
	return OpenaiResponse
}

func imageResponsesProviders(req openAIImageRequest) []string {
	if !chatGPTWebSupportsImageRequest(req) {
		return []string{Codex}
	}
	return []string{Codex, ChatGPTWeb}
}

// Models returns the image model exposed by this compatibility layer.
func (h *OpenAIImagesAPIHandler) Models() []map[string]any {
	return []map[string]any{{
		"id":       h.imagesImageModel(),
		"object":   "model",
		"created":  0,
		"owned_by": "openai",
	}}
}

type openAIImageRequest struct {
	Model             string           `json:"model"`
	Prompt            string           `json:"prompt"`
	N                 *int             `json:"n"`
	Size              string           `json:"size"`
	Quality           string           `json:"quality"`
	Background        string           `json:"background"`
	OutputFormat      string           `json:"output_format"`
	OutputCompression *int             `json:"output_compression"`
	InputFidelity     string           `json:"input_fidelity"`
	Moderation        string           `json:"moderation"`
	Stream            bool             `json:"stream"`
	PartialImages     *int             `json:"partial_images"`
	ResponseFormat    string           `json:"response_format"`
	Images            []imageReference `json:"images,omitempty"`
	Mask              *imageReference  `json:"mask,omitempty"`
}

type imageReference struct {
	ImageURL any    `json:"image_url,omitempty"`
	FileID   string `json:"file_id,omitempty"`
}

type imageOutputItem struct {
	Type          string          `json:"type"`
	Result        string          `json:"result"`
	RevisedPrompt string          `json:"revised_prompt,omitempty"`
	OutputFormat  string          `json:"output_format,omitempty"`
	Size          string          `json:"size,omitempty"`
	Background    string          `json:"background,omitempty"`
	Quality       string          `json:"quality,omitempty"`
	Usage         json.RawMessage `json:"usage,omitempty"`
	InputTokens   json.RawMessage `json:"input_tokens,omitempty"`
	OutputTokens  json.RawMessage `json:"output_tokens,omitempty"`
	TotalTokens   json.RawMessage `json:"total_tokens,omitempty"`

	InputTokensDetails  json.RawMessage `json:"input_tokens_details,omitempty"`
	OutputTokensDetails json.RawMessage `json:"output_tokens_details,omitempty"`
}

type imageResult struct {
	B64JSON       string `json:"b64_json,omitempty"`
	URL           string `json:"url,omitempty"`
	RevisedPrompt string `json:"revised_prompt,omitempty"`
	OutputFormat  string `json:"-"`
	Size          string `json:"-"`
	Background    string `json:"-"`
	Quality       string `json:"-"`
}

type imagesResponse struct {
	Created      int64           `json:"created"`
	Data         []imageResult   `json:"data"`
	Usage        json.RawMessage `json:"usage,omitempty"`
	Background   string          `json:"background,omitempty"`
	OutputFormat string          `json:"output_format,omitempty"`
	Quality      string          `json:"quality,omitempty"`
	Size         string          `json:"size,omitempty"`
}

type responsesImageObject struct {
	CreatedAt int64                      `json:"created_at"`
	Output    []imageOutputItem          `json:"output"`
	Usage     json.RawMessage            `json:"usage,omitempty"`
	ToolUsage map[string]json.RawMessage `json:"tool_usage,omitempty"`
}

type responseCompletedEvent struct {
	Type     string               `json:"type"`
	Response responsesImageObject `json:"response"`
}

type responseOutputItemDoneEvent struct {
	Type string          `json:"type"`
	Item imageOutputItem `json:"item"`
}

type responsePartialImageEvent struct {
	Type              string `json:"type"`
	PartialImageB64   string `json:"partial_image_b64"`
	PartialImageIndex *int   `json:"partial_image_index,omitempty"`
	OutputFormat      string `json:"output_format,omitempty"`
}

type imageStreamEvent struct {
	Type              string          `json:"type"`
	B64JSON           string          `json:"b64_json,omitempty"`
	URL               string          `json:"url,omitempty"`
	RevisedPrompt     string          `json:"revised_prompt,omitempty"`
	PartialImageIndex *int            `json:"partial_image_index,omitempty"`
	Usage             json.RawMessage `json:"usage,omitempty"`
}

type imageUnsupportedError struct {
	err error
}

func (e imageUnsupportedError) Error() string {
	return e.err.Error()
}

func (e imageUnsupportedError) Unwrap() error {
	return e.err
}

// Generations handles POST /v1/images/generations.
func (h *OpenAIImagesAPIHandler) Generations(c *gin.Context) {
	rawJSON, err := readOpenAIImageJSONRequestBody(c)
	if err != nil {
		h.writeImagesRequestError(c, fmt.Errorf("invalid request: %w", err))
		return
	}
	req, err := parseImageGenerationRequestBytes(rawJSON)
	if err != nil {
		h.writeImagesRequestError(c, err)
		return
	}
	if err = validateImageRequestCount(&req); err != nil {
		h.writeImagesRequestError(c, err)
		return
	}
	if h.handleXAIGenerationIfRequested(c, rawJSON) {
		return
	}
	if h.imagesNativeEnabled(imageGenerationOperation) {
		h.handleNativeImagesRequest(c, rawJSON, req, imageGenerationOperation)
		return
	}

	if err := h.validateImageRequest(&req, imageGenerationOperation); err != nil {
		h.writeImagesRequestError(c, err)
		return
	}
	h.handleImagesRequest(c, req, imageGenerationOperation)
}

// Edits handles POST /v1/images/edits.
func (h *OpenAIImagesAPIHandler) Edits(c *gin.Context) {
	contentType, _, _ := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if strings.EqualFold(contentType, "multipart/form-data") {
		if h.handleXAIEditIfRequested(c, nil) {
			return
		}
		if h.imagesNativeEnabled(imageEditOperation) {
			req, rawJSON, err := h.parseNativeImageEditRequest(c)
			if err != nil {
				h.writeImagesRequestError(c, err)
				return
			}
			if err = validateImageRequestCount(&req); err != nil {
				h.writeImagesRequestError(c, err)
				return
			}
			h.handleNativeImagesRequest(c, rawJSON, req, imageEditOperation)
			return
		}
		req, err := parseImageEditRequest(c)
		if err != nil {
			h.writeImagesRequestError(c, err)
			return
		}
		if err := h.validateImageRequest(&req, imageEditOperation); err != nil {
			h.writeImagesRequestError(c, err)
			return
		}
		h.handleImagesRequest(c, req, imageEditOperation)
		return
	}

	rawJSON, err := readOpenAIImageJSONRequestBody(c)
	if err != nil {
		h.writeImagesRequestError(c, fmt.Errorf("invalid request: %w", err))
		return
	}
	if err = validateImageRequestCountJSON(rawJSON); err != nil {
		h.writeImagesRequestError(c, err)
		return
	}
	if h.handleXAIEditIfRequested(c, rawJSON) {
		return
	}
	req, err := parseJSONImageEditRequest(rawJSON, h.imagesNativeEnabled(imageEditOperation))
	if err != nil {
		h.writeImagesRequestError(c, err)
		return
	}
	if h.imagesNativeEnabled(imageEditOperation) {
		h.handleNativeImagesRequest(c, rawJSON, req, imageEditOperation)
		return
	}
	if err := h.validateImageRequest(&req, imageEditOperation); err != nil {
		h.writeImagesRequestError(c, err)
		return
	}
	h.handleImagesRequest(c, req, imageEditOperation)
}

func (h *OpenAIImagesAPIHandler) handleImagesRequest(c *gin.Context, req openAIImageRequest, op imageOperation) {
	codexModel := h.imagesCodexModel()
	imageModel := h.imagesImageModel()
	payload, err := buildCodexImageResponsesPayload(req, op, codexModel, imageModel, h.imagesOverrideInputFidelityEnabled())
	if err != nil {
		h.writeImagesError(c, http.StatusBadRequest, err)
		return
	}
	rawJSON, err := json.Marshal(payload)
	if err != nil {
		h.writeImagesError(c, http.StatusInternalServerError, err)
		return
	}
	count := imageRequestCount(req)
	if count > 1 && !h.imagesNAggregationEnabled() {
		h.writeImagesRequestError(c, unsupportedImageErrorf("n > 1 is not supported"))
		return
	}
	responseFormat := strings.ToLower(strings.TrimSpace(req.ResponseFormat))
	providers := imageResponsesProviders(req)
	if req.Stream {
		h.handleStreamingImagesResponse(c, rawJSON, imageModel, codexModel, op, count, responseFormat, providers)
		return
	}
	h.handleNonStreamingImagesResponse(c, rawJSON, imageModel, codexModel, count, responseFormat, providers)
}

func (h *OpenAIImagesAPIHandler) handleNativeImagesRequest(c *gin.Context, rawJSON []byte, req openAIImageRequest, op imageOperation) {
	cfg := h.nativeImageEndpointConfig(op)
	imageModel := strings.TrimSpace(req.Model)
	if imageModel == "" {
		imageModel = h.imagesImageModel()
	}
	if !nativeImageModelAllowed(cfg.Models, imageModel) {
		h.writeImagesError(c, cfg.UnsupportedModelStatusCode, errors.New(nativeImageUnsupportedModelMessage(cfg.UnsupportedModelMessage, imageModel)))
		return
	}
	if strings.TrimSpace(req.Prompt) == "" {
		h.writeImagesError(c, http.StatusBadRequest, errors.New("prompt is required"))
		return
	}
	if op.action == imageEditOperation.action && len(req.Images) == 0 {
		h.writeImagesError(c, http.StatusBadRequest, errors.New("at least one image is required"))
		return
	}
	rawJSON, _ = sjson.SetBytes(rawJSON, "model", imageModel)
	rawJSON = applyNativeImageParamRules(rawJSON, cfg.ParamRules)
	if req.Stream {
		h.handleNativeStreamingImagesResponse(c, rawJSON, imageModel, op)
		return
	}
	h.handleNativeNonStreamingImagesResponse(c, rawJSON, imageModel, op)
}

func (h *OpenAIImagesAPIHandler) handleNativeNonStreamingImagesResponse(c *gin.Context, rawJSON []byte, imageModel string, op imageOperation) {
	c.Header("Content-Type", "application/json")
	cliCtx, cliCancel := h.GetContextWithCancel(h, c, context.Background())
	stopKeepAlive := h.StartNonStreamingKeepAlive(c, cliCtx)
	resp, headers, errMsg := h.ExecuteWithProvidersAndExecutionModel(cliCtx, []string{Codex}, nativeImagesHandlerType, imageModel, "", rawJSON, nativeImagesAlt(op))
	stopKeepAlive()
	if errMsg != nil {
		h.WriteErrorResponse(c, errMsg)
		cliCancel(errMsg.Error)
		return
	}
	handlers.WriteUpstreamHeaders(c.Writer.Header(), headers)
	_, _ = c.Writer.Write(resp)
	cliCancel(resp)
}

func (h *OpenAIImagesAPIHandler) handleNativeStreamingImagesResponse(c *gin.Context, rawJSON []byte, imageModel string, op imageOperation) {
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		h.writeImagesError(c, http.StatusInternalServerError, errors.New("streaming not supported"))
		return
	}
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("Access-Control-Allow-Origin", "*")

	cliCtx, cliCancel := h.GetContextWithCancel(h, c, context.Background())
	dataChan, upstreamHeaders, errChan := h.ExecuteStreamWithProvidersAndExecutionModel(cliCtx, []string{Codex}, nativeImagesHandlerType, imageModel, "", rawJSON, nativeImagesAlt(op))
	handlers.WriteUpstreamHeaders(c.Writer.Header(), upstreamHeaders)
	h.ForwardStream(c, flusher, func(err error) { cliCancel(err) }, dataChan, errChan, handlers.StreamForwardOptions{
		FlushInterval: imageStreamFlushInterval(h.Cfg),
		FlushMinBytes: imageStreamFlushMinBytes(h.Cfg),
		WriteChunk: func(chunk []byte) {
			_, _ = c.Writer.Write(chunk)
		},
		WriteTerminalError: func(errMsg *interfaces.ErrorMessage) {
			if errMsg == nil {
				return
			}
			status := http.StatusInternalServerError
			if errMsg.StatusCode > 0 {
				status = errMsg.StatusCode
			}
			errText := http.StatusText(status)
			if errMsg.Error != nil && strings.TrimSpace(errMsg.Error.Error()) != "" {
				errText = errMsg.Error.Error()
			}
			body := handlers.BuildErrorResponseBodyForMessage(status, errText, errMsg)
			_, _ = fmt.Fprintf(c.Writer, "\nevent: error\ndata: %s\n\n", string(body))
		},
	})
}

func (h *OpenAIImagesAPIHandler) handleNonStreamingImagesResponse(c *gin.Context, rawJSON []byte, imageModel, codexModel string, count int, responseFormat string, providers []string) {
	c.Header("Content-Type", "application/json")
	var combined imagesResponse
	var upstreamHeaders http.Header
	if count < 1 {
		count = 1
	}
	for i := 0; i < count && len(combined.Data) < count; i++ {
		remaining := count - len(combined.Data)
		cliCtx, cliCancel := h.GetContextWithCancel(h, c, context.Background())
		cliCtx = handlers.WithImageGenerationMaxResults(cliCtx, remaining)
		stopKeepAlive := h.StartNonStreamingKeepAlive(c, cliCtx)
		resp, headers, errMsg := h.ExecuteWithProvidersAndExecutionModel(cliCtx, providers, h.HandlerType(), imageModel, codexModel, rawJSON, "")
		stopKeepAlive()
		if errMsg != nil {
			h.WriteErrorResponse(c, errMsg)
			cliCancel(errMsg.Error)
			return
		}
		parsed, err := parseResponsesToImagesResponse(resp, time.Now().Unix())
		if err != nil {
			h.writeImagesError(c, http.StatusBadGateway, err)
			cliCancel(err)
			return
		}
		if combined.Created == 0 {
			combined.Created = parsed.Created
			combined.Background = parsed.Background
			combined.OutputFormat = parsed.OutputFormat
			combined.Quality = parsed.Quality
			combined.Size = parsed.Size
			upstreamHeaders = headers
		}
		if len(parsed.Data) > remaining {
			parsed.Data = parsed.Data[:remaining]
		}
		combined.Data = append(combined.Data, parsed.Data...)
		combined.Usage = mergeImageUsageForNAggregation(combined.Usage, parsed.Usage)
		cliCancel(resp)
	}
	applyImageResponseFormat(&combined, responseFormat)
	imagesPayload, err := json.Marshal(combined)
	if err != nil {
		h.writeImagesError(c, http.StatusInternalServerError, err)
		return
	}
	handlers.WriteUpstreamHeaders(c.Writer.Header(), upstreamHeaders)
	_, _ = c.Writer.Write(imagesPayload)
}

func (h *OpenAIImagesAPIHandler) handleStreamingImagesResponse(c *gin.Context, rawJSON []byte, imageModel, codexModel string, op imageOperation, count int, responseFormat string, providers []string) {
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		h.writeImagesError(c, http.StatusInternalServerError, errors.New("streaming not supported"))
		return
	}
	if count > 1 {
		h.handleMultiStreamingImagesResponse(c, flusher, rawJSON, imageModel, codexModel, op, count, responseFormat, providers)
		return
	}
	cliCtx, cliCancel := h.GetContextWithCancel(h, c, context.Background())
	cliCtx = handlers.WithImageGenerationStreamPassthrough(cliCtx, true)
	cliCtx = handlers.WithImageGenerationMaxResults(cliCtx, 1)
	dataChan, upstreamHeaders, errChan := h.ExecuteStreamWithProvidersAndExecutionModel(cliCtx, providers, h.HandlerType(), imageModel, codexModel, rawJSON, "")

	setSSEHeaders := func() {
		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")
		c.Header("Connection", "keep-alive")
		c.Header("Access-Control-Allow-Origin", "*")
	}

	mapper := &imageStreamMapper{operation: op, responseFormat: responseFormat, maxResults: 1}
	var firstFrame bytes.Buffer
	for {
		select {
		case <-c.Request.Context().Done():
			cliCancel(c.Request.Context().Err())
			return
		case errMsg, ok := <-errChan:
			if !ok {
				errChan = nil
				continue
			}
			h.WriteErrorResponse(c, errMsg)
			if errMsg != nil {
				cliCancel(errMsg.Error)
			} else {
				cliCancel(nil)
			}
			return
		case chunk, ok := <-dataChan:
			if !ok {
				select {
				case errMsg, okErr := <-errChan:
					if okErr && errMsg != nil {
						h.WriteErrorResponse(c, errMsg)
						cliCancel(errMsg.Error)
						return
					}
				default:
				}
				mapper.flush(&firstFrame)
				if errMapper := mapper.fatalError(); errMapper != nil {
					h.WriteErrorResponse(c, &interfaces.ErrorMessage{
						StatusCode: http.StatusBadGateway,
						Error:      errMapper,
					})
					cliCancel(errMapper)
					return
				}
				setSSEHeaders()
				handlers.WriteUpstreamHeaders(c.Writer.Header(), upstreamHeaders)
				_, _ = c.Writer.Write(firstFrame.Bytes())
				_, _ = c.Writer.Write([]byte("\n"))
				flusher.Flush()
				cliCancel(nil)
				return
			}
			mapper.writeChunk(&firstFrame, chunk)
			if errMapper := mapper.fatalError(); errMapper != nil {
				h.WriteErrorResponse(c, &interfaces.ErrorMessage{
					StatusCode: http.StatusBadGateway,
					Error:      errMapper,
				})
				cliCancel(errMapper)
				return
			}
			if firstFrame.Len() == 0 {
				continue
			}
			setSSEHeaders()
			handlers.WriteUpstreamHeaders(c.Writer.Header(), upstreamHeaders)
			_, _ = c.Writer.Write(firstFrame.Bytes())
			flusher.Flush()
			_ = mapper.consumeForceFlush()
			h.forwardImagesStream(c, flusher, func(err error) { cliCancel(err) }, dataChan, errChan, mapper)
			return
		}
	}
}

func (h *OpenAIImagesAPIHandler) handleMultiStreamingImagesResponse(c *gin.Context, flusher http.Flusher, rawJSON []byte, imageModel, codexModel string, op imageOperation, count int, responseFormat string, providers []string) {
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("Access-Control-Allow-Origin", "*")
	for i := 0; i < count; i++ {
		cliCtx, cliCancel := h.GetContextWithCancel(h, c, context.Background())
		cliCtx = handlers.WithImageGenerationStreamPassthrough(cliCtx, true)
		cliCtx = handlers.WithImageGenerationMaxResults(cliCtx, 1)
		dataChan, upstreamHeaders, errChan := h.ExecuteStreamWithProvidersAndExecutionModel(cliCtx, providers, h.HandlerType(), imageModel, codexModel, rawJSON, "")
		if i == 0 {
			handlers.WriteUpstreamHeaders(c.Writer.Header(), upstreamHeaders)
		}
		mapper := &imageStreamMapper{operation: op, omitInputUsage: i > 0, responseFormat: responseFormat, maxResults: 1}
		var streamErr error
		h.ForwardStream(c, flusher, func(err error) {
			streamErr = err
			cliCancel(err)
		}, dataChan, errChan, handlers.StreamForwardOptions{
			FlushInterval: imageStreamFlushInterval(h.Cfg),
			FlushMinBytes: imageStreamFlushMinBytes(h.Cfg),
			WriteChunk: func(chunk []byte) {
				mapper.writeChunk(c.Writer, chunk)
			},
			ChunkError: mapper.fatalError,
			FlushChunk: func([]byte) bool {
				return mapper.consumeForceFlush()
			},
			WriteTerminalError: func(errMsg *interfaces.ErrorMessage) {
				if errMsg == nil {
					return
				}
				status := http.StatusInternalServerError
				if errMsg.StatusCode > 0 {
					status = errMsg.StatusCode
				}
				errText := http.StatusText(status)
				if errMsg.Error != nil && strings.TrimSpace(errMsg.Error.Error()) != "" {
					errText = errMsg.Error.Error()
				}
				body := handlers.BuildErrorResponseBodyForMessage(status, errText, errMsg)
				_, _ = fmt.Fprintf(c.Writer, "\nevent: error\ndata: %s\n\n", string(body))
			},
			WriteDone: func() {
				mapper.flush(c.Writer)
			},
		})
		if streamErr == nil {
			streamErr = mapper.fatalError()
		}
		if streamErr != nil {
			return
		}
	}
	_, _ = c.Writer.Write([]byte("\n"))
	flusher.Flush()
}

func (h *OpenAIImagesAPIHandler) forwardImagesStream(c *gin.Context, flusher http.Flusher, cancel func(error), data <-chan []byte, errs <-chan *interfaces.ErrorMessage, mapper *imageStreamMapper) {
	if mapper == nil {
		mapper = &imageStreamMapper{operation: imageGenerationOperation}
	}
	h.ForwardStream(c, flusher, cancel, data, errs, handlers.StreamForwardOptions{
		FlushInterval: imageStreamFlushInterval(h.Cfg),
		FlushMinBytes: imageStreamFlushMinBytes(h.Cfg),
		WriteChunk: func(chunk []byte) {
			mapper.writeChunk(c.Writer, chunk)
		},
		ChunkError: mapper.fatalError,
		FlushChunk: func([]byte) bool {
			return mapper.consumeForceFlush()
		},
		WriteTerminalError: func(errMsg *interfaces.ErrorMessage) {
			if errMsg == nil {
				return
			}
			status := http.StatusInternalServerError
			if errMsg.StatusCode > 0 {
				status = errMsg.StatusCode
			}
			errText := http.StatusText(status)
			if errMsg.Error != nil && strings.TrimSpace(errMsg.Error.Error()) != "" {
				errText = errMsg.Error.Error()
			}
			body := handlers.BuildErrorResponseBodyForMessage(status, errText, errMsg)
			_, _ = fmt.Fprintf(c.Writer, "\nevent: error\ndata: %s\n\n", string(body))
		},
		WriteDone: func() {
			mapper.flush(c.Writer)
			if mapper.fatalError() == nil {
				_, _ = c.Writer.Write([]byte("\n"))
			}
		},
	})
}

func parseImageGenerationRequestBytes(rawJSON []byte) (openAIImageRequest, error) {
	var req openAIImageRequest
	if err := json.Unmarshal(rawJSON, &req); err != nil {
		return req, fmt.Errorf("invalid request: %w", err)
	}
	return req, nil
}

func parseImageEditRequest(c *gin.Context) (openAIImageRequest, error) {
	contentType := c.GetHeader("Content-Type")
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err == nil && strings.EqualFold(mediaType, "multipart/form-data") {
		return parseMultipartImageEditRequest(c)
	}
	rawJSON, err := readOpenAIImageJSONRequestBody(c)
	if err != nil {
		return openAIImageRequest{}, fmt.Errorf("invalid request: %w", err)
	}
	return parseJSONImageEditRequest(rawJSON, false)
}

func (h *OpenAIImagesAPIHandler) parseNativeImageEditRequest(c *gin.Context) (openAIImageRequest, []byte, error) {
	contentType := c.GetHeader("Content-Type")
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err == nil && strings.EqualFold(mediaType, "multipart/form-data") {
		req, errParse := parseMultipartImageEditRequest(c)
		if errParse != nil {
			return req, nil, errParse
		}
		rawJSON, errBuild := buildNativeImageRequestJSON(req)
		if errBuild != nil {
			return req, nil, errBuild
		}
		return req, rawJSON, nil
	}

	rawJSON, err := readOpenAIImageJSONRequestBody(c)
	if err != nil {
		return openAIImageRequest{}, nil, fmt.Errorf("invalid request: %w", err)
	}
	req, err := parseJSONImageEditRequest(rawJSON, true)
	return req, rawJSON, err
}

func parseJSONImageEditRequest(rawJSON []byte, allowFileID bool) (openAIImageRequest, error) {
	var req openAIImageRequest
	if err := json.Unmarshal(rawJSON, &req); err != nil {
		return req, fmt.Errorf("invalid request: %w", err)
	}
	for i := range req.Images {
		if strings.TrimSpace(req.Images[i].FileID) != "" && !allowFileID {
			return req, unsupportedImageErrorf("JSON image file_id is not supported")
		}
		if strings.TrimSpace(req.Images[i].FileID) != "" {
			continue
		}
		imageURL, err := imageURLFromReference(req.Images[i])
		if err != nil {
			return req, fmt.Errorf("invalid images[%d]: %w", i, err)
		}
		req.Images[i].ImageURL = imageURL
	}
	if req.Mask != nil {
		if strings.TrimSpace(req.Mask.FileID) != "" && !allowFileID {
			return req, unsupportedImageErrorf("JSON mask file_id is not supported")
		}
		if strings.TrimSpace(req.Mask.FileID) != "" {
			return req, nil
		}
		maskURL, err := imageURLFromReference(*req.Mask)
		if err != nil {
			return req, fmt.Errorf("invalid mask: %w", err)
		}
		req.Mask.ImageURL = maskURL
	}
	return req, nil
}

func parseMultipartImageEditRequest(c *gin.Context) (openAIImageRequest, error) {
	var req openAIImageRequest
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxImageMultipartBytes)
	if err := c.Request.ParseMultipartForm(maxImageUploadBytes); err != nil {
		return req, fmt.Errorf("invalid multipart request: %w", err)
	}
	form := c.Request.MultipartForm
	req.Model = multipartValue(form, "model")
	req.Prompt = multipartValue(form, "prompt")
	req.Size = multipartValue(form, "size")
	req.Quality = multipartValue(form, "quality")
	req.Background = multipartValue(form, "background")
	req.OutputFormat = multipartValue(form, "output_format")
	req.InputFidelity = multipartValue(form, "input_fidelity")
	req.Moderation = multipartValue(form, "moderation")
	req.ResponseFormat = multipartValue(form, "response_format")
	req.Stream = parseBoolValue(multipartValue(form, "stream"))
	if n, ok, err := parseOptionalInt(multipartValue(form, "n")); err != nil {
		return req, err
	} else if ok {
		req.N = &n
	}
	if compression, ok, err := parseOptionalInt(multipartValue(form, "output_compression")); err != nil {
		return req, err
	} else if ok {
		req.OutputCompression = &compression
	}
	if partialImages, ok, err := parseOptionalInt(multipartValue(form, "partial_images")); err != nil {
		return req, err
	} else if ok {
		req.PartialImages = &partialImages
	}

	files := append([]*multipart.FileHeader(nil), multipartFiles(form, "image")...)
	files = append(files, multipartFiles(form, "image[]")...)
	for _, fh := range files {
		dataURL, err := dataURLFromFileHeader(fh)
		if err != nil {
			return req, err
		}
		req.Images = append(req.Images, imageReference{ImageURL: dataURL})
	}
	if masks := multipartFiles(form, "mask"); len(masks) > 0 {
		if len(masks) > 1 {
			return req, errors.New("only one mask file is supported")
		}
		maskURL, err := dataURLFromFileHeader(masks[0])
		if err != nil {
			return req, err
		}
		req.Mask = &imageReference{ImageURL: maskURL}
	}
	return req, nil
}

func (h *OpenAIImagesAPIHandler) validateImageRequest(req *openAIImageRequest, op imageOperation) error {
	if req == nil {
		return errors.New("request is required")
	}
	imageModel := h.imagesImageModel()
	imageModel = strings.TrimSpace(imageModel)
	if imageModel == "" {
		imageModel = defaultImagesImageModel
	}
	model := strings.TrimSpace(req.Model)
	if model == "" {
		model = imageModel
	}
	if model != imageModel {
		return unsupportedImageErrorf("unsupported image model %q; configured image model is %s", req.Model, imageModel)
	}
	if strings.TrimSpace(req.Prompt) == "" {
		return errors.New("prompt is required")
	}
	if strings.EqualFold(strings.TrimSpace(req.ResponseFormat), "url") {
		if h.imagesResponseFormatURLDataURLEnabled() {
			req.ResponseFormat = "url"
		} else if h.imagesOverrideResponseFormatURLEnabled() {
			req.ResponseFormat = "b64_json"
		} else {
			return unsupportedImageErrorf("response_format=url is not supported; use b64_json")
		}
	}
	if rf := strings.TrimSpace(req.ResponseFormat); rf != "" && rf != "b64_json" && rf != "url" {
		return unsupportedImageErrorf("unsupported response_format %q", rf)
	}
	if strings.TrimSpace(req.OutputFormat) == "" {
		req.OutputFormat = "png"
	}
	if strings.EqualFold(strings.TrimSpace(req.Background), "transparent") && h.imagesOverrideTransparentBackgroundEnabled() {
		req.Background = "auto"
	}
	if err := validateImageRequestCount(req); err != nil {
		return err
	}
	if req.OutputCompression != nil && (*req.OutputCompression < 0 || *req.OutputCompression > 100) {
		return errors.New("output_compression must be between 0 and 100")
	}
	if req.PartialImages != nil && (*req.PartialImages < 0 || *req.PartialImages > 3) {
		return errors.New("partial_images must be between 0 and 3")
	}
	if op.action == imageEditOperation.action && len(req.Images) == 0 {
		return errors.New("at least one image is required")
	}
	return nil
}

func validateImageRequestCount(req *openAIImageRequest) error {
	if req == nil {
		return errors.New("request is required")
	}
	n := 1
	if req.N != nil {
		n = *req.N
	}
	if n < 1 {
		return errors.New("n must be at least 1")
	}
	if n > maxImageRequestCount {
		return fmt.Errorf("n must be at most %d", maxImageRequestCount)
	}
	return nil
}

func validateImageRequestCountJSON(rawJSON []byte) error {
	var requestCount struct {
		N *int `json:"n"`
	}
	if err := json.Unmarshal(rawJSON, &requestCount); err != nil {
		return fmt.Errorf("invalid request: %w", err)
	}
	return validateImageRequestCount(&openAIImageRequest{N: requestCount.N})
}

func buildCodexImageResponsesPayload(req openAIImageRequest, op imageOperation, codexModel, imageModel string, overrideInputFidelity bool) (map[string]any, error) {
	imageModel = strings.TrimSpace(imageModel)
	if imageModel == "" {
		imageModel = defaultImagesImageModel
	}
	content := []map[string]any{{
		"type": "input_text",
		"text": req.Prompt,
	}}
	for i := range req.Images {
		imageURL, err := imageURLFromReference(req.Images[i])
		if err != nil {
			return nil, fmt.Errorf("invalid image reference: %w", err)
		}
		content = append(content, map[string]any{
			"type":      "input_image",
			"image_url": imageURL,
		})
	}

	tool := map[string]any{
		"type":   "image_generation",
		"model":  imageModel,
		"action": op.action,
	}
	setOptionalString(tool, "size", req.Size)
	setOptionalString(tool, "quality", req.Quality)
	setOptionalString(tool, "background", req.Background)
	setOptionalString(tool, "output_format", req.OutputFormat)
	if shouldForwardInputFidelity(overrideInputFidelity) {
		setOptionalString(tool, "input_fidelity", req.InputFidelity)
	}
	setOptionalString(tool, "moderation", req.Moderation)
	if req.OutputCompression != nil {
		tool["output_compression"] = *req.OutputCompression
	}
	if req.Stream && req.PartialImages != nil {
		tool["partial_images"] = *req.PartialImages
	}
	if req.Mask != nil {
		maskURL, err := imageURLFromReference(*req.Mask)
		if err != nil {
			return nil, fmt.Errorf("invalid mask reference: %w", err)
		}
		tool["input_image_mask"] = map[string]any{"image_url": maskURL}
	}

	return map[string]any{
		"model":        codexModel,
		"instructions": "",
		"store":        false,
		"stream":       true,
		"input": []map[string]any{{
			"type":    "message",
			"role":    "user",
			"content": content,
		}},
		"tools":       []map[string]any{tool},
		"tool_choice": map[string]any{"type": "image_generation"},
	}, nil
}

func buildNativeImageRequestJSON(req openAIImageRequest) ([]byte, error) {
	body := map[string]any{}
	setOptionalString(body, "model", req.Model)
	setOptionalString(body, "prompt", req.Prompt)
	setOptionalString(body, "size", req.Size)
	setOptionalString(body, "quality", req.Quality)
	setOptionalString(body, "background", req.Background)
	setOptionalString(body, "output_format", req.OutputFormat)
	setOptionalString(body, "input_fidelity", req.InputFidelity)
	setOptionalString(body, "moderation", req.Moderation)
	setOptionalString(body, "response_format", req.ResponseFormat)
	if req.N != nil {
		body["n"] = *req.N
	}
	if req.OutputCompression != nil {
		body["output_compression"] = *req.OutputCompression
	}
	if req.PartialImages != nil {
		body["partial_images"] = *req.PartialImages
	}
	if req.Stream {
		body["stream"] = true
	}
	if len(req.Images) > 0 {
		images := make([]map[string]any, 0, len(req.Images))
		for i := range req.Images {
			ref, err := nativeImageReferenceMap(req.Images[i])
			if err != nil {
				return nil, fmt.Errorf("invalid images[%d]: %w", i, err)
			}
			images = append(images, ref)
		}
		body["images"] = images
	}
	if req.Mask != nil {
		mask, err := nativeImageReferenceMap(*req.Mask)
		if err != nil {
			return nil, fmt.Errorf("invalid mask: %w", err)
		}
		body["mask"] = mask
	}
	return json.Marshal(body)
}

func nativeImageReferenceMap(ref imageReference) (map[string]any, error) {
	if fileID := strings.TrimSpace(ref.FileID); fileID != "" {
		return map[string]any{"file_id": fileID}, nil
	}
	imageURL, err := imageURLFromReference(ref)
	if err != nil {
		return nil, err
	}
	return map[string]any{"image_url": imageURL}, nil
}

func shouldForwardInputFidelity(overrideInputFidelity bool) bool {
	return !overrideInputFidelity
}

func imageRequestCount(req openAIImageRequest) int {
	if req.N == nil || *req.N < 1 {
		return 1
	}
	return *req.N
}

func mimeTypeFromOutputFormat(outputFormat string) string {
	outputFormat = strings.ToLower(strings.TrimSpace(outputFormat))
	if outputFormat == "" {
		return "image/png"
	}
	if strings.Contains(outputFormat, "/") {
		return outputFormat
	}
	switch outputFormat {
	case "png":
		return "image/png"
	case "jpg", "jpeg":
		return "image/jpeg"
	case "webp":
		return "image/webp"
	case "gif":
		return "image/gif"
	default:
		return "image/png"
	}
}

func imageDataURL(b64JSON, outputFormat string) string {
	return "data:" + mimeTypeFromOutputFormat(outputFormat) + ";base64," + b64JSON
}

func imageResponseFormatIsURL(responseFormat string) bool {
	return strings.EqualFold(strings.TrimSpace(responseFormat), "url")
}

func applyImageResponseFormat(out *imagesResponse, responseFormat string) {
	if out == nil || !imageResponseFormatIsURL(responseFormat) {
		return
	}
	for i := range out.Data {
		if strings.TrimSpace(out.Data[i].B64JSON) == "" {
			continue
		}
		out.Data[i].URL = imageDataURL(out.Data[i].B64JSON, out.Data[i].OutputFormat)
		out.Data[i].B64JSON = ""
	}
}

func convertResponsesToImagesResponse(raw []byte, fallbackCreated int64) ([]byte, error) {
	out, err := parseResponsesToImagesResponse(raw, fallbackCreated)
	if err != nil {
		return nil, err
	}
	return json.Marshal(out)
}

func parseResponsesToImagesResponse(raw []byte, fallbackCreated int64) (imagesResponse, error) {
	respObj := responsesImageResult(raw)
	if !respObj.Exists() || !respObj.IsObject() {
		return imagesResponse{}, errors.New("invalid Codex response")
	}
	created := respObj.Get("created_at").Int()
	if created == 0 {
		created = fallbackCreated
	}
	out := imagesResponse{
		Created: created,
		Data:    imageResultsFromOutputResult(respObj.Get("output")),
	}
	if len(out.Data) == 0 {
		return imagesResponse{}, errors.New("upstream did not return image output")
	}
	out.applyMetadataFromFirstImage()
	if usage := imageUsageFromResponseResult(respObj); len(bytes.TrimSpace(usage)) > 0 && string(bytes.TrimSpace(usage)) != "null" {
		out.Usage = usage
	}
	return out, nil
}

func responsesImageResult(raw []byte) gjson.Result {
	root := gjson.ParseBytes(raw)
	if response := root.Get("response"); response.IsObject() {
		return response
	}
	return root
}

func imageResultsFromOutputResult(output gjson.Result) []imageResult {
	if !output.IsArray() {
		return nil
	}
	items := output.Array()
	results := make([]imageResult, 0, len(items))
	for i := range items {
		result, ok := imageResultFromOutputItemResult(items[i])
		if ok {
			results = append(results, result)
		}
	}
	return results
}

func imageResultFromOutputItemResult(item gjson.Result) (imageResult, bool) {
	if item.Get("type").String() != "image_generation_call" {
		return imageResult{}, false
	}
	b64 := strings.TrimSpace(item.Get("result").String())
	if b64 == "" {
		return imageResult{}, false
	}
	return imageResult{
		B64JSON:       b64,
		RevisedPrompt: item.Get("revised_prompt").String(),
		OutputFormat:  item.Get("output_format").String(),
		Size:          item.Get("size").String(),
		Background:    item.Get("background").String(),
		Quality:       item.Get("quality").String(),
	}, true
}

func imageUsageFromResponseResult(resp gjson.Result) json.RawMessage {
	if usage := imageUsageFromToolUsageResult(resp.Get("tool_usage")); len(bytes.TrimSpace(usage)) > 0 && string(bytes.TrimSpace(usage)) != "null" {
		return usage
	}
	if usage := imageUsageFromOutputResult(resp.Get("output")); len(bytes.TrimSpace(usage)) > 0 && string(bytes.TrimSpace(usage)) != "null" {
		return usage
	}
	return imageRawMessageFromResult(resp.Get("usage"))
}

func imageUsageFromToolUsageResult(toolUsage gjson.Result) json.RawMessage {
	for _, key := range []string{"image_gen", "image_generation"} {
		if usage := imageRawMessageFromResult(toolUsage.Get(key)); len(bytes.TrimSpace(usage)) > 0 && string(bytes.TrimSpace(usage)) != "null" {
			return usage
		}
	}
	return nil
}

func imageUsageFromOutputResult(output gjson.Result) json.RawMessage {
	if !output.IsArray() {
		return nil
	}
	var combined json.RawMessage
	for _, item := range output.Array() {
		if item.Get("type").String() != "image_generation_call" {
			continue
		}
		combined = mergeImageUsage(combined, imageUsageRawFromOutputItemResult(item))
	}
	return combined
}

func imageUsageRawFromOutputItemResult(item gjson.Result) json.RawMessage {
	if usage := imageRawMessageFromResult(item.Get("usage")); len(bytes.TrimSpace(usage)) > 0 && string(bytes.TrimSpace(usage)) != "null" {
		return usage
	}
	fields := map[string]json.RawMessage{}
	setRawUsageField(fields, "input_tokens", imageRawMessageFromResult(item.Get("input_tokens")))
	setRawUsageField(fields, "output_tokens", imageRawMessageFromResult(item.Get("output_tokens")))
	setRawUsageField(fields, "total_tokens", imageRawMessageFromResult(item.Get("total_tokens")))
	setRawUsageField(fields, "input_tokens_details", imageRawMessageFromResult(item.Get("input_tokens_details")))
	setRawUsageField(fields, "output_tokens_details", imageRawMessageFromResult(item.Get("output_tokens_details")))
	if len(fields) == 0 {
		return nil
	}
	data, err := json.Marshal(fields)
	if err != nil {
		return nil
	}
	return data
}

func imageRawMessageFromResult(result gjson.Result) json.RawMessage {
	if !result.Exists() || strings.TrimSpace(result.Raw) == "" || strings.TrimSpace(result.Raw) == "null" {
		return nil
	}
	return json.RawMessage([]byte(result.Raw))
}

func imageResultsFromOutput(items []imageOutputItem) []imageResult {
	results := make([]imageResult, 0, len(items))
	for i := range items {
		if items[i].Type != "image_generation_call" || strings.TrimSpace(items[i].Result) == "" {
			continue
		}
		results = append(results, imageResult{
			B64JSON:       items[i].Result,
			RevisedPrompt: items[i].RevisedPrompt,
			OutputFormat:  items[i].OutputFormat,
			Size:          items[i].Size,
			Background:    items[i].Background,
			Quality:       items[i].Quality,
		})
	}
	return results
}

func (r *imagesResponse) applyMetadataFromFirstImage() {
	if r == nil || len(r.Data) == 0 {
		return
	}
	first := r.Data[0]
	r.Background = first.Background
	r.OutputFormat = first.OutputFormat
	r.Quality = first.Quality
	r.Size = first.Size
}

func imageUsageFromToolUsage(toolUsage map[string]json.RawMessage) json.RawMessage {
	for _, key := range []string{"image_gen", "image_generation"} {
		if usage := toolUsage[key]; len(bytes.TrimSpace(usage)) > 0 && string(bytes.TrimSpace(usage)) != "null" {
			return usage
		}
	}
	return nil
}

func imageUsageFromOutput(items []imageOutputItem) json.RawMessage {
	var combined json.RawMessage
	for i := range items {
		if items[i].Type != "image_generation_call" {
			continue
		}
		combined = mergeImageUsage(combined, imageUsageRawFromOutputItem(items[i]))
	}
	return combined
}

func imageUsageRawFromOutputItem(item imageOutputItem) json.RawMessage {
	if len(bytes.TrimSpace(item.Usage)) > 0 && string(bytes.TrimSpace(item.Usage)) != "null" {
		return item.Usage
	}
	fields := map[string]json.RawMessage{}
	setRawUsageField(fields, "input_tokens", item.InputTokens)
	setRawUsageField(fields, "output_tokens", item.OutputTokens)
	setRawUsageField(fields, "total_tokens", item.TotalTokens)
	setRawUsageField(fields, "input_tokens_details", item.InputTokensDetails)
	setRawUsageField(fields, "output_tokens_details", item.OutputTokensDetails)
	if len(fields) == 0 {
		return nil
	}
	data, err := json.Marshal(fields)
	if err != nil {
		return nil
	}
	return data
}

func setRawUsageField(dst map[string]json.RawMessage, key string, value json.RawMessage) {
	if len(bytes.TrimSpace(value)) == 0 || string(bytes.TrimSpace(value)) == "null" {
		return
	}
	dst[key] = value
}

func mergeImageUsage(current, next json.RawMessage) json.RawMessage {
	if len(bytes.TrimSpace(next)) == 0 || string(bytes.TrimSpace(next)) == "null" {
		return current
	}
	if len(bytes.TrimSpace(current)) == 0 || string(bytes.TrimSpace(current)) == "null" {
		return next
	}
	var currentMap map[string]any
	var nextMap map[string]any
	if err := json.Unmarshal(current, &currentMap); err != nil {
		return next
	}
	if err := json.Unmarshal(next, &nextMap); err != nil {
		return current
	}
	merged := mergeImageUsageMaps(currentMap, nextMap)
	data, err := json.Marshal(merged)
	if err != nil {
		return current
	}
	return data
}

func mergeImageUsageForNAggregation(current, next json.RawMessage) json.RawMessage {
	if len(bytes.TrimSpace(next)) == 0 || string(bytes.TrimSpace(next)) == "null" {
		return current
	}
	if len(bytes.TrimSpace(current)) == 0 || string(bytes.TrimSpace(current)) == "null" {
		return next
	}
	var currentMap map[string]any
	var nextMap map[string]any
	if err := json.Unmarshal(current, &currentMap); err != nil {
		return next
	}
	if err := json.Unmarshal(next, &nextMap); err != nil {
		return current
	}
	merged := mergeImageUsageMapsForNAggregation(currentMap, nextMap)
	recomputeImageUsageTotal(merged)
	data, err := json.Marshal(merged)
	if err != nil {
		return current
	}
	return data
}

func mergeImageUsageMapsForNAggregation(current, next map[string]any) map[string]any {
	out := make(map[string]any, len(current)+len(next))
	for key, value := range current {
		out[key] = value
	}
	for key, value := range next {
		switch key {
		case "output_tokens", "output_tokens_details":
			if existing, ok := out[key]; ok {
				out[key] = mergeImageUsageValue(existing, value)
			} else {
				out[key] = value
			}
		case "input_tokens", "input_tokens_details":
			if _, ok := out[key]; !ok {
				out[key] = value
			}
		case "total_tokens":
			if _, ok := out[key]; !ok {
				out[key] = value
			}
		default:
			if existing, ok := out[key]; ok {
				out[key] = mergeImageUsageValue(existing, value)
			} else {
				out[key] = value
			}
		}
	}
	return out
}

func recomputeImageUsageTotal(usage map[string]any) {
	input, inputOK := usageNumber(usage["input_tokens"])
	output, outputOK := usageNumber(usage["output_tokens"])
	if !inputOK || !outputOK {
		return
	}
	usage["total_tokens"] = input + output
}

func omitInputImageUsage(raw json.RawMessage) json.RawMessage {
	if len(bytes.TrimSpace(raw)) == 0 || string(bytes.TrimSpace(raw)) == "null" {
		return raw
	}
	var usage map[string]any
	if err := json.Unmarshal(raw, &usage); err != nil {
		return raw
	}
	delete(usage, "input_tokens")
	delete(usage, "input_tokens_details")
	if output, ok := usageNumber(usage["output_tokens"]); ok {
		usage["total_tokens"] = output
	}
	data, err := json.Marshal(usage)
	if err != nil {
		return raw
	}
	return data
}

func usageNumber(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case json.Number:
		f, err := v.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

func mergeImageUsageMaps(current, next map[string]any) map[string]any {
	out := make(map[string]any, len(current)+len(next))
	for key, value := range current {
		out[key] = value
	}
	for key, value := range next {
		if existing, ok := out[key]; ok {
			out[key] = mergeImageUsageValue(existing, value)
			continue
		}
		out[key] = value
	}
	return out
}

func mergeImageUsageValue(current, next any) any {
	currentMap, currentMapOK := current.(map[string]any)
	nextMap, nextMapOK := next.(map[string]any)
	if currentMapOK && nextMapOK {
		return mergeImageUsageMaps(currentMap, nextMap)
	}
	currentNumber, currentOK := current.(float64)
	nextNumber, nextOK := next.(float64)
	if currentOK && nextOK {
		return currentNumber + nextNumber
	}
	return next
}

type imageStreamMapper struct {
	operation      imageOperation
	parser         imageSSEParser
	finals         []imageResult
	finalUsage     json.RawMessage
	omitInputUsage bool
	responseFormat string
	maxResults     int
	completed      bool
	forceFlush     bool
	fatalErr       error
}

func (m *imageStreamMapper) writeChunk(w io.Writer, chunk []byte) {
	if m == nil || m.completed {
		return
	}
	items := m.parser.Push(chunk)
	if err := m.parser.Error(); err != nil {
		m.completed = true
		m.fatalErr = err
		return
	}
	for _, item := range items {
		if len(item.comment) > 0 {
			_, _ = w.Write(item.comment)
			m.forceFlush = true
			continue
		}
		m.writePayload(w, item.payload)
	}
}

func (m *imageStreamMapper) consumeForceFlush() bool {
	if m == nil || !m.forceFlush {
		return false
	}
	m.forceFlush = false
	return true
}

func (m *imageStreamMapper) flush(w io.Writer) {
	if m == nil || m.completed {
		return
	}
	for _, item := range m.parser.Flush() {
		if len(item.comment) > 0 {
			_, _ = w.Write(item.comment)
			m.forceFlush = true
			continue
		}
		m.writePayload(w, item.payload)
	}
	if err := m.parser.Error(); err != nil {
		m.completed = true
		m.fatalErr = err
		return
	}
	if m.completed {
		return
	}
	if !m.completed && len(m.finals) > 0 {
		m.writeCompletedSet(w, m.finals, m.finalUsage)
		return
	}
	m.completed = true
	m.fatalErr = errors.New("upstream image stream ended without a completed response")
}

func (m *imageStreamMapper) writePayload(w io.Writer, payload []byte) {
	if m == nil || m.completed {
		return
	}
	eventType := responseEventType(payload)
	switch eventType {
	case "response.image_generation_call.partial_image":
		event := gjson.ParseBytes(payload)
		b64 := strings.TrimSpace(event.Get("partial_image_b64").String())
		if b64 == "" {
			return
		}
		streamEvent := imageStreamEvent{
			Type:              m.operation.partialEvent,
			B64JSON:           b64,
			PartialImageIndex: imagePartialIndexFromResult(event.Get("partial_image_index")),
		}
		m.applyStreamImageFormat(&streamEvent, event.Get("output_format").String())
		m.writeSSE(w, m.operation.partialEvent, streamEvent)
	case "response.output_item.done":
		item := gjson.GetBytes(payload, "item")
		result, ok := imageResultFromOutputItemResult(item)
		if !ok {
			return
		}
		m.finals = append(m.finals, result)
		m.finalUsage = mergeImageUsage(m.finalUsage, imageUsageRawFromOutputItemResult(item))
	case "response.completed":
		response := responsesImageResult(payload)
		results := imageResultsFromOutputResult(response.Get("output"))
		usage := imageUsageFromResponseResult(response)
		if len(results) > 0 {
			m.writeCompletedSet(w, results, usage)
			return
		}
		if len(m.finals) > 0 {
			m.writeCompletedSet(w, m.finals, mergeImageUsage(m.finalUsage, usage))
			return
		}
		m.completed = true
		m.fatalErr = errors.New("upstream did not return image output")
	}
}

func (m *imageStreamMapper) fatalError() error {
	if m == nil {
		return nil
	}
	return m.fatalErr
}

func imagePartialIndexFromResult(result gjson.Result) *int {
	if !result.Exists() || result.Type == gjson.Null {
		return nil
	}
	value := int(result.Int())
	return &value
}

func (m *imageStreamMapper) writeCompletedSet(w io.Writer, results []imageResult, usage json.RawMessage) {
	m.completed = true
	if m.maxResults > 0 && len(results) > m.maxResults {
		results = results[:m.maxResults]
	}
	for i := range results {
		m.writeCompleted(w, results[i], usage)
	}
}

func chatGPTWebSupportsImageRequest(req openAIImageRequest) bool {
	if strings.TrimSpace(req.Size) != "" ||
		(strings.TrimSpace(req.Quality) != "" && !strings.EqualFold(strings.TrimSpace(req.Quality), "auto")) ||
		strings.TrimSpace(req.Background) != "" ||
		(strings.TrimSpace(req.OutputFormat) != "" && !strings.EqualFold(strings.TrimSpace(req.OutputFormat), "png")) ||
		strings.TrimSpace(req.InputFidelity) != "" ||
		strings.TrimSpace(req.Moderation) != "" ||
		req.OutputCompression != nil ||
		(req.PartialImages != nil && *req.PartialImages > 0) {
		return false
	}
	references := make([]string, 0, len(req.Images)+1)
	for _, reference := range req.Images {
		if !chatGPTWebSupportsImageReference(reference) {
			return false
		}
		imageURL, _ := imageURLFromReference(reference)
		references = append(references, imageURL)
	}
	if req.Mask != nil {
		if !chatGPTWebSupportsImageReference(*req.Mask) {
			return false
		}
		maskURL, err := imageURLFromReference(*req.Mask)
		if err != nil || strings.HasPrefix(strings.ToLower(maskURL), "data:image/webp") {
			return false
		}
		references = append(references, maskURL)
		for _, reference := range req.Images {
			imageURL, errImage := imageURLFromReference(reference)
			if errImage != nil || strings.HasPrefix(strings.ToLower(imageURL), "data:image/webp") {
				return false
			}
		}
	}
	if err := executorhelps.ValidateChatGPTWebImageReferences(
		references,
		executorhelps.ChatGPTWebMaxImageBytes,
		executorhelps.ChatGPTWebMaxImageRequestBytes,
	); err != nil {
		return false
	}
	return true
}

func chatGPTWebSupportsImageReference(reference imageReference) bool {
	if strings.TrimSpace(reference.FileID) != "" {
		return false
	}
	imageURL, err := imageURLFromReference(reference)
	if err != nil {
		return false
	}
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(imageURL)), "data:image/")
}

func (m *imageStreamMapper) writeCompleted(w io.Writer, result imageResult, usage json.RawMessage) {
	event := imageStreamEvent{
		Type:          m.operation.completedEvent,
		B64JSON:       result.B64JSON,
		RevisedPrompt: result.RevisedPrompt,
	}
	m.applyStreamImageFormat(&event, result.OutputFormat)
	if len(bytes.TrimSpace(usage)) > 0 && string(bytes.TrimSpace(usage)) != "null" {
		if m.omitInputUsage {
			usage = omitInputImageUsage(usage)
		}
		event.Usage = usage
	}
	m.writeSSE(w, m.operation.completedEvent, event)
}

func (m *imageStreamMapper) applyStreamImageFormat(event *imageStreamEvent, outputFormat string) {
	if event == nil || !imageResponseFormatIsURL(m.responseFormat) || strings.TrimSpace(event.B64JSON) == "" {
		return
	}
	event.URL = imageDataURL(event.B64JSON, outputFormat)
	event.B64JSON = ""
}

func (m *imageStreamMapper) writeSSE(w io.Writer, eventName string, payload imageStreamEvent) {
	if w == nil {
		return
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", eventName, string(data))
}

type imageSSEParser struct {
	pending         []byte
	maxPendingBytes int
	err             error
}

type imageSSEItem struct {
	comment []byte
	payload []byte
}

func (p *imageSSEParser) Push(chunk []byte) []imageSSEItem {
	if p == nil || p.err != nil || len(chunk) == 0 {
		return nil
	}
	needsLineBreak := responsesSSENeedsLineBreak(p.pending, chunk)
	maxPendingBytes := p.maxPendingBytes
	if maxPendingBytes <= 0 {
		maxPendingBytes = maxImageSSEPendingBytes
	}
	var items []imageSSEItem
	appendFrameBytes := func(data []byte) bool {
		return appendBoundedSSEFrames(&p.pending, data, maxPendingBytes, func(frame []byte) {
			items = append(items, extractImageSSEItems(frame)...)
		})
	}
	if needsLineBreak && !appendFrameBytes([]byte{'\n'}) {
		p.pending = nil
		p.err = fmt.Errorf("upstream image SSE frame exceeds %d bytes", maxPendingBytes)
		return nil
	}
	if !appendFrameBytes(chunk) {
		p.pending = nil
		p.err = fmt.Errorf("upstream image SSE frame exceeds %d bytes", maxPendingBytes)
		return nil
	}
	if imageSSECanEmitWithoutDelimiter(p.pending) || imageSSELooksLikeJSON(p.pending) {
		items = append(items, extractImageSSEItems(p.pending)...)
		p.pending = p.pending[:0]
	}
	return items
}

func (p *imageSSEParser) Flush() []imageSSEItem {
	if p == nil || p.err != nil {
		return nil
	}
	if len(bytes.TrimSpace(p.pending)) == 0 {
		p.pending = p.pending[:0]
		return nil
	}
	items := extractImageSSEItems(p.pending)
	p.pending = p.pending[:0]
	return items
}

func (p *imageSSEParser) Error() error {
	if p == nil {
		return nil
	}
	return p.err
}

func extractImageSSEItems(frame []byte) []imageSSEItem {
	trimmed := bytes.TrimSpace(frame)
	if len(trimmed) == 0 {
		return nil
	}
	if imageSSELooksLikeJSON(trimmed) {
		return []imageSSEItem{{payload: bytes.Clone(trimmed)}}
	}
	if handlers.SSECommentsOnly(frame) {
		return []imageSSEItem{{comment: bytes.Clone(frame)}}
	}
	var comments bytes.Buffer
	var dataLines [][]byte
	for _, line := range handlers.SplitSSELines(trimmed) {
		line = bytes.TrimSpace(line)
		if bytes.HasPrefix(line, []byte(":")) {
			comments.Write(line)
			comments.WriteByte('\n')
			continue
		}
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		data := bytes.TrimSpace(line[len("data:"):])
		if len(data) == 0 || bytes.Equal(data, []byte("[DONE]")) {
			continue
		}
		dataLines = append(dataLines, data)
	}
	items := make([]imageSSEItem, 0, 2)
	if comments.Len() > 0 {
		comments.WriteByte('\n')
		items = append(items, imageSSEItem{comment: bytes.Clone(comments.Bytes())})
	}
	if len(dataLines) == 1 {
		items = append(items, imageSSEItem{payload: bytes.Clone(dataLines[0])})
	} else if len(dataLines) > 1 {
		items = append(items, imageSSEItem{payload: bytes.Join(dataLines, []byte{'\n'})})
	}
	return items
}

func imageSSECanEmitWithoutDelimiter(chunk []byte) bool {
	trimmed := bytes.TrimSpace(chunk)
	if len(trimmed) == 0 || responsesSSENeedsMoreData(trimmed) || !responsesSSEHasField(trimmed, []byte("data:")) {
		return false
	}
	return imageSSEDataLinesLookComplete(trimmed)
}

func imageSSEDataLinesLookComplete(chunk []byte) bool {
	for _, line := range handlers.SplitSSELines(chunk) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 || !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		data := bytes.TrimSpace(line[len("data:"):])
		if len(data) == 0 || bytes.Equal(data, []byte("[DONE]")) {
			continue
		}
		if !imageSSELooksLikeJSON(data) {
			return false
		}
	}
	return true
}

func imageSSELooksLikeJSON(chunk []byte) bool {
	trimmed := bytes.TrimSpace(chunk)
	return len(trimmed) > 1 && trimmed[0] == '{' && trimmed[len(trimmed)-1] == '}' && json.Valid(trimmed)
}

func responseEventType(payload []byte) string {
	return gjson.GetBytes(payload, "type").String()
}

func (h *OpenAIImagesAPIHandler) imagesCodexModel() string {
	if h != nil && h.Cfg != nil {
		if model := strings.TrimSpace(h.Cfg.Images.CodexModel); model != "" {
			return model
		}
	}
	return defaultImagesCodexModel
}

func (h *OpenAIImagesAPIHandler) imagesImageModel() string {
	if h != nil && h.Cfg != nil {
		if model := strings.TrimSpace(h.Cfg.Images.ImageModel); model != "" {
			return model
		}
	}
	return defaultImagesImageModel
}

func (h *OpenAIImagesAPIHandler) imagesNAggregationEnabled() bool {
	if h != nil && h.Cfg != nil && h.Cfg.Images.EnableNAggregation != nil {
		return *h.Cfg.Images.EnableNAggregation
	}
	return false
}

func (h *OpenAIImagesAPIHandler) imagesOverrideResponseFormatURLEnabled() bool {
	if h != nil && h.Cfg != nil {
		if h.Cfg.Images.OverrideResponseFormatURL != nil {
			return *h.Cfg.Images.OverrideResponseFormatURL
		}
		return h.Cfg.Images.OverrideUnsupportedParams
	}
	return false
}

func (h *OpenAIImagesAPIHandler) imagesResponseFormatURLDataURLEnabled() bool {
	if h != nil && h.Cfg != nil && h.Cfg.Images.ResponseFormatURLDataURL != nil {
		return *h.Cfg.Images.ResponseFormatURLDataURL
	}
	return false
}

func (h *OpenAIImagesAPIHandler) imagesOverrideTransparentBackgroundEnabled() bool {
	if h != nil && h.Cfg != nil {
		if h.Cfg.Images.OverrideTransparentBackground != nil {
			return *h.Cfg.Images.OverrideTransparentBackground
		}
		return h.Cfg.Images.OverrideUnsupportedParams
	}
	return false
}

func (h *OpenAIImagesAPIHandler) imagesOverrideInputFidelityEnabled() bool {
	if h != nil && h.Cfg != nil {
		if h.Cfg.Images.OverrideInputFidelity != nil {
			return *h.Cfg.Images.OverrideInputFidelity
		}
		return h.Cfg.Images.OverrideUnsupportedParams
	}
	return false
}

func (h *OpenAIImagesAPIHandler) imagesNativeEnabled(op imageOperation) bool {
	return h.nativeImageEndpointConfig(op).Enabled
}

func (h *OpenAIImagesAPIHandler) nativeImageEndpointConfig(op imageOperation) sdkconfig.NativeImageEndpointConfig {
	defaultMessage := "Native image generation is not enabled for model {model}"
	if h == nil || h.Cfg == nil {
		return nativeImageEndpointConfigWithDefaults(sdkconfig.NativeImageEndpointConfig{}, defaultMessage)
	}
	if op.action == imageEditOperation.action {
		defaultMessage = "Native image edit is not enabled for model {model}"
		return nativeImageEndpointConfigWithDefaults(h.Cfg.Images.Native.Edits, defaultMessage)
	}
	return nativeImageEndpointConfigWithDefaults(h.Cfg.Images.Native.Generations, defaultMessage)
}

func nativeImageEndpointConfigWithDefaults(cfg sdkconfig.NativeImageEndpointConfig, defaultMessage string) sdkconfig.NativeImageEndpointConfig {
	if len(cfg.Models) == 0 {
		cfg.Models = []string{"gpt-image-2", "gpt-image-1.5"}
	}
	if cfg.UnsupportedModelStatusCode < http.StatusBadRequest || cfg.UnsupportedModelStatusCode > 599 {
		cfg.UnsupportedModelStatusCode = http.StatusBadRequest
	}
	cfg.UnsupportedModelMessage = strings.TrimSpace(cfg.UnsupportedModelMessage)
	if cfg.UnsupportedModelMessage == "" {
		cfg.UnsupportedModelMessage = defaultMessage
	}
	return cfg
}

func nativeImagesAlt(op imageOperation) string {
	if op.action == imageEditOperation.action {
		return nativeImagesEdits
	}
	return nativeImagesGenerations
}

func nativeImageModelAllowed(models []string, model string) bool {
	model = strings.TrimSpace(model)
	if model == "" {
		return false
	}
	modelKey := nativeImageModelKey(model)
	for _, allowed := range models {
		allowed = strings.TrimSpace(allowed)
		if allowed == "" {
			continue
		}
		if strings.EqualFold(allowed, model) || nativeImageModelKey(allowed) == modelKey {
			return true
		}
	}
	return false
}

func nativeImageModelKey(model string) string {
	model = strings.ToLower(strings.TrimSpace(model))
	if idx := strings.LastIndex(model, "/"); idx >= 0 && idx+1 < len(model) {
		return model[idx+1:]
	}
	return model
}

func nativeImageUnsupportedModelMessage(template string, model string) string {
	template = strings.TrimSpace(template)
	if template == "" {
		template = "Native image request is not enabled for model {model}"
	}
	return strings.ReplaceAll(template, "{model}", strings.TrimSpace(model))
}

func applyNativeImageParamRules(rawJSON []byte, rules []string) []byte {
	out := rawJSON
	for _, rule := range rules {
		rule = strings.TrimSpace(rule)
		if rule == "" {
			continue
		}
		path, value, hasValue := strings.Cut(rule, "=")
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		if !hasValue {
			updated, err := sjson.DeleteBytes(out, path)
			if err == nil {
				out = updated
			}
			continue
		}
		value = strings.TrimSpace(value)
		if rawValue, ok := nativeImageRuleJSONValue(value); ok {
			updated, err := sjson.SetRawBytes(out, path, rawValue)
			if err == nil {
				out = updated
			}
			continue
		}
		updated, err := sjson.SetBytes(out, path, value)
		if err == nil {
			out = updated
		}
	}
	return out
}

func nativeImageRuleJSONValue(value string) ([]byte, bool) {
	if value == "" {
		return nil, false
	}
	raw := []byte(value)
	if !json.Valid(raw) {
		return nil, false
	}
	return raw, true
}

func imageStreamFlushInterval(cfg *sdkconfig.SDKConfig) *time.Duration {
	if cfg == nil || !imageStreamFlushEnabled(cfg) {
		return nil
	}
	ms := cfg.Images.StreamFlushIntervalMS
	if ms <= 0 {
		ms = cfg.Streaming.StreamFlushIntervalMS
	}
	if ms <= 0 {
		return nil
	}
	interval := time.Duration(ms) * time.Millisecond
	return &interval
}

func imageStreamFlushMinBytes(cfg *sdkconfig.SDKConfig) int {
	if cfg == nil || !imageStreamFlushEnabled(cfg) {
		return 0
	}
	if cfg.Images.StreamFlushMinBytes > 0 {
		return cfg.Images.StreamFlushMinBytes
	}
	if cfg.Streaming.StreamFlushMinBytes > 0 {
		return cfg.Streaming.StreamFlushMinBytes
	}
	return 0
}

func imageStreamFlushEnabled(cfg *sdkconfig.SDKConfig) bool {
	if cfg == nil || cfg.Images.EnableStreamFlush == nil {
		return true
	}
	return *cfg.Images.EnableStreamFlush
}

func responseStreamFlushInterval(cfg *sdkconfig.SDKConfig) *time.Duration {
	interval := handlers.StreamingFlushInterval(cfg)
	if interval <= 0 {
		return nil
	}
	return &interval
}

func responseStreamFlushMinBytes(cfg *sdkconfig.SDKConfig) int {
	return handlers.StreamingFlushMinBytes(cfg)
}

func (h *OpenAIImagesAPIHandler) imagesUnsupportedStatusCode() int {
	if h != nil && h.Cfg != nil {
		if code := h.Cfg.Images.UnsupportedStatusCode; code >= http.StatusBadRequest && code <= 599 {
			return code
		}
	}
	return http.StatusBadRequest
}

func (h *OpenAIImagesAPIHandler) writeImagesRequestError(c *gin.Context, err error) {
	var maxBytesErr *http.MaxBytesError
	if errors.As(err, &maxBytesErr) {
		h.writeImagesError(c, http.StatusRequestEntityTooLarge, err)
		return
	}
	var unsupported imageUnsupportedError
	if errors.As(err, &unsupported) {
		h.writeImagesError(c, h.imagesUnsupportedStatusCode(), err)
		return
	}
	h.writeImagesError(c, http.StatusBadRequest, err)
}

func (h *OpenAIImagesAPIHandler) writeImagesError(c *gin.Context, status int, err error) {
	h.WriteErrorResponse(c, &interfaces.ErrorMessage{StatusCode: status, Error: err})
}

func unsupportedImageErrorf(format string, args ...any) error {
	return imageUnsupportedError{err: fmt.Errorf(format, args...)}
}

func setOptionalString(dst map[string]any, key, value string) {
	if trimmed := strings.TrimSpace(value); trimmed != "" {
		dst[key] = trimmed
	}
}

func imageURLFromReference(ref imageReference) (string, error) {
	switch v := ref.ImageURL.(type) {
	case string:
		if strings.TrimSpace(v) == "" {
			return "", errors.New("image_url is empty")
		}
		return strings.TrimSpace(v), nil
	case map[string]any:
		raw, ok := v["url"]
		if !ok {
			return "", errors.New("image_url.url is required")
		}
		url, ok := raw.(string)
		if !ok || strings.TrimSpace(url) == "" {
			return "", errors.New("image_url.url must be a non-empty string")
		}
		return strings.TrimSpace(url), nil
	default:
		return "", errors.New("image_url is required")
	}
}

func dataURLFromFileHeader(fh *multipart.FileHeader) (string, error) {
	if fh == nil {
		return "", errors.New("image file is required")
	}
	file, err := fh.Open()
	if err != nil {
		return "", fmt.Errorf("open image file: %w", err)
	}
	defer func() { _ = file.Close() }()
	limited := io.LimitReader(file, maxImageUploadBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return "", fmt.Errorf("read image file: %w", err)
	}
	if len(data) == 0 {
		return "", errors.New("image file is empty")
	}
	if len(data) > maxImageUploadBytes {
		return "", fmt.Errorf("image file %q exceeds %d bytes", fh.Filename, maxImageUploadBytes)
	}
	mimeType := strings.TrimSpace(fh.Header.Get("Content-Type"))
	if mimeType == "" || mimeType == "application/octet-stream" {
		mimeType = http.DetectContentType(data)
	}
	return "data:" + mimeType + ";base64," + base64.StdEncoding.EncodeToString(data), nil
}

func multipartValue(form *multipart.Form, key string) string {
	if form == nil || form.Value == nil {
		return ""
	}
	values := form.Value[key]
	if len(values) == 0 {
		return ""
	}
	return strings.TrimSpace(values[0])
}

func multipartFiles(form *multipart.Form, key string) []*multipart.FileHeader {
	if form == nil || form.File == nil {
		return nil
	}
	return form.File[key]
}

func parseOptionalInt(value string) (int, bool, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, false, fmt.Errorf("invalid integer value %q", value)
	}
	return parsed, true, nil
}

func parseBoolValue(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "t", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

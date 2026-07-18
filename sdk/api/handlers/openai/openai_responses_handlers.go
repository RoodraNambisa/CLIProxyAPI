// Package openai provides HTTP handlers for OpenAIResponses API endpoints.
// This package implements the OpenAIResponses-compatible API interface, including model listing
// and chat completion functionality. It supports both streaming and non-streaming responses,
// and manages a pool of clients to interact with backend services.
// The handlers translate OpenAIResponses API requests to the appropriate backend format and
// convert responses back to OpenAIResponses-compatible format.
package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"

	"github.com/gin-gonic/gin"
	. "github.com/router-for-me/CLIProxyAPI/v6/internal/constant"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const responsesSSEMaxPendingBytes = 50 << 20

type responsesSSEFrameLimitError struct {
	limit int
}

func (err *responsesSSEFrameLimitError) Error() string {
	return fmt.Sprintf("upstream Responses SSE frame exceeds %d bytes", err.limit)
}

func (*responsesSSEFrameLimitError) StatusCode() int {
	return http.StatusBadGateway
}

func writeResponsesSSEChunk(w io.Writer, chunk []byte) {
	if w == nil || len(chunk) == 0 {
		return
	}
	if _, err := w.Write(chunk); err != nil {
		return
	}
	if bytes.HasSuffix(chunk, []byte("\n\n")) ||
		bytes.HasSuffix(chunk, []byte("\r\n\r\n")) ||
		bytes.HasSuffix(chunk, []byte("\r\r")) {
		return
	}
	suffix := []byte("\n\n")
	if bytes.HasSuffix(chunk, []byte("\r\n")) {
		suffix = []byte("\r\n")
	} else if bytes.HasSuffix(chunk, []byte("\n")) {
		suffix = []byte("\n")
	} else if bytes.HasSuffix(chunk, []byte("\r")) {
		suffix = []byte("\r")
	}
	if _, err := w.Write(suffix); err != nil {
		return
	}
}

type responsesSSEFramer struct {
	pending              []byte
	outputItems          map[int][]byte
	outputOrder          []int
	unindexedOutputItems [][]byte
	passthrough          bool
	passthroughState     *coreexecutor.ImageGenerationStreamPassthroughState
	maxPendingBytes      int
	err                  error
}

func (f *responsesSSEFramer) WriteChunk(w io.Writer, chunk []byte) {
	if len(chunk) == 0 || f.err != nil {
		return
	}
	if f.passthrough {
		if len(f.pending) > 0 {
			_, _ = w.Write(f.pending)
			f.pending = f.pending[:0]
		}
		_, _ = w.Write(chunk)
		return
	}
	needsLineBreak := responsesSSENeedsLineBreak(f.pending, chunk)
	limit := f.maxPendingBytes
	if limit <= 0 {
		limit = responsesSSEMaxPendingBytes
	}
	appendFrameBytes := func(data []byte) bool {
		return appendBoundedSSEFrames(&f.pending, data, limit, func(frame []byte) {
			if f.err == nil {
				f.writeFrame(w, frame)
			}
		})
	}
	if needsLineBreak && !appendFrameBytes([]byte{'\n'}) {
		f.pending = nil
		f.err = &responsesSSEFrameLimitError{limit: limit}
		return
	}
	if !appendFrameBytes(chunk) {
		f.pending = nil
		f.err = &responsesSSEFrameLimitError{limit: limit}
		return
	}
	if f.err != nil {
		f.pending = nil
		return
	}
	if len(bytes.TrimSpace(f.pending)) == 0 {
		f.pending = f.pending[:0]
		return
	}
	if len(f.pending) == 0 || !responsesSSECanEmitWithoutDelimiter(f.pending) {
		return
	}
	f.writeFrame(w, f.pending)
	f.pending = f.pending[:0]
}

func appendBoundedSSEFrames(pending *[]byte, chunk []byte, limit int, consume func([]byte)) bool {
	if pending == nil || limit <= 0 {
		return false
	}
	consumeComplete := func() {
		buffer := *pending
		consumed := 0
		for consumed < len(buffer) {
			frameLen := handlers.SSEFrameLen(buffer[consumed:])
			if frameLen == 0 {
				break
			}
			frame := buffer[consumed : consumed+frameLen]
			if consume != nil && len(bytes.TrimSpace(frame)) > 0 {
				consume(frame)
			}
			consumed += frameLen
		}
		if consumed > 0 {
			copy(buffer, buffer[consumed:])
			*pending = buffer[:len(buffer)-consumed]
		}
	}
	consumeComplete()
	for len(chunk) > 0 {
		room := limit - len(*pending)
		if room <= 0 {
			return false
		}
		take := len(chunk)
		if take > room {
			take = room
		}
		*pending = append(*pending, chunk[:take]...)
		chunk = chunk[take:]
		consumeComplete()
	}
	return true
}

func (f *responsesSSEFramer) Err() error {
	if f == nil {
		return nil
	}
	return f.err
}

func (f *responsesSSEFramer) Flush(w io.Writer) {
	if f.passthrough {
		if len(f.pending) > 0 {
			_, _ = w.Write(f.pending)
			f.pending = f.pending[:0]
		}
		return
	}
	if len(f.pending) == 0 {
		return
	}
	if len(bytes.TrimSpace(f.pending)) == 0 {
		f.pending = f.pending[:0]
		return
	}
	if !responsesSSECanEmitAtEOF(f.pending) {
		f.pending = f.pending[:0]
		return
	}
	f.writeFrame(w, f.pending)
	f.pending = f.pending[:0]
}

func writeResponsesStreamError(w io.Writer, errMsg *interfaces.ErrorMessage) {
	if w == nil || errMsg == nil {
		return
	}
	status := http.StatusInternalServerError
	if errMsg.StatusCode > 0 {
		status = errMsg.StatusCode
	}
	errText := http.StatusText(status)
	if errMsg.Error != nil && errMsg.Error.Error() != "" {
		errText = errMsg.Error.Error()
	}
	chunk := handlers.BuildOpenAIResponsesStreamErrorChunk(status, errText, 0)
	_, _ = fmt.Fprintf(w, "\n\nevent: error\ndata: %s\n\n", string(chunk))
}

func (f *responsesSSEFramer) passthroughEnabled() bool {
	return f != nil && (f.passthrough || (f.passthroughState != nil && f.passthroughState.Enabled()))
}

func (f *responsesSSEFramer) imagePassthroughEnabled() bool {
	return f != nil && f.passthroughState != nil && f.passthroughState.Enabled()
}

func (f *responsesSSEFramer) writeFrame(w io.Writer, frame []byte) {
	if f.imagePassthroughEnabled() {
		writeResponsesSSEChunk(w, frame)
		return
	}
	writeResponsesSSEChunk(w, f.repairFrame(frame))
}

func (f *responsesSSEFramer) repairFrame(frame []byte) []byte {
	payload, ok := responsesSSEDataPayload(frame)
	if !ok || len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) || !json.Valid(payload) {
		return frame
	}

	eventType := gjson.GetBytes(payload, "type").String()
	repairedFrame := frame
	switch eventType {
	case "response.output_item.done":
		f.recordOutputItem(payload)
	case "response.completed":
		repaired := f.repairCompletedPayload(payload)
		if !bytes.Equal(repaired, payload) {
			repairedFrame = responsesSSEFrameWithData(frame, repaired)
		}
	}
	if eventType != "" && !responsesSSEHasField(repairedFrame, []byte("event:")) {
		return responsesSSEFrameWithEvent(repairedFrame, eventType)
	}
	return repairedFrame
}

func responsesSSEDataPayload(frame []byte) ([]byte, bool) {
	var payload []byte
	found := false
	for _, line := range handlers.SplitSSELines(frame) {
		trimmed := bytes.TrimSpace(line)
		if !bytes.HasPrefix(trimmed, []byte("data:")) {
			continue
		}
		data := bytes.TrimSpace(trimmed[len("data:"):])
		if found {
			payload = append(payload, '\n')
		}
		payload = append(payload, data...)
		found = true
	}
	return payload, found
}

func responsesSSEFrameWithData(frame, payload []byte) []byte {
	var out bytes.Buffer
	for _, line := range handlers.SplitSSELines(frame) {
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 || bytes.HasPrefix(trimmed, []byte("data:")) {
			continue
		}
		out.Write(line)
		out.WriteByte('\n')
	}
	for _, line := range bytes.Split(payload, []byte("\n")) {
		out.WriteString("data: ")
		out.Write(line)
		out.WriteByte('\n')
	}
	out.WriteByte('\n')
	return out.Bytes()
}

func responsesSSEFrameWithEvent(frame []byte, eventType string) []byte {
	var out bytes.Buffer
	out.Grow(len(frame) + len(eventType) + len("event: \n"))
	out.WriteString("event: ")
	out.WriteString(eventType)
	out.WriteByte('\n')
	out.Write(frame)
	return out.Bytes()
}

func (f *responsesSSEFramer) recordOutputItem(payload []byte) {
	item := gjson.GetBytes(payload, "item")
	if !item.Exists() || !item.IsObject() || item.Get("type").String() == "" {
		return
	}

	if outputIndex := gjson.GetBytes(payload, "output_index"); outputIndex.Exists() {
		index := int(outputIndex.Int())
		if f.outputItems == nil {
			f.outputItems = make(map[int][]byte)
		}
		if _, exists := f.outputItems[index]; !exists {
			f.outputOrder = append(f.outputOrder, index)
		}
		f.outputItems[index] = append([]byte(nil), item.Raw...)
		return
	}

	f.unindexedOutputItems = append(f.unindexedOutputItems, append([]byte(nil), item.Raw...))
}

func (f *responsesSSEFramer) repairCompletedPayload(payload []byte) []byte {
	if len(f.outputOrder) == 0 && len(f.unindexedOutputItems) == 0 {
		return payload
	}
	output := gjson.GetBytes(payload, "response.output")
	if output.Exists() && (!output.IsArray() || len(output.Array()) > 0) {
		return payload
	}

	var outputJSON bytes.Buffer
	outputJSON.WriteByte('[')
	indexes := append([]int(nil), f.outputOrder...)
	sort.Ints(indexes)
	written := 0
	for _, index := range indexes {
		item, ok := f.outputItems[index]
		if !ok {
			continue
		}
		if written > 0 {
			outputJSON.WriteByte(',')
		}
		outputJSON.Write(item)
		written++
	}
	for _, item := range f.unindexedOutputItems {
		if written > 0 {
			outputJSON.WriteByte(',')
		}
		outputJSON.Write(item)
		written++
	}
	outputJSON.WriteByte(']')

	repaired, err := sjson.SetRawBytes(payload, "response.output", outputJSON.Bytes())
	if err != nil {
		return payload
	}
	return repaired
}

func responsesSSENeedsMoreData(chunk []byte) bool {
	trimmed := bytes.TrimSpace(chunk)
	if len(trimmed) == 0 {
		return false
	}
	return responsesSSEHasField(trimmed, []byte("event:")) && !responsesSSEHasField(trimmed, []byte("data:"))
}

func responsesSSEHasField(chunk []byte, prefix []byte) bool {
	for _, line := range handlers.SplitSSELines(chunk) {
		line = bytes.TrimSpace(line)
		if bytes.HasPrefix(line, prefix) {
			return true
		}
	}
	return false
}

func responsesSSECanEmitWithoutDelimiter(chunk []byte) bool {
	if len(chunk) > 0 && chunk[len(chunk)-1] == '\r' {
		return false
	}
	return responsesSSECanEmitAtEOF(chunk)
}

func responsesSSECanEmitAtEOF(chunk []byte) bool {
	trimmed := bytes.TrimSpace(chunk)
	if len(trimmed) == 0 || responsesSSENeedsMoreData(trimmed) || !responsesSSEHasField(trimmed, []byte("data:")) {
		return false
	}
	payload, found := responsesSSEDataPayload(trimmed)
	payload = bytes.TrimSpace(payload)
	return found && (len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) || json.Valid(payload))
}

func responsesSSENeedsLineBreak(pending, chunk []byte) bool {
	if len(pending) == 0 || len(chunk) == 0 {
		return false
	}
	if bytes.HasSuffix(pending, []byte("\n")) || bytes.HasSuffix(pending, []byte("\r")) {
		return false
	}
	if chunk[0] == '\n' || chunk[0] == '\r' {
		return false
	}
	trimmed := bytes.TrimLeft(chunk, " \t")
	if len(trimmed) == 0 {
		return false
	}
	if payload, ok := responsesSSEDataPayload(pending); ok &&
		len(payload) > 0 && !bytes.Equal(payload, []byte("[DONE]")) && !json.Valid(payload) {
		return false
	}
	for _, prefix := range [][]byte{[]byte("data:"), []byte("event:"), []byte("id:"), []byte("retry:"), []byte(":")} {
		if bytes.HasPrefix(trimmed, prefix) {
			return true
		}
	}
	return false
}

// OpenAIResponsesAPIHandler contains the handlers for OpenAIResponses API endpoints.
// It holds a pool of clients to interact with the backend service.
type OpenAIResponsesAPIHandler struct {
	*handlers.BaseAPIHandler
}

// NewOpenAIResponsesAPIHandler creates a new OpenAIResponses API handlers instance.
// It takes an BaseAPIHandler instance as input and returns an OpenAIResponsesAPIHandler.
//
// Parameters:
//   - apiHandlers: The base API handlers instance
//
// Returns:
//   - *OpenAIResponsesAPIHandler: A new OpenAIResponses API handlers instance
func NewOpenAIResponsesAPIHandler(apiHandlers *handlers.BaseAPIHandler) *OpenAIResponsesAPIHandler {
	return &OpenAIResponsesAPIHandler{
		BaseAPIHandler: apiHandlers,
	}
}

// HandlerType returns the identifier for this handler implementation.
func (h *OpenAIResponsesAPIHandler) HandlerType() string {
	return OpenaiResponse
}

// Models returns the OpenAIResponses-compatible model metadata supported by this handler.
func (h *OpenAIResponsesAPIHandler) Models() []map[string]any {
	// Get dynamic models from the global registry
	modelRegistry := registry.GetGlobalRegistry()
	return modelRegistry.GetAvailableModels("openai")
}

// OpenAIResponsesModels handles the /v1/models endpoint.
// It returns a list of available AI models with their capabilities
// and specifications in OpenAIResponses-compatible format.
func (h *OpenAIResponsesAPIHandler) OpenAIResponsesModels(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"object": "list",
		"data":   h.FilterModelsByProviderAccess(c, h.Models()),
	})
}

// Responses handles the /v1/responses endpoint.
// It determines whether the request is for a streaming or non-streaming response
// and calls the appropriate handler based on the model provider.
//
// Parameters:
//   - c: The Gin context containing the HTTP request and response
func (h *OpenAIResponsesAPIHandler) Responses(c *gin.Context) {
	rawJSON, err := readOpenAIJSONRequestBody(c)
	// If data retrieval fails, return a 400 Bad Request error.
	if err != nil {
		writeResponsesRequestReadError(c, err)
		return
	}

	// Check if the client requested a streaming response.
	streamResult := gjson.GetBytes(rawJSON, "stream")
	if streamResult.Type == gjson.True {
		h.handleStreamingResponse(c, rawJSON)
	} else {
		h.handleNonStreamingResponse(c, rawJSON)
	}

}

func (h *OpenAIResponsesAPIHandler) Compact(c *gin.Context) {
	rawJSON, err := readOpenAIJSONRequestBody(c)
	if err != nil {
		writeResponsesRequestReadError(c, err)
		return
	}

	streamResult := gjson.GetBytes(rawJSON, "stream")
	if streamResult.Type == gjson.True {
		c.JSON(http.StatusBadRequest, handlers.ErrorResponse{
			Error: handlers.ErrorDetail{
				Message: "Streaming not supported for compact responses",
				Type:    "invalid_request_error",
			},
		})
		return
	}
	if streamResult.Exists() {
		if updated, err := sjson.DeleteBytes(rawJSON, "stream"); err == nil {
			rawJSON = updated
		}
	}

	c.Header("Content-Type", "application/json")
	modelName := gjson.GetBytes(rawJSON, "model").String()
	cliCtx, cliCancel := h.GetContextWithCancel(h, c, context.Background())
	stopKeepAlive := h.StartNonStreamingKeepAlive(c, cliCtx)
	resp, upstreamHeaders, errMsg := h.ExecuteWithAuthManager(cliCtx, h.HandlerType(), modelName, rawJSON, "responses/compact")
	stopKeepAlive()
	if errMsg != nil {
		h.WriteErrorResponse(c, errMsg)
		cliCancel(errMsg.Error)
		return
	}
	handlers.WriteUpstreamHeaders(c.Writer.Header(), upstreamHeaders)
	_, _ = c.Writer.Write(resp)
	cliCancel()
}

func writeResponsesRequestReadError(c *gin.Context, err error) {
	status := http.StatusBadRequest
	if openAIJSONRequestTooLarge(err) {
		status = http.StatusRequestEntityTooLarge
	}
	c.JSON(status, handlers.ErrorResponse{
		Error: handlers.ErrorDetail{
			Message: fmt.Sprintf("Invalid request: %v", err),
			Type:    "invalid_request_error",
		},
	})
}

// handleNonStreamingResponse handles non-streaming chat completion responses
// for Gemini models. It selects a client from the pool, sends the request, and
// aggregates the response before sending it back to the client in OpenAIResponses format.
//
// Parameters:
//   - c: The Gin context containing the HTTP request and response
//   - rawJSON: The raw JSON bytes of the OpenAIResponses-compatible request
func (h *OpenAIResponsesAPIHandler) handleNonStreamingResponse(c *gin.Context, rawJSON []byte) {
	c.Header("Content-Type", "application/json")

	modelName := gjson.GetBytes(rawJSON, "model").String()
	cliCtx, cliCancel := h.GetContextWithCancel(h, c, context.Background())
	stopKeepAlive := h.StartNonStreamingKeepAlive(c, cliCtx)

	resp, upstreamHeaders, errMsg := h.ExecuteWithAuthManager(cliCtx, h.HandlerType(), modelName, rawJSON, "")
	stopKeepAlive()
	if errMsg != nil {
		h.WriteErrorResponse(c, errMsg)
		cliCancel(errMsg.Error)
		return
	}
	handlers.WriteUpstreamHeaders(c.Writer.Header(), upstreamHeaders)
	_, _ = c.Writer.Write(resp)
	cliCancel()
}

// handleStreamingResponse handles streaming responses for Gemini models.
// It establishes a streaming connection with the backend service and forwards
// the response chunks to the client in real-time using Server-Sent Events.
//
// Parameters:
//   - c: The Gin context containing the HTTP request and response
//   - rawJSON: The raw JSON bytes of the OpenAIResponses-compatible request
func (h *OpenAIResponsesAPIHandler) handleStreamingResponse(c *gin.Context, rawJSON []byte) {
	// Get the http.Flusher interface to manually flush the response.
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, handlers.ErrorResponse{
			Error: handlers.ErrorDetail{
				Message: "Streaming not supported",
				Type:    "server_error",
			},
		})
		return
	}

	// New core execution path
	modelName := gjson.GetBytes(rawJSON, "model").String()
	cliCtx, cliCancel := h.GetContextWithCancel(h, c, context.Background())
	requestedImageStreamPassthrough := cliproxyauth.PayloadHasImageGenerationTool(rawJSON)
	var imageStreamPassthroughState *coreexecutor.ImageGenerationStreamPassthroughState
	if requestedImageStreamPassthrough {
		imageStreamPassthroughState = &coreexecutor.ImageGenerationStreamPassthroughState{}
		cliCtx = handlers.WithImageGenerationStreamPassthrough(cliCtx, true)
		cliCtx = handlers.WithImageGenerationStreamPassthroughState(cliCtx, imageStreamPassthroughState)
	}
	dataChan, upstreamHeaders, errChan := h.ExecuteStreamWithAuthManager(cliCtx, h.HandlerType(), modelName, rawJSON, "")

	setSSEHeaders := func() {
		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")
		c.Header("Connection", "keep-alive")
		c.Header("Access-Control-Allow-Origin", "*")
	}
	trustUpstreamSSE := handlers.StreamingTrustUpstreamSSE(h.Cfg)
	framer := &responsesSSEFramer{passthrough: trustUpstreamSSE}
	if requestedImageStreamPassthrough {
		framer.passthroughState = imageStreamPassthroughState
	}
	var firstFrame bytes.Buffer

	// Peek at the first chunk
	for {
		select {
		case <-c.Request.Context().Done():
			cliCancel(c.Request.Context().Err())
			return
		case errMsg, ok := <-errChan:
			if !ok {
				// Err channel closed cleanly; wait for data channel.
				errChan = nil
				continue
			}
			// Upstream failed immediately. Return proper error status and JSON.
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
				framer.Flush(&firstFrame)
				if errFrame := framer.Err(); errFrame != nil {
					h.WriteErrorResponse(c, &interfaces.ErrorMessage{
						StatusCode: http.StatusBadGateway,
						Error:      errFrame,
					})
					cliCancel(errFrame)
					return
				}
				// Stream closed without data? Send headers and done.
				setSSEHeaders()
				handlers.WriteUpstreamHeaders(c.Writer.Header(), upstreamHeaders)
				_, _ = c.Writer.Write(firstFrame.Bytes())
				_, _ = c.Writer.Write([]byte("\n"))
				flusher.Flush()
				cliCancel(nil)
				return
			}

			framer.WriteChunk(&firstFrame, chunk)
			if errFrame := framer.Err(); errFrame != nil {
				errMsg := &interfaces.ErrorMessage{
					StatusCode: http.StatusBadGateway,
					Error:      errFrame,
				}
				h.WriteErrorResponse(c, errMsg)
				cliCancel(errFrame)
				return
			}
			if firstFrame.Len() == 0 {
				continue
			}

			setSSEHeaders()
			handlers.WriteUpstreamHeaders(c.Writer.Header(), upstreamHeaders)
			_, _ = c.Writer.Write(firstFrame.Bytes())
			flusher.Flush()

			// Continue
			h.forwardResponsesStream(c, flusher, func(err error) { cliCancel(err) }, dataChan, errChan, framer, requestedImageStreamPassthrough)
			return
		}
	}
}

func (h *OpenAIResponsesAPIHandler) forwardResponsesStream(c *gin.Context, flusher http.Flusher, cancel func(error), data <-chan []byte, errs <-chan *interfaces.ErrorMessage, framer *responsesSSEFramer, imageStreamPassthrough ...bool) {
	if framer == nil {
		framer = &responsesSSEFramer{}
	}
	resolveFlushPolicy := func() (*time.Duration, int) {
		if framer.imagePassthroughEnabled() {
			return imageStreamFlushInterval(h.Cfg), imageStreamFlushMinBytes(h.Cfg)
		}
		return responseStreamFlushInterval(h.Cfg), responseStreamFlushMinBytes(h.Cfg)
	}
	flushInterval, flushMinBytes := resolveFlushPolicy()
	commentDetector := &handlers.SSECommentDetector{}
	h.ForwardStream(c, flusher, cancel, data, errs, handlers.StreamForwardOptions{
		FlushInterval:      flushInterval,
		FlushMinBytes:      flushMinBytes,
		ResolveFlushPolicy: resolveFlushPolicy,
		WriteChunk: func(chunk []byte) {
			framer.WriteChunk(c.Writer, chunk)
		},
		ChunkError: func() error {
			return framer.Err()
		},
		FlushChunk: func(chunk []byte) bool {
			return commentDetector.Feed(chunk)
		},
		WriteTerminalError: func(errMsg *interfaces.ErrorMessage) {
			framer.Flush(c.Writer)
			writeResponsesStreamError(c.Writer, errMsg)
		},
		WriteDone: func() {
			framer.Flush(c.Writer)
			_, _ = c.Writer.Write([]byte("\n"))
		},
	})
}

package gemini

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/constant"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const interactionsAgentAuthSelectionModel = "gemini-2.5-flash"

type interactionsRequestTarget struct {
	Model  string
	Agent  string
	Stream bool
}

func parseInteractionsRequestTarget(rawJSON []byte) (interactionsRequestTarget, error) {
	if !gjson.ValidBytes(rawJSON) {
		return interactionsRequestTarget{}, fmt.Errorf("invalid JSON body")
	}
	root := gjson.ParseBytes(rawJSON)
	model := strings.TrimSpace(root.Get("model").String())
	agent := strings.TrimSpace(root.Get("agent").String())
	if (model == "") == (agent == "") {
		return interactionsRequestTarget{}, fmt.Errorf("request requires exactly one of model or agent")
	}
	streamNode := root.Get("stream")
	if streamNode.Exists() && !streamNode.IsBool() {
		return interactionsRequestTarget{}, fmt.Errorf("stream must be a boolean")
	}
	return interactionsRequestTarget{Model: model, Agent: agent, Stream: streamNode.Bool()}, nil
}

func prepareInteractionsExecutionTarget(rawJSON []byte, target interactionsRequestTarget) (string, []byte) {
	if target.Agent != "" {
		return target.Agent, rawJSON
	}
	model := strings.TrimPrefix(strings.TrimSpace(target.Model), "models/")
	if model == target.Model {
		return model, rawJSON
	}
	updated, errSet := sjson.SetBytes(rawJSON, "model", model)
	if errSet != nil {
		return model, rawJSON
	}
	return model, updated
}

// Interactions handles both POST /v1/interactions and POST /v1beta/interactions.
func (h *GeminiAPIHandler) Interactions(c *gin.Context) {
	rawJSON, errRead := c.GetRawData()
	if errRead != nil {
		c.JSON(http.StatusBadRequest, handlers.ErrorResponse{Error: handlers.ErrorDetail{Message: errRead.Error(), Type: "invalid_request_error"}})
		return
	}
	target, errParse := parseInteractionsRequestTarget(rawJSON)
	if errParse != nil {
		c.JSON(http.StatusBadRequest, handlers.ErrorResponse{Error: handlers.ErrorDetail{Message: errParse.Error(), Type: "invalid_request_error"}})
		return
	}
	modelName, rawJSON := prepareInteractionsExecutionTarget(rawJSON, target)
	version := "v1beta"
	if c.FullPath() == "/v1/interactions" || c.Request.URL.Path == "/v1/interactions" {
		version = "v1"
	}
	alt := h.GetAlt(c)

	cliCtx, cliCancel := h.GetContextWithCancel(h, c, context.Background())
	cliCtx = handlers.WithInteractionsAPIMetadata(cliCtx, version, c.GetHeader("Api-Revision"))
	if target.Stream {
		h.handleInteractionsStream(c, cliCtx, cliCancel, target, modelName, rawJSON, alt)
		return
	}
	h.handleInteractionsNonStream(c, cliCtx, cliCancel, target, modelName, rawJSON, alt)
}

func (h *GeminiAPIHandler) handleInteractionsNonStream(c *gin.Context, cliCtx context.Context, cliCancel handlers.APIHandlerCancelFunc, target interactionsRequestTarget, modelName string, rawJSON []byte, alt string) {
	defer cliCancel(nil)
	c.Header("Content-Type", "application/json")
	stopKeepAlive := h.StartNonStreamingKeepAlive(c, cliCtx)

	var resp []byte
	var upstreamHeaders http.Header
	var errMsg *interfaces.ErrorMessage
	if target.Agent != "" {
		resp, upstreamHeaders, errMsg = h.ExecuteWithProvidersAndExecutionModel(cliCtx, []string{constant.GeminiInteractions}, constant.Interactions, interactionsAgentAuthSelectionModel, modelName, rawJSON, alt)
	} else {
		resp, upstreamHeaders, errMsg = h.ExecuteWithAuthManager(cliCtx, constant.Interactions, modelName, rawJSON, alt)
	}
	stopKeepAlive()
	if errMsg != nil {
		h.WriteErrorResponse(c, errMsg)
		cliCancel(errMsg.Error)
		return
	}
	handlers.WriteUpstreamHeaders(c.Writer.Header(), upstreamHeaders)
	_, _ = c.Writer.Write(resp)
}

func (h *GeminiAPIHandler) handleInteractionsStream(c *gin.Context, cliCtx context.Context, cliCancel handlers.APIHandlerCancelFunc, target interactionsRequestTarget, modelName string, rawJSON []byte, alt string) {
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, handlers.ErrorResponse{Error: handlers.ErrorDetail{Message: "Streaming not supported", Type: "server_error"}})
		cliCancel(fmt.Errorf("streaming not supported"))
		return
	}

	var data <-chan []byte
	var upstreamHeaders http.Header
	var errs <-chan *interfaces.ErrorMessage
	if target.Agent != "" {
		data, upstreamHeaders, errs = h.ExecuteStreamWithProvidersAndExecutionModel(cliCtx, []string{constant.GeminiInteractions}, constant.Interactions, interactionsAgentAuthSelectionModel, modelName, rawJSON, alt)
	} else {
		data, upstreamHeaders, errs = h.ExecuteStreamWithAuthManager(cliCtx, constant.Interactions, modelName, rawJSON, alt)
	}

	for {
		select {
		case <-c.Request.Context().Done():
			cliCancel(c.Request.Context().Err())
			return
		case errMsg, okErr := <-errs:
			if !okErr {
				errs = nil
				continue
			}
			h.WriteErrorResponse(c, errMsg)
			if errMsg != nil {
				cliCancel(errMsg.Error)
			} else {
				cliCancel(nil)
			}
			return
		case chunk, okData := <-data:
			if !okData {
				if errMsg := pendingInteractionsStreamError(errs); errMsg != nil {
					h.WriteErrorResponse(c, errMsg)
					cliCancel(errMsg.Error)
					return
				}
				setInteractionsStreamHeaders(c, upstreamHeaders)
				flusher.Flush()
				cliCancel(nil)
				return
			}
			setInteractionsStreamHeaders(c, upstreamHeaders)
			writeInteractionsStreamChunk(c, chunk)
			flusher.Flush()
			h.forwardInteractionsStream(c, flusher, func(err error) { cliCancel(err) }, data, errs)
			return
		}
	}
}

func pendingInteractionsStreamError(errs <-chan *interfaces.ErrorMessage) *interfaces.ErrorMessage {
	if errs == nil {
		return nil
	}
	select {
	case errMsg, ok := <-errs:
		if ok {
			return errMsg
		}
	default:
	}
	return nil
}

func setInteractionsStreamHeaders(c *gin.Context, upstream http.Header) {
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("Access-Control-Allow-Origin", "*")
	handlers.WriteUpstreamHeaders(c.Writer.Header(), upstream)
}

func writeInteractionsStreamChunk(c *gin.Context, chunk []byte) {
	if len(chunk) == 0 {
		return
	}
	trimmed := bytes.TrimSpace(chunk)
	if bytes.HasPrefix(trimmed, []byte("event:")) || bytes.HasPrefix(trimmed, []byte("data:")) {
		_, _ = c.Writer.Write(chunk)
	} else {
		_, _ = c.Writer.Write([]byte("data: "))
		_, _ = c.Writer.Write(chunk)
	}
	if bytes.HasSuffix(chunk, []byte("\n\n")) {
		return
	}
	if bytes.HasSuffix(chunk, []byte("\n")) {
		_, _ = c.Writer.Write([]byte("\n"))
	} else {
		_, _ = c.Writer.Write([]byte("\n\n"))
	}
}

func (h *GeminiAPIHandler) forwardInteractionsStream(c *gin.Context, flusher http.Flusher, cancel func(error), data <-chan []byte, errs <-chan *interfaces.ErrorMessage) {
	h.ForwardStream(c, flusher, cancel, data, errs, handlers.StreamForwardOptions{
		WriteChunk: func(chunk []byte) {
			writeInteractionsStreamChunk(c, chunk)
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
			if errMsg.Error != nil && errMsg.Error.Error() != "" {
				errText = errMsg.Error.Error()
			}
			body := handlers.BuildErrorResponseBody(status, errText)
			_, _ = fmt.Fprintf(c.Writer, "event: error\ndata: %s\n\n", string(body))
		},
	})
}

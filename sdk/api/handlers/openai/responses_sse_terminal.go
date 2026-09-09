package openai

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	"github.com/tidwall/gjson"
)

var errResponsesSSEMissingTerminal = errors.New("upstream Responses stream closed before a terminal event")

func responsesSSETerminalEvent(eventType string) bool {
	return responsesWebsocketTerminalEvent(eventType) || eventType == "response.error"
}

func (f *responsesSSEFramer) observeTerminal(frame []byte) {
	payload, found := responsesSSEDataPayload(frame)
	if !found || len(bytes.TrimSpace(payload)) == 0 {
		return
	}
	if bytes.Equal(bytes.TrimSpace(payload), []byte("[DONE]")) {
		f.terminalSeen = f.imagePassthroughEnabled()
		return
	}
	root := gjson.ParseBytes(payload)
	eventType := root.Get("type").String()
	hasError := eventType == "error" || eventType == "response.failed" || eventType == "response.error"
	for _, line := range handlers.SplitSSELines(frame) {
		if value, ok := bytes.CutPrefix(bytes.TrimSpace(line), []byte("event:")); ok {
			headerType := string(bytes.TrimSpace(value))
			if eventType == "" || responsesSSETerminalEvent(headerType) {
				eventType = headerType
			}
		}
	}
	hasError = hasError || eventType == "error" || eventType == "response.failed" || eventType == "response.error"
	for _, path := range []string{"error", "response.error"} {
		value := root.Get(path)
		hasError = hasError || (value.Exists() && value.Type != gjson.Null)
	}
	hasError = hasError || (root.Get("code").Exists() && root.Get("message").Exists())
	f.terminalSeen = hasError || responsesSSETerminalEvent(eventType) || (f.imagePassthroughEnabled() && eventType == "image_generation.completed")
	if hasError {
		status := http.StatusBadGateway
		for _, path := range []string{"status", "status_code", "response.status_code", "error.status", "error.status_code", "response.error.status", "response.error.status_code"} {
			value := root.Get(path)
			if value.Type == gjson.Number && value.Float() >= 400 && value.Float() <= 599 && value.Float() == float64(value.Int()) {
				status = int(value.Int())
				break
			}
		}
		f.terminalBeforeData = !f.sawNonTerminalData
		f.terminalError = &interfaces.ErrorMessage{StatusCode: status, Error: fmt.Errorf("%s", payload)}
	} else {
		f.sawNonTerminalData = true
	}
}

func (f *responsesSSEFramer) CloseError() *interfaces.ErrorMessage {
	if f.passthrough {
		return nil
	}
	if f.err != nil {
		return &interfaces.ErrorMessage{StatusCode: http.StatusBadGateway, Error: f.err}
	}
	if f.terminalError != nil {
		return f.terminalError
	}
	if !f.terminalSeen {
		return &interfaces.ErrorMessage{StatusCode: http.StatusBadGateway, Error: errResponsesSSEMissingTerminal}
	}
	return nil
}

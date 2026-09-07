package openai

import (
	"bytes"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func normalizeResponsesWebsocketRequestWithContext(raw, previous, output []byte, lastResponseID string, incremental bool) ([]byte, []byte, *interfaces.ErrorMessage) {
	previousID := strings.TrimSpace(gjson.GetBytes(raw, "previous_response_id").String())
	previousWasIncremental := strings.TrimSpace(gjson.GetBytes(previous, "previous_response_id").String()) != ""
	requestType := strings.TrimSpace(gjson.GetBytes(raw, "type").String())
	replayRequired := func() ([]byte, []byte, *interfaces.ErrorMessage) {
		return nil, previous, &interfaces.ErrorMessage{StatusCode: http.StatusBadRequest, Error: executor.NewUpstreamWebsocketReplayRequiredError()}
	}
	if previousID != "" && (!incremental || len(previous) == 0) {
		// Local HTTP history may replace a previous_response_id only when that
		// history is complete and belongs to exactly the referenced response.
		if previousWasIncremental || lastResponseID == "" || previousID != lastResponseID {
			return replayRequired()
		}
	}
	if previousWasIncremental && previousID == "" {
		if requestType == wsRequestTypeCreate {
			if !gjson.GetBytes(raw, "input").IsArray() {
				return normalizeResponsesWebsocketRequestWithMode(raw, previous, output, incremental)
			}
			// A new full create is self-contained. Never append the last partial
			// request as though it were the entire conversation.
			normalized := normalizeResponseTranscriptReplacement(raw, previous)
			return normalized, bytes.Clone(normalized), nil
		}
		if requestType == wsRequestTypeAppend {
			if !incremental || lastResponseID == "" {
				return replayRequired()
			}
			var err error
			raw, err = sjson.SetBytes(raw, "previous_response_id", lastResponseID)
			if err != nil {
				return replayRequired()
			}
		}
	}
	return normalizeResponsesWebsocketRequestWithMode(raw, previous, output, incremental)
}

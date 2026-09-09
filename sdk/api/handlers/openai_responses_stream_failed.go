package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
)

type openAIResponsesStreamFailedResponse struct {
	Status string          `json:"status"`
	Error  json.RawMessage `json:"error"`
}

type openAIResponsesStreamFailedChunk struct {
	Type           string                              `json:"type"`
	SequenceNumber int                                 `json:"sequence_number"`
	Response       openAIResponsesStreamFailedResponse `json:"response"`
}

// BuildOpenAIResponsesStreamFailedChunk emits the terminal form understood by
// Codex clients without rounding numeric fields in structured upstream errors.
func BuildOpenAIResponsesStreamFailedChunk(status int, errText string, sequenceNumber int) []byte {
	if status <= 0 {
		status = http.StatusInternalServerError
	}
	if sequenceNumber < 0 {
		sequenceNumber = 0
	}
	validJSON := json.Valid([]byte(errText))
	if sequenceNumber == 0 && validJSON {
		sequence := gjson.Get(strings.TrimSpace(errText), "sequence_number")
		if value, errParse := strconv.Atoi(sequence.Raw); sequence.Type == gjson.Number && errParse == nil && value >= 0 {
			sequenceNumber = value
		}
	}
	var detail json.RawMessage
	if validJSON {
		root := gjson.Parse(errText)
		for _, path := range []string{"error", "response.error"} {
			if node := root.Get(path); node.IsObject() {
				detail = json.RawMessage(node.Raw)
				break
			}
		}
	}
	if len(detail) == 0 {
		legacy := gjson.ParseBytes(BuildOpenAIResponsesStreamErrorChunk(status, errText, sequenceNumber))
		errorType := "invalid_request_error"
		if status >= http.StatusInternalServerError {
			errorType = "server_error"
		}
		detail, _ = json.Marshal(map[string]string{"type": errorType, "code": legacy.Get("code").String(), "message": legacy.Get("message").String()})
	}
	data, errMarshal := json.Marshal(openAIResponsesStreamFailedChunk{
		Type: "response.failed", SequenceNumber: sequenceNumber,
		Response: openAIResponsesStreamFailedResponse{Status: "failed", Error: detail},
	})
	if errMarshal == nil {
		return data
	}
	return []byte(`{"type":"response.failed","sequence_number":0,"response":{"status":"failed","error":{"type":"server_error","code":"internal_server_error","message":"internal error"}}}`)
}

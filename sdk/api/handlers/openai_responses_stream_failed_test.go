package handlers

import (
	"net/http"
	"testing"

	"github.com/tidwall/gjson"
)

func TestBuildOpenAIResponsesStreamFailedPreservesDetailsAndSequence(t *testing.T) {
	if got := BuildOpenAIResponsesStreamFailedChunk(502, `{"sequence_number":12`, 0); gjson.GetBytes(got, "sequence_number").Int() != 0 {
		t.Fatal("malformed error JSON supplied the terminal sequence")
	}
	for _, path := range []string{"error", "response"} {
		detail := `{"code":"misalignment_policy_violation","type":"invalid_request_error","message":"denied","param":"input","reset_at":9007199254740993}`
		source := `{"error":` + detail + `,"sequence_number":12}`
		if path == "response" {
			source = `{"response":{"error":` + detail + `},"sequence_number":12}`
		}
		for _, sequence := range []int{0, 4, -1} {
			got := BuildOpenAIResponsesStreamFailedChunk(http.StatusBadGateway, source, sequence)
			want := int64(12)
			if sequence == 4 {
				want = 4
			}
			if gjson.GetBytes(got, "type").String() != "response.failed" || gjson.GetBytes(got, "response.status").String() != "failed" || gjson.GetBytes(got, "response.error").Raw != detail || gjson.GetBytes(got, "sequence_number").Int() != want {
				t.Fatal("failed terminal changed structured error fields or sequence priority")
			}
		}
	}
	for _, sequence := range []string{"-1", "1.5", `"2"`, "9223372036854775808"} {
		got := BuildOpenAIResponsesStreamFailedChunk(http.StatusBadGateway, `{"type":"error","message":"failed","sequence_number":`+sequence+`}`, 0)
		if gjson.GetBytes(got, "sequence_number").Int() != 0 || gjson.GetBytes(got, "response.error.message").String() != "failed" {
			t.Fatal("invalid sequence reached a failed terminal")
		}
	}
	for _, tc := range []struct {
		status                   int
		message, code, errorType string
	}{
		{0, "", "internal_server_error", "server_error"},
		{http.StatusBadRequest, "bad input", "invalid_request_error", "invalid_request_error"},
		{http.StatusTooManyRequests, "limited", "rate_limit_exceeded", "invalid_request_error"},
	} {
		got := BuildOpenAIResponsesStreamFailedChunk(tc.status, tc.message, 0)
		if gjson.GetBytes(got, "response.error.code").String() != tc.code || gjson.GetBytes(got, "response.error.type").String() != tc.errorType || gjson.GetBytes(got, "response.error.message").String() == "" {
			t.Fatal("plain failure lost its public fallback")
		}
	}
}

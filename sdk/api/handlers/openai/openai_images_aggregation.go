package openai

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

type webImageAggregationSlot struct {
	result               parallelWebImageResult
	err                  *interfaces.ErrorMessage
	attempt              int
	generated, delivered bool
}
type webImageAggregationFailure struct {
	Index int    `json:"image_index"`
	Code  string `json:"code"`
}
type webImageAggregationSummary struct {
	Requested     int                          `json:"requested"`
	Generated     int                          `json:"generated"`
	Delivered     int                          `json:"delivered"`
	RetryAttempts int                          `json:"retry_attempts"`
	Partial       bool                         `json:"partial"`
	Failed        []webImageAggregationFailure `json:"failures,omitempty"`
}

func webImageCanRefill(requested, succeeded int) bool {
	return succeeded >= 2 && succeeded*2 >= requested
}
func webImageBatchSummary(slots []webImageAggregationSlot, retries int) *webImageAggregationSummary {
	summary := &webImageAggregationSummary{Requested: len(slots), RetryAttempts: retries}
	for i, slot := range slots {
		if slot.generated {
			summary.Generated++
		} else {
			summary.Failed = append(summary.Failed, webImageAggregationFailure{Index: i, Code: webImageErrorCode(slot.err)})
		}
		if slot.delivered {
			summary.Delivered++
		}
	}
	summary.Partial = summary.Generated > 0 && summary.Generated < summary.Requested
	return summary
}

var webImageSafeCode = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,128}$`)

func webImageErrorCode(msg *interfaces.ErrorMessage) string {
	if msg == nil || msg.Error == nil {
		return "image_not_executed"
	}
	var coded interface{ ExecutionResultErrorCode() string }
	code := ""
	if errors.As(msg.Error, &coded) {
		code = coded.ExecutionResultErrorCode()
	}
	if code == "" {
		code = gjson.Get(handlers.OriginalErrorText(msg), "error.code").String()
	}
	if webImageSafeCode.MatchString(code) {
		return code
	}
	return "image_generation_failed"
}
func webImageBatchStopped(msg *interfaces.ErrorMessage) bool {
	if msg == nil {
		return false
	}
	return core.IsImageRequestTimeout(msg.Error) || webImageErrorCode(msg) == "chatgpt_web_image_task_canceled"
}

type webImageAggregationError struct{ summary *webImageAggregationSummary }

func (e *webImageAggregationError) Error() string {
	b, _ := json.Marshal(map[string]any{"error": map[string]any{"code": "image_aggregation_incomplete", "type": "server_error", "message": "The image request could not deliver all requested images.", "failure_stage": "aggregation", "aggregation": e.summary}})
	return string(b)
}
func (*webImageAggregationError) StatusCode() int { return http.StatusBadGateway }
func (*webImageAggregationError) ExecutionResultErrorCode() string {
	return "image_aggregation_incomplete"
}
func (*webImageAggregationError) ChatGPTWebFailureStage() string { return "aggregation" }
func (*webImageAggregationError) SkipAuthResult() bool           { return true }
func (*webImageAggregationError) RetryOtherAuth() bool           { return false }

// Successful writes are the server's delivery boundary, not proof of client storage.
type webImageDeliveryWriter struct {
	ctx    context.Context
	writer io.Writer
	err    error
}

func (w *webImageDeliveryWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	if err := w.ctx.Err(); err != nil {
		w.err = err
		return 0, err
	}
	n, err := w.writer.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	w.err = err
	return n, err
}

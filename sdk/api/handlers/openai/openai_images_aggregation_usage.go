package openai

import (
	"context"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	helps "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

// Retain only the last primary record per bounded slot, never image bytes.
type webImageBatchAccounting struct {
	mu             sync.Mutex
	last           []usage.Record
	seen           []bool
	started        []bool
	startedAt      time.Time
	batchID, model string
}

func newWebImageBatchAccounting(n int, id, model string) *webImageBatchAccounting {
	return &webImageBatchAccounting{last: make([]usage.Record, n), seen: make([]bool, n), started: make([]bool, n), startedAt: time.Now(), batchID: id, model: model}
}
func (a *webImageBatchAccounting) observe(r usage.Record) {
	if r.Auxiliary {
		return
	}
	i := r.ImageAggregation.ImageIndex
	a.mu.Lock()
	defer a.mu.Unlock()
	if i >= 0 && i < len(a.last) && (!a.seen[i] || r.ImageAggregation.AttemptIndex >= a.last[i].ImageAggregation.AttemptIndex) {
		a.last[i] = r
		a.seen[i] = true
	}
}
func (a *webImageBatchAccounting) start(i int) { a.mu.Lock(); a.started[i] = true; a.mu.Unlock() }
func (a *webImageBatchAccounting) publish(ctx context.Context, slots []webImageAggregationSlot, retries int) {
	summary := webImageBatchSummary(slots, retries)
	a.mu.Lock()
	last := append([]usage.Record(nil), a.last...)
	started := append([]bool(nil), a.started...)
	a.mu.Unlock()
	for i, slot := range slots {
		record := last[i]
		if record.Provider == "" {
			record = usage.Record{Provider: "chatgpt-web", Model: a.model, APIKey: helps.APIKeyFromContext(ctx), Stream: usage.StreamFromContext(ctx)}
		}
		record.RequestedAt = a.startedAt
		record.Latency = time.Since(a.startedAt)
		record.TTFT = 0
		record.FirstPacketLatency = 0
		originalCode, originalStage := record.ErrorCode, record.FailureStage
		if slot.err != nil {
			originalCode = webImageErrorCode(slot.err)
		}
		record.ImageAggregation = usage.ImageAggregation{Role: "delivery", BatchID: a.batchID, ImageIndex: i, AttemptIndex: slot.attempt, Requested: len(slots), Generated: summary.Generated, Delivered: summary.Delivered, RetryAttempts: retries, Executed: started[i], OriginalCode: originalCode, OriginalStage: originalStage}
		record.Detail = usage.Detail{}
		record.Auxiliary = false
		record.Failed = !slot.delivered
		record.RequestID = logging.GetRequestID(ctx)
		record.ErrorMessage = ""
		record.ErrorResponse = ""
		record.ErrorCode = ""
		record.ErrorType = ""
		record.FailureStage = ""
		record.StatusCode = 200
		record.UpstreamStatusCode = 0
		record.UpstreamRequestID = ""
		if record.Failed {
			record.StatusCode = 502
			record.FailureStage = "aggregation"
			record.ErrorType = "server_error"
			record.ErrorCode = "image_aggregation_incomplete"
			record.ErrorMessage = "The requested image was not delivered."
			if slot.generated {
				record.ErrorCode = "image_aggregation_discarded"
				record.ErrorMessage = "The image was generated but the batch could not be delivered."
			} else if !started[i] {
				record.ErrorCode = "image_aggregation_not_executed"
			}
			if ctx.Err() != nil {
				record.StatusCode = 499
			}
			if core.IsImageRequestTimeout(core.ImageRequestContextError(ctx, nil)) {
				record.ErrorCode = "image_request_timeout"
			}
		}
		usage.PublishRecord(ctx, record)
	}
}

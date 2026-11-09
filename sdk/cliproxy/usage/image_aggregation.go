package usage

import (
	"context"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

// ImageAggregation separates billed execution from final delivery. All fields
// are immutable scalars, so asynchronous queues and snapshots own value copies.
type ImageAggregation struct {
	Role          string `json:"role"`
	BatchID       string `json:"batch_id"`
	ImageIndex    int    `json:"image_index"`
	AttemptIndex  int    `json:"attempt_index"`
	Requested     int    `json:"requested"`
	Generated     int    `json:"generated"`
	Delivered     int    `json:"delivered"`
	RetryAttempts int    `json:"retry_attempts"`
	Executed      bool   `json:"executed"`
	OriginalCode  string `json:"original_code,omitempty"`
	OriginalStage string `json:"original_stage,omitempty"`
}
type imageAggregationObserverKey struct{}

func WithImageAggregationObserver(ctx context.Context, observe func(Record)) context.Context {
	return context.WithValue(ctx, imageAggregationObserverKey{}, observe)
}
func annotateImageAggregation(ctx context.Context, record Record) Record {
	if a, ok := core.ImageAggregationAttemptFromContext(ctx); ok {
		record.ImageAggregation = ImageAggregation{Role: "attempt", BatchID: a.BatchID, ImageIndex: a.ImageIndex, AttemptIndex: a.AttemptIndex, Requested: a.Requested, Executed: true}
		if observe, ok := ctx.Value(imageAggregationObserverKey{}).(func(Record)); ok {
			observe(record)
		}
	}
	return record
}

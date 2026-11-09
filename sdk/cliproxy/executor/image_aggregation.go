package executor

import "context"

// ImageAggregationAttempt identifies a bounded image slot and generation round.
// It contains no upstream identity or request material.
type ImageAggregationAttempt struct {
	BatchID      string `json:"batch_id"`
	ImageIndex   int    `json:"image_index"`
	AttemptIndex int    `json:"attempt_index"`
	Requested    int    `json:"requested"`
}
type imageAggregationAttemptKey struct{}

func WithImageAggregationAttempt(ctx context.Context, attempt ImageAggregationAttempt) context.Context {
	return context.WithValue(ctx, imageAggregationAttemptKey{}, attempt)
}
func ImageAggregationAttemptFromContext(ctx context.Context) (ImageAggregationAttempt, bool) {
	if ctx == nil {
		return ImageAggregationAttempt{}, false
	}
	v, ok := ctx.Value(imageAggregationAttemptKey{}).(ImageAggregationAttempt)
	return v, ok
}

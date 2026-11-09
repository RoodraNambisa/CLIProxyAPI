package openai

import (
	"context"
	"sync"
	"testing"

	helps "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	stats "github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

type imageBatchUsageCapture struct {
	mu   sync.Mutex
	rows []usage.Record
	done <-chan struct{}
}

func (p *imageBatchUsageCapture) HandleUsage(_ context.Context, r usage.Record) {
	select {
	case <-p.done:
		return
	default:
	}
	if r.ImageAggregation.Role == "" {
		return
	}
	p.mu.Lock()
	p.rows = append(p.rows, r)
	p.mu.Unlock()
}

type reportingRefillExecutor struct{ *refillExecutor }

func (e *reportingRefillExecutor) report(ctx context.Context, a *auth.Auth, r core.Request, o core.Options) ([]byte, error) {
	reporter := helps.NewUsageReporter(ctx, "chatgpt-web", r.Model, a)
	reporter.SetRequestUsageOutcome(o.UsageOutcome)
	body, err := e.run(ctx, r, o)
	if err != nil {
		reporter.PublishFailure(ctx, err)
	} else {
		reporter.Publish(ctx, usage.Detail{InputTokens: 7, OutputTokens: 3, TotalTokens: 10})
	}
	return body, err
}
func (e *reportingRefillExecutor) Execute(ctx context.Context, a *auth.Auth, r core.Request, o core.Options) (core.Response, error) {
	b, err := e.report(ctx, a, r, o)
	return core.Response{Payload: b}, err
}
func (e *reportingRefillExecutor) ExecuteStream(ctx context.Context, a *auth.Auth, r core.Request, o core.Options) (*core.StreamResult, error) {
	b, err := e.report(ctx, a, r, o)
	ch := make(chan core.StreamChunk, 1)
	if err != nil {
		ch <- core.StreamChunk{Err: err}
	} else {
		ch <- core.StreamChunk{Payload: []byte("data: {\"type\":\"response.completed\",\"response\":" + string(b) + "}\n\n")}
	}
	close(ch)
	return &core.StreamResult{Chunks: ch}, nil
}
func TestWebImageBatchCountsDeliveryWithoutLosingAttemptTokens(t *testing.T) {
	for _, tc := range []struct {
		name                                  string
		stream, partial                       bool
		failRounds, success, attempts, tokens int
	}{
		{"recovered", false, false, 0, 10, 11, 100},
		{"discarded", false, false, 3, 0, 11, 90},
		{"partial", false, true, 3, 9, 11, 90},
		{"stream", true, false, 3, 9, 11, 90},
	} {
		t.Run(tc.name, func(t *testing.T) {
			capture := &imageBatchUsageCapture{done: t.Context().Done()}
			usage.RegisterPlugin(capture)
			h, e := refillHandler(t, 9, tc.failRounds, 3)
			h.AuthManager.RegisterExecutor(&reportingRefillExecutor{e})
			h.Cfg.Images.ChatGPTWeb.NAggregationReturnPartial = tc.partial
			_, done := runParallelImages(h, t.Context(), tc.stream, 10)
			waitParallelDone(t, done)
			if err := usage.DefaultManager().Barrier(t.Context()); err != nil {
				t.Fatal(err)
			}
			capture.mu.Lock()
			rows := append([]usage.Record(nil), capture.rows...)
			capture.mu.Unlock()
			s := stats.NewRequestStatistics()
			attempts, deliveries, discarded := 0, 0, 0
			for _, r := range rows {
				s.Record(context.Background(), r)
				if r.ImageAggregation.Role == "attempt" {
					attempts++
				} else {
					deliveries++
					if r.ErrorCode == "image_aggregation_discarded" {
						discarded++
					}
				}
			}
			got := s.Snapshot()
			if attempts != tc.attempts || deliveries != 10 || got.TotalRequests != 10 || got.SuccessCount != int64(tc.success) || got.FailureCount != int64(10-tc.success) || got.TotalTokens != int64(tc.tokens) {
				t.Fatalf("attempts=%d delivery=%d requests=%d success=%d failed=%d tokens=%d rows=%+v", attempts, deliveries, got.TotalRequests, got.SuccessCount, got.FailureCount, got.TotalTokens, rows)
			}
			if tc.name == "discarded" && discarded != 9 {
				t.Fatalf("discarded=%d", discarded)
			}
		})
	}
}

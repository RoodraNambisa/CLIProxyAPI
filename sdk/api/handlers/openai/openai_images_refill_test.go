package openai

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

type refillExecutor struct {
	*imageCaptureExecutor
	mu                         sync.Mutex
	initialSuccess, failRounds int
	code                       string
	calls                      map[int]int
	inputs                     [][]byte
}

func (e *refillExecutor) run(ctx context.Context, req core.Request, opts core.Options) ([]byte, error) {
	a, _ := core.ImageAggregationAttemptFromContext(ctx)
	e.mu.Lock()
	e.calls[a.ImageIndex]++
	e.inputs = append(e.inputs, append([]byte(nil), req.Payload...))
	e.mu.Unlock()
	if release := core.RequestBodyReleaseControllerFromOptions(opts); release != nil {
		release.Release()
	}
	if a.ImageIndex >= e.initialSuccess && a.AttemptIndex <= e.failRounds {
		return nil, &refillTestError{code: e.code}
	}
	return []byte(fmt.Sprintf(`{"output":[{"type":"image_generation_call","result":"aGVsbG8=","revised_prompt":"slot-%d"}],"tool_usage":{"image_gen":{"input_tokens":7,"output_tokens":3,"total_tokens":10}}`, a.ImageIndex)), nil
}
func (e *refillExecutor) Execute(ctx context.Context, _ *auth.Auth, req core.Request, opts core.Options) (core.Response, error) {
	b, err := e.run(ctx, req, opts)
	return core.Response{Payload: b}, err
}
func (e *refillExecutor) ExecuteStream(ctx context.Context, _ *auth.Auth, req core.Request, opts core.Options) (*core.StreamResult, error) {
	b, err := e.run(ctx, req, opts)
	ch := make(chan core.StreamChunk, 1)
	if err != nil {
		ch <- core.StreamChunk{Err: err}
	} else {
		ch <- core.StreamChunk{Payload: []byte("data: {\"type\":\"response.completed\",\"response\":" + string(b) + "}\n\n")}
	}
	close(ch)
	return &core.StreamResult{Chunks: ch}, nil
}

type refillTestError struct{ code string }

func (e *refillTestError) Error() string {
	return fmt.Sprintf(`{"error":{"code":%q,"message":"synthetic generation failure"}}`, e.code)
}
func (*refillTestError) StatusCode() int                    { return 400 }
func (*refillTestError) SkipAuthResult() bool               { return true }
func (*refillTestError) RetryOtherAuth() bool               { return false }
func (e *refillTestError) ExecutionResultErrorCode() string { return e.code }

func refillHandler(t *testing.T, success, rounds, concurrency int) (*OpenAIImagesAPIHandler, *refillExecutor) {
	h, _ := parallelImagesHandler(t, concurrency)
	e := &refillExecutor{imageCaptureExecutor: &imageCaptureExecutor{provider: "chatgpt-web"}, initialSuccess: success, failRounds: rounds, code: "image_size_mismatch", calls: map[int]int{}}
	h.AuthManager.RegisterExecutor(e)
	return h, e
}
func TestWebImageRefillThresholdAndBounds(t *testing.T) {
	for _, tc := range []struct {
		n, success, retries, failRounds, attempts int
		ok                                        bool
	}{
		{10, 9, 1, 0, 11, true}, {10, 5, 1, 0, 15, true}, {10, 4, 1, 0, 10, false}, {10, 1, 1, 0, 10, false}, {10, 0, 1, 0, 10, false}, {2, 1, 1, 0, 2, false}, {3, 2, 1, 0, 4, true},
		{10, 9, 0, 0, 10, false}, {10, 9, 3, 3, 13, false}, {10, 9, 3, 1, 12, true},
	} {
		for _, concurrency := range []int{1, 3} {
			t.Run(fmt.Sprintf("%+v/concurrency=%d", tc, concurrency), func(t *testing.T) {
				h, e := refillHandler(t, tc.success, tc.failRounds, concurrency)
				h.Cfg.Images.ChatGPTWeb.NAggregationMaxRetries = new(tc.retries)
				w, done := runParallelImages(h, t.Context(), false, tc.n)
				waitParallelDone(t, done)
				total := 0
				for index, calls := range e.calls {
					total += calls
					if index < tc.success && calls != 1 {
						t.Fatal("regenerated a successful slot")
					}
				}
				if total != tc.attempts || (w.Code == 200) != tc.ok {
					t.Fatalf("calls=%d status=%d body=%s", total, w.Code, w.Body.String())
				}
				if tc.ok && gjson.Get(w.Body.String(), "data.#").Int() != int64(tc.n) {
					t.Fatal("missing images")
				}
				for _, input := range e.inputs {
					if gjson.GetBytes(input, "input.0.content.0.text").String() == "" {
						t.Fatal("input disappeared on replay")
					}
				}
			})
		}
	}
}
func TestWebImageRefillAllErrorsAndPartialDelivery(t *testing.T) {
	for _, code := range []string{"moderation_blocked", "chatgpt_web_image_no_output", "image_size_mismatch", "cloudflare_challenge", "unexpected_error"} {
		t.Run(code, func(t *testing.T) {
			h, e := refillHandler(t, 2, 0, 3)
			e.code = code
			w, done := runParallelImages(h, t.Context(), false, 3)
			waitParallelDone(t, done)
			if w.Code != 200 || e.calls[2] != 2 {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
		})
	}
	for _, stream := range []bool{false, true} {
		for _, partial := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%t/partial=%t", stream, partial), func(t *testing.T) {
				h, e := refillHandler(t, 2, 3, 3)
				h.Cfg.Images.ChatGPTWeb.NAggregationReturnPartial = partial
				w, done := runParallelImages(h, t.Context(), stream, 3)
				waitParallelDone(t, done)
				if e.calls[0] != 1 || e.calls[1] != 1 || e.calls[2] != 2 {
					t.Fatalf("bad calls: %v", e.calls)
				}
				if stream {
					if strings.Count(w.Body.String(), "event: image_generation.completed") != 2 || strings.Count(w.Body.String(), "event: error") != 1 {
						t.Fatal(w.Body.String())
					}
				} else if partial {
					if w.Code != 200 || gjson.Get(w.Body.String(), "data.#").Int() != 2 || gjson.Get(w.Body.String(), "aggregation.delivered").Int() != 2 {
						t.Fatal(w.Body.String())
					}
				} else if w.Code != 502 || gjson.Get(w.Body.String(), "error.code").String() != "image_aggregation_incomplete" || gjson.Get(w.Body.String(), "data").Exists() {
					t.Fatalf("%d %s", w.Code, w.Body.String())
				}
			})
		}
	}
}
func TestWebImageRefillStreamInputUsageWhenFirstSlotFails(t *testing.T) {
	h, e := refillHandler(t, 0, 3, 3)
	// A pinned alias order is irrelevant: slot zero fails and the remaining two succeed.
	e.initialSuccess = 2
	// Change the slot identity only in the fake executor to put the failed slot first.
	h.AuthManager.RegisterExecutor(&firstSlotFailureExecutor{refillExecutor: e})
	w, done := runParallelImages(h, t.Context(), true, 3)
	waitParallelDone(t, done)
	inputs := int64(0)
	for _, line := range strings.Split(w.Body.String(), "\n") {
		if strings.HasPrefix(line, "data: ") {
			inputs += gjson.Get(strings.TrimPrefix(line, "data: "), "usage.input_tokens").Int()
		}
	}
	if inputs != 7 {
		t.Fatalf("input usage=%d body=%s", inputs, w.Body.String())
	}
}

type firstSlotFailureExecutor struct{ *refillExecutor }

func (e *firstSlotFailureExecutor) ExecuteStream(ctx context.Context, a *auth.Auth, r core.Request, o core.Options) (*core.StreamResult, error) {
	v, _ := core.ImageAggregationAttemptFromContext(ctx)
	v.ImageIndex = (v.ImageIndex + 2) % 3
	return e.refillExecutor.ExecuteStream(core.WithImageAggregationAttempt(ctx, v), a, r, o)
}

type failImageWrite struct{ *httptest.ResponseRecorder }

func (w *failImageWrite) Write(p []byte) (int, error) { return 0, fmt.Errorf("downstream closed") }
func TestWebImageRefillBrokenWriterStops(t *testing.T) {
	h, _ := refillHandler(t, 3, 0, 3)
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)
	for _, stream := range []bool{false, true} {
		body, _ := json.Marshal(map[string]any{"model": "gpt-image-2", "prompt": "test", "n": 3, "stream": stream})
		req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(&failImageWrite{httptest.NewRecorder()}, req)
	}
}

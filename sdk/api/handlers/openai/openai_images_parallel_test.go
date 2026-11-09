package openai

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	helps "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

type parallelImageExecutor struct {
	*imageCaptureExecutor
	active, peak, calls atomic.Int32
	entered             chan string
	release             chan struct{}
	mu                  sync.Mutex
	seen                map[string]int
	snapshots           []core.ChatGPTWebImageConfigSnapshot
	fail                bool
	blob                string
	noise               int
}

func (e *parallelImageExecutor) enter(ctx context.Context, a *auth.Auth, opts core.Options) error {
	e.calls.Add(1)
	active := e.active.Add(1)
	for {
		p := e.peak.Load()
		if active <= p || e.peak.CompareAndSwap(p, active) {
			break
		}
	}
	e.mu.Lock()
	e.seen[a.ID]++
	s, _ := opts.Metadata[core.ChatGPTWebImageConfigSnapshotMetadataKey].(core.ChatGPTWebImageConfigSnapshot)
	e.snapshots = append(e.snapshots, s)
	e.mu.Unlock()
	select {
	case e.entered <- a.ID:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-e.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	if controller := core.RequestBodyReleaseControllerFromOptions(opts); controller != nil {
		controller.Release()
	}
	if e.fail {
		return genericImageProviderTestError{message: "upstream test failure", code: "test_failure", status: 400}
	}
	return nil
}
func (e *parallelImageExecutor) payload() []byte {
	b := e.blob
	if b == "" {
		b = base64.StdEncoding.EncodeToString([]byte("image"))
	}
	return []byte(fmt.Sprintf(`{"created_at":1700000000,"output":[{"type":"image_generation_call","result":%q}],"tool_usage":{"image_gen":{"input_tokens":7,"output_tokens":3,"total_tokens":10}}}`, b))
}
func (e *parallelImageExecutor) Execute(ctx context.Context, a *auth.Auth, _ core.Request, opts core.Options) (core.Response, error) {
	defer e.active.Add(-1)
	if err := e.enter(ctx, a, opts); err != nil {
		return core.Response{}, err
	}
	return core.Response{Payload: e.payload()}, nil
}
func (e *parallelImageExecutor) ExecuteStream(ctx context.Context, a *auth.Auth, _ core.Request, opts core.Options) (*core.StreamResult, error) {
	chunks := make(chan core.StreamChunk)
	go func() {
		defer close(chunks)
		defer e.active.Add(-1)
		if err := e.enter(ctx, a, opts); err != nil {
			select {
			case chunks <- core.StreamChunk{Err: err}:
			case <-ctx.Done():
			}
			return
		}
		for i := 0; i < e.noise; i++ {
			select {
			case chunks <- core.StreamChunk{Payload: []byte("data: {\"type\":\"response.created\"}\n\n")}:
			case <-ctx.Done():
				return
			}
		}
		for _, data := range [][]byte{[]byte("data: {\"type\":\"response.image_generation_call.partial_image\",\"partial_image_b64\":\"aW1hZ2U=\",\"partial_image_index\":0}\n\n"), append(append([]byte("data: {\"type\":\"response.completed\",\"response\":"), e.payload()...), []byte("}\n\n")...)} {
			select {
			case chunks <- core.StreamChunk{Payload: data}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return &core.StreamResult{Chunks: chunks}, nil
}
func parallelImagesHandler(t *testing.T, concurrency int) (*OpenAIImagesAPIHandler, *parallelImageExecutor) {
	t.Helper()
	base := &imageCaptureExecutor{provider: "chatgpt-web"}
	h := newImagesTestHandler(t, base)
	e := &parallelImageExecutor{imageCaptureExecutor: base, entered: make(chan string, 20), release: make(chan struct{}), seen: map[string]int{}}
	h.AuthManager.RegisterExecutor(e)
	h.AuthManager.SetSelector(&auth.FillFirstSelector{})
	h.Cfg.Images.EnableNAggregation = new(true)
	h.Cfg.Images.ChatGPTWeb.MaxN = new(10)
	h.Cfg.Images.ChatGPTWeb.NAggregationConcurrency = &concurrency
	for i := 0; i < 9; i++ {
		id := fmt.Sprintf("parallel-web-%d", i)
		_, err := h.AuthManager.Register(t.Context(), &auth.Auth{ID: id, Provider: "chatgpt-web", Status: auth.StatusActive, Metadata: map[string]any{"access_token": "test", "lifecycle_state": auth.LifecycleStateActive}})
		if err != nil {
			t.Fatal(err)
		}
		registry.GetGlobalRegistry().RegisterClient(id, "chatgpt-web", []*registry.ModelInfo{{ID: "gpt-image-2"}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
	}
	return h, e
}
func runParallelImages(h *OpenAIImagesAPIHandler, ctx context.Context, stream bool, n int) (*httptest.ResponseRecorder, <-chan struct{}) {
	r := gin.New()
	r.POST("/v1/images/generations", h.Generations)
	w := httptest.NewRecorder()
	q := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(fmt.Sprintf(`{"model":"gpt-image-2","prompt":"ball","n":%d,"stream":%t}`, n, stream))).WithContext(ctx)
	q.Header.Set("Content-Type", "application/json")
	done := make(chan struct{})
	go func() { defer close(done); r.ServeHTTP(w, q) }()
	return w, done
}
func waitParallelDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("parallel aggregation did not finish")
	}
}
func TestWebImageParallelAggregation(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("TMPDIR", dir)
			h, e := parallelImagesHandler(t, 3)
			w, done := runParallelImages(h, t.Context(), stream, 10)
			first := map[string]bool{}
			for i := 0; i < 3; i++ {
				select {
				case id := <-e.entered:
					first[id] = true
				case <-time.After(3 * time.Second):
					close(e.release)
					waitParallelDone(t, done)
					t.Fatal("n aggregation remained serial")
				}
			}
			if len(first) != 3 {
				t.Error("batch reused credentials before exhausting alternatives")
			}
			*h.Cfg.Images.ChatGPTWeb.NAggregationConcurrency = 10
			close(e.release)
			waitParallelDone(t, done)
			if w.Code != 200 {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if e.calls.Load() != 10 || e.peak.Load() != 3 || e.active.Load() != 0 {
				t.Fatalf("calls=%d peak=%d active=%d", e.calls.Load(), e.peak.Load(), e.active.Load())
			}
			if stream {
				if strings.Count(w.Body.String(), "event: image_generation.completed") != 10 || !strings.Contains(w.Body.String(), `"image_index":9`) {
					t.Fatalf("bad stream %s", w.Body.String())
				}
			} else {
				if gjson.Get(w.Body.String(), "data.#").Int() != 10 || gjson.Get(w.Body.String(), "usage.input_tokens").Int() != 7 || gjson.Get(w.Body.String(), "usage.output_tokens").Int() != 30 {
					t.Fatalf("bad aggregate %s", w.Body.String())
				}
			}
			for _, snapshot := range e.snapshots {
				if snapshot.NAggregationConcurrency != 3 {
					t.Fatal("in-flight snapshot changed")
				}
			}
			if traffic := h.AuthManager.TrafficController().Overview(time.Now(), 60, false, false); traffic.Requests != 1 || traffic.Active != 0 {
				t.Fatalf("logical admission leaked or counted per image: %+v", traffic)
			}
			files, _ := filepath.Glob(filepath.Join(dir, "cpa-web-image-aggregate-*"))
			if len(files) != 0 {
				t.Fatal("spools leaked")
			}
		})
	}
}
func TestWebImageParallelCancellationAndFailure(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, failure := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%t/failure=%t", stream, failure), func(t *testing.T) {
				dir := t.TempDir()
				t.Setenv("TMPDIR", dir)
				h, e := parallelImagesHandler(t, 3)
				e.fail = failure
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				w, done := runParallelImages(h, ctx, stream, 10)
				for i := 0; i < 3; i++ {
					select {
					case <-e.entered:
					case <-time.After(3 * time.Second):
						cancel()
						t.Fatal("workers not started")
					}
				}
				if failure {
					close(e.release)
				} else {
					cancel()
				}
				waitParallelDone(t, done)
				// The auth stream wrapper may close its output before the canceled
				// executor goroutine has returned; require bounded cleanup.
				deadline := time.Now().Add(time.Second)
				for e.active.Load() != 0 && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
				if e.calls.Load() > 3 || e.active.Load() != 0 {
					t.Fatalf("continued or leaked: calls=%d active=%d", e.calls.Load(), e.active.Load())
				}
				if failure && ((!stream && w.Code == 200) || (stream && strings.Count(w.Body.String(), "event: error") != 1)) {
					t.Fatalf("error response %d %s", w.Code, w.Body.String())
				}
				files, _ := os.ReadDir(dir)
				if len(files) != 0 {
					t.Fatal("temporary results survived failure")
				}
			})
		}
	}
}
func TestWebImageParallelResponseBudget(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			h, e := parallelImagesHandler(t, 2)
			h.Cfg.Images.ChatGPTWeb.MaxImageResponseMegabytes = new(1)
			e.blob = base64.StdEncoding.EncodeToString(make([]byte, 700<<10))
			close(e.release)
			w, done := runParallelImages(h, t.Context(), stream, 2)
			waitParallelDone(t, done)
			if (!stream && w.Code == 200) || (stream && !strings.Contains(w.Body.String(), "event: error")) {
				t.Fatalf("aggregate budget bypassed: %d", w.Code)
			}
		})
	}
}
func TestWebImageChildMemoryRejectsImpossibleInputDependency(t *testing.T) {
	original := helps.ChatGPTWebImageMemorySnapshot().CapacityBytes
	helps.ConfigureChatGPTWebImageMemoryCapacity(100)
	defer helps.ConfigureChatGPTWebImageMemoryCapacity(original)
	child := helps.NewChatGPTWebImageChildMemoryLeaseSet(80)
	defer child.Release()
	release, err := child.BeginFinalization(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if err := child.AcquireExact(t.Context(), 30); err != helps.ErrChatGPTWebImageMemoryWorkingSetTooLarge {
		t.Fatalf("got %v", err)
	}
}

func TestWebImageParallelPinnedCredentialRespectsConcurrency(t *testing.T) {
	h, e := parallelImagesHandler(t, 3)
	h.AuthManager.SetConfig(&config.Config{Routing: config.RoutingConfig{PerAuthConcurrency: 1, Queue: config.AdmissionQueue{Enabled: true}}})
	ctx := handlers.WithPinnedAuthID(t.Context(), "codex-auth")
	w, done := runParallelImages(h, ctx, false, 3)
	select {
	case <-e.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("pinned credential not selected")
	}
	select {
	case <-e.entered:
		close(e.release)
		waitParallelDone(t, done)
		t.Fatal("credential concurrency limit bypassed")
	case <-time.After(30 * time.Millisecond):
	}
	close(e.release)
	waitParallelDone(t, done)
	if w.Code != 200 || e.peak.Load() != 1 || len(e.seen) != 1 || e.seen["codex-auth"] != 3 {
		t.Fatalf("pinned credential: status=%d peak=%d seen=%v body=%s", w.Code, e.peak.Load(), e.seen, w.Body.String())
	}
}
func TestWebImageParallelSharedImageTimeout(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			h, e := parallelImagesHandler(t, 3)
			h.Cfg.Images.ChatGPTWeb.RequestTimeoutSeconds = 1
			w, done := runParallelImages(h, t.Context(), stream, 10)
			waitParallelDone(t, done)
			if e.active.Load() != 0 || e.calls.Load() != 3 {
				t.Fatalf("budget launched more work or leaked: %d %d", e.calls.Load(), e.active.Load())
			}
			if !strings.Contains(w.Body.String(), "image_request_timeout") {
				t.Fatalf("budget not preserved: %d %s", w.Code, w.Body.String())
			}
		})
	}
}
func TestWebImageParallelRouteIsolation(t *testing.T) {
	h, _ := parallelImagesHandler(t, 3)
	s := h.chatGPTWebImageConfigSnapshot()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	if h.webImageAggregationConcurrency(c, []string{"chatgpt-web"}, 10, s) != 3 {
		t.Fatal("Web route not parallel")
	}
	h.AuthManager.RegisterExecutor(&imageCaptureExecutor{provider: "codex"})
	if h.webImageAggregationConcurrency(c, []string{"codex", "chatgpt-web"}, 10, s) != 3 {
		t.Fatal("empty Codex executor incorrectly made the route mixed")
	}
	if _, err := h.AuthManager.Register(t.Context(), &auth.Auth{ID: "mixed-route-codex", Provider: "codex", Status: auth.StatusActive}); err != nil {
		t.Fatal(err)
	}
	if h.webImageAggregationConcurrency(c, []string{"codex", "chatgpt-web"}, 10, s) != 1 {
		t.Fatal("mixed route changed")
	}
	s.NAggregationConcurrency = 1
	if h.webImageAggregationConcurrency(c, []string{"chatgpt-web"}, 10, s) != 1 {
		t.Fatal("serial setting ignored")
	}
}

type parallelFlushRecorder struct {
	*httptest.ResponseRecorder
	flushes int
}

func (w *parallelFlushRecorder) Flush() { w.flushes++; w.ResponseRecorder.Flush() }
func TestWebImageParallelDoesNotFlushEmptyProtocolEvents(t *testing.T) {
	h, e := parallelImagesHandler(t, 3)
	e.noise = 100
	close(e.release)
	r := gin.New()
	r.POST("/v1/images/generations", h.Generations)
	w := &parallelFlushRecorder{ResponseRecorder: httptest.NewRecorder()}
	q := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"ball","n":3,"stream":true}`))
	q.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, q)
	if strings.Count(w.Body.String(), "event: image_generation.completed") != 3 || w.flushes > 6 {
		t.Fatalf("empty metadata flushed: count=%d body=%s", w.flushes, w.Body.String())
	}
}

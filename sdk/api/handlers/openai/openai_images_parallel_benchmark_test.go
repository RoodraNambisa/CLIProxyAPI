package openai

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	helps "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type imageAggregationBenchmarkExecutor struct {
	*imageCaptureExecutor
	response      []byte
	failLastFirst bool
	active, peak  atomic.Int32
}

func (e *imageAggregationBenchmarkExecutor) Execute(ctx context.Context, _ *auth.Auth, _ core.Request, opts core.Options) (core.Response, error) {
	active := e.active.Add(1)
	defer e.active.Add(-1)
	for {
		p := e.peak.Load()
		if active <= p || e.peak.CompareAndSwap(p, active) {
			break
		}
	}
	timer := time.NewTimer(20 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return core.Response{}, ctx.Err()
	}
	if e.failLastFirst {
		if a, ok := core.ImageAggregationAttemptFromContext(ctx); ok && a.ImageIndex == 9 && a.AttemptIndex == 0 {
			if release := core.RequestBodyReleaseControllerFromOptions(opts); release != nil {
				release.Release()
			}
			return core.Response{}, &refillTestError{code: "image_size_mismatch"}
		}
	}
	return core.Response{Payload: e.response}, nil
}

// BenchmarkWebImageNAggregation compares the same 10 x 1 MiB fixture and
// simulated 20 ms upstream work. It is not an official-network speed claim.
func BenchmarkWebImageNAggregation(b *testing.B) {
	gin.SetMode(gin.ReleaseMode)
	for _, scenario := range []struct {
		concurrency int
		refill      bool
	}{{1, false}, {3, false}, {3, true}} {
		concurrency := scenario.concurrency
		b.Run(fmt.Sprintf("concurrency_%d/refill_%t", concurrency, scenario.refill), func(b *testing.B) {
			e := &imageAggregationBenchmarkExecutor{failLastFirst: scenario.refill, imageCaptureExecutor: &imageCaptureExecutor{provider: "chatgpt-web"}, response: []byte(`{"created_at":1,"output":[{"type":"image_generation_call","result":"` + base64.StdEncoding.EncodeToString(make([]byte, 1<<20)) + `"}]}`)}
			m := auth.NewManager(nil, nil, nil)
			m.RegisterExecutor(e)
			for i := 0; i < 3; i++ {
				id := fmt.Sprintf("benchmark-image-%d", i)
				_, err := m.Register(b.Context(), &auth.Auth{ID: id, Provider: "chatgpt-web", Status: auth.StatusActive, Metadata: map[string]any{"access_token": "test", "lifecycle_state": auth.LifecycleStateActive}})
				if err != nil {
					b.Fatal(err)
				}
				registry.GetGlobalRegistry().RegisterClient(id, "chatgpt-web", []*registry.ModelInfo{{ID: "gpt-image-2.6"}})
				b.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
			}
			cfg := &config.SDKConfig{}
			cfg.Images.EnableNAggregation = new(true)
			cfg.Images.ChatGPTWeb.MaxN = new(10)
			cfg.Images.ChatGPTWeb.NAggregationConcurrency = &concurrency
			h := NewOpenAIImagesAPIHandler(handlers.NewBaseAPIHandlers(cfg, m))
			router := gin.New()
			router.POST("/v1/images/generations", h.Generations)
			b.ReportAllocs()
			b.SetBytes(10 << 20)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2.6","prompt":"ball","n":10}`))
				r.Header.Set("Content-Type", "application/json")
				w := httptest.NewRecorder()
				router.ServeHTTP(w, r)
				if w.Code != 200 {
					b.Fatalf("status %d", w.Code)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(e.peak.Load()), "peak_attempts")
			spools := helps.ChatGPTWebImageSpoolSnapshot()
			b.ReportMetric(float64(spools.PeakBytes), "peak_spool_B")
			if spools.CurrentFiles != 0 || spools.CurrentBytes != 0 {
				b.Fatal("spool leak")
			}
		})
	}
}

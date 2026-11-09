package openai

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
)

func TestWebImageMaxNEnablesAggregationWithoutLegacySwitch(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, concurrency := range []int{1, 3} {
			t.Run(fmt.Sprintf("stream=%t/concurrency=%d", stream, concurrency), func(t *testing.T) {
				h, e := parallelImagesHandler(t, concurrency)
				h.Cfg.Images.EnableNAggregation = new(false)
				// Production services can register an executor before any Codex credential exists.
				codex := &imageCaptureExecutor{provider: "codex"}
				h.AuthManager.RegisterExecutor(codex)
				w, done := runParallelImages(h, t.Context(), stream, 3)
				for i := 0; i < concurrency; i++ {
					select {
					case <-e.entered:
					case <-time.After(3 * time.Second):
						close(e.release)
						waitParallelDone(t, done)
						t.Fatal("configured concurrency was not reached")
					}
				}
				close(e.release)
				waitParallelDone(t, done)
				if e.peak.Load() != int32(concurrency) {
					t.Fatalf("peak=%d, want %d", e.peak.Load(), concurrency)
				}
				if w.Code != 200 || e.calls.Load() != 3 || codex.calls+codex.streamCalls != 0 {
					t.Fatalf("Web still depends on legacy switch: %d %s", w.Code, w.Body.String())
				}
				if stream {
					if strings.Count(w.Body.String(), "event: image_generation.completed") != 3 {
						t.Fatal("missing stream images")
					}
				} else if gjson.Get(w.Body.String(), "data.#").Int() != 3 {
					t.Fatal("missing images")
				}
			})
		}
	}
}

func TestWebMaxNRejectsBeforeGenerating(t *testing.T) {
	h, e := parallelImagesHandler(t, 3)
	h.Cfg.Images.EnableNAggregation = new(false)
	h.Cfg.Images.ChatGPTWeb.MaxN = new(1)
	close(e.release)
	w, done := runParallelImages(h, t.Context(), false, 2)
	waitParallelDone(t, done)
	if w.Code != 400 || e.calls.Load() != 0 || !strings.Contains(w.Body.String(), "limit of 1") {
		t.Fatalf("max-n not enforced before execution: %d %s", w.Code, w.Body.String())
	}
}

func TestLegacyAggregationSwitchStillControlsCodexOnlyKey(t *testing.T) {
	codex := &imageCaptureExecutor{provider: "codex"}
	web := &imageCaptureExecutor{provider: "chatgpt-web"}
	h := newMixedImagesTestHandler(t, codex, web)
	h.Cfg.Images.EnableNAggregation = new(false)
	h.Cfg.Images.ChatGPTWeb.MaxN = new(3)
	r := gin.New()
	r.POST("/v1/images/generations", func(c *gin.Context) {
		c.Set("accessMetadata", map[string]string{sdkaccess.MetadataAllowedProviders: "codex"})
		h.Generations(c)
	})
	q := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"ball","n":2}`))
	q.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, q)
	if w.Code != 400 || codex.calls != 0 || web.calls != 0 {
		t.Fatalf("Codex legacy switch bypassed: %d %s", w.Code, w.Body.String())
	}
}

func TestWebAggregationSurvivesNativeCodexConfiguration(t *testing.T) {
	web := &imageCaptureExecutor{provider: "chatgpt-web"}
	h := newImagesTestHandler(t, web, "web-only")
	h.Cfg.Images.ChatGPTWeb.ImageModels = []string{"web-only"}
	h.Cfg.Images.ChatGPTWeb.MaxN = new(2)
	h.Cfg.Images.EnableNAggregation = new(false)
	h.Cfg.Images.Native.Generations = sdkconfig.NativeImageEndpointConfig{Enabled: true, Models: []string{"codex-only"}}
	w := serveImageModelRequest(h, "generations", `{"model":"web-only","prompt":"ball","n":2}`)
	if w.Code != 200 || web.calls != 2 {
		t.Fatalf("native configuration blocked Web: %d %s", w.Code, w.Body.String())
	}
}

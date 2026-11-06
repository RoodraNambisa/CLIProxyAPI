package openai

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

type usagePolicyCaptureExecutor struct {
	*imageCaptureExecutor
	profile  string
	profiles map[string]string
}

func (e *usagePolicyCaptureExecutor) ImageUsagePolicySnapshot() (string, map[string]string, string) {
	clone := make(map[string]string, len(e.profiles))
	for k, v := range e.profiles {
		clone[k] = v
	}
	return e.profile, clone, "medium"
}

func TestWebImageUsageProfileFrozenAcrossAggregation(t *testing.T) {
	base := &imageCaptureExecutor{provider: "chatgpt-web"}
	h := newImagesTestHandler(t, base, "gpt-image-2.5")
	source := &usagePolicyCaptureExecutor{base, "by-model", map[string]string{"gpt-image-2.5": "gpt-image-2.5"}}
	h.AuthManager.RegisterExecutor(source)
	enabled := true
	h.Cfg.Images.EnableNAggregation = &enabled
	maxN := 2
	h.Cfg.Images.ChatGPTWeb.MaxN = &maxN
	base.beforeExecute = func() { source.profile = "gpt-image-2.6"; source.profiles["gpt-image-2.5"] = "gpt-image-2" }
	r := gin.New()
	r.POST("/v1/images/generations", h.Generations)
	w := httptest.NewRecorder()
	q := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2.5","prompt":"a ball","n":2}`))
	q.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, q)
	if w.Code != 200 || len(base.imageConfigSnapshots) != 2 {
		t.Fatalf("status=%d snapshots=%d body=%s", w.Code, len(base.imageConfigSnapshots), w.Body.String())
	}
	for _, s := range base.imageConfigSnapshots {
		if s.UsageEstimationProfile != "by-model" || s.UsageModelProfiles["gpt-image-2.5"] != "gpt-image-2.5" {
			t.Fatal("aggregation policy changed")
		}
	}
	if h.chatGPTWebImageConfigSnapshot().UsageEstimationProfile != "gpt-image-2.6" {
		t.Fatal("new request missed policy")
	}
}

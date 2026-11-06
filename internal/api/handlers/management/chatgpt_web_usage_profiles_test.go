package management

import (
	"net/http"
	"reflect"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestChatGPTWebUsageProfilesManagement(t *testing.T) {
	h := &Handler{cfg: &config.Config{}, configFilePath: writeTestConfigFile(t)}
	patch := func(body string, want int) {
		t.Helper()
		ctx, w := newChatGPTWebUsageCacheRequest(http.MethodPatch, body)
		h.PatchChatGPTWebUsageCache(ctx)
		if w.Code != want {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
	}
	patch(`{"image-usage":{"estimation-profile":"by-model","model-profiles":{" GPT-IMAGE-2.5 ":"gpt-image-2.6"}}}`, 200)
	ctx, w := newChatGPTWebUsageCacheRequest(http.MethodGet, "")
	h.GetChatGPTWebUsageCache(ctx)
	s := decodeChatGPTWebUsageCacheResponse(t, w)
	if s.ImageUsage.EstimationProfile != "by-model" || s.ImageUsage.EffectiveModelProfiles["gpt-image-2.5"] != "gpt-image-2.6" {
		t.Fatalf("bad response %+v", s.ImageUsage)
	}
	patch(`{"image-usage":{"estimation-profile":"gpt-image-2"}}`, 200)
	for _, bad := range []string{`{"estimation-profile":false}`, `{"estimation-profile":null}`, `{"estimation-profile":"bad"}`, `{"model-profiles":null}`, `{"model-profiles":[]}`, `{"model-profiles":{"missing":"gpt-image-2"}}`, `{"model-profiles":{"gpt-image-2":"by-model"}}`, `{"model-profiles":{"gpt-image-2":"gpt-image-2"," GPT-IMAGE-2 ":"gpt-image-2.6"}}`} {
		before := h.cfg.ChatGPTWeb.ImageUsage
		patch(`{"image-usage":`+bad+`}`, 400)
		if !reflect.DeepEqual(before, h.cfg.ChatGPTWeb.ImageUsage) {
			t.Fatal("invalid request mutated state")
		}
	}
	body := `{"estimate-token-usage":true,"usage-cache":{"enabled":false,"disk-threshold-mb":1,"max-disk-size-mb":1024,"path":""},"image-usage":{"auto-output-quality":"high"}}`
	ctx, w = newChatGPTWebUsageCacheRequest(http.MethodPut, body)
	h.PutChatGPTWebUsageCache(ctx)
	if w.Code != 200 {
		t.Fatalf("old PUT: %d %s", w.Code, w.Body.String())
	}
	if h.cfg.ChatGPTWeb.ImageUsage.EstimationProfile != "gpt-image-2" || h.cfg.ChatGPTWeb.ImageUsage.ModelProfiles["gpt-image-2.5"] != "gpt-image-2.6" {
		t.Fatal("old client cleared profile")
	}
	patch(`{"image-usage":{"model-profiles":{}}}`, 200)
	loaded, err := config.LoadConfig(h.configFilePath)
	if err != nil || len(loaded.ChatGPTWeb.ImageUsage.ModelProfiles) != 0 {
		t.Fatalf("clear failed: %v", err)
	}
	before := h.cfg.ChatGPTWeb.ImageUsage
	h.configFilePath = t.TempDir()
	patch(`{"image-usage":{"estimation-profile":"gpt-image-2.5","model-profiles":{"gpt-image-2":"gpt-image-2.6"}}}`, 500)
	if !reflect.DeepEqual(before, h.cfg.ChatGPTWeb.ImageUsage) {
		t.Fatal("failed persistence retained mutation")
	}
}

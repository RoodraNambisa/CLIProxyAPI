package executor

import (
	"context"
	"github.com/tidwall/gjson"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestChatGPTWebUsageProfileAliasSelectionAndSnapshot(t *testing.T) {
	cfg := &config.Config{}
	cfg.Images.ChatGPTWeb.ImageModels = []string{"gpt-image-2", "gpt-image-2.5", "gpt-image-2.6", "custom"}
	cfg.ChatGPTWeb.ImageUsage.ModelProfiles = map[string]string{"custom": "gpt-image-2.5"}
	e := NewChatGPTWebExecutor(cfg, nil)
	defer func() { _ = e.Close() }()
	opts := core.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex}
	for _, tc := range []struct{ model, tool, want string }{
		{"gpt-image-2", "gpt-image-2.6", "gpt-image-2"},
		{"gpt-image-2.5", "gpt-image-2.6", "gpt-image-2.5"},
		{"gpt-image-2.6", "gpt-image-2", "gpt-image-2.6"},
		{"custom", "gpt-image-2.6", "gpt-image-2.5"},
		{"gpt-5.4", "gpt-image-2.6", "gpt-image-2.6"},
		{"gpt-5.4", "", "gpt-image-2"},
	} {
		body := `{"model":"` + tc.model + `","input":"a ball","tools":[{"type":"image_generation"`
		if tc.tool != "" {
			body += `,"model":"` + tc.tool + `"`
		}
		body += `}],"tool_choice":{"type":"image_generation"}}`
		req := core.Request{Model: tc.model, Payload: []byte(body)}
		prepared, err := e.prepareRuntimeRequestTemplate(context.Background(), req, opts, false)
		if err != nil {
			t.Fatal(err)
		}
		if prepared.imageUsageProfile != tc.want {
			t.Fatalf("%s / %s: %s want %s", tc.model, tc.tool, prepared.imageUsageProfile, tc.want)
		}
		pinned := prepared.imageConfigSnapshot.Clone()
		cfg.ChatGPTWeb.ImageUsage.EstimationProfile = "gpt-image-2.6"
		cfg.ChatGPTWeb.ImageUsage.ModelProfiles["custom"] = "gpt-image-2"
		cfg.ChatGPTWeb.ImageUsage.AutoOutputQuality = "high"
		e.UpdateConfig(cfg)
		retryOpts := opts
		retryOpts.Metadata = map[string]any{core.ChatGPTWebImageConfigSnapshotMetadataKey: pinned}
		retry, err := e.prepareRuntimeRequestTemplate(context.Background(), req, retryOpts, false)
		if err != nil || retry.imageUsageProfile != tc.want || retry.usageProjectionOpts.AutoOutputQuality != "medium" {
			t.Fatalf("snapshot drift: %v", err)
		}
		next, err := e.prepareRuntimeRequestTemplate(context.Background(), req, opts, false)
		if err != nil || next.imageUsageProfile != "gpt-image-2.6" {
			t.Fatalf("new request missed update: %v", err)
		}
		cfg.ChatGPTWeb.ImageUsage.EstimationProfile = "by-model"
		cfg.ChatGPTWeb.ImageUsage.ModelProfiles["custom"] = "gpt-image-2.5"
		cfg.ChatGPTWeb.ImageUsage.AutoOutputQuality = "medium"
		e.UpdateConfig(cfg)
	}
}

func TestChatGPTWebUsageProfileReachesImageResponse(t *testing.T) {
	for _, tc := range []struct {
		mode     string
		override string
		want     int64
	}{
		{"by-model", "", 439}, {"by-model", "gpt-image-2", 1756},
		{"gpt-image-2.5", "gpt-image-2.6", 439}, {"gpt-image-2.6", "gpt-image-2", 1449},
	} {
		fixture := newChatGPTWebImageFixture(t)
		cfg := &config.Config{}
		cfg.ChatGPTWeb.ImageUsage.EstimationProfile = tc.mode
		if tc.override != "" {
			cfg.ChatGPTWeb.ImageUsage.ModelProfiles = map[string]string{"gpt-image-2.5": tc.override}
		}
		cfg.Images.ChatGPTWeb.AdaptSizeToAspectRatio = true
		cfg.Images.ChatGPTWeb.ResizeToRequestedSize = true
		e := NewChatGPTWebExecutor(cfg, nil)
		e.runtimeBaseURL = fixture.URL
		disableChatGPTWebImagePollWaits(e)
		req := core.Request{Model: "gpt-image-2.5", Payload: []byte(`{"model":"gpt-image-2.5","input":"a ball","tools":[{"type":"image_generation","size":"1024x1024"}]}`)}
		result, err := e.Execute(t.Context(), chatGPTWebRuntimeAuth(), req, core.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex})
		_ = e.Close()
		fixture.Close()
		if err != nil {
			t.Fatal(err)
		}
		if got := gjson.GetBytes(result.Payload, "response.tool_usage.image_gen.output_tokens").Int(); got != tc.want {
			t.Fatalf("mode=%s override=%s got=%d want=%d", tc.mode, tc.override, got, tc.want)
		}
		if gjson.GetBytes(result.Payload, "response.model").String() != "gpt-image-2.5" {
			t.Fatal("billing profile rewrote response alias")
		}
	}
}

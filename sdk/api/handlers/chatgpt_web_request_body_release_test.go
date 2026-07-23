package handlers

import (
	"context"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestChatGPTWebForcedBodyReleaseIgnoresGlobalTimerAndSize(t *testing.T) {
	t.Parallel()
	handler := &BaseAPIHandler{Cfg: &config.SDKConfig{RequestBodyRelease: config.RequestBodyReleaseConfig{
		Enable:       true,
		AfterSeconds: 1,
		MinBodyBytes: 1 << 20,
	}}}
	metadata := make(map[string]any)
	ctx, controller := handler.attachRequestBodyRelease(context.Background(), []byte(`{"model":"gpt-5"}`), metadata, true)
	if controller == nil {
		t.Fatal("forced ChatGPT Web controller = nil")
	}
	if coreexecutor.RequestBodyReleaseControllerFromContext(ctx) != controller ||
		coreexecutor.RequestBodyReleaseControllerFromMetadata(metadata) != controller {
		t.Fatal("forced controller was not propagated")
	}
	time.Sleep(1100 * time.Millisecond)
	if controller.Released() {
		t.Fatal("global timer released ChatGPT Web body before the executor commit point")
	}
	if !controller.Release() || controller.Replayable() {
		t.Fatal("explicit Web release did not make the request non-replayable")
	}
}

func TestConfiguredBodyReleaseStillUsesTimerOutsideChatGPTWeb(t *testing.T) {
	t.Parallel()
	handler := &BaseAPIHandler{Cfg: &config.SDKConfig{RequestBodyRelease: config.RequestBodyReleaseConfig{
		Enable:       true,
		AfterSeconds: 1,
	}}}
	_, controller := handler.attachRequestBodyRelease(context.Background(), []byte(`{"model":"gpt-5"}`), nil, false)
	if controller == nil {
		t.Fatal("configured controller = nil")
	}
	deadline := time.Now().Add(1500 * time.Millisecond)
	for !controller.Released() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !controller.Released() {
		t.Fatal("non-Web global timer did not release the request body after cancellation")
	}
}

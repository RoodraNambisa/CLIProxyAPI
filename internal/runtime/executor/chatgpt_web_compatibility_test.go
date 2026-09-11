package executor

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelcompat"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestChatGPTWebCompatibilityPinsPreflightAcrossModelRebuild(t *testing.T) {
	cfg := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{Sentinel: config.ChatGPTWebSentinelConfig{GoVMCompatibility: sentinelcompat.Config{Enabled: true}}}}
	e := NewChatGPTWebExecutor(cfg, nil)
	t.Cleanup(func() { _ = e.Close() })
	opts := core.Options{SourceFormat: translator.FormatCodex, ResponseFormat: translator.FormatCodex}
	req := core.Request{Model: "gpt-5", Payload: []byte(`{"model":"gpt-5","input":"hello"}`)}
	prepared, err := e.PrepareProviderRequest(t.Context(), req, opts, core.RequestOperationExecute)
	if err != nil {
		t.Fatal(err)
	}
	first := prepared.(*chatGPTWebPreparedRequest).sentinelPolicy
	if first == nil {
		t.Fatal("missing compiled policy")
	}
	opts = core.WithProviderPreparedRequest(opts, e.Identifier(), prepared)
	cfg.ChatGPTWeb.Sentinel.GoVMCompatibility.Enabled = false
	e.UpdateConfig(cfg)
	req.Model = "gpt-5-5"
	second, err := e.prepareRuntimeRequest(t.Context(), nil, req, opts, false)
	if err != nil {
		t.Fatal(err)
	}
	defer second.discardUsageProjection()
	if second.sentinelPolicy != first {
		t.Fatal("retry/model rebuild used new policy")
	}
	next, err := e.prepareRuntimeRequestTemplate(t.Context(), req, core.Options{SourceFormat: translator.FormatCodex}, false)
	if err != nil {
		t.Fatal(err)
	}
	if next.sentinelPolicy != nil {
		t.Fatal("new request did not see disabled policy")
	}
	cfg.ChatGPTWeb.Sentinel.GoVMCompatibility = sentinelcompat.Config{Enabled: true, WritableWindowProperties: []string{"fetch"}}
	e.UpdateConfig(cfg)
	if e.configSnapshot().ChatGPTWeb.Sentinel.GoVMPolicy != nil {
		t.Fatal("invalid update was published")
	}
}

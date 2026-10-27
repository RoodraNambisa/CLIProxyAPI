package executor

import (
	"fmt"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestCodexServiceTierAtActualTransport(t *testing.T) {
	for _, source := range []translator.Format{translator.FormatOpenAIResponse, translator.FormatOpenAI, translator.FormatCodex, translator.FormatClaude} {
		for _, transport := range []string{"http", "stream", "websocket", "websocket-stream", "websocket-http-fallback", "compact"} {
			if transport == "compact" && (source == translator.FormatOpenAI || source == translator.FormatClaude) {
				continue
			}
			for _, allow := range []bool{false, true} {
				for _, tier := range []string{"", "priority", "flex", "default"} {
					if source == translator.FormatClaude && tier != "priority" {
						continue
					}
					t.Run(fmt.Sprintf("%s/%s/allow=%t/tier=%s", source, transport, allow, tier), func(t *testing.T) {
						payload := []byte(`{"input":[{"role":"user","content":"hello"}]}`)
						if source == translator.FormatOpenAI || source == translator.FormatClaude {
							payload = []byte(`{"messages":[{"role":"user","content":"hello"}]}`)
						}
						if source == translator.FormatClaude {
							payload, _ = sjson.SetBytes(payload, "speed", "fast")
						} else if tier != "" {
							payload, _ = sjson.SetBytes(payload, "service_tier", tier)
						}
						cfg := &config.Config{Codex: config.CodexConfig{AllowClientServiceTier: allow}}
						body := testCodexRequestControlsWire(t, source, transport, payload, false, cfg)
						want := ""
						if allow {
							want = tier
						}
						if got := gjson.GetBytes(body, "service_tier"); got.String() != want || got.Exists() != (want != "") {
							t.Fatalf("wire service_tier=%s, want %q", got.Raw, want)
						}
					})
				}
			}
		}
	}
}

func TestCodexRequestControlsPreservedAtActualTransport(t *testing.T) {
	for _, source := range []translator.Format{translator.FormatOpenAIResponse, translator.FormatOpenAI, translator.FormatCodex} {
		for _, transport := range []string{"http", "stream", "websocket", "websocket-stream", "websocket-http-fallback", "compact"} {
			if transport == "compact" && source == translator.FormatOpenAI {
				continue
			}
			t.Run(fmt.Sprintf("%s/%s", source, transport), func(t *testing.T) {
				payload := []byte(`{"input":[{"role":"user","content":[{"type":"input_image","image_url":"https://example.invalid/image.png","detail":"original"}]}],"text":{"verbosity":"low"},"include":["output.encrypted_content","reasoning.encrypted_content","web_search_call.action.sources"],"stream_options":{"reasoning_summary_delivery":"sequential_cutoff"}}`)
				if source == translator.FormatOpenAI {
					payload = []byte(`{"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.invalid/image.png","detail":"original"}}]}],"verbosity":"low","response_format":{"type":"json_schema","json_schema":{"name":"answer","schema":{"type":"object","properties":{"answer":{"type":"string"}}}}}}`)
				}
				body := testCodexRequestControlsWire(t, source, transport, payload, false, nil)
				if gjson.GetBytes(body, "text.verbosity").String() != "low" || gjson.GetBytes(body, "input.0.content.0.detail").String() != "original" {
					t.Fatal("verbosity or image detail lost on the wire")
				}
				if source == translator.FormatOpenAI {
					if gjson.GetBytes(body, "text.format.schema.properties.answer.type").String() != "string" {
						t.Fatal("output schema lost on the wire")
					}
				} else {
					include := gjson.GetBytes(body, "include")
					if include.Raw != `["output.encrypted_content","reasoning.encrypted_content","web_search_call.action.sources"]` {
						t.Fatalf("include changed on the wire: %s", include.Raw)
					}
					if gjson.GetBytes(body, "stream_options.reasoning_summary_delivery").String() != "sequential_cutoff" {
						t.Fatal("summary delivery was lost")
					}
				}
			})
		}
	}
}

func TestCodexServiceTierAdminRulesAtActualTransport(t *testing.T) {
	for _, transport := range []string{"http", "stream", "websocket", "websocket-stream", "websocket-http-fallback", "compact"} {
		for _, allow := range []bool{false, true} {
			for _, policy := range []string{"default", "override", "filter"} {
				t.Run(fmt.Sprintf("%s/allow=%t/%s", transport, allow, policy), func(t *testing.T) {
					cfg := &config.Config{Codex: config.CodexConfig{AllowClientServiceTier: allow}}
					protocol := "codex"
					if transport == "compact" {
						protocol = "openai-response"
					}
					models := []config.PayloadModelRule{{Name: "*", Protocol: protocol}}
					rule := config.PayloadRule{Models: models, Params: map[string]any{"service_tier": "flex"}}
					want := "flex"
					switch policy {
					case "default":
						cfg.Payload.Default = []config.PayloadRule{rule}
						if allow {
							want = "priority"
						}
					case "override":
						cfg.Payload.Override = []config.PayloadRule{rule}
					case "filter":
						cfg.Payload.Filter = []config.PayloadFilterRule{{Models: models, Params: []string{"service_tier"}}}
						want = ""
					}
					body := testCodexRequestControlsWire(t, translator.FormatOpenAIResponse, transport, []byte(`{"input":[],"service_tier":"priority"}`), false, cfg)
					if got := gjson.GetBytes(body, "service_tier").String(); got != want {
						t.Fatalf("wire service_tier=%q, want %q", got, want)
					}
				})
			}
		}
	}
}

func TestCodexServiceTierSnapshotSurvivesConfigUpdate(t *testing.T) {
	for _, allow := range []bool{false, true} {
		cfg := &config.Config{Codex: config.CodexConfig{AllowClientServiceTier: allow}}
		executor := NewCodexExecutor(cfg)
		request := core.Request{Model: "gpt-6-astra", Payload: []byte(`{"input":[],"service_tier":"priority"}`)}
		opts := core.Options{SourceFormat: translator.FormatOpenAIResponse}
		original, err := executor.PrepareProviderRequest(t.Context(), request, opts, core.RequestOperationExecute)
		if err != nil {
			t.Fatal(err)
		}
		frozenOpts := core.WithProviderPreparedRequest(opts, "codex", original)
		executor.cfg = &config.Config{Codex: config.CodexConfig{AllowClientServiceTier: !allow}}
		snapshot := executor.codexPreparedSessionIdentity(t.Context(), request, frozenOpts)
		body := helps.ApplyCodexPayloadConfig(executor.cfg, snapshot.AllowClientServiceTier, request.Model, "codex", request.Payload, request.Payload, request.Model)
		if gjson.GetBytes(body, "service_tier").Exists() != allow {
			t.Fatal("retry adopted a changed service tier policy")
		}
		next := executor.codexPreparedSessionIdentity(t.Context(), request, opts)
		if next.AllowClientServiceTier != !allow {
			t.Fatal("new request did not adopt the changed service tier policy")
		}
	}
}

func TestCodexServiceTierRulesAtActualTransport(t *testing.T) {
	for _, transport := range []string{"http", "stream", "websocket", "websocket-stream", "websocket-http-fallback", "compact"} {
		for _, value := range []string{"priority", "fast", "ultrafast", "flex", "default", "auto", ""} {
			t.Run(transport+"/"+value, func(t *testing.T) {
				cfg := &config.Config{Codex: config.CodexConfig{AllowClientServiceTier: true, ServiceTierRules: []config.CodexServiceTierRule{{Models: []string{"gpt-6-astra"}, Tiers: []string{"priority"}, Default: "priority"}}}}
				payload := []byte(`{"input":[]}`)
				if value != "" {
					payload, _ = sjson.SetBytes(payload, "service_tier", value)
				}
				out := testCodexRequestControlsWire(t, translator.FormatOpenAIResponse, transport, payload, false, cfg)
				want := ""
				if value == "priority" || value == "fast" {
					want = "priority"
				}
				if value == "default" {
					want = "default"
				}
				if got := gjson.GetBytes(out, "service_tier").String(); got != want {
					t.Fatalf("service_tier=%q, want %q", got, want)
				}
			})
		}
	}
}

func TestCodexServiceTierRuleSnapshotUsesClientAlias(t *testing.T) {
	cfg := &config.Config{Codex: config.CodexConfig{AllowClientServiceTier: true, ServiceTierRules: []config.CodexServiceTierRule{{Models: []string{"public-alias"}, Tiers: []string{"priority"}}}}}
	e := NewCodexExecutor(cfg)
	request := core.Request{Model: "upstream-model", Payload: []byte(`{"input":[]}`)}
	opts := core.Options{SourceFormat: translator.FormatOpenAIResponse, Metadata: map[string]any{core.RequestedModelMetadataKey: "public-alias(high)"}}
	prepared, err := e.PrepareProviderRequest(t.Context(), request, opts, core.RequestOperationExecute)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Codex.ServiceTierRules[0].Tiers[0] = "ultrafast"
	snapshot := prepared.(codexPreparedSessionIdentity)
	if snapshot.ServiceTierRule == nil || snapshot.ServiceTierRule.Tiers[0] != "priority" {
		t.Fatal("request did not retain its client-alias policy snapshot")
	}
}

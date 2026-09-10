package executor

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexQuotaHTTPUsesLogicalRequestPolicyAcrossHotReload(t *testing.T) {
	for _, operation := range []string{"http", "stream", "compact", "image-passthrough", "alpha-search", "error"} {
		for _, initiallyEnabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/enabled=%t", operation, initiallyEnabled), func(t *testing.T) {
				cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}, Codex: config.CodexConfig{ObserveQuota: initiallyEnabled}}
				manager := coreauth.NewManager(nil, nil, nil)
				manager.SetConfig(cfg)
				manager.RegisterExecutor(NewCodexExecutor(cfg))
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					call := calls.Add(1)
					if call == 1 {
						next := *cfg
						next.Codex.ObserveQuota = !initiallyEnabled
						manager.SetConfig(&next)
					}
					w.Header().Set("X-Codex-Primary-Used-Percent", fmt.Sprint(call-1))
					w.Header().Set("X-Codex-Primary-Window-Minutes", "300")
					w.Header().Set("X-Codex-Credits-Has-Credits", "false")
					w.Header().Set("X-Codex-Private-Token", "test-private-value")
					w.Header().Set("Set-Cookie", "fixture=private")
					switch operation {
					case "compact":
						_, _ = fmt.Fprint(w, `{"id":"resp_test","object":"response.compaction","output":[]}`)
					case "alpha-search":
						_, _ = fmt.Fprint(w, `{"results":[]}`)
					case "error":
						w.WriteHeader(http.StatusBadRequest)
						_, _ = fmt.Fprint(w, `{"error":{"code":"misalignment_policy_violation","message":"blocked"}}`)
					default:
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_test\",\"output\":[]}}\n\n")
					}
				}))
				t.Cleanup(server.Close)
				id := t.Name()
				if _, err := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{ID: id, Provider: "codex", Attributes: map[string]string{"api_key": "test-key", "base_url": server.URL, coreauth.CodexAlphaSearchAttributeKey: "true"}}); err != nil {
					t.Fatal(err)
				}
				reg := registry.GetGlobalRegistry()
				reg.RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "gpt-5.4"}})
				t.Cleanup(func() { reg.UnregisterClient(id) })
				for attempt := 0; attempt < 2; attempt++ {
					req := core.Request{Model: "gpt-5.4", Payload: []byte(`{"model":"gpt-5.4","input":"fixture"}`)}
					opts := core.Options{SourceFormat: translator.FormatOpenAIResponse}
					if operation == "compact" {
						opts.Alt = "responses/compact"
					} else if operation == "alpha-search" {
						opts.SourceFormat = translator.FormatCodexAlphaSearch
					} else if operation == "image-passthrough" {
						opts.Metadata = map[string]any{core.ImageGenerationStreamPassthroughMetadataKey: true}
					}
					var err error
					if operation == "stream" || operation == "image-passthrough" {
						var result *core.StreamResult
						result, err = manager.ExecuteStream(t.Context(), []string{"codex"}, req, opts)
						if err == nil {
							for chunk := range result.Chunks {
								if chunk.Err != nil {
									t.Fatal(chunk.Err)
								}
							}
						}
					} else {
						_, err = manager.Execute(t.Context(), []string{"codex"}, req, opts)
					}
					if (err != nil) != (operation == "error") {
						t.Fatalf("unexpected request outcome: %v", err)
					}
					current, _ := manager.GetByID(id)
					observed := current.CodexQuotaSnapshot()
					if attempt == 0 && !initiallyEnabled {
						if observed != nil {
							t.Fatal("hot reload enabled capture for an already started request")
						}
						continue
					}
					wantPercent := "0"
					if !initiallyEnabled {
						wantPercent = "1"
					}
					if observed == nil || observed.Source != "http" || observed.ObservedAt.IsZero() || len(observed.Signals) != 3 || observed.Signals["X-Codex-Primary-Used-Percent"] != wantPercent || observed.Signals["X-Codex-Credits-Has-Credits"] != "false" {
						t.Fatalf("wrong bounded observation after request %d: %+v", attempt, observed)
					}
				}
				if calls.Load() != 2 {
					t.Fatalf("observation introduced extra upstream calls: %d", calls.Load())
				}
			})
		}
	}
}

func TestCodexQuotaHTTPRetryObservesBothCredentialInstances(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}, Codex: config.CodexConfig{ObserveQuota: true}}
			manager := coreauth.NewManager(nil, nil, nil)
			manager.SetConfig(cfg)
			manager.SetRetryConfig(2, 0, 0)
			manager.RegisterExecutor(NewCodexExecutor(cfg))
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				call := calls.Add(1)
				w.Header().Set("X-Codex-Primary-Used-Percent", fmt.Sprint(call))
				if call == 1 {
					next := *cfg
					next.Codex.ObserveQuota = false
					manager.SetConfig(&next)
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = fmt.Fprint(w, `{"error":{"message":"temporary failure"}}`)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_test\",\"output\":[]}}\n\n")
			}))
			t.Cleanup(server.Close)
			ids := []string{t.Name() + "-a", t.Name() + "-b"}
			for _, id := range ids {
				if _, err := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{ID: id, Provider: "codex", Attributes: map[string]string{"api_key": "test-key", "base_url": server.URL}}); err != nil {
					t.Fatal(err)
				}
				registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "gpt-5.4"}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
			}
			req := core.Request{Model: "gpt-5.4", Payload: []byte(`{"model":"gpt-5.4","input":"fixture"}`)}
			opts := core.Options{SourceFormat: translator.FormatOpenAIResponse}
			if stream {
				result, err := manager.ExecuteStream(t.Context(), []string{"codex"}, req, opts)
				if err != nil {
					t.Fatal(err)
				}
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						t.Fatal(chunk.Err)
					}
				}
			} else if _, err := manager.Execute(t.Context(), []string{"codex"}, req, opts); err != nil {
				t.Fatal(err)
			}
			seen := map[string]bool{}
			for _, id := range ids {
				current, _ := manager.GetByID(id)
				observation := current.CodexQuotaSnapshot()
				if observation == nil {
					t.Fatal("retry lost its original observation policy or credential instance")
				}
				seen[observation.Signals["X-Codex-Primary-Used-Percent"]] = true
			}
			if calls.Load() != 2 || !seen["1"] || !seen["2"] {
				t.Fatal("quota observation changed retries or mixed credential snapshots")
			}
		})
	}
}

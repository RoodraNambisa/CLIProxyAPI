package management

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/translator"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

const probeResponsesFixture = "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"OK\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-fixture\",\"model\":\"grok-4.6\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"id\":\"msg-fixture\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"OK\"}]}]}}\n\n"

func runModelProbe(t *testing.T, h *Handler, ctx context.Context, input modelProbeRequest) (int, modelProbeResult) {
	t.Helper()
	body, _ := json.Marshal(input)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/management/auth-files/models/probe", strings.NewReader(string(body))).WithContext(ctx)
	c.Request.Header.Set("Authorization", "Bearer management-secret-must-not-forward")
	h.ProbeAuthFileModel(c)
	var result modelProbeResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return w.Code, result
}

func TestAuthFileModelProbeRoutesPinningAndOverrides(t *testing.T) {
	for _, provider := range []string{"xai", "codex"} {
		for _, protocol := range []string{"responses", "chat", "chat-responses", "chat-direct"} {
			for _, stream := range []bool{false, true} {
				t.Run(provider+"/"+protocol+"/"+map[bool]string{true: "stream", false: "json"}[stream], func(t *testing.T) {
					var calls atomic.Int32
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						calls.Add(1)
						if r.Header.Get("Authorization") != "Bearer chosen-token" {
							t.Error("probe used another credential or management headers")
						}
						payload, _ := io.ReadAll(r.Body)
						if gjson.GetBytes(payload, "model").String() != "grok-4.6" {
							t.Errorf("alias not resolved: %s", payload)
						}
						w.Header().Set("x-request-id", "probe-fixture")
						if protocol == "chat-direct" {
							if r.URL.Path != "/override/chat/completions" {
								t.Error(r.URL.Path)
							}
							if stream {
								w.Header().Set("Content-Type", "text/event-stream")
								_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"OK\"}}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
							} else {
								_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"OK"},"finish_reason":"stop"}]}`)
							}
						} else {
							if !strings.HasSuffix(r.URL.Path, "/responses") {
								t.Error(r.URL.Path)
							}
							w.Header().Set("Content-Type", "text/event-stream")
							_, _ = io.WriteString(w, probeResponsesFixture)
						}
					}))
					defer server.Close()
					cfg := &config.Config{OAuthModelAlias: map[string][]config.OAuthModelAlias{provider: {{Name: "grok-4.6", Alias: "friendly"}}}}
					manager := coreauth.NewManager(nil, nil, nil)
					manager.SetConfig(cfg)
					auth, err := manager.Register(t.Context(), &coreauth.Auth{ID: "chosen.json", FileName: "chosen.json", Provider: provider, Attributes: map[string]string{"base_url": server.URL, "api_key": "chosen-token"}})
					if err != nil {
						t.Fatal(err)
					}
					_, err = manager.Register(t.Context(), &coreauth.Auth{ID: "other.json", FileName: "other.json", Provider: provider, Attributes: map[string]string{"base_url": server.URL, "api_key": "other-token"}})
					if err != nil {
						t.Fatal(err)
					}
					// OAuth aliases apply to OAuth credentials, not native API keys.
					model := "grok-4.6"
					input := modelProbeRequest{Name: auth.FileName, Model: model, Protocol: protocol, Stream: stream}
					if provider == "xai" {
						input.Upstream = server.URL + "/override"
					}
					h := &Handler{cfg: cfg, authManager: manager}
					status, result := runModelProbe(t, h, t.Context(), input)
					if provider == "codex" && protocol == "chat-direct" {
						if status != 400 || calls.Load() != 0 {
							t.Fatal("Codex direct Chat was accepted")
						}
						return
					}
					if status != 200 || !result.Success || result.Response != "OK" || calls.Load() != 1 {
						t.Fatalf("probe status %d calls %d result %+v", status, calls.Load(), result)
					}
					current, _ := manager.GetByID(auth.ID)
					if current.Attributes["base_url"] != server.URL || len(current.ModelStates) != 0 || current.Metadata["xai_model_routes"] != nil || cfg.XAI.ChatCompletionsMode != "" {
						t.Fatal("probe changed routing or cooldown state")
					}
				})
			}
		}
	}
}

func TestAuthFileModelProbeFailureAndCancellation(t *testing.T) {
	for _, failure := range []string{"http", "sse", "truncated", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			var calls atomic.Int32
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if failure == "http" {
					w.WriteHeader(403)
					_, _ = io.WriteString(w, `{"error":{"message":"bad chosen-token"}}`)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				if failure == "sse" {
					_, _ = io.WriteString(w, "data: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"message\":\"overloaded\"}}}\n\n")
					return
				}
				_, _ = io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"OK\"}\n\n")
				if failure == "cancel" {
					w.(http.Flusher).Flush()
					cancel()
					<-r.Context().Done()
				}
			}))
			defer server.Close()
			manager := coreauth.NewManager(nil, nil, nil)
			_, err := manager.Register(t.Context(), &coreauth.Auth{ID: "chosen.json", FileName: "chosen.json", Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "chosen-token"}})
			if err != nil {
				t.Fatal(err)
			}
			h := &Handler{cfg: &config.Config{}, authManager: manager}
			started := time.Now()
			_, result := runModelProbe(t, h, ctx, modelProbeRequest{Name: "chosen.json", Model: "grok-4.6", Stream: true})
			if result.Success || result.Error == "" || calls.Load() != 1 || strings.Contains(result.Error, "chosen-token") {
				t.Fatalf("invalid failure result: %+v", result)
			}
			if failure == "cancel" && time.Since(started) > time.Second {
				t.Fatal("cancel did not interrupt upstream")
			}
		})
	}
}

func TestAuthFileModelProbeAliasAndPersistentIdentity(t *testing.T) {
	var agent, session string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if gjson.GetBytes(body, "model").String() != "grok-4.6" || r.Header.Get("X-Global") != "kept" {
			t.Error("probe bypassed aliases or global headers")
		}
		nextAgent, nextSession := r.Header.Get("x-grok-agent-id"), r.Header.Get("x-grok-session-id")
		if nextAgent == "" || nextSession == "" || (agent != "" && (agent != nextAgent || session != nextSession)) {
			t.Error("probe did not reuse the credential identity pool")
		}
		agent, session = nextAgent, nextSession
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, probeResponsesFixture)
	}))
	defer server.Close()
	manager := coreauth.NewManager(nil, nil, nil)
	cfg := &config.Config{XAI: config.XAIConfig{SessionIdentityConvergence: true, SessionIdentityPoolSize: 1, Headers: map[string]string{"X-Global": "kept"}}}
	manager.SetConfig(cfg)
	manager.SetOAuthModelAlias(map[string][]config.OAuthModelAlias{"xai": {{Name: "grok-4.6", Alias: "friendly"}}})
	_, err := manager.Register(t.Context(), &coreauth.Auth{ID: "alias.json", FileName: "alias.json", Provider: "xai", Attributes: map[string]string{"base_url": server.URL}, Metadata: map[string]any{"access_token": "fixture-token"}})
	if err != nil {
		t.Fatal(err)
	}
	h := &Handler{cfg: cfg, authManager: manager}
	for range 2 {
		_, result := runModelProbe(t, h, t.Context(), modelProbeRequest{Name: "alias.json", Model: "friendly"})
		if !result.Success || result.UpstreamModel != "grok-4.6" {
			t.Fatalf("alias probe: %+v", result)
		}
	}
	current, _ := manager.GetByID("alias.json")
	if len(helps.XAIIdentitySeed(current)) != 32 {
		t.Fatal("probe did not persist its identity seed")
	}
}

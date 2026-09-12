package cliproxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	sdkauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type startupCodexTransport struct{ calls atomic.Int32 }

func (transport *startupCodexTransport) RoundTripperFor(*coreauth.Auth) http.RoundTripper {
	return transport
}

func (transport *startupCodexTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	defer func() { _ = request.Body.Close() }()
	if request.Method != http.MethodPost || !strings.HasSuffix(request.URL.Path, "/responses") {
		return nil, fmt.Errorf("unexpected startup fixture route")
	}
	var payload struct {
		Model string `json:"model"`
	}
	if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
		return nil, err
	}
	transport.calls.Add(1)
	response := map[string]any{
		"type": "response.completed",
		"response": map[string]any{
			"id": "resp_startup", "status": "completed", "model": payload.Model,
			"output": []any{map[string]any{
				"id": "msg_startup", "type": "message", "role": "assistant", "status": "completed",
				"content": []any{map[string]any{"type": "output_text", "text": "startup fixture reply", "annotations": []any{}}},
			}},
			"usage": map[string]any{"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
		},
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader("data: " + string(encoded) + "\n\n")),
		Request:    request,
	}, nil
}

func TestPreloadedCodexExecutesImmediatelyAfterBootstrap(t *testing.T) {
	authDir := t.TempDir()
	const authID = "startup-executable-codex.json"
	path := filepath.Join(authDir, authID)
	if err := os.WriteFile(path, []byte(`{"type":"codex","access_token":"startup-token-fixture","plan_type":"pro"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	// Repeat the cold load using the same persisted file, without a watcher update
	// or token refresh. Every upstream request stays in this in-memory transport.
	for restart := range 2 {
		t.Run(fmt.Sprintf("restart-%d", restart), func(t *testing.T) {
			store := sdkauth.NewFileTokenStore()
			store.SetBaseDir(authDir)
			manager := coreauth.NewManager(store, nil, nil)
			cfg := &config.Config{AuthDir: authDir}
			manager.SetConfig(cfg)
			transport := &startupCodexTransport{}
			manager.SetRoundTripperProvider(transport)
			service := &Service{cfg: cfg, coreManager: manager}
			t.Cleanup(func() {
				GlobalModelRegistry().UnregisterClient(authID)
				if err := manager.CloseExecutors(); err != nil {
					t.Error(err)
				}
			})
			if err := manager.Load(t.Context()); err != nil {
				t.Fatal(err)
			}
			if processed := service.installAuthMaintenanceHook(t.Context()); processed != 1 {
				t.Fatalf("bootstrap processed %d credentials, want 1", processed)
			}
			for _, model := range []string{"gpt-6-astra", "gpt-5.6-sol"} {
				for _, stream := range []bool{false, true} {
					body := []byte(fmt.Sprintf(`{"model":%q,"input":"startup fixture"}`, model))
					request := coreexecutor.Request{Model: model, Payload: body}
					options := coreexecutor.Options{SourceFormat: "codex", Stream: stream}
					var output []byte
					if stream {
						result, err := manager.ExecuteStream(t.Context(), []string{"codex"}, request, options)
						if err != nil {
							t.Fatalf("initial stream for %s: %v", model, err)
						}
						for chunk := range result.Chunks {
							if chunk.Err != nil {
								t.Fatalf("initial stream chunk for %s: %v", model, chunk.Err)
							}
							output = append(output, chunk.Payload...)
						}
					} else {
						result, err := manager.Execute(t.Context(), []string{"codex"}, request, options)
						if err != nil {
							t.Fatalf("initial request for %s: %v", model, err)
						}
						output = result.Payload
					}
					if !bytes.Contains(output, []byte("startup fixture reply")) {
						t.Fatalf("initial response for %s, stream=%t lost output", model, stream)
					}
				}
			}
			if calls := transport.calls.Load(); calls != 4 {
				t.Fatalf("initial requests made %d upstream attempts, want 4", calls)
			}
		})
	}
}

func TestBootstrapBindsAllPreloadedProviders(t *testing.T) {
	for _, provider := range []string{"codex", "claude", "gemini", "gemini-interactions", "vertex", "xai", "antigravity"} {
		t.Run(provider, func(t *testing.T) {
			manager := coreauth.NewManager(nil, nil, nil)
			service := &Service{cfg: &config.Config{}, coreManager: manager}
			auth := &coreauth.Auth{ID: "bootstrap-" + provider, Provider: provider, Status: coreauth.StatusActive}
			t.Cleanup(func() {
				GlobalModelRegistry().UnregisterClient(auth.ID)
				if err := manager.CloseExecutors(); err != nil {
					t.Error(err)
				}
			})
			if _, err := manager.Register(t.Context(), auth); err != nil {
				t.Fatal(err)
			}
			service.installAuthMaintenanceHook(t.Context())
			if _, ok := manager.Executor(provider); !ok {
				t.Fatal("preloaded provider has no executor after bootstrap")
			}
		})
	}
}

func TestBootstrapRetainsCodexExecutorAndSkipsDisabledProvider(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	for _, auth := range []*coreauth.Auth{
		{ID: "bootstrap-existing-codex", Provider: "codex", Status: coreauth.StatusActive},
		{ID: "bootstrap-disabled-claude", Provider: "claude", Disabled: true, Status: coreauth.StatusDisabled},
	} {
		if _, err := manager.Register(t.Context(), auth); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(auth.ID) })
	}
	t.Cleanup(func() {
		if err := manager.CloseExecutors(); err != nil {
			t.Error(err)
		}
	})
	service.ensureExecutorsForAuth(&coreauth.Auth{Provider: "codex"})
	before, _ := manager.Executor("codex")
	service.installAuthMaintenanceHook(t.Context())
	after, _ := manager.Executor("codex")
	if before != after {
		t.Fatal("bootstrap replaced an existing Codex executor")
	}
	if _, registered := manager.Executor("claude"); registered {
		t.Fatal("disabled credential registered a provider executor")
	}
}

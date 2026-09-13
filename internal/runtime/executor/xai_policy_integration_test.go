package executor

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	sdkauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestXAIGlobalDefaultsPreserveExplicitParametersAndSearchChoice(t *testing.T) {
	cfg := &config.Config{XAI: config.XAIConfig{RequestDefaults: map[string]any{"max_output_tokens": 99, "temperature": 1, "top_p": 0.9, "parallel_tool_calls": true, "stream_tool_calls": true}, InjectWebSearch: true, InjectXSearch: true}}
	exec := NewXAIExecutor(cfg)
	auth := &coreauth.Auth{ID: "grok-policy", Provider: "xai"}
	for _, format := range []sdktranslator.Format{sdktranslator.FormatOpenAI, sdktranslator.FormatOpenAIResponse} {
		body := []byte(`{"model":"grok-4.3","messages":[{"role":"user","content":"hello"}],"input":[{"role":"user","content":"hello"}],"max_tokens":17,"max_output_tokens":17,"temperature":0,"top_p":0.5,"parallel_tool_calls":false,"stream_tool_calls":false,"tool_choice":"none"}`)
		prepared, err := exec.prepareResponsesRequest(t.Context(), auth, core.Request{Model: "grok-4.3", Payload: body}, core.Options{SourceFormat: format}, true)
		if err != nil {
			t.Fatal(err)
		}
		if gjson.GetBytes(prepared.body, "max_output_tokens").Int() != 17 || gjson.GetBytes(prepared.body, "temperature").Float() != 0 || gjson.GetBytes(prepared.body, "top_p").Float() != 0.5 || gjson.GetBytes(prepared.body, "parallel_tool_calls").Bool() || gjson.GetBytes(prepared.body, "stream_tool_calls").Bool() || gjson.GetBytes(prepared.body, "tool_choice").String() != "none" || len(gjson.GetBytes(prepared.body, "tools").Array()) != 2 {
			t.Fatalf("%s lost explicit parameters: %s", format, prepared.body)
		}
	}
}

func TestXAIPayloadOverridesWinOverDefaultsAndSearchInjection(t *testing.T) {
	cfg := &config.Config{XAI: config.XAIConfig{RequestDefaults: map[string]any{"max_output_tokens": 99}, InjectWebSearch: true, InjectXSearch: true}}
	cfg.Payload.Override = []config.PayloadRule{{Models: []config.PayloadModelRule{{Name: "grok-4.3"}}, Params: map[string]any{"max_output_tokens": 7, "tools": []any{}}}}
	exec := NewXAIExecutor(cfg)
	prepared, err := exec.prepareResponsesRequest(t.Context(), &coreauth.Auth{ID: "override", Provider: "xai"}, core.Request{Model: "grok-4.3", Payload: []byte(`{"input":"hello","max_output_tokens":17}`)}, core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse}, true)
	if err != nil {
		t.Fatal(err)
	}
	if gjson.GetBytes(prepared.body, "max_output_tokens").Int() != 7 || gjson.GetBytes(prepared.body, "tools").Exists() {
		t.Fatalf("forced payload rules lost priority: %s", prepared.body)
	}
}

func TestXAIConcurrentManagedRequestsPersistOneStableIdentity(t *testing.T) {
	var mu sync.Mutex
	var identities []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.Header.Get("x-grok-session-id") != gjson.GetBytes(body, "prompt_cache_key").String() || r.Header.Get("x-grok-model-override") != "grok-4.3" {
			t.Error("inconsistent identity envelope")
		}
		mu.Lock()
		identities = append(identities, r.Header.Get("x-grok-session-id"))
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"fixture-response\",\"status\":\"completed\",\"output\":[]}}\n\n"))
	}))
	defer server.Close()
	dir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(dir)
	selector := coreauth.NewSessionAffinitySelector(&coreauth.RoundRobinSelector{})
	defer selector.Stop()
	manager := coreauth.NewManager(store, selector, nil)
	cfg := &config.Config{XAI: config.XAIConfig{SessionIdentityConvergence: true, SessionIdentityPoolSize: 4}}
	manager.RegisterExecutor(NewXAIExecutor(cfg))
	auth, err := manager.Register(t.Context(), &coreauth.Auth{ID: "xai-pool-test.json", FileName: "xai-pool-test.json", Provider: "xai", Metadata: map[string]any{"type": "xai", "access_token": "fixture", "base_url": server.URL, "auth_kind": "oauth"}, Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}})
	if err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, "xai", []*registry.ModelInfo{{ID: "grok-4.3", Object: "model", Type: "xai"}})
	defer registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	var wg sync.WaitGroup
	for index := range 6 {
		wg.Go(func() {
			_, errExecute := manager.Execute(t.Context(), []string{"xai"}, core.Request{Model: "grok-4.3", Payload: []byte(`{"input":[{"role":"user","content":"hello"}]}`)}, core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Headers: http.Header{"X-Grok-Session-Id": {"one-session"}}, Metadata: map[string]any{core.CallerScopeMetadataKey: strconv.Itoa(index)}})
			if errExecute != nil {
				t.Error(errExecute)
			}
		})
	}
	wg.Wait()
	if len(identities) != 6 {
		t.Fatalf("got %d requests", len(identities))
	}
	for _, id := range identities {
		if id == "" || id != identities[0] {
			t.Fatal("same session split across identities")
		}
	}
	raw, err := os.ReadFile(filepath.Join(dir, "xai-pool-test.json"))
	if err != nil {
		t.Fatal(err)
	}
	var metadata map[string]any
	if json.Unmarshal(raw, &metadata) != nil || len(helps.XAIIdentitySeed(&coreauth.Auth{Metadata: metadata})) != 32 {
		t.Fatal("identity seed not persisted")
	}
	loaded, err := store.List(t.Context())
	if err != nil || len(loaded) != 1 {
		t.Fatalf("reload: %v", err)
	}
	plan, err := helps.NewXAIRequestPlan(t.Context(), cfg, core.Request{Payload: []byte(`{}`)}, core.Options{Headers: http.Header{"X-Grok-Session-Id": {"one-session"}}})
	if err != nil {
		t.Fatal(err)
	}
	_, identity, err := plan.Project(loaded[0], []byte(`{}`), nil, "")
	if err != nil || identity.Session != identities[0] {
		t.Fatal("reload changed the converged slot identity")
	}
}

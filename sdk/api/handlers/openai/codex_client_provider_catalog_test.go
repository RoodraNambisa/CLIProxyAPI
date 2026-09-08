package openai

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
)

func TestCodexClientDerivedCapabilitiesStayWithinProviderAccess(t *testing.T) {
	r := registry.GetGlobalRegistry()
	const id = "provider-capability-alias"
	r.RegisterClient("capability-allowed", "codex", []*registry.ModelInfo{{ID: id, Description: "Allowed details", Thinking: &registry.ThinkingSupport{Levels: []string{"low"}}, SupportedInputModalities: []string{"text"}}})
	r.RegisterClient("capability-forbidden", "xai", []*registry.ModelInfo{{ID: id, DisplayName: "Forbidden label", Description: "Forbidden details", ContextLength: 131072, Thinking: &registry.ThinkingSupport{Levels: []string{"high"}}, SupportedInputModalities: []string{"image"}}})
	t.Cleanup(func() { r.UnregisterClient("capability-allowed"); r.UnregisterClient("capability-forbidden") })
	base := handlers.NewBaseAPIHandlers(&config.SDKConfig{CodexOptimizeMultiAgentV2: true}, nil)
	for _, version := range []string{"0.143.0", "0.153.4"} {
		t.Run(version, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest(http.MethodGet, "/v1/models?client_version="+version, nil)
			c.Request.Header.Set("User-Agent", "codex_cli_rs/"+version)
			c.Set("accessMetadata", map[string]string{sdkaccess.MetadataAllowedProviders: "codex"})
			NewOpenAIAPIHandler(base).OpenAIModels(c)
			model := gjson.GetBytes(w.Body.Bytes(), fmt.Sprintf(`models.#(slug==%q)`, id))
			if model.Get("display_name").String() != id || model.Get("description").String() != "Allowed details" || model.Get("supported_reasoning_levels.0.effort").String() != "low" || model.Get("input_modalities.0").String() != "text" || model.Get("context_window").Int() == 131072 {
				t.Fatal("derived capability used another provider's metadata")
			}
			payload := []byte(fmt.Sprintf(`{"input":[],"tools":%s}`, multiAgentBoundaryTools))
			prepared := NewOpenAIResponsesAPIHandler(base).prepareCodexMultiAgentV2(c, payload)
			description := gjson.GetBytes(prepared, "tools.0.tools.0.description").String()
			if !strings.Contains(description, "Allowed details") || strings.Contains(description, "Forbidden") || strings.Contains(description, "high") {
				t.Fatal("collaboration model list used forbidden capability metadata")
			}
		})
	}
}

func TestCodexClientSearchCapabilitiesUseAllowedProviders(t *testing.T) {
	r := registry.GetGlobalRegistry()
	const id = "gpt-6-astra"
	r.RegisterClient("search-allowed", "codex", []*registry.ModelInfo{{ID: id}})
	r.RegisterClient("search-other", "xai", []*registry.ModelInfo{{ID: id}})
	t.Cleanup(func() { r.UnregisterClient("search-allowed"); r.UnregisterClient("search-other") })
	for _, scope := range []string{"codex", "xai", "codex,xai", ""} {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodGet, "/v1/models?client_version=0.153.4", nil)
		c.Set("accessMetadata", map[string]string{sdkaccess.MetadataAllowedProviders: scope})
		(&OpenAIAPIHandler{}).OpenAIModels(c)
		support := gjson.GetBytes(w.Body.Bytes(), `models.#(slug=="gpt-6-astra").supports_search_tool`).Bool()
		if support != (scope == "codex") {
			t.Fatalf("scope %q: search support = %t", scope, support)
		}
	}
}

func TestCodexClientScopedSnapshotSurvivesRegistryChanges(t *testing.T) {
	const id = "provider-snapshot-alias"
	r := registry.GetGlobalRegistry()
	r.RegisterClient(id, "codex", []*registry.ModelInfo{{ID: id, DisplayName: "Original", Description: "Original details", ContextLength: 65536, Thinking: &registry.ThinkingSupport{Levels: []string{"low"}}}})
	t.Cleanup(func() { r.UnregisterClient(id) })
	catalog := r.GetModelCatalogForProviders("openai", []string{"codex"})
	r.RegisterClient(id, "codex", []*registry.ModelInfo{{ID: id, DisplayName: "Updated", Description: "Updated details", ContextLength: 131072, Thinking: &registry.ThinkingSupport{Levels: []string{"high"}}}})
	models := buildCodexClientModels(catalog.Models, func(id string) []string { return catalog.Providers[id] }, func(id string) *registry.ModelInfo { return catalog.Metadata[id] })
	var selected map[string]any
	for _, model := range models {
		if model["slug"] == id {
			selected = model
			break
		}
	}
	if selected["display_name"] != "Original" || selected["description"] != "Original details" || intModelValue(selected, "context_window") != 65536 || stringModelValue(selected, "default_reasoning_level") != "low" {
		t.Fatal("catalog combined metadata from different registry versions")
	}
	missing := buildCodexClientModels([]map[string]any{{"id": id}}, nil, nil)[0]
	if missing["display_name"] != id || missing["description"] != id {
		t.Fatal("missing scoped metadata fell back to the global registry")
	}
}

func TestCodexClientHTTPConcurrentCatalogUpdates(t *testing.T) {
	const client = "http-catalog-concurrency"
	r := registry.GetGlobalRegistry()
	register := func(revision int) {
		var models []*registry.ModelInfo
		for _, id := range []string{client + "-one", client + "-two"} {
			models = append(models, &registry.ModelInfo{ID: id, DisplayName: fmt.Sprint(revision), Description: fmt.Sprint(revision), ContextLength: revision})
		}
		r.RegisterClient(client, "codex", models)
	}
	register(1000)
	t.Cleanup(func() { r.UnregisterClient(client) })
	var readers sync.WaitGroup
	for _, scope := range []string{"", "codex"} {
		for range 4 {
			readers.Go(func() {
				for range 50 {
					w := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(w)
					c.Request = httptest.NewRequest(http.MethodGet, "/v1/models?client_version=0.153.4", nil)
					c.Set("accessMetadata", map[string]string{sdkaccess.MetadataAllowedProviders: scope})
					(&OpenAIAPIHandler{}).OpenAIModels(c)
					one := gjson.GetBytes(w.Body.Bytes(), `models.#(slug=="http-catalog-concurrency-one")`)
					two := gjson.GetBytes(w.Body.Bytes(), `models.#(slug=="http-catalog-concurrency-two")`)
					if !one.Exists() || !two.Exists() || one.Get("display_name").String() != two.Get("display_name").String() || one.Get("display_name").Int() != one.Get("context_window").Int() || one.Get("description").String() != one.Get("display_name").String() || two.Get("description").String() != two.Get("display_name").String() {
						t.Errorf("scope %q returned mixed registry revisions", scope)
						return
					}
				}
			})
		}
	}
	for revision := 1001; revision < 1500; revision++ {
		register(revision)
	}
	readers.Wait()
}

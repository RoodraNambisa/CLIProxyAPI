package openai

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

func TestCodexClientContextOverrideTypesAndInheritance(t *testing.T) {
	for _, id := range []string{"gpt-5.5", "custom-context-alias", "tenant/gpt-5.5"} {
		t.Run(id, func(t *testing.T) {
			baseline := buildCodexClientModels([]map[string]any{{"id": id}}, nil)[0]
			for _, limit := range []any{131072, int64(131072), json.Number("131072"), float64(131072), config.MaxModelContextLength} {
				model := map[string]any{"id": id, "max_context_length": limit}
				entry := buildCodexClientModels([]map[string]any{model}, nil)[0]
				want := 131072
				if limit == config.MaxModelContextLength {
					want = config.MaxModelContextLength
				}
				if intModelValue(entry, "context_window") != want || intModelValue(entry, "max_context_window") != want {
					t.Fatal("valid override not applied to both window fields")
				}
				if _, exists := model["context_window"]; exists {
					t.Fatal("query mutated input metadata")
				}
			}
			for _, limit := range []any{nil, 0, -1, "131072", true, 1.5, math.NaN(), math.Inf(1), int64(math.MaxInt64), json.Number("9223372036854775808"), float64(config.MaxModelContextLength) + 1} {
				entry := buildCodexClientModels([]map[string]any{{"id": id, "max_context_length": limit}}, nil)[0]
				if intModelValue(entry, "context_window") != intModelValue(baseline, "context_window") || intModelValue(entry, "max_context_window") != intModelValue(baseline, "max_context_window") {
					t.Fatal("unset or invalid override changed directory inheritance")
				}
			}
		})
	}
}

func TestCodexClientContextUsesQueryMetadataBeforeGlobalFallback(t *testing.T) {
	const id = "context-query-isolation"
	baseline := buildCodexClientModels([]map[string]any{{"id": id}}, nil)[0]
	r := registry.GetGlobalRegistry()
	r.RegisterClient(id, "codex", []*registry.ModelInfo{{ID: id, ContextLength: 999999, MaxContextLength: 999999}})
	t.Cleanup(func() { r.UnregisterClient(id) })
	for _, tc := range []struct {
		model map[string]any
		want  int
	}{
		{map[string]any{"id": id, "context_length": 2048, "max_context_length": 4096}, 4096},
		{map[string]any{"id": id, "context_length": 2048}, 2048},
		{map[string]any{"id": id}, intModelValue(baseline, "context_window")},
	} {
		got := buildCodexClientModels([]map[string]any{tc.model}, nil)[0]
		if intModelValue(got, "context_window") != tc.want {
			t.Fatal("another registry entry overwrote this query's context declaration")
		}
	}
}

func TestCodexClientContextOverrideThroughHTTPModels(t *testing.T) {
	const client = "context-http-fixture"
	r := registry.GetGlobalRegistry()
	r.RegisterClient(client, "codex", []*registry.ModelInfo{{ID: "gpt-5.5", ContextLength: 131072, MaxContextLength: 131072}, {ID: "tenant/context-http-alias", ContextLength: 262144, MaxContextLength: 262144}})
	t.Cleanup(func() { r.UnregisterClient(client) })
	for _, version := range []string{"0.143.0", "0.153.4"} {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodGet, "/v1/models?client_version="+version, nil)
		(&OpenAIAPIHandler{}).OpenAIModels(c)
		var body struct {
			Models []struct {
				Slug       string `json:"slug"`
				Context    int    `json:"context_window"`
				MaxContext int    `json:"max_context_window"`
			} `json:"models"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		found := 0
		for _, model := range body.Models {
			want := 0
			if model.Slug == "gpt-5.5" {
				want = 131072
			}
			if model.Slug == "tenant/context-http-alias" {
				want = 262144
			}
			if want == 0 {
				continue
			}
			found++
			if model.Context != want || model.MaxContext != want {
				t.Fatal("HTTP model catalog lost the configured context override")
			}
		}
		if found != 2 {
			t.Fatal("HTTP catalog omitted native or prefixed model")
		}
	}
}

func TestCodexClientInheritsGoogleTokenLimitsThroughHTTP(t *testing.T) {
	r := registry.GetGlobalRegistry()
	const client = "google-token-limits-http"
	var models []*registry.ModelInfo
	for _, id := range []string{"gemini-token-limit-fixture", "tenant/google-token-alias"} {
		models = append(models, &registry.ModelInfo{ID: id, UpstreamID: "gemini-2.5-pro", InputTokenLimit: 1048576, OutputTokenLimit: 65536})
	}
	r.RegisterClient(client, "gemini", models)
	t.Cleanup(func() { r.UnregisterClient(client) })
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/models?client_version=0.153.4", nil)
	(&OpenAIAPIHandler{}).OpenAIModels(c)
	var payload codexClientModelsPayload
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, model := range payload.Models {
		if model["slug"] != "gemini-token-limit-fixture" && model["slug"] != "tenant/google-token-alias" {
			continue
		}
		found++
		if intModelValue(model, "context_window") != 1048576 || intModelValue(model, "max_context_window") != 1048576 || intModelValue(model, "max_tokens") != 65536 {
			t.Fatal("Google token limits were replaced by a Codex fallback template")
		}
	}
	if found != 2 {
		t.Fatal("native or prefixed Google model missing")
	}
}

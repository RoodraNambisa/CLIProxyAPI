package openai

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

func TestCodexClientModelsAdvertiseRegisteredCompletionLimits(t *testing.T) {
	for _, id := range []string{"gpt-5.5", "custom-completion-limit"} {
		t.Run(id, func(t *testing.T) {
			baseline := buildCodexClientModels([]map[string]any{{"id": id}}, nil)[0]
			for _, limit := range []int{0, -1, 32768, 65536} {
				model := map[string]any{"id": id, "max_completion_tokens": limit}
				entry := buildCodexClientModels([]map[string]any{model}, nil)[0]
				if limit > 0 && intModelValue(entry, "max_tokens") != limit {
					t.Fatalf("limit %d was not advertised", limit)
				}
				if limit <= 0 && intModelValue(entry, "max_tokens") != intModelValue(baseline, "max_tokens") {
					t.Fatal("unset completion limit changed the template")
				}
				if _, mutated := model["max_tokens"]; mutated {
					t.Fatal("catalog generation mutated registry input")
				}
			}
			if intModelValue(buildCodexClientModels([]map[string]any{{"id": id}}, nil)[0], "max_tokens") != intModelValue(baseline, "max_tokens") {
				t.Fatal("model query mutated a shared template")
			}
		})
	}
}

func TestOpenAIModelsCodexCompletionLimitFromRegistry(t *testing.T) {
	const modelID = "completion-limit-http-fixture"
	const clientID = "completion-limit-client"
	r := registry.GetGlobalRegistry()
	r.RegisterClient(clientID, "openai", []*registry.ModelInfo{{ID: modelID, Object: "model", MaxCompletionTokens: 32768}})
	t.Cleanup(func() { r.UnregisterClient(clientID) })
	h := &OpenAIAPIHandler{}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/models?client_version=0.153.4", nil)
	h.OpenAIModels(c)
	var body struct {
		Models []struct {
			Slug      string `json:"slug"`
			MaxTokens int    `json:"max_tokens"`
		} `json:"models"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	for _, model := range body.Models {
		if model.Slug == modelID {
			if model.MaxTokens != 32768 {
				t.Fatalf("HTTP max_tokens=%d", model.MaxTokens)
			}
			return
		}
	}
	t.Fatal("registered model missing from Codex catalog")
}

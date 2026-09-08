package openai

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	"github.com/tidwall/gjson"
)

func TestCodexClientUsesStaticClaudeAndKimiModalities(t *testing.T) {
	for _, tc := range []struct {
		provider, id string
		load         func() []*registry.ModelInfo
		input        []string
		wantImage    bool
	}{
		{"kimi", "kimi-k2", registry.GetKimiModels, []string{"text"}, false},
		{"kimi", "kimi-k2-thinking", registry.GetKimiModels, []string{"text"}, false},
		{"kimi", "kimi-k2.5", registry.GetKimiModels, []string{"text", "image", "video"}, true},
		{"kimi", "kimi-k2.6", registry.GetKimiModels, []string{"text", "image", "video"}, true},
		{"claude", "claude-opus-4-6", registry.GetClaudeModels, []string{"text", "image"}, true},
		{"claude", "claude-sonnet-4-6", registry.GetClaudeModels, []string{"text", "image"}, true},
	} {
		t.Run(tc.id, func(t *testing.T) {
			var info *registry.ModelInfo
			for _, model := range tc.load() {
				if model.ID == tc.id {
					info = model
					break
				}
			}
			if info == nil {
				t.Fatal("existing model disappeared")
			}
			if !reflect.DeepEqual(info.SupportedInputModalities, tc.input) || !reflect.DeepEqual(info.SupportedOutputModalities, []string{"text"}) {
				t.Error("static model has incomplete modalities")
			}
			r := registry.GetGlobalRegistry()
			client := t.Name()
			r.RegisterClient(client, tc.provider, []*registry.ModelInfo{info})
			t.Cleanup(func() { r.UnregisterClient(client) })
			for _, scope := range []string{"", tc.provider} {
				w := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(w)
				c.Request = httptest.NewRequest(http.MethodGet, "/v1/models?client_version=0.153.4", nil)
				c.Set("accessMetadata", map[string]string{sdkaccess.MetadataAllowedProviders: scope})
				(&OpenAIAPIHandler{}).OpenAIModels(c)
				model := gjson.GetBytes(w.Body.Bytes(), fmt.Sprintf(`models.#(slug==%q)`, tc.id))
				want := []string{"text"}
				if tc.wantImage {
					want = append(want, "image")
				}
				var input []string
				for _, modality := range model.Get("input_modalities").Array() {
					input = append(input, modality.String())
				}
				if !reflect.DeepEqual(input, want) || model.Get("supports_image_detail_original").Bool() != tc.wantImage {
					t.Fatalf("scope %q returned wrong client image capability", scope)
				}
				if model.Get("context_window").Int() != int64(info.ContextLength) || model.Get("max_tokens").Int() != int64(info.MaxCompletionTokens) {
					t.Fatal("modality metadata changed token capacity")
				}
			}
		})
	}
}

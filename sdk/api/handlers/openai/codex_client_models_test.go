package openai

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

func TestOpenAIModels_ClientVersionSelectsCodexCatalog(t *testing.T) {
	gin.SetMode(gin.TestMode)
	modelRegistry := registry.GetGlobalRegistry()
	clientID := "codex-client-version-handler-test"
	modelRegistry.RegisterClient(clientID, "openai", []*registry.ModelInfo{{
		ID:      "gpt-5.5",
		Object:  "model",
		OwnedBy: "openai",
		Type:    "openai",
	}})
	t.Cleanup(func() {
		modelRegistry.UnregisterClient(clientID)
	})

	handler := &OpenAIAPIHandler{}

	codexRecorder := httptest.NewRecorder()
	codexContext, _ := gin.CreateTestContext(codexRecorder)
	codexContext.Request = httptest.NewRequest(http.MethodGet, "/v1/models?client_version=0.144.0", nil)
	handler.OpenAIModels(codexContext)

	var codexResponse map[string]any
	if err := json.Unmarshal(codexRecorder.Body.Bytes(), &codexResponse); err != nil {
		t.Fatalf("decode Codex response: %v", err)
	}
	if codexRecorder.Code != http.StatusOK {
		t.Fatalf("Codex response status = %d, want 200", codexRecorder.Code)
	}
	if _, ok := codexResponse["models"]; !ok {
		t.Fatalf("Codex response = %#v, want models", codexResponse)
	}
	if _, ok := codexResponse["data"]; ok {
		t.Fatalf("Codex response = %#v, should not contain data", codexResponse)
	}

	plainRecorder := httptest.NewRecorder()
	plainContext, _ := gin.CreateTestContext(plainRecorder)
	plainContext.Request = httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	handler.OpenAIModels(plainContext)

	var plainResponse map[string]any
	if err := json.Unmarshal(plainRecorder.Body.Bytes(), &plainResponse); err != nil {
		t.Fatalf("decode plain response: %v", err)
	}
	if got := plainResponse["object"]; got != "list" {
		t.Fatalf("plain response object = %#v, want list", got)
	}
	if _, ok := plainResponse["data"]; !ok {
		t.Fatalf("plain response = %#v, want data", plainResponse)
	}
	if _, ok := plainResponse["models"]; ok {
		t.Fatalf("plain response = %#v, should not contain models", plainResponse)
	}
}

func TestCodexClientModelsResponse_InputModalitiesFromRegistry(t *testing.T) {
	modelID := "mimo-v2.5-pro-codex-test"
	textOnlyModelID := "mimo-text-only-codex-test"
	modelRegistry := registry.GetGlobalRegistry()
	modelRegistry.RegisterClient("codex-input-modalities-test", "openai-compatibility", []*registry.ModelInfo{
		{
			ID:                       modelID,
			Object:                   "model",
			OwnedBy:                  "mimo",
			Type:                     "openai-compatibility",
			DisplayName:              modelID,
			SupportedInputModalities: []string{"text", "image"},
		},
		{
			ID:                       textOnlyModelID,
			Object:                   "model",
			OwnedBy:                  "mimo",
			Type:                     "openai-compatibility",
			DisplayName:              textOnlyModelID,
			SupportedInputModalities: []string{"text"},
		},
		{
			ID:                       "mimo-mixed-modalities-codex-test",
			Object:                   "model",
			OwnedBy:                  "mimo",
			Type:                     "openai-compatibility",
			DisplayName:              "mimo-mixed-modalities-codex-test",
			SupportedInputModalities: []string{"text", "image", "audio", "video", "TEXT", "IMAGE"},
		},
		{
			ID:      "compat-image-only-codex-test",
			Object:  "model",
			OwnedBy: "mimo",
			Type:    registry.OpenAIImageModelType,
		},
	})
	t.Cleanup(func() {
		modelRegistry.UnregisterClient("codex-input-modalities-test")
	})

	openaiModels := modelRegistry.GetAvailableModels("openai")
	resp := CodexClientModelsResponse(openaiModels)
	models, ok := resp["models"].([]map[string]any)
	if !ok {
		t.Fatalf("models type = %T, want []map[string]any", resp["models"])
	}

	var visionEntry map[string]any
	var textOnlyEntry map[string]any
	var mixedEntry map[string]any
	var imageEntry map[string]any
	for _, entry := range models {
		slug := stringModelValue(entry, "slug")
		switch slug {
		case modelID:
			visionEntry = entry
		case textOnlyModelID:
			textOnlyEntry = entry
		case "mimo-mixed-modalities-codex-test":
			mixedEntry = entry
		case "compat-image-only-codex-test":
			imageEntry = entry
		}
	}
	if visionEntry == nil {
		t.Fatalf("expected codex entry for %q", modelID)
	}
	modalities, ok := visionEntry["input_modalities"].([]any)
	if !ok || len(modalities) != 2 {
		t.Fatalf("input_modalities = %#v, want [text image]", visionEntry["input_modalities"])
	}
	if got, _ := modalities[0].(string); got != "text" {
		t.Fatalf("input_modalities[0] = %q, want text", got)
	}
	if got, _ := modalities[1].(string); got != "image" {
		t.Fatalf("input_modalities[1] = %q, want image", got)
	}
	if got, ok := visionEntry["supports_image_detail_original"].(bool); !ok || !got {
		t.Fatalf("supports_image_detail_original = %#v, want true", visionEntry["supports_image_detail_original"])
	}

	if textOnlyEntry == nil {
		t.Fatalf("expected codex entry for %q", textOnlyModelID)
	}
	textOnlyModalities, ok := textOnlyEntry["input_modalities"].([]any)
	if !ok || len(textOnlyModalities) != 1 {
		t.Fatalf("text-only input_modalities = %#v, want [text]", textOnlyEntry["input_modalities"])
	}
	if got, _ := textOnlyModalities[0].(string); got != "text" {
		t.Fatalf("text-only input_modalities[0] = %q, want text", got)
	}
	if _, exists := textOnlyEntry["supports_image_detail_original"]; exists {
		t.Fatalf("text-only model should not expose supports_image_detail_original: %#v", textOnlyEntry["supports_image_detail_original"])
	}

	if mixedEntry == nil {
		t.Fatal("expected codex entry for mixed-modalities model")
	}
	mixedModalities, ok := mixedEntry["input_modalities"].([]any)
	if !ok || len(mixedModalities) != 2 {
		t.Fatalf("mixed input_modalities = %#v, want [text image]", mixedEntry["input_modalities"])
	}
	if got, _ := mixedModalities[0].(string); got != "text" {
		t.Fatalf("mixed input_modalities[0] = %q, want text", got)
	}
	if got, _ := mixedModalities[1].(string); got != "image" {
		t.Fatalf("mixed input_modalities[1] = %q, want image", got)
	}
	if got, ok := mixedEntry["supports_image_detail_original"].(bool); !ok || !got {
		t.Fatalf("mixed supports_image_detail_original = %#v, want true", mixedEntry["supports_image_detail_original"])
	}

	if imageEntry == nil {
		t.Fatal("expected codex entry for image-only compat model")
	}
	if got, _ := imageEntry["visibility"].(string); got != "hide" {
		t.Fatalf("image model visibility = %q, want hide", got)
	}
	if _, exists := imageEntry["input_modalities"]; exists {
		t.Fatalf("image endpoint model should not expose input_modalities from registry: %#v", imageEntry["input_modalities"])
	}
}

func TestCodexClientModelsResponse_PreservesUltraReasoningEffort(t *testing.T) {
	resp := CodexClientModelsResponse([]map[string]any{{"id": "gpt-5.6-sol"}})
	models, ok := resp["models"].([]map[string]any)
	if !ok {
		t.Fatalf("models type = %T, want []map[string]any", resp["models"])
	}

	var sol map[string]any
	for _, entry := range models {
		if stringModelValue(entry, "slug") == "gpt-5.6-sol" {
			sol = entry
			break
		}
	}
	if sol == nil {
		t.Fatal("expected codex client entry for gpt-5.6-sol")
	}

	levels, ok := sol["supported_reasoning_levels"].([]any)
	if !ok {
		t.Fatalf("supported_reasoning_levels = %T, want []any", sol["supported_reasoning_levels"])
	}
	for _, rawLevel := range levels {
		level, ok := rawLevel.(map[string]any)
		if ok && stringModelValue(level, "effort") == "ultra" {
			return
		}
	}

	t.Fatalf("supported_reasoning_levels = %#v, want ultra", levels)
}

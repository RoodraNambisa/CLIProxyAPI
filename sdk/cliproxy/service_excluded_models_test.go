package cliproxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestRegisterModelsForAuth_UsesPreMergedExcludedModelsAttribute(t *testing.T) {
	service := &Service{
		cfg: &config.Config{
			OAuthExcludedModels: map[string][]string{
				"gemini": {"gemini-2.5-pro"},
			},
		},
	}
	auth := &coreauth.Auth{
		ID:       "auth-gemini",
		Provider: "gemini",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"auth_kind":       "oauth",
			"excluded_models": "gemini-2.5-flash",
		},
	}

	registry := GlobalModelRegistry()
	registry.UnregisterClient(auth.ID)
	t.Cleanup(func() {
		registry.UnregisterClient(auth.ID)
	})

	service.registerModelsForAuth(auth)

	models := registry.GetAvailableModelsByProvider("gemini")
	if len(models) == 0 {
		t.Fatal("expected gemini models to be registered")
	}

	for _, model := range models {
		if model == nil {
			continue
		}
		modelID := strings.TrimSpace(model.ID)
		if strings.EqualFold(modelID, "gemini-2.5-flash") {
			t.Fatalf("expected model %q to be excluded by auth attribute", modelID)
		}
	}

	seenGlobalExcluded := false
	for _, model := range models {
		if model == nil {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(model.ID), "gemini-2.5-pro") {
			seenGlobalExcluded = true
			break
		}
	}
	if !seenGlobalExcluded {
		t.Fatal("expected global excluded model to be present when attribute override is set")
	}
}

func TestRegisterModelsForAuth_CodexImageModelFollowsPlanType(t *testing.T) {
	testCases := []struct {
		name            string
		planType        string
		enableFreeImage bool
		want            bool
	}{
		{name: "free disabled", planType: "free", want: false},
		{name: "free enabled", planType: "free", enableFreeImage: true, want: true},
		{name: "plus", planType: "plus", want: true},
		{name: "pro", planType: "pro", want: true},
		{name: "team", planType: "team", want: true},
		{name: "business", planType: "business", want: true},
		{name: "go", planType: "go", want: true},
		{name: "missing plan", planType: "", want: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			service := &Service{
				cfg: &config.Config{
					SDKConfig: config.SDKConfig{
						Images: config.ImagesConfig{
							ImageModel:               "gpt-image-custom",
							EnableFreePlanImageModel: tc.enableFreeImage,
						},
					},
				},
			}
			auth := &coreauth.Auth{
				ID:       "auth-codex-" + strings.ReplaceAll(tc.name, " ", "-"),
				Provider: "codex",
				Status:   coreauth.StatusActive,
				Attributes: map[string]string{
					"plan_type": tc.planType,
				},
			}
			reg := GlobalModelRegistry()
			reg.UnregisterClient(auth.ID)
			t.Cleanup(func() {
				reg.UnregisterClient(auth.ID)
			})

			service.registerModelsForAuth(auth)

			models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID)
			hasImage := false
			for _, model := range models {
				if model != nil && strings.EqualFold(strings.TrimSpace(model.ID), "gpt-image-custom") {
					hasImage = true
					break
				}
			}
			if hasImage != tc.want {
				t.Fatalf("image model presence = %v, want %v", hasImage, tc.want)
			}
		})
	}
}

func TestRegisterModelsForAuth_CodexImageModelRespectsExcludedModels(t *testing.T) {
	service := &Service{
		cfg: &config.Config{
			SDKConfig: config.SDKConfig{
				Images: config.ImagesConfig{ImageModel: "gpt-image-custom"},
			},
		},
	}
	auth := &coreauth.Auth{
		ID:       "auth-codex-image-excluded",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"plan_type":       "plus",
			"excluded_models": "gpt-image-custom",
		},
	}

	reg := GlobalModelRegistry()
	reg.UnregisterClient(auth.ID)
	t.Cleanup(func() {
		reg.UnregisterClient(auth.ID)
	})

	service.registerModelsForAuth(auth)

	for _, model := range registry.GetGlobalRegistry().GetModelsForClient(auth.ID) {
		if model != nil && strings.EqualFold(strings.TrimSpace(model.ID), "gpt-image-custom") {
			t.Fatalf("expected excluded image model to be absent, got %q", model.ID)
		}
	}
}

func TestRegisterModelsForAuth_AuthModelExclusionsFilterByPriority(t *testing.T) {
	service := &Service{
		cfg: &config.Config{
			SDKConfig: config.SDKConfig{
				Images: config.ImagesConfig{ImageModel: "gpt-image-2"},
			},
			AuthModelExclusions: []config.AuthModelExclusionRule{
				{Models: []string{"gpt-image-2"}, Priorities: []int{-1}},
			},
		},
	}
	testCases := []struct {
		name      string
		priority  string
		wantImage bool
	}{
		{name: "excluded priority", priority: "-1", wantImage: false},
		{name: "allowed priority", priority: "0", wantImage: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			auth := &coreauth.Auth{
				ID:       "auth-codex-model-exclusion-priority-" + strings.ReplaceAll(tc.name, " ", "-"),
				Provider: "codex",
				Status:   coreauth.StatusActive,
				Attributes: map[string]string{
					"plan_type": "plus",
					"priority":  tc.priority,
				},
			}
			reg := GlobalModelRegistry()
			reg.UnregisterClient(auth.ID)
			t.Cleanup(func() {
				reg.UnregisterClient(auth.ID)
			})

			service.registerModelsForAuth(auth)

			hasImage := containsRegisteredModel(registry.GetGlobalRegistry().GetModelsForClient(auth.ID), "gpt-image-2")
			if hasImage != tc.wantImage {
				t.Fatalf("gpt-image-2 presence = %v, want %v", hasImage, tc.wantImage)
			}
		})
	}
}

func TestRegisterModelsForAuth_DisableImageGenerationHidesConfiguredImageModels(t *testing.T) {
	service := &Service{
		cfg: &config.Config{
			SDKConfig: config.SDKConfig{
				Images: config.ImagesConfig{
					ImageModel: "gpt-image-2",
					Native: config.NativeImagesConfig{
						Generations: config.NativeImageEndpointConfig{Enabled: true, Models: []string{"gpt-image-1.5"}},
						Edits:       config.NativeImageEndpointConfig{Enabled: true, Models: []string{"gpt-image-edit"}},
					},
				},
			},
			AuthModelExclusions: []config.AuthModelExclusionRule{
				{DisableImageGeneration: true, Priorities: []int{-1}},
			},
		},
	}
	auth := &coreauth.Auth{
		ID:       "auth-codex-disable-image-generation",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"plan_type": "plus",
			"priority":  "-1",
		},
	}
	reg := GlobalModelRegistry()
	reg.UnregisterClient(auth.ID)
	t.Cleanup(func() {
		reg.UnregisterClient(auth.ID)
	})

	service.registerModelsForAuth(auth)

	models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID)
	for _, modelID := range []string{"gpt-image-2", "gpt-image-1.5", "gpt-image-edit"} {
		if containsRegisteredModel(models, modelID) {
			t.Fatalf("expected disabled image model %q to be absent; got %v", modelID, registeredModelIDs(models))
		}
	}
	if !containsRegisteredModel(models, "gpt-5.4") {
		t.Fatalf("expected text model to remain; got %v", registeredModelIDs(models))
	}
}

func TestRegisterModelsForAuth_AuthModelExclusionsFilterByKeyword(t *testing.T) {
	service := &Service{
		cfg: &config.Config{
			SDKConfig: config.SDKConfig{
				Images: config.ImagesConfig{ImageModel: "gpt-image-2"},
			},
			AuthModelExclusions: []config.AuthModelExclusionRule{
				{Models: []string{"gpt-image-2"}, KeywordContains: []string{"free"}},
			},
		},
	}
	auth := &coreauth.Auth{
		ID:       "auth-codex-model-exclusion-keyword",
		Provider: "codex",
		FileName: "codex-free-account.json",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"plan_type": "plus",
		},
		Metadata: map[string]any{
			"email": "tester-free@example.com",
		},
	}
	reg := GlobalModelRegistry()
	reg.UnregisterClient(auth.ID)
	t.Cleanup(func() {
		reg.UnregisterClient(auth.ID)
	})

	service.registerModelsForAuth(auth)

	models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID)
	if containsRegisteredModel(models, "gpt-image-2") {
		t.Fatal("expected keyword-matched auth to exclude gpt-image-2")
	}
	if len(models) == 0 {
		t.Fatal("expected other codex models to remain registered")
	}
}

func TestRegisterModelsForAuth_AuthModelExclusionsApplyBeforePrefix(t *testing.T) {
	service := &Service{
		cfg: &config.Config{
			SDKConfig: config.SDKConfig{
				ForceModelPrefix: true,
				Images:           config.ImagesConfig{ImageModel: "gpt-image-2"},
			},
			AuthModelExclusions: []config.AuthModelExclusionRule{
				{Models: []string{"gpt-image-2"}, Priorities: []int{0}},
			},
		},
	}
	auth := &coreauth.Auth{
		ID:       "auth-codex-model-exclusion-prefix",
		Provider: "codex",
		Prefix:   "team-a",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"plan_type": "plus",
			"priority":  "0",
		},
	}
	reg := GlobalModelRegistry()
	reg.UnregisterClient(auth.ID)
	t.Cleanup(func() {
		reg.UnregisterClient(auth.ID)
	})

	service.registerModelsForAuth(auth)

	models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID)
	if containsRegisteredModel(models, "gpt-image-2") {
		t.Fatal("expected unprefixed image model to be absent with forced prefix")
	}
	if containsRegisteredModel(models, "team-a/gpt-image-2") {
		t.Fatal("expected model exclusion to remove image model before prefixing")
	}
}

func TestRegisterModelsForAuth_AuthModelExclusionsApplyToOpenAICompatibility(t *testing.T) {
	service := &Service{
		cfg: &config.Config{
			OpenAICompatibility: []config.OpenAICompatibility{
				{
					Name:    "compat-images",
					BaseURL: "https://example.invalid/v1",
					Models: []config.OpenAICompatibilityModel{
						{Name: "gpt-image-2"},
						{Name: "gpt-5.5"},
					},
				},
			},
			AuthModelExclusions: []config.AuthModelExclusionRule{
				{Models: []string{"gpt-image-2"}, Priorities: []int{-1}},
			},
		},
	}
	auth := &coreauth.Auth{
		ID:       "auth-openai-compat-model-exclusion",
		Provider: "openai-compatibility",
		Label:    "compat-images",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"priority": "-1",
		},
	}
	reg := GlobalModelRegistry()
	reg.UnregisterClient(auth.ID)
	t.Cleanup(func() {
		reg.UnregisterClient(auth.ID)
	})

	service.registerModelsForAuth(auth)

	models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID)
	if containsRegisteredModel(models, "gpt-image-2") {
		t.Fatal("expected OpenAI compatibility image model to be filtered")
	}
	if !containsRegisteredModel(models, "gpt-5.5") {
		t.Fatal("expected non-filtered OpenAI compatibility model to remain")
	}
}

func TestRegisterModelsForAuth_AuthModelExclusionsAllAllowsSelectedCodexModel(t *testing.T) {
	service := &Service{
		cfg: &config.Config{
			SDKConfig: config.SDKConfig{
				Images: config.ImagesConfig{ImageModel: "gpt-image-2"},
			},
			AuthModelExclusions: []config.AuthModelExclusionRule{
				{Models: []string{"", " -all ", "+gpt-5.5"}, Priorities: []int{-1}},
			},
		},
	}
	auth := &coreauth.Auth{
		ID:       "auth-codex-model-exclusion-all-allow",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"plan_type": "plus",
			"priority":  "-1",
		},
	}
	reg := GlobalModelRegistry()
	reg.UnregisterClient(auth.ID)
	t.Cleanup(func() {
		reg.UnregisterClient(auth.ID)
	})

	service.registerModelsForAuth(auth)

	assertOnlyRegisteredModels(t, registry.GetGlobalRegistry().GetModelsForClient(auth.ID), "gpt-5.5")
}

func TestRegisterModelsForAuth_AuthModelExclusionsAllWithoutAllowedModelsClearsCodexModels(t *testing.T) {
	service := &Service{
		cfg: &config.Config{
			AuthModelExclusions: []config.AuthModelExclusionRule{
				{Models: []string{"-all"}, Priorities: []int{-1}},
			},
		},
	}
	auth := &coreauth.Auth{
		ID:       "auth-codex-model-exclusion-all-empty",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"plan_type": "plus",
			"priority":  "-1",
		},
	}
	reg := GlobalModelRegistry()
	reg.UnregisterClient(auth.ID)
	t.Cleanup(func() {
		reg.UnregisterClient(auth.ID)
	})

	service.registerModelsForAuth(auth)

	if models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID); len(models) != 0 {
		t.Fatalf("expected all matched codex models to be removed, got %v", registeredModelIDs(models))
	}
}

func TestRegisterModelsForAuth_AuthModelExclusionsAllMustBeFirstEffectiveModel(t *testing.T) {
	service := &Service{
		cfg: &config.Config{
			SDKConfig: config.SDKConfig{
				Images: config.ImagesConfig{ImageModel: "gpt-image-2"},
			},
			AuthModelExclusions: []config.AuthModelExclusionRule{
				{Models: []string{"gpt-image-2", "-all", "+gpt-5.5"}, Priorities: []int{-1}},
			},
		},
	}
	auth := &coreauth.Auth{
		ID:       "auth-codex-model-exclusion-all-not-first",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"plan_type": "plus",
			"priority":  "-1",
		},
	}
	reg := GlobalModelRegistry()
	reg.UnregisterClient(auth.ID)
	t.Cleanup(func() {
		reg.UnregisterClient(auth.ID)
	})

	service.registerModelsForAuth(auth)

	models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID)
	if containsRegisteredModel(models, "gpt-image-2") {
		t.Fatal("expected ordinary exclusion to remove gpt-image-2")
	}
	if containsRegisteredModel(models, "gpt-5.5") {
		t.Fatal("expected +gpt-5.5 to be treated as an ordinary exclusion without leading -all")
	}
	if len(models) == 0 {
		t.Fatal("expected other codex models to remain when -all is not first")
	}
}

func TestRegisterModelsForAuth_AuthModelExclusionsAllApplyBeforePrefix(t *testing.T) {
	service := &Service{
		cfg: &config.Config{
			SDKConfig: config.SDKConfig{
				ForceModelPrefix: true,
			},
			AuthModelExclusions: []config.AuthModelExclusionRule{
				{Models: []string{"-all", "+gpt-5.5"}, Priorities: []int{0}},
			},
		},
	}
	auth := &coreauth.Auth{
		ID:       "auth-codex-model-exclusion-all-prefix",
		Provider: "codex",
		Prefix:   "team-a",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"plan_type": "plus",
			"priority":  "0",
		},
	}
	reg := GlobalModelRegistry()
	reg.UnregisterClient(auth.ID)
	t.Cleanup(func() {
		reg.UnregisterClient(auth.ID)
	})

	service.registerModelsForAuth(auth)

	assertOnlyRegisteredModels(t, registry.GetGlobalRegistry().GetModelsForClient(auth.ID), "team-a/gpt-5.5")
}

func TestRegisterModelsForAuth_AuthModelExclusionsAllApplyToOpenAICompatibility(t *testing.T) {
	service := &Service{
		cfg: &config.Config{
			OpenAICompatibility: []config.OpenAICompatibility{
				{
					Name:    "compat-images-all",
					BaseURL: "https://example.invalid/v1",
					Models: []config.OpenAICompatibilityModel{
						{Name: "gpt-image-2"},
						{Name: "gpt-5.5"},
						{Name: "gpt-5.6"},
					},
				},
			},
			AuthModelExclusions: []config.AuthModelExclusionRule{
				{Models: []string{"-all", "+gpt-5.5"}, Priorities: []int{-1}},
			},
		},
	}
	auth := &coreauth.Auth{
		ID:       "auth-openai-compat-model-exclusion-all",
		Provider: "openai-compatibility",
		Label:    "compat-images-all",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"priority": "-1",
		},
	}
	reg := GlobalModelRegistry()
	reg.UnregisterClient(auth.ID)
	t.Cleanup(func() {
		reg.UnregisterClient(auth.ID)
	})

	service.registerModelsForAuth(auth)

	assertOnlyRegisteredModels(t, registry.GetGlobalRegistry().GetModelsForClient(auth.ID), "gpt-5.5")
}

func TestRegisterModelsForAuth_CodexNativeImageModelsWhenEnabled(t *testing.T) {
	testCases := []struct {
		name       string
		native     config.NativeImagesConfig
		wantModel  string
		want       bool
		planType   string
		enableFree bool
	}{
		{
			name: "native disabled does not expose default native models",
			native: config.NativeImagesConfig{
				Generations: config.NativeImageEndpointConfig{
					Models: []string{"gpt-image-1.5"},
				},
			},
			wantModel: "gpt-image-1.5",
		},
		{
			name: "enabled generation model is registered",
			native: config.NativeImagesConfig{
				Generations: config.NativeImageEndpointConfig{
					Enabled: true,
					Models:  []string{"gpt-image-1.5"},
				},
			},
			wantModel: "gpt-image-1.5",
			want:      true,
		},
		{
			name: "enabled edit default models are registered",
			native: config.NativeImagesConfig{
				Edits: config.NativeImageEndpointConfig{Enabled: true},
			},
			wantModel: "gpt-image-1.5",
			want:      true,
		},
		{
			name: "free plan still follows image permission",
			native: config.NativeImagesConfig{
				Generations: config.NativeImageEndpointConfig{
					Enabled: true,
					Models:  []string{"gpt-image-1.5"},
				},
			},
			planType:  "free",
			wantModel: "gpt-image-1.5",
		},
		{
			name: "free plan opt in registers native models",
			native: config.NativeImagesConfig{
				Generations: config.NativeImageEndpointConfig{
					Enabled: true,
					Models:  []string{"gpt-image-1.5"},
				},
			},
			planType:   "free",
			enableFree: true,
			wantModel:  "gpt-image-1.5",
			want:       true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			service := &Service{
				cfg: &config.Config{
					SDKConfig: config.SDKConfig{
						Images: config.ImagesConfig{
							ImageModel:               "gpt-image-2",
							EnableFreePlanImageModel: tc.enableFree,
							Native:                   tc.native,
						},
					},
				},
			}
			auth := &coreauth.Auth{
				ID:       "auth-codex-native-image-" + strings.ReplaceAll(tc.name, " ", "-"),
				Provider: "codex",
				Status:   coreauth.StatusActive,
				Attributes: map[string]string{
					"plan_type": tc.planType,
				},
			}
			reg := GlobalModelRegistry()
			reg.UnregisterClient(auth.ID)
			t.Cleanup(func() {
				reg.UnregisterClient(auth.ID)
			})

			service.registerModelsForAuth(auth)

			hasModel := false
			for _, model := range registry.GetGlobalRegistry().GetModelsForClient(auth.ID) {
				if model != nil && strings.EqualFold(strings.TrimSpace(model.ID), tc.wantModel) {
					hasModel = true
					break
				}
			}
			if hasModel != tc.want {
				t.Fatalf("native image model presence = %v, want %v", hasModel, tc.want)
			}
		})
	}
}

func TestRegisterModelsForAuth_CodexCustomModelsFollowPlanGroups(t *testing.T) {
	testCases := []struct {
		name     string
		planType string
		want     bool
	}{
		{name: "free excluded by groups", planType: "free", want: false},
		{name: "plus", planType: "plus", want: true},
		{name: "pro", planType: "pro", want: true},
		{name: "team", planType: "team", want: true},
		{name: "business", planType: "business", want: true},
		{name: "go", planType: "go", want: true},
		{name: "missing plan defaults to pro", planType: "", want: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			service := &Service{
				cfg: &config.Config{
					CodexCustomModels: []config.CodexCustomModel{
						{ID: "gpt-5.5-codex", DisplayName: "GPT 5.5 Codex", Groups: []string{"plus", "pro", "team", "business", "go"}},
					},
				},
			}
			auth := &coreauth.Auth{
				ID:       "auth-codex-custom-" + strings.ReplaceAll(tc.name, " ", "-"),
				Provider: "codex",
				Status:   coreauth.StatusActive,
				Attributes: map[string]string{
					"plan_type": tc.planType,
				},
			}
			reg := GlobalModelRegistry()
			reg.UnregisterClient(auth.ID)
			t.Cleanup(func() {
				reg.UnregisterClient(auth.ID)
			})

			service.registerModelsForAuth(auth)

			hasCustom := containsRegisteredModel(registry.GetGlobalRegistry().GetModelsForClient(auth.ID), "gpt-5.5-codex")
			if hasCustom != tc.want {
				t.Fatalf("custom model presence = %v, want %v", hasCustom, tc.want)
			}
		})
	}
}

func TestRegisterModelsForAuth_CodexCustomModelsCanIncludeFree(t *testing.T) {
	service := &Service{
		cfg: &config.Config{
			CodexCustomModels: []config.CodexCustomModel{
				{ID: "gpt-5.5-codex", Groups: []string{"free"}},
			},
		},
	}
	auth := &coreauth.Auth{
		ID:       "auth-codex-custom-free",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"plan_type": "free",
		},
	}
	reg := GlobalModelRegistry()
	reg.UnregisterClient(auth.ID)
	t.Cleanup(func() {
		reg.UnregisterClient(auth.ID)
	})

	service.registerModelsForAuth(auth)

	if !containsRegisteredModel(registry.GetGlobalRegistry().GetModelsForClient(auth.ID), "gpt-5.5-codex") {
		t.Fatal("expected free custom model to be registered when free is listed in groups")
	}
}

func TestRegisterModelsForAuth_CodexCustomModelsOverrideBuiltInGroups(t *testing.T) {
	testCases := []struct {
		name     string
		groups   []string
		planType string
		want     bool
	}{
		{name: "pro only removes plus built in", groups: []string{"pro"}, planType: "plus", want: false},
		{name: "pro only keeps pro", groups: []string{"pro"}, planType: "pro", want: true},
		{name: "plus only keeps plus", groups: []string{"plus"}, planType: "plus", want: true},
		{name: "plus only removes pro built in", groups: []string{"plus"}, planType: "pro", want: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			service := &Service{
				cfg: &config.Config{
					CodexCustomModels: []config.CodexCustomModel{
						{ID: "gpt-5.4-mini", DisplayName: "Custom GPT 5.4 Mini", Groups: tc.groups},
					},
				},
			}
			auth := &coreauth.Auth{
				ID:       "auth-codex-custom-override-" + strings.ReplaceAll(tc.name, " ", "-"),
				Provider: "codex",
				Status:   coreauth.StatusActive,
				Attributes: map[string]string{
					"plan_type": tc.planType,
				},
			}
			reg := GlobalModelRegistry()
			reg.UnregisterClient(auth.ID)
			t.Cleanup(func() {
				reg.UnregisterClient(auth.ID)
			})

			service.registerModelsForAuth(auth)

			hasModel := containsRegisteredModel(registry.GetGlobalRegistry().GetModelsForClient(auth.ID), "gpt-5.4-mini")
			if hasModel != tc.want {
				t.Fatalf("gpt-5.4-mini presence = %v, want %v", hasModel, tc.want)
			}
		})
	}
}

func TestRegisterModelsForAuth_CodexCustomModelsRespectExcludedModels(t *testing.T) {
	service := &Service{
		cfg: &config.Config{
			CodexCustomModels: []config.CodexCustomModel{
				{ID: "gpt-5.5-codex", Groups: []string{"plus"}},
			},
		},
	}
	auth := &coreauth.Auth{
		ID:       "auth-codex-custom-excluded",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"plan_type":       "plus",
			"excluded_models": "gpt-5.5-codex",
		},
	}
	reg := GlobalModelRegistry()
	reg.UnregisterClient(auth.ID)
	t.Cleanup(func() {
		reg.UnregisterClient(auth.ID)
	})

	service.registerModelsForAuth(auth)

	if containsRegisteredModel(registry.GetGlobalRegistry().GetModelsForClient(auth.ID), "gpt-5.5-codex") {
		t.Fatal("expected custom model to be removed by excluded_models")
	}
}

func TestRegisterModelsForAuth_CodexCustomModelsApplyOAuthAlias(t *testing.T) {
	service := &Service{
		cfg: &config.Config{
			CodexCustomModels: []config.CodexCustomModel{
				{ID: "gpt-5.5-codex", Groups: []string{"plus"}},
			},
			OAuthModelAlias: map[string][]config.OAuthModelAlias{
				"codex": {
					{Name: "gpt-5.5-codex", Alias: "codex-latest", Fork: true},
				},
			},
		},
	}
	auth := &coreauth.Auth{
		ID:       "auth-codex-custom-alias",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"plan_type": "plus",
		},
	}
	reg := GlobalModelRegistry()
	reg.UnregisterClient(auth.ID)
	t.Cleanup(func() {
		reg.UnregisterClient(auth.ID)
	})

	service.registerModelsForAuth(auth)

	models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID)
	if !containsRegisteredModel(models, "gpt-5.5-codex") {
		t.Fatal("expected original custom model to remain with fork alias")
	}
	if !containsRegisteredModel(models, "codex-latest") {
		t.Fatal("expected alias for custom model to be registered")
	}
}

func TestRegisterModelsForAuth_AntigravityFetchesWebSearchCapability(t *testing.T) {
	type requestDetails struct {
		method        string
		path          string
		authorization string
	}
	requests := make(chan requestDetails, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- requestDetails{
			method:        r.Method,
			path:          r.URL.Path,
			authorization: r.Header.Get("Authorization"),
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"models": {
				"gemini-3.1-flash-lite": {
					"displayName": "Fetched Metadata Must Not Replace Static Metadata",
					"maxTokens": 1,
					"maxOutputTokens": 2
				},
				"fetched-only-search-model": {
					"displayName": "Fetched Only Search Model"
				}
			},
			"webSearchModelIds": [
				" GEMINI-3.1-FLASH-LITE ",
				"gemini-3-flash-agent",
				"fetched-only-search-model"
			]
		}`))
	}))
	defer server.Close()

	service := &Service{
		cfg: &config.Config{
			SDKConfig: config.SDKConfig{ForceModelPrefix: true},
			OAuthModelAlias: map[string][]config.OAuthModelAlias{
				"antigravity": {
					{Name: "gemini-3.1-flash-lite", Alias: "search-capable"},
				},
			},
		},
	}
	auth := &coreauth.Auth{
		ID:       "auth-antigravity-fetch-models",
		Provider: "antigravity",
		Prefix:   "team-a",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"auth_kind":       "oauth",
			"base_url":        server.URL,
			"excluded_models": "gemini-3-flash-agent",
		},
		Metadata: map[string]any{
			"access_token": "token",
		},
	}

	reg := registry.GetGlobalRegistry()
	reg.UnregisterClient(auth.ID)
	t.Cleanup(func() {
		reg.UnregisterClient(auth.ID)
	})

	hints, okFetch := service.fetchAntigravityModelCapabilityHintsForAuth(context.Background(), auth)
	if !okFetch {
		t.Fatal("expected fetchAvailableModels capability response")
	}
	service.antigravityModelCapabilities.Store(auth.ID, &antigravityModelCapabilityCacheEntry{
		RuntimeInstanceID: auth.RuntimeInstanceID(),
		Hints:             hints,
	})
	service.registerModelsForAuth(auth)
	select {
	case request := <-requests:
		if request.method != http.MethodPost {
			t.Fatalf("method = %q, want POST", request.method)
		}
		if request.path != antigravityModelsPath {
			t.Fatalf("path = %q, want %s", request.path, antigravityModelsPath)
		}
		if request.authorization != "Bearer token" {
			t.Fatalf("Authorization = %q, want bearer token", request.authorization)
		}
	default:
		t.Fatal("expected fetchAvailableModels request")
	}

	models := reg.GetModelsForClient(auth.ID)
	staticModels := registry.GetAntigravityModels()
	if got, want := len(models), len(staticModels)-1; got != want {
		t.Fatalf("registered model count = %d, want %d static models after one exclusion", got, want)
	}

	var webSearchModel, unsupportedModel, staticOnlyModel *registry.ModelInfo
	for _, model := range models {
		if model == nil {
			continue
		}
		switch strings.TrimSpace(model.ID) {
		case "team-a/search-capable":
			webSearchModel = model
		case "team-a/gemini-3-flash":
			unsupportedModel = model
		case "team-a/gpt-oss-120b-medium":
			staticOnlyModel = model
		case "team-a/gemini-3-flash-agent":
			t.Fatal("locally excluded model should not be registered")
		case "team-a/fetched-only-search-model", "fetched-only-search-model":
			t.Fatalf("fetched-only model should not be registered: %#v", model)
		default:
			if !strings.HasPrefix(model.ID, "team-a/") {
				t.Fatalf("model %q should retain the forced prefix", model.ID)
			}
		}
	}

	if webSearchModel == nil {
		t.Fatal("expected aliased static web search model to be registered")
	}
	if !webSearchModel.SupportsWebSearch {
		t.Fatal("expected aliased static model to retain web search capability")
	}
	staticWebSearchModel := findAntigravityModelByID(staticModels, "gemini-3.1-flash-lite")
	if staticWebSearchModel == nil {
		t.Fatal("expected static gemini-3.1-flash-lite definition")
	}
	if webSearchModel.DisplayName != staticWebSearchModel.DisplayName ||
		webSearchModel.ContextLength != staticWebSearchModel.ContextLength ||
		webSearchModel.MaxCompletionTokens != staticWebSearchModel.MaxCompletionTokens {
		t.Fatalf("static metadata should be preserved, got=%#v static=%#v", webSearchModel, staticWebSearchModel)
	}
	if unsupportedModel == nil {
		t.Fatal("expected unsupported static model to remain registered")
	}
	if unsupportedModel.SupportsWebSearch {
		t.Fatal("static model absent from webSearchModelIds should not support web search")
	}
	if staticOnlyModel == nil {
		t.Fatal("expected static-only Antigravity model to remain registered")
	}
	if !reg.ClientSupportsWebSearch(auth.ID, "gemini-3.1-flash-lite(high)") {
		t.Fatal("prefixed alias should retain capability for its resolved upstream model")
	}
	if reg.ClientSupportsWebSearch(auth.ID, "team-a/search-capable") {
		t.Fatal("capability lookup should not use the public prefixed alias")
	}
}

func TestRegisterModelsForAuth_AntigravityIgnoresCacheFromReusedAuthID(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	authID := "auth-antigravity-reused-capability-cache"
	oldAuth, errRegister := manager.Register(context.Background(), &coreauth.Auth{
		ID:       authID,
		Provider: "antigravity",
		Status:   coreauth.StatusActive,
	})
	if errRegister != nil {
		t.Fatalf("register old auth: %v", errRegister)
	}
	service.antigravityModelCapabilities.Store(authID, &antigravityModelCapabilityCacheEntry{
		RuntimeInstanceID: oldAuth.RuntimeInstanceID(),
		Hints: antigravityModelCapabilityHints{
			WebSearchModelIDs: map[string]struct{}{"gemini-3.1-flash-lite": {}},
		},
	})
	if errDelete := manager.Delete(context.Background(), authID); errDelete != nil {
		t.Fatalf("delete old auth: %v", errDelete)
	}
	newAuth, errRegister := manager.Register(context.Background(), &coreauth.Auth{
		ID:       authID,
		Provider: "antigravity",
		Status:   coreauth.StatusActive,
	})
	if errRegister != nil {
		t.Fatalf("register reused auth ID: %v", errRegister)
	}
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })

	service.registerModelsForAuth(newAuth)

	if _, exists := service.antigravityModelCapabilities.Load(authID); exists {
		t.Fatal("cache entry from the retired auth instance was not removed")
	}
	for _, model := range registry.GetGlobalRegistry().GetModelsForClient(authID) {
		if model != nil && model.SupportsWebSearch {
			t.Fatalf("reused auth inherited unverified web search capability for %q", model.ID)
		}
	}
}

func findAntigravityModelByID(models []*registry.ModelInfo, modelID string) *registry.ModelInfo {
	for _, model := range models {
		if model != nil && strings.EqualFold(strings.TrimSpace(model.ID), strings.TrimSpace(modelID)) {
			return model
		}
	}
	return nil
}

func assertOnlyRegisteredModels(t *testing.T, models []*registry.ModelInfo, wants ...string) {
	t.Helper()
	wantSet := make(map[string]struct{}, len(wants))
	for _, want := range wants {
		want = strings.ToLower(strings.TrimSpace(want))
		if want != "" {
			wantSet[want] = struct{}{}
		}
	}
	gotSet := make(map[string]struct{}, len(models))
	for _, model := range models {
		if model == nil {
			continue
		}
		modelID := strings.ToLower(strings.TrimSpace(model.ID))
		if modelID == "" {
			continue
		}
		gotSet[modelID] = struct{}{}
		if _, ok := wantSet[modelID]; !ok {
			t.Fatalf("unexpected model %q, got all models %v, want only %v", model.ID, registeredModelIDs(models), wants)
		}
	}
	if len(gotSet) != len(wantSet) {
		t.Fatalf("registered models = %v, want only %v", registeredModelIDs(models), wants)
	}
	for want := range wantSet {
		if _, ok := gotSet[want]; !ok {
			t.Fatalf("registered models = %v, want %q", registeredModelIDs(models), want)
		}
	}
}

func registeredModelIDs(models []*registry.ModelInfo) []string {
	ids := make([]string, 0, len(models))
	for _, model := range models {
		if model == nil {
			continue
		}
		modelID := strings.TrimSpace(model.ID)
		if modelID != "" {
			ids = append(ids, modelID)
		}
	}
	return ids
}

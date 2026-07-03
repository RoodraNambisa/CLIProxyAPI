package cliproxy

import (
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
				"gemini-cli": {"gemini-2.5-pro"},
			},
		},
	}
	auth := &coreauth.Auth{
		ID:       "auth-gemini-cli",
		Provider: "gemini-cli",
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

	models := registry.GetAvailableModelsByProvider("gemini-cli")
	if len(models) == 0 {
		t.Fatal("expected gemini-cli models to be registered")
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

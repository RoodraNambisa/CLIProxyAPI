package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSanitizeOAuthModelAlias_PreservesForkFlag(t *testing.T) {
	cfg := &Config{
		OAuthModelAlias: map[string][]OAuthModelAlias{
			" CoDeX ": {
				{Name: " gpt-5 ", Alias: " g5 ", Fork: true, ForceMapping: true},
				{Name: "gpt-6", Alias: "g6"},
			},
		},
	}

	cfg.SanitizeOAuthModelAlias()

	aliases := cfg.OAuthModelAlias["codex"]
	if len(aliases) != 2 {
		t.Fatalf("expected 2 sanitized aliases, got %d", len(aliases))
	}
	if aliases[0].Name != "gpt-5" || aliases[0].Alias != "g5" || !aliases[0].Fork || !aliases[0].ForceMapping {
		t.Fatalf("expected first alias to be gpt-5->g5 fork=true force-mapping=true, got name=%q alias=%q fork=%v force-mapping=%v", aliases[0].Name, aliases[0].Alias, aliases[0].Fork, aliases[0].ForceMapping)
	}
	if aliases[1].Name != "gpt-6" || aliases[1].Alias != "g6" || aliases[1].Fork {
		t.Fatalf("expected second alias to be gpt-6->g6 fork=false, got name=%q alias=%q fork=%v", aliases[1].Name, aliases[1].Alias, aliases[1].Fork)
	}
}

func TestSanitizeOAuthModelAlias_AllowsMultipleAliasesForSameName(t *testing.T) {
	cfg := &Config{
		OAuthModelAlias: map[string][]OAuthModelAlias{
			"antigravity": {
				{Name: "gemini-claude-opus-4-5-thinking", Alias: "claude-opus-4-5-20251101", Fork: true},
				{Name: "gemini-claude-opus-4-5-thinking", Alias: "claude-opus-4-5-20251101-thinking", Fork: true},
				{Name: "gemini-claude-opus-4-5-thinking", Alias: "claude-opus-4-5", Fork: true},
			},
		},
	}

	cfg.SanitizeOAuthModelAlias()

	aliases := cfg.OAuthModelAlias["antigravity"]
	expected := []OAuthModelAlias{
		{Name: "gemini-claude-opus-4-5-thinking", Alias: "claude-opus-4-5-20251101", Fork: true},
		{Name: "gemini-claude-opus-4-5-thinking", Alias: "claude-opus-4-5-20251101-thinking", Fork: true},
		{Name: "gemini-claude-opus-4-5-thinking", Alias: "claude-opus-4-5", Fork: true},
	}
	if len(aliases) != len(expected) {
		t.Fatalf("expected %d sanitized aliases, got %d", len(expected), len(aliases))
	}
	for i, exp := range expected {
		if aliases[i].Name != exp.Name || aliases[i].Alias != exp.Alias || aliases[i].Fork != exp.Fork {
			t.Fatalf("expected alias %d to be name=%q alias=%q fork=%v, got name=%q alias=%q fork=%v", i, exp.Name, exp.Alias, exp.Fork, aliases[i].Name, aliases[i].Alias, aliases[i].Fork)
		}
	}
}

func TestSanitizeCodexCustomModels(t *testing.T) {
	cfg := &Config{
		CodexCustomModels: []CodexCustomModel{
			{ID: " ", DisplayName: "missing id", Groups: []string{"plus"}},
			{ID: "gpt-empty-groups", Groups: []string{"unknown"}},
			{ID: " gpt-5.5-codex ", DisplayName: " GPT 5.5 Codex ", Groups: []string{" Pro ", "plus", "PLUS", "unknown"}},
			{ID: "GPT-5.5-CODEX", DisplayName: "ignored duplicate", Groups: []string{"team", "go"}},
			{ID: "gpt-5.5-mini-codex", Groups: []string{"business", "free"}},
		},
	}

	cfg.SanitizeCodexCustomModels()

	if len(cfg.CodexCustomModels) != 2 {
		t.Fatalf("expected 2 sanitized custom models, got %d", len(cfg.CodexCustomModels))
	}

	first := cfg.CodexCustomModels[0]
	if first.ID != "gpt-5.5-codex" || first.DisplayName != "GPT 5.5 Codex" {
		t.Fatalf("unexpected first custom model: %+v", first)
	}
	expectedFirstGroups := []string{"plus", "pro", "team", "go"}
	if len(first.Groups) != len(expectedFirstGroups) {
		t.Fatalf("first groups = %v, want %v", first.Groups, expectedFirstGroups)
	}
	for i, group := range expectedFirstGroups {
		if first.Groups[i] != group {
			t.Fatalf("first groups = %v, want %v", first.Groups, expectedFirstGroups)
		}
	}

	second := cfg.CodexCustomModels[1]
	if second.ID != "gpt-5.5-mini-codex" || second.DisplayName != "gpt-5.5-mini-codex" {
		t.Fatalf("unexpected second custom model: %+v", second)
	}
	expectedSecondGroups := []string{"free", "business"}
	if len(second.Groups) != len(expectedSecondGroups) {
		t.Fatalf("second groups = %v, want %v", second.Groups, expectedSecondGroups)
	}
	for i, group := range expectedSecondGroups {
		if second.Groups[i] != group {
			t.Fatalf("second groups = %v, want %v", second.Groups, expectedSecondGroups)
		}
	}
}

func TestNormalizeStatusCodes(t *testing.T) {
	got := NormalizeStatusCodes([]int{0, 429, 401, 429, 99, 600, 500})
	want := []int{429, 401, 500}
	if len(got) != len(want) {
		t.Fatalf("status codes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("status codes = %v, want %v", got, want)
		}
	}
}

func TestNormalizeFixedErrorCooldowns(t *testing.T) {
	got := NormalizeFixedErrorCooldowns([]FixedErrorCooldownRule{
		{StatusCode: 0, MessageContains: " message only ", CooldownSeconds: 60, Scope: "auth"},
		{StatusCode: 401, MessageContains: " token invalidated ", CooldownSeconds: 3600, Scope: " AUTH "},
		{StatusCode: 429, CooldownSeconds: 30},
		{StatusCode: 0, CooldownSeconds: 60, Scope: "auth"},
		{StatusCode: 500, CooldownSeconds: -1, Scope: "model"},
		{StatusCode: 503, CooldownSeconds: 10, Scope: "bad"},
	})
	want := []FixedErrorCooldownRule{
		{StatusCode: 0, MessageContains: "message only", CooldownSeconds: 60, Scope: "auth"},
		{StatusCode: 401, MessageContains: "token invalidated", CooldownSeconds: 3600, Scope: "auth"},
		{StatusCode: 429, CooldownSeconds: 30, Scope: "model"},
	}
	if len(got) != len(want) {
		t.Fatalf("rules = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("rules = %#v, want %#v", got, want)
		}
	}
}

func TestLoadConfigOptional_NonRetryableErrorsDefaultAndExplicitEmpty(t *testing.T) {
	t.Run("default image errors", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(`request-retry: 2`), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}

		cfg, err := LoadConfigOptional(path, false)
		if err != nil {
			t.Fatalf("LoadConfigOptional() error = %v", err)
		}
		if len(cfg.NonRetryableErrors) != 2 {
			t.Fatalf("NonRetryableErrors len = %d, want 2", len(cfg.NonRetryableErrors))
		}
		if cfg.NonRetryableErrors[0].StatusCode != 400 ||
			cfg.NonRetryableErrors[0].Type != "image_generation_user_error" ||
			cfg.NonRetryableErrors[0].Code != "invalid_value" {
			t.Fatalf("first default rule = %+v", cfg.NonRetryableErrors[0])
		}
	})

	t.Run("explicit empty disables defaults", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(`non-retryable-errors: []`), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}

		cfg, err := LoadConfigOptional(path, false)
		if err != nil {
			t.Fatalf("LoadConfigOptional() error = %v", err)
		}
		if len(cfg.NonRetryableErrors) != 0 {
			t.Fatalf("NonRetryableErrors = %+v, want empty", cfg.NonRetryableErrors)
		}
	})
}

func TestNormalizeNonRetryableErrorRules(t *testing.T) {
	input := []NonRetryableErrorRule{
		{StatusCode: 99, Type: "bad", Code: "bad"},
		{StatusCode: 400, Type: " Image_Generation_User_Error ", Code: " INVALID_VALUE "},
		{StatusCode: 400, Type: "image_generation_user_error", Code: "invalid_value"},
		{MessageContains: " Safety System "},
		{},
	}
	got := NormalizeNonRetryableErrorRules(input)
	want := []NonRetryableErrorRule{
		{StatusCode: 400, Type: "image_generation_user_error", Code: "invalid_value"},
		{MessageContains: "safety system"},
	}
	if len(got) != len(want) {
		t.Fatalf("rules = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("rules = %#v, want %#v", got, want)
		}
	}
}

func TestNormalizeAuthModelExclusionRules(t *testing.T) {
	got := NormalizeAuthModelExclusionRules([]AuthModelExclusionRule{
		{Models: []string{"gpt-image-2"}},
		{Models: []string{" gpt-image-2 ", "GPT-IMAGE-2", ""}, Priorities: []int{-1, -1, 0}},
		{Models: []string{"gpt-image-1.5"}, Providers: []string{" CoDeX ", "codex"}, KeywordContains: []string{" Free ", "free", ""}},
		{Models: []string{" -ALL ", "+GPT-5.5", "+gpt-5.5"}, Priorities: []int{1, 1}, KeywordContains: []string{" Team ", "team"}},
		{DisableImageGeneration: true, Priorities: []int{-1, -1}, KeywordContains: []string{" Trial "}},
		{Priorities: []int{0}},
	})
	if len(got) != 4 {
		t.Fatalf("rules = %#v, want 4 valid rules", got)
	}
	if len(got[0].Models) != 1 || got[0].Models[0] != "gpt-image-2" {
		t.Fatalf("first models = %#v", got[0].Models)
	}
	if len(got[0].Priorities) != 2 || got[0].Priorities[0] != -1 || got[0].Priorities[1] != 0 {
		t.Fatalf("first priorities = %#v", got[0].Priorities)
	}
	if len(got[1].Providers) != 1 || got[1].Providers[0] != "codex" {
		t.Fatalf("second providers = %#v", got[1].Providers)
	}
	if len(got[1].KeywordContains) != 1 || got[1].KeywordContains[0] != "free" {
		t.Fatalf("second keywords = %#v", got[1].KeywordContains)
	}
	if len(got[2].Models) != 2 || got[2].Models[0] != "-all" || got[2].Models[1] != "+gpt-5.5" {
		t.Fatalf("third models = %#v", got[2].Models)
	}
	if len(got[2].Priorities) != 1 || got[2].Priorities[0] != 1 {
		t.Fatalf("third priorities = %#v", got[2].Priorities)
	}
	if len(got[2].KeywordContains) != 1 || got[2].KeywordContains[0] != "team" {
		t.Fatalf("third keywords = %#v", got[2].KeywordContains)
	}
	if len(got[3].Models) != 0 || !got[3].DisableImageGeneration {
		t.Fatalf("fourth rule = %#v, want disable-image-generation rule without models", got[3])
	}
	if len(got[3].Priorities) != 1 || got[3].Priorities[0] != -1 {
		t.Fatalf("fourth priorities = %#v", got[3].Priorities)
	}
	if len(got[3].KeywordContains) != 1 || got[3].KeywordContains[0] != "trial" {
		t.Fatalf("fourth keywords = %#v", got[3].KeywordContains)
	}
}

func TestNormalizeDisabledImageGenerationToolDefaults(t *testing.T) {
	cfg := &Config{}
	if err := cfg.NormalizeDisabledImageGenerationTool(); err != nil {
		t.Fatalf("NormalizeDisabledImageGenerationTool() error = %v", err)
	}
	if cfg.DisabledImageGenerationToolAction != DisabledImageGenerationToolActionRemove {
		t.Fatalf("action = %q, want %q", cfg.DisabledImageGenerationToolAction, DisabledImageGenerationToolActionRemove)
	}
	if cfg.DisabledImageGenerationToolError.StatusCode != DefaultDisabledImageGenerationToolStatusCode {
		t.Fatalf("status = %d, want %d", cfg.DisabledImageGenerationToolError.StatusCode, DefaultDisabledImageGenerationToolStatusCode)
	}
	if cfg.DisabledImageGenerationToolError.Message != DefaultDisabledImageGenerationToolMessage {
		t.Fatalf("message = %q, want default", cfg.DisabledImageGenerationToolError.Message)
	}
	if cfg.DisabledImageGenerationToolError.Type != DefaultDisabledImageGenerationToolType {
		t.Fatalf("type = %q, want default", cfg.DisabledImageGenerationToolError.Type)
	}
	if cfg.DisabledImageGenerationToolError.Code != DefaultDisabledImageGenerationToolCode {
		t.Fatalf("code = %q, want default", cfg.DisabledImageGenerationToolError.Code)
	}
}

func TestNormalizeDisabledImageGenerationToolRejectsInvalidAction(t *testing.T) {
	cfg := &Config{DisabledImageGenerationToolAction: "block"}
	if err := cfg.NormalizeDisabledImageGenerationTool(); err == nil {
		t.Fatal("NormalizeDisabledImageGenerationTool() error = nil, want invalid action error")
	}
}

func TestLoadConfigOptionalRejectsInvalidDisabledImageGenerationToolAction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(`disabled-image-generation-tool-action: block`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if _, err := LoadConfigOptional(path, false); err == nil {
		t.Fatal("LoadConfigOptional() error = nil, want invalid action error")
	}
}

package cliproxy

import (
	"strconv"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestImagePriorityScopeModelRegistration(t *testing.T) {
	cfg := &config.Config{
		SDKConfig: config.SDKConfig{Images: config.ImagesConfig{ImageModel: "gpt-image-2"}},
		AuthModelExclusions: []config.AuthModelExclusionRule{{
			Providers: []string{"codex"}, Priorities: []int{3, 5}, Models: []string{"excluded"},
			DisableImageGeneration: true, DisableImageGenerationPriorities: []int{5},
		}},
	}
	models := []*ModelInfo{{ID: "text"}, {ID: "excluded"}, {ID: "gpt-image-2"}}
	for _, priority := range []int{3, 5} {
		a := &coreauth.Auth{Provider: "codex", Attributes: map[string]string{"priority": strconv.Itoa(priority)}}
		got := applyAuthModelExclusions(cfg, a, "codex", models)
		if containsRegisteredModel(got, "excluded") || !containsRegisteredModel(got, "text") ||
			containsRegisteredModel(got, "gpt-image-2") != (priority == 3) {
			t.Fatalf("priority %d models = %v", priority, registeredModelIDs(got))
		}
	}
	before := authModelExclusionsSignature(cfg)
	cfg.AuthModelExclusions[0].DisableImageGenerationPriorities = []int{3}
	if before == authModelExclusionsSignature(cfg) {
		t.Fatal("scope update did not invalidate model registration")
	}
	a := &coreauth.Auth{Provider: "codex", Attributes: map[string]string{"priority": "5"}}
	if !containsRegisteredModel(applyAuthModelExclusions(cfg, a, "codex", models), "gpt-image-2") {
		t.Fatal("scope update did not restore the now-unrestricted image model")
	}
	cfg.AuthModelExclusions[0].Models = []string{"-all", "+text"}
	if containsRegisteredModel(applyAuthModelExclusions(cfg, a, "codex", models), "gpt-image-2") {
		t.Fatal("image scope bypassed the shared model allowlist")
	}
	cfg.AuthModelExclusions[0].Models = []string{"gpt-image-2"}
	if containsRegisteredModel(applyAuthModelExclusions(cfg, a, "codex", models), "gpt-image-2") {
		t.Fatal("image scope bypassed the explicit model exclusion")
	}
}

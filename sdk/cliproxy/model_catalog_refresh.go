package cliproxy

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func appendModelCatalogOverrides[T modelEntry](out [][]string, family string, index int, models []T) [][]string {
	for _, model := range models {
		label, limit := strings.TrimSpace(model.GetDisplayName()), model.GetMaxContextLength()
		thinking := config.ModelThinkingSignature(model.GetThinking())
		modalities := ""
		if declared, ok := any(model).(interface{ GetInputModalities() []string }); ok {
			modalities = strings.Join(declared.GetInputModalities(), ",")
		}
		if label != "" || limit > 0 || thinking != "" || model.GetIsCompat() || modalities != "" {
			out = append(out, []string{family, strconv.Itoa(index), model.GetName(), model.GetAlias(), label, strconv.Itoa(limit), thinking, strconv.FormatBool(model.GetIsCompat()), modalities})
		}
	}
	return out
}

// Catalog declarations are excluded from credential routing hashes. Editing them
// must not retire an in-flight credential or clear its availability state.
func configuredModelCatalogOverrides(cfg *config.Config) [][]string {
	if cfg == nil {
		return nil
	}
	var out [][]string
	for i, entry := range cfg.GeminiKey {
		out = appendModelCatalogOverrides(out, "gemini", i, entry.Models)
	}
	for i, entry := range cfg.InteractionsKey {
		out = appendModelCatalogOverrides(out, "interactions", i, entry.Models)
	}
	for i, entry := range cfg.ClaudeKey {
		out = appendModelCatalogOverrides(out, "claude", i, entry.Models)
	}
	for i, entry := range cfg.CodexKey {
		out = appendModelCatalogOverrides(out, "codex", i, entry.Models)
	}
	for i, entry := range cfg.XAIKey {
		out = appendModelCatalogOverrides(out, "xai", i, entry.Models)
	}
	for i, entry := range cfg.VertexCompatAPIKey {
		out = appendModelCatalogOverrides(out, "vertex", i, entry.Models)
	}
	for i, entry := range cfg.OpenAICompatibility {
		out = appendModelCatalogOverrides(out, "openai", i, entry.Models)
	}
	providers := make([]string, 0, len(cfg.OAuthModelAlias))
	for provider := range cfg.OAuthModelAlias {
		providers = append(providers, provider)
	}
	sort.Strings(providers)
	for _, provider := range providers {
		for index, alias := range cfg.OAuthModelAlias[provider] {
			if label := strings.TrimSpace(alias.DisplayName); label != "" {
				out = append(out, []string{"oauth", provider, strconv.Itoa(index), alias.Name, alias.Alias, label})
			}
		}
	}
	return out
}

func (s *Service) refreshConfiguredModelCatalog(ctx context.Context, before, after *config.Config) error {
	if s == nil || s.coreManager == nil || reflect.DeepEqual(configuredModelCatalogOverrides(before), configuredModelCatalogOverrides(after)) {
		return nil
	}
	for _, summary := range s.coreManager.ListMetadataSummaries("type", "provider_key", "compat_name") {
		current, exists := s.coreManager.GetByID(summary.ID)
		if !exists || current == nil || isNativeChatGPTWebAuth(current) {
			continue
		}
		if err := func() error {
			lockedCtx, unlockMutation, err := s.coreManager.LockAuthMutation(ctx, current)
			if err != nil {
				return err
			}
			defer unlockMutation()
			unlockTransition, err := s.lockAuthModelTransitionContext(lockedCtx, current.ID)
			if err != nil {
				return err
			}
			defer unlockTransition()
			installed, ok := s.coreManager.CurrentAuthInstallation(current)
			if !ok {
				return nil
			}
			s.registerModelsForAuthPreservingState(installed)
			return nil
		}(); err != nil {
			return fmt.Errorf("refresh model catalog overrides: %w", err)
		}
	}
	return nil
}

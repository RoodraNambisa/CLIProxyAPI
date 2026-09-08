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

func appendModelDisplayNames[T modelEntry](out [][]string, family string, index int, models []T) [][]string {
	for _, model := range models {
		if label := strings.TrimSpace(model.GetDisplayName()); label != "" {
			out = append(out, []string{family, strconv.Itoa(index), model.GetName(), model.GetAlias(), label})
		}
	}
	return out
}

// Catalog labels are excluded from credential routing hashes. Changing a label
// must not retire an in-flight credential or clear its availability state.
func configuredModelDisplayNames(cfg *config.Config) [][]string {
	if cfg == nil {
		return nil
	}
	var out [][]string
	for i, entry := range cfg.GeminiKey {
		out = appendModelDisplayNames(out, "gemini", i, entry.Models)
	}
	for i, entry := range cfg.InteractionsKey {
		out = appendModelDisplayNames(out, "interactions", i, entry.Models)
	}
	for i, entry := range cfg.ClaudeKey {
		out = appendModelDisplayNames(out, "claude", i, entry.Models)
	}
	for i, entry := range cfg.CodexKey {
		out = appendModelDisplayNames(out, "codex", i, entry.Models)
	}
	for i, entry := range cfg.VertexCompatAPIKey {
		out = appendModelDisplayNames(out, "vertex", i, entry.Models)
	}
	for i, entry := range cfg.OpenAICompatibility {
		out = appendModelDisplayNames(out, "openai", i, entry.Models)
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

func (s *Service) refreshConfiguredModelDisplayNames(ctx context.Context, before, after *config.Config) error {
	if s == nil || s.coreManager == nil || reflect.DeepEqual(configuredModelDisplayNames(before), configuredModelDisplayNames(after)) {
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
			return fmt.Errorf("refresh model display labels: %w", err)
		}
	}
	return nil
}

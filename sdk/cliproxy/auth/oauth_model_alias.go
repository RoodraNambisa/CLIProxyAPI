package auth

import (
	"strings"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
)

type modelAliasEntry interface {
	GetName() string
	GetAlias() string
	GetForceMapping() bool
}

type oauthModelAliasEntry struct {
	upstreamModel string
	configAlias   string
	forceMapping  bool
}

type oauthModelAliasTable struct {
	// reverse maps channel -> alias (lower) -> entry with upstream model and flags.
	reverse map[string]map[string]oauthModelAliasEntry
}

type OAuthModelAliasResult struct {
	UpstreamModel string
	ForceMapping  bool
	OriginalAlias string
}

func compileOAuthModelAliasTable(aliases map[string][]internalconfig.OAuthModelAlias) *oauthModelAliasTable {
	if len(aliases) == 0 {
		return &oauthModelAliasTable{}
	}
	out := &oauthModelAliasTable{
		reverse: make(map[string]map[string]oauthModelAliasEntry, len(aliases)),
	}
	for rawChannel, entries := range aliases {
		channel := strings.ToLower(strings.TrimSpace(rawChannel))
		if channel == "" || len(entries) == 0 {
			continue
		}
		rev := make(map[string]oauthModelAliasEntry, len(entries))
		for _, entry := range entries {
			name := strings.TrimSpace(entry.Name)
			alias := strings.TrimSpace(entry.Alias)
			if name == "" || alias == "" {
				continue
			}
			if strings.EqualFold(name, alias) {
				continue
			}
			aliasKey := strings.ToLower(alias)
			if _, exists := rev[aliasKey]; exists {
				continue
			}
			rev[aliasKey] = oauthModelAliasEntry{
				upstreamModel: name,
				configAlias:   alias,
				forceMapping:  entry.ForceMapping,
			}
		}
		if len(rev) > 0 {
			out.reverse[channel] = rev
		}
	}
	if len(out.reverse) == 0 {
		out.reverse = nil
	}
	return out
}

// SetOAuthModelAlias updates the OAuth model name alias table used during execution.
// The alias is applied per-auth channel to resolve the upstream model name while keeping the
// client-visible model name unchanged for translation/response formatting.
func (m *Manager) SetOAuthModelAlias(aliases map[string][]internalconfig.OAuthModelAlias) {
	if m == nil {
		return
	}
	table := compileOAuthModelAliasTable(aliases)
	// atomic.Value requires non-nil store values.
	if table == nil {
		table = &oauthModelAliasTable{}
	}
	m.oauthModelAlias.Store(table)
}

// applyOAuthModelAlias resolves the upstream model from OAuth model alias.
// If an alias exists, the returned model is the upstream model.
func (m *Manager) applyOAuthModelAlias(auth *Auth, requestedModel string) string {
	upstreamModel := m.resolveOAuthUpstreamModel(auth, requestedModel)
	if upstreamModel == "" {
		return requestedModel
	}
	return upstreamModel
}

func modelAliasLookupCandidates(requestedModel string) (thinking.SuffixResult, []string) {
	requestedModel = strings.TrimSpace(requestedModel)
	if requestedModel == "" {
		return thinking.SuffixResult{}, nil
	}
	requestResult := thinking.ParseSuffix(requestedModel)
	base := requestResult.ModelName
	if base == "" {
		base = requestedModel
	}
	candidates := []string{base}
	if base != requestedModel {
		candidates = append(candidates, requestedModel)
	}
	return requestResult, candidates
}

func preserveResolvedModelSuffix(resolved string, requestResult thinking.SuffixResult) string {
	resolved = strings.TrimSpace(resolved)
	if resolved == "" {
		return ""
	}
	if thinking.ParseSuffix(resolved).HasSuffix {
		return resolved
	}
	if requestResult.HasSuffix && requestResult.RawSuffix != "" {
		return resolved + "(" + requestResult.RawSuffix + ")"
	}
	return resolved
}

func oauthModelAliasForceMappingResponseModel(configAlias string) string {
	return strings.TrimSpace(configAlias)
}

func resolveModelAliasPoolFromConfigModels(requestedModel string, models []modelAliasEntry) []string {
	requestedModel = strings.TrimSpace(requestedModel)
	if requestedModel == "" {
		return nil
	}
	if len(models) == 0 {
		return nil
	}

	requestResult, candidates := modelAliasLookupCandidates(requestedModel)
	if len(candidates) == 0 {
		return nil
	}

	out := make([]string, 0)
	seen := make(map[string]struct{})
	for i := range models {
		name := strings.TrimSpace(models[i].GetName())
		alias := strings.TrimSpace(models[i].GetAlias())
		for _, candidate := range candidates {
			if candidate == "" || alias == "" || !strings.EqualFold(alias, candidate) {
				continue
			}
			resolved := candidate
			if name != "" {
				resolved = name
			}
			resolved = preserveResolvedModelSuffix(resolved, requestResult)
			key := strings.ToLower(strings.TrimSpace(resolved))
			if key == "" {
				break
			}
			if _, exists := seen[key]; exists {
				break
			}
			seen[key] = struct{}{}
			out = append(out, resolved)
			break
		}
	}
	if len(out) > 0 {
		return out
	}

	for i := range models {
		name := strings.TrimSpace(models[i].GetName())
		for _, candidate := range candidates {
			if candidate == "" || name == "" || !strings.EqualFold(name, candidate) {
				continue
			}
			return []string{preserveResolvedModelSuffix(name, requestResult)}
		}
	}
	return nil
}

func resolveModelAliasFromConfigModels(requestedModel string, models []modelAliasEntry) string {
	resolved := resolveModelAliasPoolFromConfigModels(requestedModel, models)
	if len(resolved) > 0 {
		return resolved[0]
	}
	return ""
}

func resolveModelAliasResultFromConfigModels(requestedModel string, models []modelAliasEntry) OAuthModelAliasResult {
	requestedModel = strings.TrimSpace(requestedModel)
	if requestedModel == "" || len(models) == 0 {
		return OAuthModelAliasResult{}
	}
	requestResult, candidates := modelAliasLookupCandidates(requestedModel)
	if len(candidates) == 0 {
		return OAuthModelAliasResult{}
	}
	baseModel := requestResult.ModelName
	if baseModel == "" {
		baseModel = requestedModel
	}
	for i := range models {
		original := strings.TrimSpace(models[i].GetName())
		alias := strings.TrimSpace(models[i].GetAlias())
		if original == "" || alias == "" {
			continue
		}
		for _, candidate := range candidates {
			key := strings.TrimSpace(candidate)
			if key == "" || !strings.EqualFold(alias, key) {
				continue
			}
			if strings.EqualFold(original, baseModel) {
				if !models[i].GetForceMapping() {
					return OAuthModelAliasResult{}
				}
				return OAuthModelAliasResult{
					UpstreamModel: preserveResolvedModelSuffix(original, requestResult),
					ForceMapping:  true,
					OriginalAlias: oauthModelAliasForceMappingResponseModel(alias),
				}
			}
			originalAlias := requestedModel
			if models[i].GetForceMapping() {
				originalAlias = oauthModelAliasForceMappingResponseModel(alias)
			}
			return OAuthModelAliasResult{
				UpstreamModel: preserveResolvedModelSuffix(original, requestResult),
				ForceMapping:  models[i].GetForceMapping(),
				OriginalAlias: originalAlias,
			}
		}
	}
	return OAuthModelAliasResult{}
}

// resolveOAuthUpstreamModel resolves the upstream model name from OAuth model alias.
// If an alias exists, returns the original (upstream) model name that corresponds
// to the requested alias.
//
// If the requested model contains a thinking suffix (e.g., "gemini-2.5-pro(8192)"),
// the suffix is preserved in the returned model name. However, if the alias's
// original name already contains a suffix, the config suffix takes priority.
func (m *Manager) resolveOAuthUpstreamModel(auth *Auth, requestedModel string) string {
	return m.resolveOAuthModelAliasWithResult(auth, requestedModel).UpstreamModel
}

func (m *Manager) applyOAuthModelAliasWithResult(auth *Auth, requestedModel string) OAuthModelAliasResult {
	result := m.resolveOAuthModelAliasWithResult(auth, requestedModel)
	if result.UpstreamModel == "" {
		return OAuthModelAliasResult{UpstreamModel: requestedModel}
	}
	return result
}

func (m *Manager) resolveOAuthModelAliasWithResult(auth *Auth, requestedModel string) OAuthModelAliasResult {
	return resolveUpstreamModelFromAliasTable(m, auth, requestedModel, modelAliasChannel(auth))
}

func resolveUpstreamModelFromAliasTable(m *Manager, auth *Auth, requestedModel, channel string) OAuthModelAliasResult {
	if m == nil || auth == nil {
		return OAuthModelAliasResult{}
	}
	if channel == "" {
		return OAuthModelAliasResult{}
	}

	// Extract thinking suffix from requested model using ParseSuffix
	requestResult := thinking.ParseSuffix(requestedModel)
	baseModel := requestResult.ModelName

	// Candidate keys to match: base model and raw input (handles suffix-parsing edge cases).
	candidates := []string{baseModel}
	if baseModel != requestedModel {
		candidates = append(candidates, requestedModel)
	}

	raw := m.oauthModelAlias.Load()
	table, _ := raw.(*oauthModelAliasTable)
	if table == nil || table.reverse == nil {
		return OAuthModelAliasResult{}
	}
	rev := table.reverse[channel]
	if rev == nil {
		return OAuthModelAliasResult{}
	}

	for _, candidate := range candidates {
		key := strings.ToLower(strings.TrimSpace(candidate))
		if key == "" {
			continue
		}
		entry, exists := rev[key]
		if !exists {
			continue
		}
		original := strings.TrimSpace(entry.upstreamModel)
		if original == "" {
			continue
		}
		if strings.EqualFold(original, baseModel) {
			if !entry.forceMapping {
				return OAuthModelAliasResult{}
			}
			return OAuthModelAliasResult{
				UpstreamModel: preserveResolvedModelSuffix(original, requestResult),
				ForceMapping:  true,
				OriginalAlias: oauthModelAliasForceMappingResponseModel(entry.configAlias),
			}
		}

		upstreamModel := original
		if !thinking.ParseSuffix(original).HasSuffix && requestResult.HasSuffix && requestResult.RawSuffix != "" {
			upstreamModel = original + "(" + requestResult.RawSuffix + ")"
		}
		originalAlias := requestedModel
		if entry.forceMapping {
			originalAlias = oauthModelAliasForceMappingResponseModel(entry.configAlias)
		}
		return OAuthModelAliasResult{
			UpstreamModel: upstreamModel,
			ForceMapping:  entry.forceMapping,
			OriginalAlias: originalAlias,
		}
	}

	return OAuthModelAliasResult{}
}

// modelAliasChannel extracts the OAuth model alias channel from an Auth object.
// It determines the provider and auth kind from the Auth's attributes and delegates
// to OAuthModelAliasChannel for the actual channel resolution.
func modelAliasChannel(auth *Auth) string {
	if auth == nil {
		return ""
	}
	provider := strings.ToLower(strings.TrimSpace(auth.Provider))
	authKind := ""
	if auth.Attributes != nil {
		authKind = strings.ToLower(strings.TrimSpace(auth.Attributes["auth_kind"]))
	}
	if authKind == "" {
		if kind, _ := auth.AccountInfo(); strings.EqualFold(kind, "api_key") {
			authKind = "apikey"
		}
	}
	return OAuthModelAliasChannel(provider, authKind)
}

// OAuthModelAliasChannel returns the OAuth model alias channel name for a given provider
// and auth kind. Returns empty string if the provider/authKind combination doesn't support
// OAuth model alias (e.g., API key authentication).
//
// Supported channels: vertex, aistudio, antigravity, claude, codex, kimi, xai.
func OAuthModelAliasChannel(provider, authKind string) string {
	provider = strings.ToLower(strings.TrimSpace(provider))
	authKind = strings.ToLower(strings.TrimSpace(authKind))
	switch provider {
	case "gemini":
		// Gemini uses gemini-api-key config, not oauth-model-alias.
		return ""
	case "vertex":
		if authKind == "apikey" {
			return ""
		}
		return "vertex"
	case "claude":
		if authKind == "apikey" {
			return ""
		}
		return "claude"
	case "codex":
		if authKind == "apikey" {
			return ""
		}
		return "codex"
	case "xai":
		if authKind == "apikey" {
			return ""
		}
		return "xai"
	case "aistudio", "antigravity", "kimi":
		return provider
	default:
		return ""
	}
}

package auth

import (
	"maps"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

const resolvedAPIKeyModelInfoMetadataKey = "cliproxy.resolved_api_key_model_info"

type apiKeyModelCapabilityRoute struct {
	upstreamModel string
	modelInfo     *registry.ModelInfo
}

type apiKeyModelCapabilityTable map[string]map[string][]apiKeyModelCapabilityRoute

// The published maps contain model definitions, never live credential instances.
type apiKeyModelRoutingSnapshot struct {
	config       *config.Config
	aliases      apiKeyModelAliasTable
	capabilities apiKeyModelCapabilityTable
}

type resolvedAPIKeyModelInfo struct{ info *registry.ModelInfo }

func (m *Manager) loadAPIKeyModelRouting() *apiKeyModelRoutingSnapshot {
	if m != nil {
		if snapshot := m.apiKeyModelRouting.Load(); snapshot != nil {
			return snapshot
		}
	}
	return &apiKeyModelRoutingSnapshot{config: &config.Config{}}
}

func (m *Manager) modelRoutingForAttempt(snapshots []*apiKeyModelRoutingSnapshot) *apiKeyModelRoutingSnapshot {
	if len(snapshots) > 0 && snapshots[0] != nil {
		return snapshots[0]
	}
	return m.loadAPIKeyModelRouting()
}

func buildAPIKeyModelRoutingSnapshot(auths map[string]*Auth, cfg *config.Config) *apiKeyModelRoutingSnapshot {
	cfg = copyAPIKeyModelRoutingConfig(cfg)
	capabilities := make(apiKeyModelCapabilityTable)
	for id, auth := range auths {
		if models := compileAPIKeyModelCapabilitiesForAuth(cfg, auth); len(models) > 0 {
			capabilities[id] = models
		}
	}
	return &apiKeyModelRoutingSnapshot{config: cfg, aliases: buildAPIKeyModelAliasTable(auths, cfg), capabilities: capabilities}
}

// ResolvedAPIKeyModelInfo returns a private copy of this attempt's capabilities.
func ResolvedAPIKeyModelInfo(req cliproxyexecutor.Request) (*registry.ModelInfo, bool) {
	bound, ok := req.Metadata[resolvedAPIKeyModelInfoMetadataKey].(resolvedAPIKeyModelInfo)
	if !ok || bound.info == nil {
		return nil, false
	}
	info := *bound.info
	info.SupportedInputModalities = append([]string(nil), info.SupportedInputModalities...)
	if info.Thinking != nil {
		support := *info.Thinking
		support.Levels = append([]string(nil), support.Levels...)
		info.Thinking = &support
	}
	return &info, true
}

// ResolvedModelInputModalities reports the attempt's explicit declaration, not
// inherited catalog capabilities. A present empty declaration prevents an old
// executor config from reactivating a removed override after hot reload.
func ResolvedModelInputModalities(req cliproxyexecutor.Request) ([]string, bool) {
	bound, ok := req.Metadata[resolvedAPIKeyModelInfoMetadataKey].(resolvedAPIKeyModelInfo)
	if !ok {
		return nil, false
	}
	if bound.info == nil {
		return nil, true
	}
	return append([]string(nil), bound.info.SupportedInputModalities...), true
}

func attachResolvedAPIKeyModelInfo(routing *apiKeyModelRoutingSnapshot, req cliproxyexecutor.Request, auth *Auth, routeModel, upstreamModel string) cliproxyexecutor.Request {
	info, ok := lookupAPIKeyModelCapability(routing, auth, routeModel, upstreamModel)
	_, previouslyBound := req.Metadata[resolvedAPIKeyModelInfoMetadataKey]
	compatAttempt := isOpenAICompatAPIKeyAuth(auth)
	if !ok && !previouslyBound && !compatAttempt {
		return req
	}
	metadata := make(map[string]any, len(req.Metadata)+1)
	maps.Copy(metadata, req.Metadata)
	delete(metadata, resolvedAPIKeyModelInfoMetadataKey)
	if ok || compatAttempt {
		metadata[resolvedAPIKeyModelInfoMetadataKey] = resolvedAPIKeyModelInfo{info: info}
	}
	req.Metadata = metadata
	return req
}

func lookupAPIKeyModelCapability(routing *apiKeyModelRoutingSnapshot, auth *Auth, routeModel, upstreamModel string) (*registry.ModelInfo, bool) {
	if routing == nil || !isAPIKeyAuth(auth) {
		return nil, false
	}
	byRoute := routing.capabilities[strings.TrimSpace(auth.ID)]
	_, candidates := modelAliasLookupCandidates(rewriteModelForAuth(strings.TrimSpace(routeModel), auth))
	selected := strings.TrimSpace(upstreamModel)
	// An exact suffix match wins over a base-name fallback across all candidates.
	for _, exact := range []bool{true, false} {
		for _, candidate := range candidates {
			for _, route := range byRoute[strings.ToLower(strings.TrimSpace(candidate))] {
				matches := strings.EqualFold(route.upstreamModel, selected)
				if !exact {
					configured := thinking.ParseSuffix(route.upstreamModel)
					matches = !configured.HasSuffix && strings.EqualFold(configured.ModelName, thinking.ParseSuffix(selected).ModelName)
				}
				if matches {
					return route.modelInfo, route.modelInfo != nil
				}
			}
		}
	}
	return nil, false
}

func compileAPIKeyModelCapabilitiesForAuth(cfg *config.Config, auth *Auth) map[string][]apiKeyModelCapabilityRoute {
	if cfg == nil || !isAPIKeyAuth(auth) {
		return nil
	}
	out := make(map[string][]apiKeyModelCapabilityRoute)
	switch strings.ToLower(strings.TrimSpace(auth.Provider)) {
	case "gemini":
		if entry := resolveGeminiAPIKeyConfig(cfg, auth); entry != nil {
			compileConfiguredModelCapabilities(out, entry.Models, "gemini")
		}
	case "gemini-interactions":
		if entry := resolveInteractionsAPIKeyConfig(cfg, auth); entry != nil {
			compileConfiguredModelCapabilities(out, entry.Models, "interactions")
		}
	case "claude":
		if entry := resolveClaudeAPIKeyConfig(cfg, auth); entry != nil {
			compileConfiguredModelCapabilities(out, entry.Models, "claude")
		}
	case "codex":
		if entry := resolveCodexAPIKeyConfig(cfg, auth); entry != nil {
			compileConfiguredModelCapabilities(out, entry.Models, "codex")
		}
	case "vertex":
		if entry := resolveVertexAPIKeyConfig(cfg, auth); entry != nil {
			compileConfiguredModelCapabilities(out, entry.Models, "gemini")
		}
	default:
		if isOpenAICompatAPIKeyAuth(auth) {
			if entry := resolveOpenAICompatConfig(cfg, auth.Attributes["provider_key"], auth.Attributes["compat_name"], auth.Provider); entry != nil {
				compileConfiguredModelCapabilities(out, entry.Models, "openai-compatibility")
			}
		}
	}
	return out
}

func compileConfiguredModelCapabilities[T interface {
	GetName() string
	GetAlias() string
	GetThinking() *registry.ThinkingSupport
	GetIsCompat() bool
}](out map[string][]apiKeyModelCapabilityRoute, models []T, modelType string) {
	for _, model := range models {
		name, alias := strings.TrimSpace(model.GetName()), strings.TrimSpace(model.GetAlias())
		if name == "" {
			name = alias
		}
		if alias == "" {
			alias = name
		}
		if name == "" {
			continue
		}
		info := &registry.ModelInfo{ID: name, UpstreamID: name, Type: modelType, UserDefined: true, IsCompat: model.GetIsCompat()}
		if upstream := registry.LookupStaticModelInfo(thinking.ParseSuffix(name).ModelName); upstream != nil {
			info.Thinking = config.NormalizeModelThinkingSupport(upstream.Thinking)
		}
		if modelType == "openai-compatibility" {
			info.Thinking = &registry.ThinkingSupport{Levels: []string{"low", "medium", "high"}}
			info.UserDefined = false
		}
		// Only explicit configuration enters the attempt. Static input capability
		// inheritance belongs to model advertisement, never content rewriting.
		if declared, ok := any(model).(interface{ GetInputModalities() []string }); ok {
			info.SupportedInputModalities = declared.GetInputModalities()
		}
		if support := model.GetThinking(); support != nil {
			info.Thinking = config.NormalizeModelThinkingSupport(support)
			info.UserDefined = false
		}
		route := apiKeyModelCapabilityRoute{upstreamModel: name, modelInfo: info}
		for _, routeModel := range []string{alias, name} {
			_, candidates := modelAliasLookupCandidates(routeModel)
			for _, candidate := range candidates {
				key := strings.ToLower(strings.TrimSpace(candidate))
				duplicate := false
				for _, prior := range out[key] {
					if strings.EqualFold(prior.upstreamModel, name) {
						duplicate = true
						break
					}
				}
				if !duplicate {
					out[key] = append(out[key], route)
				}
			}
		}
	}
}

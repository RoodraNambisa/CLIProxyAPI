package helps

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

const XAIModelCatalogsKey = "xai_model_catalogs"

// XAIModelCatalogsForAuth reads independent node snapshots, including the legacy
// single-node format. It never combines one account's permissions with another's.
func XAIModelCatalogsForAuth(auth *coreauth.Auth) map[string]*XAIModelCatalog {
	result := make(map[string]*XAIModelCatalog)
	if auth == nil {
		return result
	}
	if legacy := XAIModelsForAuth(auth); legacy != nil {
		result[legacy.Source] = legacy
	}
	raw, err := json.Marshal(auth.Metadata[XAIModelCatalogsKey])
	if err != nil || len(raw) > config.XAIMaxCatalogSources*1024*1024 {
		return result
	}
	var catalogs []*XAIModelCatalog
	if json.Unmarshal(raw, &catalogs) != nil || len(catalogs) > config.XAIMaxCatalogSources {
		return result
	}
	for _, catalog := range catalogs {
		if catalog == nil {
			continue
		}
		// Reuse validation and take an owned copy before model registration.
		if valid := XAIModelsForAuth(&coreauth.Auth{Metadata: map[string]any{XAIModelCatalogKey: catalog}}); valid != nil {
			result[valid.Source] = valid
		}
	}
	return result
}

// MergeXAIModelCatalogs retains recent snapshots of temporarily deselected nodes
// as well as active ones. Concurrent refreshes cannot replace a newer snapshot.
func MergeXAIModelCatalogs(auth *coreauth.Auth, fresh map[string]*XAIModelCatalog, endpoints []string) []*XAIModelCatalog {
	catalogs := XAIModelCatalogsForAuth(auth)
	for source, catalog := range fresh {
		if old := catalogs[source]; old == nil || old.UpdatedAt.Before(catalog.UpdatedAt) {
			catalogs[source] = catalog
		}
	}
	out := make([]*XAIModelCatalog, 0, config.XAIMaxCatalogSources)
	for _, endpoint := range endpoints {
		if catalog := catalogs[endpoint]; catalog != nil {
			out = append(out, catalog)
			delete(catalogs, endpoint)
		}
	}
	recent := make([]*XAIModelCatalog, 0, len(catalogs))
	for _, catalog := range catalogs {
		recent = append(recent, catalog)
	}
	sort.Slice(recent, func(i, j int) bool {
		if recent[i].UpdatedAt.Equal(recent[j].UpdatedAt) {
			return recent[i].Source < recent[j].Source
		}
		return recent[i].UpdatedAt.After(recent[j].UpdatedAt)
	})
	for _, catalog := range recent {
		if len(out) >= config.XAIMaxCatalogSources {
			break
		}
		out = append(out, catalog)
	}
	return out
}

// XAIMergedModelsForAuth unions only currently selected sources. Exact manual
// rules may expose models omitted by a directory; wildcard rules cannot invent IDs.
func XAIMergedModelsForAuth(auth *coreauth.Auth, cfg *config.Config) []*registry.ModelInfo {
	endpoints, _ := XAICatalogEndpoints(auth, cfg)
	catalogs := XAIModelCatalogsForAuth(auth)
	models := make([]*registry.ModelInfo, 0)
	positions := make(map[string]int)
	hasCatalog := false
	for _, endpoint := range endpoints {
		catalog := catalogs[endpoint]
		if catalog == nil {
			continue
		}
		hasCatalog = true
		for _, model := range catalog.Models {
			if index, exists := positions[model.ID]; exists {
				if route, err := ResolveXAIModelUpstream(auth, cfg, model.ID); err == nil && route.BaseURL+"/models" == endpoint {
					models[index] = model
				}
				continue
			}
			positions[model.ID] = len(models)
			models = append(models, model)
		}
	}
	if !hasCatalog {
		models = registry.GetXAIModels()
		for i, model := range models {
			positions[model.ID] = i
		}
	}
	rules, _ := XAICredentialModelRoutes(auth)
	if cfg != nil {
		rules = append(rules, cfg.XAI.ModelRoutes...)
	}
	known := make(map[string]*registry.ModelInfo)
	for _, model := range registry.GetXAIModels() {
		known[model.ID] = model
	}
	for _, rule := range rules {
		for _, id := range rule.Models {
			if _, exists := positions[id]; exists || strings.Contains(id, "*") {
				continue
			}
			model := known[id]
			if model == nil {
				model = &registry.ModelInfo{ID: id, Object: "model", Type: "xai", OwnedBy: "xai", DisplayName: id, UpstreamID: id}
			}
			positions[id] = len(models)
			models = append(models, model)
		}
	}
	return models
}

func XAIModelCatalogProvenance(auth *coreauth.Auth, cfg *config.Config) map[string][]string {
	result := make(map[string][]string)
	endpoints, _ := XAICatalogEndpoints(auth, cfg)
	catalogs := XAIModelCatalogsForAuth(auth)
	for _, endpoint := range endpoints {
		if catalog := catalogs[endpoint]; catalog != nil {
			for _, model := range catalog.Models {
				result[model.ID] = append(result[model.ID], endpoint)
			}
		}
	}
	return result
}

package helps

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

const XAIModelCatalogKey = "xai_model_catalog"

// XAIModelCatalog belongs to one credential and survives provider outages.
type XAIModelCatalog struct {
	Models               []*registry.ModelInfo              `json:"models"`
	DeclaredCapabilities map[string]XAIDeclaredCapabilities `json:"declared_capabilities,omitempty"`
	UpdatedAt            time.Time                          `json:"updated_at"`
	Source               string                             `json:"source"`
}

func ParseXAIModels(raw []byte, source string) (*XAIModelCatalog, error) {
	var envelope struct {
		Data []*struct {
			registry.ModelInfo
			ContextWindow           int   `json:"context_window"`
			SupportsReasoningEffort *bool `json:"supports_reasoning_effort"`
			ReasoningEfforts        []struct {
				Value string `json:"value"`
			} `json:"reasoning_efforts"`
		} `json:"data"`
	}
	if len(raw) > 1024*1024 || json.Unmarshal(raw, &envelope) != nil || envelope.Data == nil || len(envelope.Data) > 256 {
		return nil, fmt.Errorf("invalid Grok model catalog")
	}
	known := make(map[string]*registry.ModelInfo)
	for _, model := range registry.GetXAIModels() {
		known[model.ID] = model
	}
	declarations := make(map[string]XAIDeclaredCapabilities)
	rawEntries := make(map[string]gjson.Result)
	for _, entry := range gjson.GetBytes(raw, "data").Array() {
		id := entry.Get("id").String()
		if _, exists := rawEntries[id]; !exists {
			rawEntries[id] = entry
		}
	}
	seen := make(map[string]bool)
	models := make([]*registry.ModelInfo, 0, len(envelope.Data))
	for _, entry := range envelope.Data {
		if entry == nil || strings.TrimSpace(entry.ID) == "" || len(entry.ID) > 256 || strings.ContainsAny(entry.ID, "\r\n\x00") {
			return nil, fmt.Errorf("invalid Grok model identifier")
		}
		model := &entry.ModelInfo
		if seen[model.ID] {
			continue
		}
		seen[model.ID] = true
		// The CLI catalog uses different capability fields from the API catalog.
		// Normalize them before applying static fallbacks or persisting the cache.
		if model.ContextLength == 0 && entry.ContextWindow > 0 {
			model.ContextLength = entry.ContextWindow
		}
		reasoningDisabled := entry.SupportsReasoningEffort != nil && !*entry.SupportsReasoningEffort
		if model.Thinking == nil && !reasoningDisabled {
			var levels []string
			seenLevels := make(map[string]bool)
			for _, effort := range entry.ReasoningEfforts {
				level := strings.ToLower(strings.TrimSpace(effort.Value))
				if level != "" && !seenLevels[level] {
					seenLevels[level] = true
					levels = append(levels, level)
				}
			}
			if len(levels) > 0 {
				model.Thinking = &registry.ThinkingSupport{Levels: levels}
			}
		}
		declared := XAIDeclaredCapabilities{}
		rawEntry := rawEntries[model.ID]
		declared.ThinkingKnown = rawEntry.Get("thinking").Exists() || rawEntry.Get("supports_reasoning_effort").Exists() || rawEntry.Get("reasoning_efforts").Exists()
		if reasoningDisabled {
			model.Thinking = nil
		}
		declared.Thinking = model.Thinking
		if value := rawEntry.Get("image_capabilities.max_edit_images"); value.Type == gjson.Number {
			n := value.Int()
			if n < 1 || n > 64 || value.Float() != float64(n) {
				return nil, fmt.Errorf("invalid Grok image capability limit")
			}
			v := int(n)
			declared.MaxEditImages = &v
		}
		if value := rawEntry.Get("image_capabilities.quality_values"); value.Exists() {
			if !value.IsArray() {
				return nil, fmt.Errorf("invalid Grok quality capabilities")
			}
			levels := []string{}
			for _, entry := range value.Array() {
				if entry.Type != gjson.String {
					return nil, fmt.Errorf("invalid Grok quality capability")
				}
				levels = append(levels, entry.String())
			}
			declared.QualityValues = &levels
		}
		if value := rawEntry.Get("image_capabilities.file_id_inputs"); value.Exists() {
			if value.Type != gjson.True && value.Type != gjson.False {
				return nil, fmt.Errorf("invalid Grok file_id capability")
			}
			v := value.Bool()
			declared.FileIDInputs = &v
		}
		declarations[model.ID] = declared
		// Keep known capabilities when the directory only returns an ID.
		if baseline := known[model.ID]; baseline != nil {
			if model.Thinking == nil && !declared.ThinkingKnown {
				model.Thinking = baseline.Thinking
			}
			if model.ContextLength == 0 {
				model.ContextLength = baseline.ContextLength
			}
			if model.MaxCompletionTokens == 0 {
				model.MaxCompletionTokens = baseline.MaxCompletionTokens
			}
			if len(model.SupportedParameters) == 0 {
				model.SupportedParameters = baseline.SupportedParameters
			}
		}
		model.Type, model.Object, model.UpstreamID = "xai", "model", model.ID
		if model.OwnedBy == "" {
			model.OwnedBy = "xai"
		}
		if model.DisplayName == "" {
			model.DisplayName = strings.TrimSpace(model.Name)
			if model.DisplayName == "" {
				model.DisplayName = model.ID
			}
		}
		models = append(models, model)
	}
	catalog := &XAIModelCatalog{Models: models, DeclaredCapabilities: declarations, Source: source, UpdatedAt: time.Now().UTC()}
	if serialized, err := json.Marshal(catalog); err != nil || len(serialized) > 1024*1024 {
		return nil, fmt.Errorf("Grok model catalog exceeds the storage limit")
	}
	return catalog, nil
}

func XAIModelsForAuth(auth *coreauth.Auth) *XAIModelCatalog {
	if auth == nil {
		return nil
	}
	raw, err := json.Marshal(auth.Metadata[XAIModelCatalogKey])
	if err != nil || len(raw) > 1024*1024 {
		return nil
	}
	var catalog XAIModelCatalog
	if json.Unmarshal(raw, &catalog) != nil || catalog.Models == nil || len(catalog.Models) > 256 || catalog.UpdatedAt.IsZero() {
		return nil
	}
	for _, model := range catalog.Models {
		if model == nil || strings.TrimSpace(model.ID) == "" {
			return nil
		}
		model.UpstreamID = model.ID
	}
	if !validXAIDeclarations(catalog.DeclaredCapabilities) {
		return nil
	}
	return &catalog
}

func validXAIDeclarations(values map[string]XAIDeclaredCapabilities) bool {
	if len(values) > 256 {
		return false
	}
	for id, value := range values {
		if id == "" || len(id) > 256 || strings.ContainsAny(id, "\r\n\x00") {
			return false
		}
		if value.MaxEditImages != nil && (*value.MaxEditImages < 1 || *value.MaxEditImages > 64) {
			return false
		}
		if value.QualityValues != nil {
			if len(*value.QualityValues) > 32 {
				return false
			}
			for _, quality := range *value.QualityValues {
				if len(quality) > 64 || strings.ContainsAny(quality, "\r\n\x00") {
					return false
				}
			}
		}
	}
	return true
}

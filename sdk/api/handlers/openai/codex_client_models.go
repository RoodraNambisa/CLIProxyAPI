package openai

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

type codexClientModelsPayload struct {
	Models []map[string]any `json:"models"`
}

var (
	codexClientModelTemplatesMu       sync.Mutex
	codexClientModelTemplatesRevision uint64
	codexClientModelTemplatesLoaded   bool
	codexClientModelTemplates         map[string]map[string]any
	codexClientDefaultTemplate        map[string]any
	codexClientModelTemplatesErr      error
)

var codexClientAllowedReasoningLevels = map[string]struct{}{
	"none":    {},
	"minimal": {},
	"low":     {},
	"medium":  {},
	"high":    {},
	"xhigh":   {},
	"max":     {},
	"ultra":   {},
}

func CodexClientModelsResponse(models []map[string]any) map[string]any {
	return map[string]any{
		"models": buildCodexClientModels(models),
	}
}

// CodexClientModelsResponseForClient filters only capabilities the real client
// cannot decode. Outbound software identity does not determine client capability.
func CodexClientModelsResponseForClient(models []map[string]any, clientVersion string) map[string]any {
	response := CodexClientModelsResponse(models)
	if supportsExtendedCodexClientReasoning(clientVersion) {
		return response
	}
	for _, model := range response["models"].([]map[string]any) {
		levels, ok := model["supported_reasoning_levels"].([]any)
		if !ok {
			continue
		}
		retained := make([]any, 0, len(levels))
		for _, raw := range levels {
			entry, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			level := stringModelValue(entry, "effort")
			if level != "max" && level != "ultra" {
				retained = append(retained, entry)
			}
		}
		model["supported_reasoning_levels"] = retained
		sanitizeCodexClientReasoningMetadata(model)
	}
	return response
}

func supportsExtendedCodexClientReasoning(version string) bool {
	core := strings.TrimSpace(version)
	if strings.HasPrefix(core, "v") || strings.HasPrefix(core, "V") {
		core = core[1:]
	}
	if index := strings.IndexAny(core, "-+"); index >= 0 {
		core = core[:index]
	}
	parts := strings.Split(core, ".")
	if len(parts) < 2 || len(parts) > 3 {
		return true
	}
	major, errMajor := strconv.Atoi(parts[0])
	minor, errMinor := strconv.Atoi(parts[1])
	if errMajor != nil || errMinor != nil || major < 0 || minor < 0 {
		return true
	}
	if len(parts) == 3 {
		patch, errPatch := strconv.Atoi(parts[2])
		if errPatch != nil || patch < 0 {
			return true
		}
	}
	return major > 0 || minor >= 144
}

func buildCodexClientModels(models []map[string]any) []map[string]any {
	templates, defaultTemplate, err := loadCodexClientModelTemplates()
	if err != nil || defaultTemplate == nil {
		return nil
	}

	result := make([]map[string]any, 0, len(models))
	for _, model := range models {
		id := strings.TrimSpace(stringModelValue(model, "id"))
		if id == "" {
			continue
		}

		if template, ok := templates[id]; ok {
			entry := cloneCodexClientModelMap(template)
			sanitizeCodexClientReasoningMetadata(entry)
			applyCodexClientVisibilityOverride(entry, id)
			result = append(result, entry)
			continue
		}

		entry := cloneCodexClientModelMap(defaultTemplate)
		applyCodexClientModelMetadata(entry, id, model)
		sanitizeCodexClientReasoningMetadata(entry)
		applyCodexClientVisibilityOverride(entry, id)
		result = append(result, entry)
	}

	applyCodexClientNonTemplatePriorities(result, templates)

	sort.SliceStable(result, func(i, j int) bool {
		return codexClientModelPriority(result[i]) < codexClientModelPriority(result[j])
	})

	return result
}

func maxCodexClientTemplatePriority(templates map[string]map[string]any) int {
	maxPriority := 0
	for _, template := range templates {
		priority := codexClientModelPriority(template)
		if priority > maxPriority {
			maxPriority = priority
		}
	}
	return maxPriority
}

func applyCodexClientNonTemplatePriorities(result []map[string]any, templates map[string]map[string]any) {
	if len(result) == 0 {
		return
	}

	basePriority := maxCodexClientTemplatePriority(templates)
	type nonTemplateEntry struct {
		index       int
		displayName string
		slug        string
	}

	pending := make([]nonTemplateEntry, 0)
	for index, entry := range result {
		slug := stringModelValue(entry, "slug")
		if _, ok := templates[slug]; ok {
			continue
		}
		displayName := stringModelValue(entry, "display_name")
		if displayName == "" {
			displayName = slug
		}
		pending = append(pending, nonTemplateEntry{
			index:       index,
			displayName: displayName,
			slug:        slug,
		})
	}

	sort.SliceStable(pending, func(i, j int) bool {
		left := strings.ToLower(pending[i].displayName)
		right := strings.ToLower(pending[j].displayName)
		if left == right {
			return pending[i].slug < pending[j].slug
		}
		return left < right
	})

	for rank, entry := range pending {
		result[entry.index]["priority"] = basePriority + 100*(rank+1)
	}
}

func loadCodexClientModelTemplates() (map[string]map[string]any, map[string]any, error) {
	raw, revision := registry.GetCodexClientModelsSnapshot()
	return loadCodexClientModelTemplatesSnapshot(raw, revision)
}

func loadCodexClientModelTemplatesSnapshot(raw []byte, revision uint64) (map[string]map[string]any, map[string]any, error) {
	codexClientModelTemplatesMu.Lock()
	defer codexClientModelTemplatesMu.Unlock()
	if !codexClientModelTemplatesLoaded || codexClientModelTemplatesRevision != revision {
		var payload codexClientModelsPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return nil, nil, err
		}

		templates := make(map[string]map[string]any, len(payload.Models))
		var defaultTemplate map[string]any
		for _, model := range payload.Models {
			slug := strings.TrimSpace(stringModelValue(model, "slug"))
			if slug == "" {
				continue
			}
			templates[slug] = cloneCodexClientModelMap(model)
			if slug == "gpt-5.5" {
				defaultTemplate = cloneCodexClientModelMap(model)
			}
		}
		codexClientModelTemplates = templates
		codexClientDefaultTemplate = defaultTemplate
		codexClientModelTemplatesErr = nil
		codexClientModelTemplatesRevision = revision
		codexClientModelTemplatesLoaded = true
	}

	return codexClientModelTemplates, codexClientDefaultTemplate, codexClientModelTemplatesErr
}

func applyCodexClientModelMetadata(entry map[string]any, id string, model map[string]any) {
	info := registry.LookupModelInfo(id)

	displayName := stringModelValue(model, "display_name")
	description := stringModelValue(model, "description")
	contextWindow := intModelValue(model, "context_length")

	if info != nil {
		if info.DisplayName != "" {
			displayName = info.DisplayName
		}
		if info.Description != "" {
			description = info.Description
		}
		if info.ContextLength > 0 {
			contextWindow = info.ContextLength
		}
		if info.Type == registry.OpenAIImageModelType {
			entry["visibility"] = "hide"
			delete(entry, "input_modalities")
			delete(entry, "supports_image_detail_original")
		} else {
			applyCodexClientInputModalitiesMetadata(entry, info.SupportedInputModalities)
		}
		applyCodexClientThinkingMetadata(entry, info.Thinking)
	}

	if displayName == "" {
		displayName = id
	}
	if description == "" {
		description = id
	}

	entry["slug"] = id
	entry["display_name"] = displayName
	entry["description"] = description
	entry["prefer_websockets"] = false
	entry["service_tiers"] = []any{}
	delete(entry, "apply_patch_tool_type")
	delete(entry, "upgrade")
	delete(entry, "availability_nux")

	if contextWindow > 0 {
		entry["context_window"] = contextWindow
		entry["max_context_window"] = contextWindow
	}

	if baseInstructions := stringModelValue(model, "base_instructions"); baseInstructions != "" {
		entry["base_instructions"] = baseInstructions
	}
	if plans, ok := model["available_in_plans"]; ok {
		entry["available_in_plans"] = cloneCodexClientModelValue(plans)
	}
}

func applyCodexClientVisibilityOverride(entry map[string]any, id string) {
	switch strings.TrimSpace(id) {
	case "grok-imagine-image-quality", "gpt-image-1.5", "gpt-image-2", "grok-imagine-image", "grok-imagine-video", "grok-imagine-video-1.5-preview":
		entry["visibility"] = "hide"
	}
}

func applyCodexClientInputModalitiesMetadata(entry map[string]any, modalities []string) {
	if len(modalities) == 0 {
		return
	}
	// Codex client only accepts text/image input modalities.
	codexModalities := make([]any, 0, 2)
	seen := make(map[string]struct{}, 2)
	supportsImage := false
	for _, raw := range modalities {
		switch modality := strings.ToLower(strings.TrimSpace(raw)); modality {
		case "text", "image":
			if _, ok := seen[modality]; ok {
				continue
			}
			seen[modality] = struct{}{}
			codexModalities = append(codexModalities, modality)
			if modality == "image" {
				supportsImage = true
			}
		}
	}
	if len(codexModalities) == 0 {
		return
	}
	entry["input_modalities"] = codexModalities
	if supportsImage {
		entry["supports_image_detail_original"] = true
	} else {
		delete(entry, "supports_image_detail_original")
	}
}

func applyCodexClientThinkingMetadata(entry map[string]any, thinking *registry.ThinkingSupport) {
	// Budget-only metadata retains the existing template's discrete choices.
	if thinking == nil || len(thinking.Levels) == 0 {
		return
	}

	levels := make([]any, 0, len(thinking.Levels))
	defaultLevel := ""
	firstLevel := ""
	for _, rawLevel := range thinking.Levels {
		level := normalizeCodexClientReasoningLevel(rawLevel)
		if level == "" {
			continue
		}
		if firstLevel == "" {
			firstLevel = level
		}
		if (defaultLevel == "" && level != "none") || level == "medium" {
			defaultLevel = level
		}
		levels = append(levels, map[string]any{
			"effort":      level,
			"description": codexClientReasoningDescription(level),
		})
	}
	if len(levels) == 0 {
		entry["supported_reasoning_levels"] = levels
		delete(entry, "default_reasoning_level")
		return
	}
	if defaultLevel == "" {
		defaultLevel = firstLevel
	}

	entry["supported_reasoning_levels"] = levels
	entry["default_reasoning_level"] = defaultLevel
}

func sanitizeCodexClientReasoningMetadata(entry map[string]any) {
	rawLevels, ok := entry["supported_reasoning_levels"].([]any)
	if !ok {
		return
	}

	levels := make([]any, 0, len(rawLevels))
	allowedDefaults := make(map[string]struct{}, len(rawLevels))
	for _, rawLevelEntry := range rawLevels {
		levelEntry, ok := rawLevelEntry.(map[string]any)
		if !ok {
			continue
		}
		level := normalizeCodexClientReasoningLevel(stringModelValue(levelEntry, "effort"))
		if level == "" {
			continue
		}
		clonedEntry := cloneCodexClientModelMap(levelEntry)
		clonedEntry["effort"] = level
		levels = append(levels, clonedEntry)
		allowedDefaults[level] = struct{}{}
	}

	if len(levels) == 0 {
		entry["supported_reasoning_levels"] = levels
		delete(entry, "default_reasoning_level")
		return
	}

	defaultLevel := normalizeCodexClientReasoningLevel(stringModelValue(entry, "default_reasoning_level"))
	if _, ok := allowedDefaults[defaultLevel]; !ok {
		defaultLevel = stringModelValue(levels[0].(map[string]any), "effort")
	}

	entry["supported_reasoning_levels"] = levels
	entry["default_reasoning_level"] = defaultLevel
}

func normalizeCodexClientReasoningLevel(rawLevel string) string {
	level := strings.ToLower(strings.TrimSpace(rawLevel))
	if _, ok := codexClientAllowedReasoningLevels[level]; !ok {
		return ""
	}
	return level
}

func codexClientReasoningDescription(level string) string {
	switch level {
	case "none":
		return "No reasoning"
	case "minimal":
		return "Minimal reasoning for low-latency responses"
	case "low":
		return "Fast responses with lighter reasoning"
	case "medium":
		return "Balances speed and reasoning depth for everyday tasks"
	case "high":
		return "Greater reasoning depth for complex problems"
	case "xhigh":
		return "Extra high reasoning depth for complex problems"
	case "max":
		return "Maximum available reasoning depth for complex problems"
	default:
		return level
	}
}

func codexClientModelPriority(model map[string]any) int {
	if priority, ok := model["priority"].(int); ok {
		return priority
	}
	if priority, ok := model["priority"].(float64); ok {
		return int(priority)
	}
	return 100
}

func stringModelValue(model map[string]any, key string) string {
	if model == nil {
		return ""
	}
	value, ok := model[key]
	if !ok {
		return ""
	}
	if s, ok := value.(string); ok {
		return strings.TrimSpace(s)
	}
	return ""
}

func intModelValue(model map[string]any, key string) int {
	if model == nil {
		return 0
	}
	switch value := model[key].(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	default:
		return 0
	}
}

func cloneCodexClientModelMap(model map[string]any) map[string]any {
	if model == nil {
		return nil
	}
	cloned := make(map[string]any, len(model))
	for key, value := range model {
		cloned[key] = cloneCodexClientModelValue(value)
	}
	return cloned
}

func cloneCodexClientModelValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneCodexClientModelMap(typed)
	case []any:
		cloned := make([]any, len(typed))
		for i, entry := range typed {
			cloned[i] = cloneCodexClientModelValue(entry)
		}
		return cloned
	case []string:
		return append([]string(nil), typed...)
	default:
		return value
	}
}

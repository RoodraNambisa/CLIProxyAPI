package registry

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
)

//go:embed models/codex_client_models.json
var codexClientModelsJSON []byte

const maxModelsCatalogBytes = 8 << 20

type codexClientModelsPayload struct {
	Models []map[string]any `json:"models"`
}

type codexClientModelsStore struct {
	mu       sync.RWMutex
	data     []byte
	revision uint64
}

var codexClientCatalogStore codexClientModelsStore

func init() {
	if _, err := loadCodexClientModelsFromBytes(codexClientModelsJSON, "embed"); err != nil {
		log.Errorf("registry: embedded Codex client catalog is invalid: %v", err)
	}
}

// GetCodexClientModelsJSON returns a copy of the current validated catalog.
func GetCodexClientModelsJSON() []byte {
	data, _ := GetCodexClientModelsSnapshot()
	return data
}

// GetCodexClientModelsSnapshot captures the bytes and revision together. Callers
// must use one snapshot for a complete response, including cached templates.
func GetCodexClientModelsSnapshot() ([]byte, uint64) {
	codexClientCatalogStore.mu.RLock()
	defer codexClientCatalogStore.mu.RUnlock()
	return bytes.Clone(codexClientCatalogStore.data), codexClientCatalogStore.revision
}

func loadCodexClientModelsFromBytes(data []byte, source string) (bool, error) {
	prepared, err := prepareCodexClientModels(data)
	if err != nil {
		return false, fmt.Errorf("%s: %w", source, err)
	}
	return codexClientCatalogStore.publish(prepared), nil
}

func (s *codexClientModelsStore) publish(data []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if bytes.Equal(data, s.data) {
		return false
	}
	s.data = bytes.Clone(data)
	s.revision++
	return true
}

// ValidateCodexClientModelsJSON checks a remote catalog without publishing it.
func ValidateCodexClientModelsJSON(data []byte) error {
	_, err := prepareCodexClientModels(data)
	return err
}

func prepareCodexClientModels(data []byte) ([]byte, error) {
	if len(data) > maxModelsCatalogBytes {
		return nil, fmt.Errorf("Codex client catalog exceeds %d bytes", maxModelsCatalogBytes)
	}
	if !json.Valid(data) {
		return nil, fmt.Errorf("Codex client catalog is not valid JSON")
	}
	var payload map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode Codex client catalog: %w", err)
	}
	models, ok := payload["models"].([]any)
	if !ok || len(models) == 0 {
		return nil, fmt.Errorf("Codex client catalog has no models")
	}
	seen := make(map[string]bool, len(models))
	for i, rawModel := range models {
		model, ok := rawModel.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("Codex client model %d must be an object", i)
		}
		slug, ok := model["slug"].(string)
		if !ok || strings.TrimSpace(slug) == "" || slug != strings.TrimSpace(slug) || seen[slug] {
			return nil, fmt.Errorf("Codex client model %d has a missing, padded or duplicate slug", i)
		}
		seen[slug] = true
		if err := validateCodexClientModel(model); err != nil {
			return nil, fmt.Errorf("Codex client model %q: %w", slug, err)
		}
		if slug == "gpt-5.5" {
			messages, _ := model["model_messages"].(map[string]any)
			template, _ := messages["instructions_template"].(string)
			if strings.TrimSpace(template) == "" {
				return nil, fmt.Errorf("Codex client fallback model requires instructions_template")
			}
		}
		if slug == "gpt-6-astra" {
			correctKnownAstraTemplate(model)
		}
	}
	if !seen["gpt-5.5"] {
		return nil, fmt.Errorf("Codex client catalog is missing the gpt-5.5 fallback template")
	}
	// Canonical encoding prevents whitespace or object key order from invalidating
	// templates and retains exact integer values during validation.
	return json.Marshal(payload)
}

func validateCodexClientModel(model map[string]any) error {
	for _, key := range []string{"display_name", "base_instructions", "minimal_client_version"} {
		if value, ok := model[key].(string); !ok || strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s must be a non-empty string", key)
		}
	}
	for _, key := range []string{"description", "tool_mode", "multi_agent_version"} {
		if value := model[key]; value != nil {
			if _, ok := value.(string); !ok {
				return fmt.Errorf("%s must be a string or null", key)
			}
		}
	}
	priority, ok := model["priority"].(json.Number)
	priorityValue, err := priority.Int64()
	if !ok || err != nil || priorityValue < math.MinInt32 || priorityValue > math.MaxInt32 {
		return fmt.Errorf("priority must be a signed 32-bit integer")
	}
	for key, allowed := range map[string][]string{
		"visibility": {"list", "hide", "none"},
		"shell_type": {"unified_exec", "default", "local", "shell_command", "disabled"},
	} {
		value, _ := model[key].(string)
		valid := false
		for _, candidate := range allowed {
			valid = valid || value == candidate
		}
		if !valid {
			return fmt.Errorf("%s has an unsupported value", key)
		}
	}
	integers := make(map[string]int64, 3)
	for _, key := range []string{"context_window", "max_context_window", "auto_compact_token_limit"} {
		value := model[key]
		if value == nil {
			continue
		}
		number, ok := value.(json.Number)
		integer, err := number.Int64()
		if !ok || err != nil || integer <= 0 || integer > 1<<53-1 {
			return fmt.Errorf("%s must be an in-range integer", key)
		}
		integers[key] = integer
	}
	if maxContext, exists := integers["max_context_window"]; exists && integers["context_window"] > maxContext {
		return fmt.Errorf("context_window exceeds max_context_window")
	}
	truncation, ok := model["truncation_policy"].(map[string]any)
	mode, _ := truncation["mode"].(string)
	limit, isNumber := truncation["limit"].(json.Number)
	limitValue, err := limit.Int64()
	if !ok || (mode != "bytes" && mode != "tokens") || !isNumber || err != nil || limitValue < 0 || limitValue > 1<<53-1 {
		return fmt.Errorf("truncation_policy requires a supported mode and non-negative integer limit")
	}
	for _, key := range []string{"supported_in_api", "support_verbosity"} {
		if _, ok := model[key].(bool); !ok {
			return fmt.Errorf("%s must be a boolean", key)
		}
	}
	for _, key := range []string{"prefer_websockets", "support_verbosity", "supports_image_detail_original", "supports_parallel_tool_calls", "use_responses_lite", "node_repl_auto_review_required", "node_repl_disabled", "requires_sandboxed_review", "supported_in_api", "supports_search_tool", "supports_reasoning_summary_parameter", "supports_reasoning_summaries"} {
		if value, exists := model[key]; exists {
			if _, ok := value.(bool); !ok {
				return fmt.Errorf("%s must be a boolean", key)
			}
		}
	}
	for _, key := range []string{"input_modalities", "experimental_supported_tools", "available_in_plans"} {
		if _, exists := model[key]; key == "experimental_supported_tools" && !exists {
			return fmt.Errorf("%s must be an array", key)
		}
		if value, exists := model[key]; exists {
			items, ok := value.([]any)
			if !ok {
				return fmt.Errorf("%s must be an array", key)
			}
			for _, item := range items {
				if name, ok := item.(string); !ok || strings.TrimSpace(name) == "" {
					return fmt.Errorf("%s must contain non-empty strings", key)
				}
			}
		}
	}
	if value := model["model_messages"]; value != nil {
		messages, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("model_messages must be an object or null")
		}
		for _, key := range []string{"instructions_template", "persistent_instructions"} {
			if value := messages[key]; value != nil {
				if _, ok := value.(string); !ok {
					return fmt.Errorf("model_messages.%s must be a string or null", key)
				}
			}
		}
	}
	levels, ok := model["supported_reasoning_levels"].([]any)
	if !ok {
		return fmt.Errorf("supported_reasoning_levels must be an array")
	}
	seen := make(map[string]bool, len(levels))
	for _, raw := range levels {
		level, ok := raw.(map[string]any)
		effort, _ := level["effort"].(string)
		if !ok || seen[effort] {
			return fmt.Errorf("invalid or duplicate reasoning effort")
		}
		if _, ok := level["description"].(string); !ok {
			return fmt.Errorf("reasoning effort description must be a string")
		}
		switch effort {
		case "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra":
		default:
			return fmt.Errorf("unsupported reasoning effort")
		}
		seen[effort] = true
	}
	defaultLevel, isString := model["default_reasoning_level"].(string)
	if model["default_reasoning_level"] != nil && !isString {
		return fmt.Errorf("default_reasoning_level must be a string")
	}
	if isString && defaultLevel != "" && !seen[defaultLevel] {
		return fmt.Errorf("default_reasoning_level is not supported")
	}
	return nil
}

func correctKnownAstraTemplate(model map[string]any) {
	const obsolete = "408a3625ee04de5c3e8aadfca916fa0d729efb7565fa304df3d1f583186b4856"
	replace := func(value any) bool {
		text, ok := value.(string)
		if !ok {
			return false
		}
		hash := sha256.Sum256([]byte(text))
		return hex.EncodeToString(hash[:]) == obsolete
	}
	messages, _ := model["model_messages"].(map[string]any)
	if !replace(messages["instructions_template"]) && !replace(model["base_instructions"]) {
		return
	}
	var reviewed codexClientModelsPayload
	if json.Unmarshal(codexClientModelsJSON, &reviewed) != nil {
		return
	}
	for _, candidate := range reviewed.Models {
		if candidate["slug"] != "gpt-6-astra" {
			continue
		}
		canonical, _ := candidate["model_messages"].(map[string]any)
		if replace(messages["instructions_template"]) {
			messages["instructions_template"] = canonical["instructions_template"]
		}
		if replace(model["base_instructions"]) {
			model["base_instructions"] = canonical["instructions_template"]
		}
		return
	}
}

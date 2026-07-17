package chatgptweb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// CatalogModel is the subset of ChatGPT Web model metadata used by the runtime
// registry.
type CatalogModel struct {
	Slug        string
	Created     int64
	OwnedBy     string
	DisplayName string
}

// DecodeCatalog parses /backend-api/models and drops malformed model entries.
// A valid empty models array is a successful empty catalog.
func DecodeCatalog(payload []byte) ([]CatalogModel, error) {
	var root struct {
		Models json.RawMessage `json:"models"`
	}
	if err := json.Unmarshal(payload, &root); err != nil {
		return nil, fmt.Errorf("decode chatgpt web model catalog: %w", err)
	}
	if len(root.Models) == 0 || string(root.Models) == "null" {
		return nil, fmt.Errorf("chatgpt web model catalog is missing models")
	}
	var entries []json.RawMessage
	if err := json.Unmarshal(root.Models, &entries); err != nil {
		return nil, fmt.Errorf("decode chatgpt web model entries: %w", err)
	}
	seen := make(map[string]struct{}, len(entries))
	models := make([]CatalogModel, 0, len(entries))
	recognizedEntries := 0
	for _, rawEntry := range entries {
		var entry map[string]any
		decoder := json.NewDecoder(bytes.NewReader(rawEntry))
		decoder.UseNumber()
		if err := decoder.Decode(&entry); err != nil || entry == nil {
			continue
		}
		slug := strings.TrimSpace(valueString(entry["slug"]))
		key := strings.ToLower(slug)
		if slug == "" {
			continue
		}
		recognizedEntries++
		if key == "auto" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		created := int64(0)
		switch value := entry["created"].(type) {
		case float64:
			created = int64(value)
		case json.Number:
			created, _ = value.Int64()
		}
		ownedBy := strings.TrimSpace(valueString(entry["owned_by"]))
		if ownedBy == "" {
			ownedBy = "openai"
		}
		displayName := firstCatalogString(entry, "title", "display_name", "name")
		if displayName == "" {
			displayName = slug
		}
		models = append(models, CatalogModel{
			Slug: slug, Created: created, OwnedBy: ownedBy, DisplayName: displayName,
		})
	}
	if len(entries) > 0 && recognizedEntries == 0 {
		return nil, fmt.Errorf("chatgpt web model catalog contains no recognizable entries")
	}
	sort.SliceStable(models, func(i, j int) bool {
		return strings.ToLower(models[i].Slug) < strings.ToLower(models[j].Slug)
	})
	return models, nil
}

func firstCatalogString(entry map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(valueString(entry[key])); value != "" {
			return value
		}
	}
	return ""
}

func valueString(value any) string {
	if value == nil {
		return ""
	}
	text, _ := value.(string)
	return text
}

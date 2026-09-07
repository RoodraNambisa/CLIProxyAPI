package helps

import (
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	codexSpawnAgentDescriptionMarker = "Spawns an agent"
	codexSpawnAgentModelsHeading     = "Available model overrides (optional; inherited parent model is preferred):"
)

// FormatCodexCollaborationModels formats a caller-filtered client model snapshot.
// The caller owns ordering, compatibility filtering and template revision.
func FormatCodexCollaborationModels(models []map[string]any) string {
	var out strings.Builder
	seen := make(map[string]struct{}, len(models))
	value := func(model map[string]any, field string) string {
		text, _ := model[field].(string)
		return strings.TrimSpace(text)
	}
	for _, model := range models {
		id := value(model, "slug")
		if id == "" || strings.ContainsAny(id, "\r\n") {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		out.WriteString("- " + codexMarkdownCode(id) + ":")
		if description := strings.Join(strings.Fields(value(model, "description")), " "); description != "" {
			out.WriteByte(' ')
			out.WriteString(description)
			if !strings.ContainsAny(description[len(description)-1:], ".!?") {
				out.WriteByte('.')
			}
		}
		rawLevels, _ := model["supported_reasoning_levels"].([]any)
		efforts := make([]string, 0, len(rawLevels))
		seenEfforts := make(map[string]bool, len(rawLevels))
		for _, raw := range rawLevels {
			level, _ := raw.(map[string]any)
			effort := strings.ToLower(value(level, "effort"))
			switch effort {
			case "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra":
				if !seenEfforts[effort] {
					efforts = append(efforts, effort)
					seenEfforts[effort] = true
				}
			}
		}
		if len(efforts) > 0 {
			defaultEffort := strings.ToLower(value(model, "default_reasoning_level"))
			if !seenEfforts[defaultEffort] {
				defaultEffort = efforts[0]
			}
			out.WriteString(" Reasoning efforts: ")
			for index, effort := range efforts {
				if index > 0 {
					out.WriteString(", ")
				}
				out.WriteString(effort)
				if effort == defaultEffort {
					out.WriteString(" (default)")
				}
			}
			out.WriteByte('.')
		}
		rawTiers, _ := model["service_tiers"].([]any)
		var tiers []string
		seenTiers := make(map[string]bool, len(rawTiers))
		for _, raw := range rawTiers {
			tier, _ := raw.(map[string]any)
			id := strings.Join(strings.Fields(value(tier, "id")), " ")
			if id != "" && !seenTiers[id] {
				tiers = append(tiers, id)
				seenTiers[id] = true
			}
		}
		if len(tiers) > 0 {
			out.WriteString(" Service tiers: " + strings.Join(tiers, ", ") + ".")
		}
		out.WriteByte('\n')
	}
	return strings.TrimSuffix(out.String(), "\n")
}

// PrepareCodexCollaborationTools updates descriptions and plaintext schemas
// without renaming tools. Empty model data preserves the caller's description.
func PrepareCodexCollaborationTools(payload []byte, modelList string) []byte {
	scan := scanCodexCollaborationTools(payload)
	updated := payload
	if !scan.conflict && modelList != "" {
		for _, path := range scan.spawnAgentPaths {
			path += ".description"
			description := gjson.GetBytes(updated, path)
			if description.Type != gjson.String {
				continue
			}
			replacement := replaceCodexSpawnAgentModels(description.String(), modelList)
			if replacement == description.String() {
				continue
			}
			var errSet error
			updated, errSet = sjson.SetBytes(updated, path, replacement)
			if errSet != nil {
				return payload
			}
		}
	}
	return removeCodexCollaborationMessageEncryption(updated, scan.messagePaths)
}

func codexMarkdownCode(value string) string {
	longest, current := 0, 0
	for _, ch := range value {
		if ch == 0x60 {
			current++
			if current > longest {
				longest = current
			}
		} else {
			current = 0
		}
	}
	delimiter := strings.Repeat(string(rune(0x60)), longest+1)
	if longest > 0 {
		return delimiter + " " + value + " " + delimiter
	}
	return delimiter + value + delimiter
}

func replaceCodexSpawnAgentModels(description, modelList string) string {
	if modelList == "" {
		return description
	}
	var cleaned strings.Builder
	headingIndent := ""
	lines := strings.SplitAfter(description, "\n")
	for index := 0; index < len(lines); {
		line := lines[index]
		if strings.TrimSpace(line) != codexSpawnAgentModelsHeading {
			cleaned.WriteString(line)
			index++
			continue
		}
		if headingIndent == "" {
			headingIndent = line[:strings.Index(line, codexSpawnAgentModelsHeading)]
		}
		index++
		for index < len(lines) && strings.HasPrefix(strings.TrimSpace(lines[index]), "- ") {
			index++
		}
	}
	text := cleaned.String()
	section := headingIndent + codexSpawnAgentModelsHeading + "\n" + modelList + "\n"
	if markerIndex := strings.Index(text, codexSpawnAgentDescriptionMarker); markerIndex >= 0 {
		lineStart := strings.LastIndex(text[:markerIndex], "\n") + 1
		return text[:lineStart] + section + text[lineStart:]
	}
	separator := ""
	if text != "" && !strings.HasSuffix(text, "\n") {
		separator = "\n\n"
	}
	return text + separator + strings.TrimSuffix(section, "\n")
}

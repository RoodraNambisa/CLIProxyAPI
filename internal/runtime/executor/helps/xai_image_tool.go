package helps

import (
	"strconv"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func XAIHasImageGenerationTool(body []byte) bool {
	for _, tool := range gjson.GetBytes(body, "tools").Array() {
		if tool.Get("type").String() == "image_generation" {
			return true
		}
	}
	return false
}

// NormalizeXAIImageToolChoice prevents a forced hosted image call from selecting
// injected search tools. The image-only auto mode remains optional.
func NormalizeXAIImageToolChoice(body []byte) []byte {
	choice := gjson.GetBytes(body, "tool_choice")
	mode := "required"
	if choice.Get("type").String() != "image_generation" {
		if choice.Get("type").String() != "allowed_tools" {
			return body
		}
		refs := choice.Get("tools").Array()
		if len(refs) == 0 {
			return body
		}
		for _, ref := range refs {
			if ref.Get("type").String() != "image_generation" {
				return body
			}
		}
		mode = choice.Get("mode").String()
	}
	tools := []byte(`[]`)
	for _, tool := range gjson.GetBytes(body, "tools").Array() {
		if tool.Get("type").String() == "image_generation" {
			tools, _ = sjson.SetRawBytes(tools, "-1", []byte(tool.Raw))
		}
	}
	if string(tools) == "[]" {
		return body
	}
	body, _ = sjson.SetRawBytes(body, "tools", tools)
	body, _ = sjson.SetBytes(body, "tool_choice", mode)
	return body
}

// xaiGrokImageGenerationMinVersion is the first Grok line that accepts xAI's
// native Responses image_generation tool. Older conversation models still
// reject that hosted type, so the executor keeps stripping it there.
var xaiGrokImageGenerationMinVersion = xaiGrokVersion{major: 4, minor: 6}

type xaiGrokVersion struct {
	major int
	minor int
}

// xaiSupportsNativeImageGeneration reports whether the Grok model accepts
// xAI's native Responses image_generation tool. grok-4.20-* is an older
// product line whose dotted minor is not comparable to grok-4.6.
func XAISupportsNativeImageGeneration(model string) bool {
	name := strings.ToLower(strings.TrimSpace(thinking.ParseSuffix(model).ModelName))
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		name = name[idx+1:]
	}
	if name == "" || !strings.HasPrefix(name, "grok-") {
		return false
	}
	rest := strings.TrimPrefix(name, "grok-")
	if rest == "4.20" || strings.HasPrefix(rest, "4.20-") {
		return false
	}
	ver, ok := xaiParseGrokVersionPrefix(rest)
	if !ok {
		return false
	}
	return xaiCompareGrokVersion(ver, xaiGrokImageGenerationMinVersion) >= 0
}

func xaiParseGrokVersionPrefix(rest string) (xaiGrokVersion, bool) {
	i := 0
	for i < len(rest) && rest[i] >= '0' && rest[i] <= '9' {
		i++
	}
	if i == 0 {
		return xaiGrokVersion{}, false
	}
	major, err := strconv.Atoi(rest[:i])
	if err != nil {
		return xaiGrokVersion{}, false
	}
	if i == len(rest) || rest[i] != '.' {
		return xaiGrokVersion{major: major, minor: -1}, true
	}
	j := i + 1
	for j < len(rest) && rest[j] >= '0' && rest[j] <= '9' {
		j++
	}
	if j == i+1 {
		return xaiGrokVersion{major: major, minor: -1}, true
	}
	minor, err := strconv.Atoi(rest[i+1 : j])
	if err != nil {
		return xaiGrokVersion{}, false
	}
	return xaiGrokVersion{major: major, minor: minor}, true
}

func xaiCompareGrokVersion(a, b xaiGrokVersion) int {
	if a.major != b.major {
		if a.major < b.major {
			return -1
		}
		return 1
	}
	aMinor := a.minor
	if aMinor < 0 {
		aMinor = 0
	}
	bMinor := b.minor
	if bMinor < 0 {
		bMinor = 0
	}
	if aMinor < bMinor {
		return -1
	}
	if aMinor > bMinor {
		return 1
	}
	return 0
}

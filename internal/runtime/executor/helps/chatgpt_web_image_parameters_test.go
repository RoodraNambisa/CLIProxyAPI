package helps

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestChatGPTWebImageNewParametersHonorIgnorePolicy(t *testing.T) {
	for _, field := range []string{"complexity", "prompt_rewriter", "markup", "svg"} {
		tool := map[string]any{"type": "image_generation", field: false}
		payload, err := json.Marshal(map[string]any{"model": "gpt-image-2.6", "input": "draw", "tools": []any{tool}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ParseChatGPTWebRequest(payload); err == nil || !strings.Contains(err.Error(), field) {
			t.Fatalf("%s silently dropped: %v", field, err)
		}
		if _, err := ParseChatGPTWebRequestWithOptions(payload, ChatGPTWebParseOptions{IgnoreUnsupportedImageParams: true}); err != nil {
			t.Fatalf("%s ignore policy: %v", field, err)
		}
		tool[field] = nil
		if err := validateChatGPTWebImageTool(tool, false); err != nil {
			t.Fatalf("%s null should be omitted: %v", field, err)
		}
	}
}

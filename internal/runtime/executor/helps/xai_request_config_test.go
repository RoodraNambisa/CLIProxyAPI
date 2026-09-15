package helps

import (
	"testing"

	"github.com/tidwall/gjson"
)

func TestXAIChatDefaultsUseChatNamesAndRespectExplicitValues(t *testing.T) {
	defaults := map[string]any{"max_output_tokens": 99, "parallel_tool_calls": true, "stream_tool_calls": true, "temperature": 1, "top_p": 0.8, "reasoning": map[string]any{"effort": "high"}, "tool_choice": map[string]any{"type": "function", "name": "a"}}
	for _, budget := range []string{"max_tokens", "max_completion_tokens"} {
		body := []byte(`{"` + budget + `":17,"parallel_tool_calls":false,"temperature":0,"reasoning_effort":"low","tool_choice":"none"}`)
		out := ApplyXAIChatDefaults(body, defaults)
		if gjson.GetBytes(out, budget).Int() != 17 || gjson.GetBytes(out, "reasoning_effort").String() != "low" || gjson.GetBytes(out, "parallel_tool_calls").Bool() || gjson.GetBytes(out, "temperature").Float() != 0 || gjson.GetBytes(out, "tool_choice").String() != "none" {
			t.Fatalf("client parameters changed: %s", out)
		}
		if gjson.GetBytes(out, "max_output_tokens").Exists() || gjson.GetBytes(out, "reasoning").Exists() || budget == "max_tokens" && gjson.GetBytes(out, "max_completion_tokens").Exists() {
			t.Fatalf("Responses fields leaked: %s", out)
		}
	}
	out := ApplyXAIChatDefaults([]byte(`{}`), defaults)
	if gjson.GetBytes(out, "max_completion_tokens").Int() != 99 || gjson.GetBytes(out, "reasoning_effort").String() != "high" || gjson.GetBytes(out, "tool_choice.function.name").String() != "a" {
		t.Fatalf("wrong defaults: %s", out)
	}
	defaults["tool_choice"] = map[string]any{"type": "allowed_tools", "mode": "required", "tools": []any{map[string]any{"type": "function", "name": "a"}}}
	out = ApplyXAIChatDefaults([]byte(`{}`), defaults)
	if gjson.GetBytes(out, "tool_choice.allowed_tools.tools.0.function.name").String() != "a" {
		t.Fatalf("wrong Chat allowed_tools default: %s", out)
	}
}

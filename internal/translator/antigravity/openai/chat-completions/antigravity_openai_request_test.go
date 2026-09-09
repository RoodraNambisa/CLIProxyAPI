package chat_completions

import (
	"testing"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestConvertOpenAIRequestToAntigravitySkipsEmptyTextPartsWithoutNulls(t *testing.T) {
	inputJSON := `{
		"model": "gemini-3-flash",
		"messages": [
			{
				"role": "user",
				"content": [
					{"type": "text", "text": ""},
					{"type": "input_audio", "input_audio": {"data": "SUQzBA==", "format": "mp3"}}
				]
			},
			{
				"role": "assistant",
				"content": [{"type": "text", "text": ""}],
				"tool_calls": [{
					"id": "call_1",
					"type": "function",
					"function": {"name": "read_file", "arguments": "{\"path\":\"a.txt\"}"}
				}]
			},
			{"role": "tool", "tool_call_id": "call_1", "content": "{\"output\":\"ok\"}"},
			{"role": "user", "content": "done"}
		]
	}`

	result := ConvertOpenAIRequestToAntigravity("gemini-3-flash", []byte(inputJSON), false)
	userParts := gjson.GetBytes(result, "request.contents.0.parts").Array()
	if len(userParts) != 1 {
		t.Fatalf("user parts length = %d, want 1. Output: %s", len(userParts), result)
	}
	if userParts[0].Type == gjson.Null {
		t.Fatalf("user parts.0 is null. Output: %s", result)
	}
	if got := userParts[0].Get("inlineData.mime_type").String(); got != "audio/mpeg" {
		t.Fatalf("audio mime_type = %q, want audio/mpeg. Output: %s", got, result)
	}

	assistantParts := gjson.GetBytes(result, "request.contents.1.parts").Array()
	if len(assistantParts) != 1 {
		t.Fatalf("assistant parts length = %d, want 1. Output: %s", len(assistantParts), result)
	}
	if assistantParts[0].Type == gjson.Null {
		t.Fatalf("assistant parts.0 is null. Output: %s", result)
	}
	if !assistantParts[0].Get("functionCall").Exists() {
		t.Fatalf("functionCall missing. Output: %s", result)
	}
}

func TestConvertOpenAIRequestToAntigravityThinkingAliases(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		want   bool
		absent bool
	}{
		{
			name: "Model name does not opt into thoughts",
			body: `{
				"model":"gemini-3.1-pro-low",
				"messages":[{"role":"user","content":"hi"}]
			}`,
			absent: true,
		},
		{
			name: "GenerationConfig snake include thoughts",
			body: `{
				"model":"gemini-3.1-pro-low",
				"messages":[{"role":"user","content":"hi"}],
				"generationConfig":{"thinkingConfig":{"include_thoughts":true}}
			}`,
			want: true,
		},
		{
			name: "Top-level thinking include thoughts",
			body: `{
				"model":"gemini-3.1-pro-low",
				"messages":[{"role":"user","content":"hi"}],
				"thinking":{"include_thoughts":true}
			}`,
			want: true,
		},
		{
			name: "Reasoning exclude false includes thoughts",
			body: `{
				"model":"gemini-3.1-pro-low",
				"messages":[{"role":"user","content":"hi"}],
				"reasoning":{"exclude":false}
			}`,
			want: true,
		},
		{
			name: "Reasoning exclude true hides thoughts",
			body: `{
				"model":"gemini-3.1-pro-low",
				"messages":[{"role":"user","content":"hi"}],
				"reasoning":{"exclude":true}
			}`,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertOpenAIRequestToAntigravity("gemini-3.1-pro-low", []byte(tt.body), false)
			includeThoughts := gjson.GetBytes(result, "request.generationConfig.thinkingConfig.includeThoughts")
			if includeThoughts.Exists() == tt.absent {
				t.Fatalf("unexpected includeThoughts presence. Output: %s", result)
			}
			if got := includeThoughts.Bool(); !tt.absent && got != tt.want {
				t.Fatalf("includeThoughts = %v, want %v. Output: %s", got, tt.want, result)
			}
			if snake := gjson.GetBytes(result, "request.generationConfig.thinkingConfig.include_thoughts"); snake.Exists() {
				t.Fatalf("include_thoughts should be normalized away. Output: %s", result)
			}
		})
	}
}

func TestConvertOpenAIRequestToAntigravitySummaryAliasesRequireBooleans(t *testing.T) {
	for _, path := range []string{
		"generationConfig.thinkingConfig.includeThoughts", "generationConfig.thinkingConfig.include_thoughts",
		"generationConfig.thinking_config.includeThoughts", "generationConfig.thinking_config.include_thoughts",
		"generationConfig.includeThoughts", "generationConfig.include_thoughts",
	} {
		for _, value := range []any{true, false, "true", "false", 0, 1, nil} {
			body, err := sjson.SetBytes([]byte(`{"messages":[{"role":"user","content":"fixture"}],"generationConfig":{"thinkingConfig":{"thinkingBudget":2048}}}`), path, value)
			if err != nil {
				t.Fatal(err)
			}
			out := ConvertOpenAIRequestToAntigravity("gemini-3.1-pro-low", body, false)
			got := gjson.GetBytes(out, "request.generationConfig.thinkingConfig.includeThoughts")
			want, valid := value.(bool)
			if got.Exists() != valid || (valid && (!got.IsBool() || got.Bool() != want)) {
				t.Fatalf("%s value type %T has invalid summary output: %s", path, value, out)
			}
			if gjson.GetBytes(out, "request.generationConfig.thinkingConfig.thinkingBudget").Int() != 2048 {
				t.Fatal("visibility normalization changed the amount")
			}
		}
	}
}

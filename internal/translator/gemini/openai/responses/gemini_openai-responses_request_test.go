package responses

import (
	"encoding/base64"
	"testing"

	"github.com/tidwall/gjson"
)

const testResponsesGeminiThoughtSignature = "EjQKMgEMOdbHO0Gd+c9Mxk4ELwPGbpCEcp2mFfYYLix2UVtBH3fL8GECc4+JITVnHF4qZDsA"

func TestConvertOpenAIResponsesRequestToGemini_PreservesReasoningOnlyHistory(t *testing.T) {
	input := []byte(`{
		"model": "gpt-5",
		"input": [{
			"type": "reasoning",
			"encrypted_content": "gemini#` + testResponsesGeminiThoughtSignature + `",
			"summary": [{"type": "summary_text", "text": "reasoning summary"}]
		}]
	}`)

	output := ConvertOpenAIResponsesRequestToGemini("gemini-3.5-flash", input, false)
	contents := gjson.GetBytes(output, "contents").Array()
	if len(contents) != 1 {
		t.Fatalf("contents length = %d, want 1. Output: %s", len(contents), output)
	}
	parts := contents[0].Get("parts").Array()
	if len(parts) != 2 {
		t.Fatalf("parts length = %d, want 2. Output: %s", len(parts), output)
	}
	if !parts[0].Get("thought").Bool() {
		t.Fatalf("parts[0] should be thought. Output: %s", output)
	}
	if got := parts[0].Get("thoughtSignature").String(); got != "" {
		t.Fatalf("parts[0].thoughtSignature = %q, want empty. Output: %s", got, output)
	}
	if got := parts[0].Get("text").String(); got != "reasoning summary" {
		t.Fatalf("thought text = %q, want reasoning summary. Output: %s", got, output)
	}
	if got := parts[1].Get("thoughtSignature").String(); got != testResponsesGeminiThoughtSignature {
		t.Fatalf("visible thoughtSignature = %q, want %q. Output: %s", got, testResponsesGeminiThoughtSignature, output)
	}
}

func TestConvertOpenAIResponsesRequestToGemini_PreservesReasoningBeforeTrailingAssistantPrefill(t *testing.T) {
	input := []byte(`{
		"model": "gpt-5.4",
		"input": [
			{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]},
			{"type":"reasoning","encrypted_content":"gemini#` + testResponsesGeminiThoughtSignature + `","summary":[{"type":"summary_text","text":"reasoning summary"}]},
			{"type":"message","role":"assistant","content":[{"type":"output_text","text":"previous answer"}]}
		]
	}`)

	output := ConvertOpenAIResponsesRequestToGemini("gemini-3.5-flash", input, false)
	contents := gjson.GetBytes(output, "contents").Array()
	if len(contents) != 2 {
		t.Fatalf("contents length = %d, want 2. Output: %s", len(contents), output)
	}
	if got := contents[0].Get("role").String(); got != "user" {
		t.Fatalf("contents[0].role = %q, want user", got)
	}
	if got := contents[1].Get("parts.1.thoughtSignature").String(); got != testResponsesGeminiThoughtSignature {
		t.Fatalf("reasoning visible thoughtSignature = %q, want preserved signature", got)
	}
}

func TestConvertOpenAIResponsesRequestToGemini_ReasoningSignatureCompatibility(t *testing.T) {
	tests := []struct {
		name          string
		encrypted     string
		wantSignature string
	}{
		{
			name:          "GPT encrypted_content uses Gemini bypass",
			encrypted:     validResponsesGPTReasoningSignature(),
			wantSignature: geminiResponsesThoughtSignature,
		},
		{
			name:          "Gemini encrypted_content is preserved",
			encrypted:     "gemini#" + testResponsesGeminiThoughtSignature,
			wantSignature: testResponsesGeminiThoughtSignature,
		},
		{
			name:          "Missing encrypted_content uses Gemini bypass",
			encrypted:     "",
			wantSignature: geminiResponsesThoughtSignature,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := []byte(`{
				"model": "gpt-5",
				"input": [{
					"type": "reasoning",
					"encrypted_content": "` + tt.encrypted + `",
					"summary": [{"type": "summary_text", "text": "reasoning summary"}]
				}]
			}`)

			output := ConvertOpenAIResponsesRequestToGemini("gemini-3.5-flash", input, false)
			parts := gjson.GetBytes(output, "contents.0.parts").Array()
			if len(parts) != 2 {
				t.Fatalf("parts length = %d, want 2. Output: %s", len(parts), output)
			}
			if got := parts[1].Get("thoughtSignature").String(); got != tt.wantSignature {
				t.Fatalf("visible thoughtSignature = %q, want %q. Output: %s", got, tt.wantSignature, output)
			}
			if got := parts[0].Get("text").String(); got != "reasoning summary" {
				t.Fatalf("thought text = %q, want reasoning summary. Output: %s", got, output)
			}
		})
	}
}

func TestConvertOpenAIResponsesRequestToGemini_MergesReasoningWithAssistantVisibleAnswer(t *testing.T) {
	input := []byte(`{
		"model": "gemini-3.5-flash",
		"input": [
			{"type":"reasoning","encrypted_content":"gemini#` + testResponsesGeminiThoughtSignature + `","summary":[{"type":"summary_text","text":"internal reasoning"}]},
			{"type":"message","role":"assistant","content":[{"type":"output_text","text":"visible answer"}]},
			{"type":"message","role":"user","content":[{"type":"input_text","text":"continue"}]}
		]
	}`)

	output := ConvertOpenAIResponsesRequestToGemini("gemini-3.5-flash", input, false)
	contents := gjson.GetBytes(output, "contents").Array()
	if len(contents) != 2 {
		t.Fatalf("contents length = %d, want 2. Output: %s", len(contents), output)
	}
	parts := contents[0].Get("parts").Array()
	if len(parts) != 2 {
		t.Fatalf("model parts length = %d, want 2. Output: %s", len(parts), output)
	}
	if !parts[0].Get("thought").Bool() {
		t.Fatalf("parts[0] should be thought. Output: %s", output)
	}
	if got := parts[0].Get("thoughtSignature").String(); got != "" {
		t.Fatalf("parts[0].thoughtSignature = %q, want empty. Output: %s", got, output)
	}
	if got := parts[1].Get("text").String(); got != "visible answer" {
		t.Fatalf("visible text = %q, want visible answer. Output: %s", got, output)
	}
	if got := parts[1].Get("thoughtSignature").String(); got != testResponsesGeminiThoughtSignature {
		t.Fatalf("visible thoughtSignature = %q, want preserved signature", got)
	}
}

func TestConvertOpenAIResponsesRequestToGemini_MergesReasoningWithUserRoleOutputText(t *testing.T) {
	input := []byte(`{
		"model":"gemini-3.5-flash",
		"input":[
			{"type":"reasoning","encrypted_content":"gemini#` + testResponsesGeminiThoughtSignature + `","summary":[{"type":"summary_text","text":"reasoning summary"}]},
			{"type":"message","role":"user","content":[{"type":"output_text","text":"visible from user role"}]}
		]
	}`)
	output := ConvertOpenAIResponsesRequestToGemini("gemini-3.5-flash", input, false)
	contents := gjson.GetBytes(output, "contents").Array()
	if len(contents) != 1 {
		t.Fatalf("contents length = %d, want 1. Output: %s", len(contents), output)
	}
	if got := contents[0].Get("parts.1.text").String(); got != "visible from user role" {
		t.Fatalf("visible text = %q", got)
	}
}

func TestConvertOpenAIResponsesRequestToGemini_MergesReasoningWithAssistantStringContent(t *testing.T) {
	input := []byte(`{
		"model":"gemini-3.5-flash",
		"input":[
			{"type":"reasoning","encrypted_content":"gemini#` + testResponsesGeminiThoughtSignature + `","summary":[{"type":"summary_text","text":"reasoning summary"}]},
			{"type":"message","role":"assistant","content":"string visible answer"}
		]
	}`)
	output := ConvertOpenAIResponsesRequestToGemini("gemini-3.5-flash", input, false)
	if got := gjson.GetBytes(output, "contents.0.parts.1.text").String(); got != "string visible answer" {
		t.Fatalf("visible text = %q", got)
	}
}

func TestConvertOpenAIResponsesRequestToGemini_PreservesWhitespaceWhenMergingReasoning(t *testing.T) {
	input := []byte(`{
		"model":"gemini-3.5-flash",
		"input":[
			{"type":"reasoning","encrypted_content":"gemini#` + testResponsesGeminiThoughtSignature + `","summary":[{"type":"summary_text","text":"reasoning summary"}]},
			{"type":"message","role":"assistant","content":[{"type":"output_text","text":"  lead trail  "}]},
			{"type":"message","role":"user","content":[{"type":"input_text","text":"next"}]}
		]
	}`)
	output := ConvertOpenAIResponsesRequestToGemini("gemini-3.5-flash", input, false)
	if got := gjson.GetBytes(output, "contents.0.parts.1.text").String(); got != "  lead trail  " {
		t.Fatalf("visible text = %q, want preserved whitespace", got)
	}
}

func validResponsesGPTReasoningSignature() string {
	raw := make([]byte, 1+8+16+16+32)
	raw[0] = 0x80
	raw[8] = 1
	for i := 9; i < len(raw); i++ {
		raw[i] = byte(i)
	}
	return base64.URLEncoding.EncodeToString(raw)
}

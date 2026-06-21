package executor

import (
	"context"
	"strings"
	"testing"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexStreamBodyRefsRealReleaseKeepsSlimMetadata(t *testing.T) {
	ctrl := cliproxyexecutor.NewRequestBodyReleaseController(1024, []byte("<released>"))
	opts := cliproxyexecutor.Options{
		Metadata: map[string]any{
			cliproxyexecutor.BodyReleaseControllerMetadataKey: ctrl,
		},
	}

	original := []byte(`{"messages":[{"content":"large prompt"}],"tools":[{"type":"function","function":{"name":"tool_original","description":"large schema"}}]}`)
	body := []byte(`{"input":"large translated prompt","tools":[{"type":"image_generation","model":"gpt-image-1.5"},{"type":"function","function":{"name":"tool_translated"}}]}`)
	releasedOriginal := slimCodexOriginalPayloadForTranslation(sdktranslator.FormatOpenAI, original)
	releasedBody := slimCodexBodyForStreamUsage(body)
	originalRef, bodyRef, unregister := codexStreamBodyRefs(context.Background(), opts, original, body, releasedOriginal, releasedBody)
	defer unregister()

	ctrl.Release()

	gotOriginal := string(originalRef.Bytes())
	if !strings.Contains(gotOriginal, "tool_original") {
		t.Fatalf("original payload after release = %q, want tool metadata", gotOriginal)
	}
	if strings.Contains(gotOriginal, "large prompt") || strings.Contains(gotOriginal, "large schema") {
		t.Fatalf("original payload after release retained large fields: %q", gotOriginal)
	}
	gotBody := string(bodyRef.Bytes())
	if !strings.Contains(gotBody, "image_generation") || !strings.Contains(gotBody, "gpt-image-1.5") {
		t.Fatalf("translated body after release = %q, want image tool metadata", gotBody)
	}
	if strings.Contains(gotBody, "large translated prompt") || strings.Contains(gotBody, "tool_translated") {
		t.Fatalf("translated body after release retained unrelated fields: %q", gotBody)
	}
}

func TestCodexStreamBodyRefsLogOnlyKeepsPayloads(t *testing.T) {
	ctrl := cliproxyexecutor.NewRequestBodyReleaseControllerWithMode(1024, []byte("<released>"), true)
	opts := cliproxyexecutor.Options{
		Metadata: map[string]any{
			cliproxyexecutor.BodyReleaseControllerMetadataKey: ctrl,
		},
	}

	originalRef, bodyRef, unregister := codexStreamBodyRefs(context.Background(), opts, []byte("original"), []byte("translated"), []byte("slim-original"), []byte("slim-translated"))
	defer unregister()

	ctrl.Release()

	if got := string(originalRef.Bytes()); got != "original" {
		t.Fatalf("original payload after log-only release = %q, want original", got)
	}
	if got := string(bodyRef.Bytes()); got != "translated" {
		t.Fatalf("translated body after log-only release = %q, want translated", got)
	}
}

func TestSlimCodexOriginalPayloadForTranslationKeepsProviderToolNames(t *testing.T) {
	tests := []struct {
		name string
		from sdktranslator.Format
		body []byte
		want string
	}{
		{
			name: "openai",
			from: sdktranslator.FormatOpenAI,
			body: []byte(`{"tools":[{"type":"function","function":{"name":"openai_tool","description":"drop"}}],"input":"drop"}`),
			want: "openai_tool",
		},
		{
			name: "claude",
			from: sdktranslator.FormatClaude,
			body: []byte(`{"tools":[{"name":"claude_tool","description":"drop"}],"messages":[{"content":"drop"}]}`),
			want: "claude_tool",
		},
		{
			name: "gemini",
			from: sdktranslator.FormatGemini,
			body: []byte(`{"tools":[{"functionDeclarations":[{"name":"gemini_tool","description":"drop"}]}],"contents":[{"parts":[{"text":"drop"}]}]}`),
			want: "gemini_tool",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(slimCodexOriginalPayloadForTranslation(tt.from, tt.body))
			if !strings.Contains(got, tt.want) {
				t.Fatalf("slim payload = %q, want %s", got, tt.want)
			}
			if strings.Contains(got, "drop") {
				t.Fatalf("slim payload retained dropped metadata: %q", got)
			}
		})
	}
}

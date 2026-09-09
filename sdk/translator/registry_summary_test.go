package translator

import (
	"bytes"
	"errors"
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/tidwall/gjson"
)

func TestRegistrySummaryProjectionUsesInboundVisibility(t *testing.T) {
	for _, tc := range []struct{ name, from, to, source, target, path, want string }{
		{"responses hide", "openai-response", "gemini", `{"reasoning":{"effort":"high","summary":null}}`, `{"generationConfig":{"thinkingConfig":{"thinkingLevel":"high","includeThoughts":true}}}`, "generationConfig.thinkingConfig.includeThoughts", "false"},
		{"google show", "gemini", "codex", `{"generationConfig":{"thinkingConfig":{"includeThoughts":true}}}`, `{"reasoning":{"effort":"high"}}`, "reasoning.summary", "auto"},
		{"google hide", "gemini", "codex", `{"generationConfig":{"thinkingConfig":{"includeThoughts":false}}}`, `{"reasoning":{"effort":"high","summary":"auto"}}`, "reasoning.summary", ""},
		{"claude hide", "claude", "codex", `{"thinking":{"type":"adaptive","display":"omitted"}}`, `{"reasoning":{"effort":"high","summary":"auto"}}`, "reasoning.summary", ""},
		{"chat compatibility", "openai", "codex", `{"reasoning_effort":"high"}`, `{}`, "reasoning.summary", "auto"},
		{"chat explicit hide", "openai", "gemini", `{"reasoning_effort":"high","reasoning":{"exclude":true}}`, `{}`, "generationConfig.thinkingConfig.includeThoughts", "false"},
		{"responses detail", "codex", "openai-response", `{"reasoning":{"summary":"concise"}}`, `{"reasoning":{"summary":"auto"}}`, "reasoning.summary", "concise"},
		{"interactions collapse", "openai-response", "interactions", `{"reasoning":{"summary":"detailed"}}`, `{}`, "generation_config.thinking_summaries", "auto"},
		{"native Claude no default", "claude", "claude", `{"thinking":{"type":"adaptive"}}`, `{"thinking":{"type":"adaptive"}}`, "thinking.display", ""},
		{"unknown target", "openai-response", "unsupported", `{"reasoning":{"summary":"auto"}}`, `{"fixture":true}`, "reasoning", ""},
	} {
		for _, stream := range []bool{false, true} {
			t.Run(tc.name+map[bool]string{false: "/http", true: "/stream"}[stream], func(t *testing.T) {
				r := NewRegistry()
				from, to := FromString(tc.from), FromString(tc.to)
				source := []byte(tc.source)
				calls := 0
				r.Register(from, to, func(model string, body []byte, gotStream bool) []byte {
					calls++
					if model != "fixture" || gotStream != stream || !bytes.Equal(body, source) {
						t.Error("transform invocation changed")
					}
					return []byte(tc.target)
				}, ResponseTransform{})
				if !r.HasRequestTransformer(from, to) {
					t.Fatal("missing registered converter")
				}
				out, err := r.TranslateRequestChecked(from, to, "fixture", source, stream)
				if err != nil || calls != 1 || gjson.GetBytes(out, tc.path).String() != tc.want {
					t.Fatalf("summary conversion failed: %s; %v", out, err)
				}
				if !bytes.Equal(source, []byte(tc.source)) {
					t.Fatal("modified inbound request")
				}
			})
		}
	}
}

func TestRegistrySummaryProjectionKeepsFallbackSourceShaped(t *testing.T) {
	for _, responseOnly := range []bool{false, true} {
		r := NewRegistry()
		if responseOnly {
			r.Register(FormatOpenAIResponse, FormatGemini, nil, ResponseTransform{})
		}
		if r.HasRequestTransformer(FormatOpenAIResponse, FormatGemini) {
			t.Fatal("missing request converter reported present")
		}
		source := []byte(`{"model":"local/fixture","reasoning":{"summary":"auto"},"input":"fixture"}`)
		out, err := r.TranslateRequestChecked(FormatOpenAIResponse, FormatGemini, "fixture", source, false)
		if err != nil || gjson.GetBytes(out, "generationConfig").Exists() || gjson.GetBytes(out, "reasoning.summary").String() != "auto" || gjson.GetBytes(out, "model").String() != "fixture" {
			t.Fatalf("fallback mixed protocols or lost model normalization: %s; %v", out, err)
		}
		if gjson.GetBytes(source, "model").String() != "local/fixture" {
			t.Fatal("fallback modified inbound buffer")
		}
	}
	r := NewRegistry()
	retired := FromString("gemini-cli")
	r.Register(retired, FormatGemini, func(string, []byte, bool) []byte { t.Fatal("retired transformer invoked"); return nil }, ResponseTransform{})
	if r.HasRequestTransformer(retired, FormatGemini) {
		t.Fatal("retired format reported supported")
	}
	if _, err := r.TranslateRequestChecked(retired, FormatGemini, "fixture", []byte(`{}`), false); !errors.Is(err, ErrGeminiCLIFormatNotSupported) {
		t.Fatalf("lost retired-format error: %v", err)
	}
}

func TestRegistrySummaryProjectionClaudeActivationUsesCatalog(t *testing.T) {
	models := []*registry.ModelInfo{{ID: t.Name() + "-adaptive", Type: "claude", Thinking: &registry.ThinkingSupport{Levels: []string{"low", "high"}}}, {ID: t.Name() + "-manual", Type: "claude", Thinking: &registry.ThinkingSupport{Min: 1024, Max: 8192}}}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(t.Name(), "claude", models)
	t.Cleanup(func() { reg.UnregisterClient(t.Name()) })
	r := NewRegistry()
	r.Register(FormatOpenAIResponse, FormatClaude, func(string, []byte, bool) []byte { return []byte(`{"messages":[],"max_tokens":4096}`) }, ResponseTransform{})
	for _, info := range models {
		out, err := r.TranslateRequestChecked(FormatOpenAIResponse, FormatClaude, info.ID, []byte(`{"reasoning":{"summary":"auto"}}`), false)
		wantType := "enabled"
		if len(info.Thinking.Levels) > 0 {
			wantType = "adaptive"
		}
		if err != nil || gjson.GetBytes(out, "thinking.type").String() != wantType || gjson.GetBytes(out, "thinking.display").String() != "summarized" {
			t.Fatalf("invalid catalog activation: %s; %v", out, err)
		}
		out, err = r.TranslateRequestChecked(FormatOpenAIResponse, FormatClaude, info.ID, []byte(`{"reasoning":{"summary":null}}`), false)
		if err != nil || gjson.GetBytes(out, "thinking").Exists() {
			t.Fatalf("hidden summary activated thinking: %s; %v", out, err)
		}
	}
}

func TestRegistrySummaryProjectionConcurrentRegistration(t *testing.T) {
	r := NewRegistry()
	transform := func(string, []byte, bool) []byte { return []byte(`{}`) }
	r.Register(FormatOpenAIResponse, FormatGemini, transform, ResponseTransform{})
	var workers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := 0; i < 100; i++ {
				r.Register(FormatOpenAIResponse, FormatGemini, transform, ResponseTransform{})
				if !r.HasRequestTransformer(FormatOpenAIResponse, FormatGemini) {
					t.Error("lost registered request converter")
					return
				}
				out, err := r.TranslateRequestChecked(FormatOpenAIResponse, FormatGemini, "fixture", []byte(`{"reasoning":{"summary":null}}`), false)
				if err != nil || gjson.GetBytes(out, "generationConfig.thinkingConfig.includeThoughts").Raw != "false" {
					t.Error("inconsistent concurrent summary conversion")
					return
				}
			}
		}()
	}
	workers.Wait()
}

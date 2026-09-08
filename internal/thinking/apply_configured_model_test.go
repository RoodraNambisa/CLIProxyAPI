package thinking_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/thinking/provider/claude"
	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/thinking/provider/codex"
	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/thinking/provider/openai"
	"github.com/tidwall/gjson"
)

func TestConfiguredThinkingUsesSelectedCapabilitiesWithoutChangingRegistry(t *testing.T) {
	name := t.Name()
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(name, "codex", []*registry.ModelInfo{{ID: name, Type: "codex", Thinking: &registry.ThinkingSupport{Levels: []string{"low", "high"}}}})
	defer reg.UnregisterClient(name)
	selected := &registry.ModelInfo{ID: name, Type: "codex", Thinking: &registry.ThinkingSupport{Levels: []string{"low", "xhigh"}}}
	source := []byte(`{"reasoning":{"effort":"xhigh"}}`)
	translated := []byte(`{"reasoning":{"effort":"low"},"input":"fixture"}`)
	before := append([]string(nil), selected.Thinking.Levels...)
	got, err := thinking.ApplyThinkingWithModelInfo(translated, source, name, "codex", "codex", "codex", selected)
	if err != nil || gjson.GetBytes(got, "reasoning.effort").String() != "xhigh" {
		t.Fatal("selected capability was replaced by aggregate registration")
	}
	if !reflect.DeepEqual(before, selected.Thinking.Levels) || !bytes.Equal(source, []byte(`{"reasoning":{"effort":"xhigh"}}`)) || !bytes.Equal(translated, []byte(`{"reasoning":{"effort":"low"},"input":"fixture"}`)) {
		t.Fatal("thinking mutated its capability or source snapshots")
	}
	if _, err := thinking.ApplyThinking(source, name, "codex", "codex", "codex"); err == nil {
		t.Fatal("legacy registry path changed its strict validation")
	}
	got, err = thinking.ApplyThinkingWithModelInfo(translated, source, name+"(low)", "codex", "codex", "codex", selected)
	if err != nil || gjson.GetBytes(got, "reasoning.effort").String() != "low" {
		t.Fatal("source effort overrode the model suffix")
	}
}

func TestConfiguredThinkingMapsOnlyCrossFamilyMaximumIntent(t *testing.T) {
	for _, tc := range []struct {
		source string
		levels []string
		want   string
	}{
		{"xhigh", []string{"high", "max", "xhigh"}, "xhigh"},
		{"xhigh", []string{"high", "max"}, "max"},
		{"xhigh", []string{"high"}, "high"},
		{"max", []string{"high", "xhigh", "max"}, "max"},
		{"max", []string{"high", "xhigh"}, "xhigh"},
		{"max", []string{"high"}, "high"},
	} {
		info := &registry.ModelInfo{ID: "private-claude", Type: "claude", Thinking: &registry.ThinkingSupport{Levels: tc.levels}}
		got, err := thinking.ApplyThinkingWithModelInfo([]byte(`{"thinking":{"type":"adaptive"},"output_config":{"effort":"low"}}`), []byte(`{"reasoning_effort":"`+tc.source+`"}`), info.ID, "openai", "claude", "claude", info)
		if err != nil || gjson.GetBytes(got, "output_config.effort").String() != tc.want {
			t.Fatalf("cross-family %s wanted %s: %v", tc.source, tc.want, err)
		}
	}
	native := &registry.ModelInfo{ID: "private-openai", Type: "openai", Thinking: &registry.ThinkingSupport{Levels: []string{"low", "high"}}}
	body := []byte(`{"reasoning_effort":"xhigh"}`)
	got, err := thinking.ApplyThinkingWithModelInfo(body, body, native.ID, "openai", "openai", "openai", native)
	if err == nil || !bytes.Equal(got, body) {
		t.Fatal("native unsupported level was silently remapped or error body lost")
	}
	compat := &registry.ModelInfo{ID: "private-compat", Type: "openai-compatibility", Thinking: &registry.ThinkingSupport{Levels: []string{"high", "max"}}}
	got, err = thinking.ApplyThinkingWithModelInfo([]byte(`{"reasoning_effort":"low"}`), body, compat.ID, "openai", "openai", "fixture", compat)
	if err != nil || gjson.GetBytes(got, "reasoning_effort").String() != "max" {
		t.Fatal("compatible provider did not use its declared maximum spelling")
	}
}

func TestConfiguredThinkingHandlesResponsesFallbackAndNonThinkingModels(t *testing.T) {
	info := &registry.ModelInfo{ID: "private-codex", Type: "codex", Thinking: &registry.ThinkingSupport{Levels: []string{"high", "xhigh"}}}
	for _, target := range []string{"codex", "openai-response"} {
		got, err := thinking.ApplyThinkingWithModelInfo([]byte(`{"reasoning":{"effort":"high"}}`), []byte(`{"reasoning":{"effort":"max"}}`), info.ID, "openai-response", target, "codex", info)
		if err != nil || gjson.GetBytes(got, "reasoning.effort").String() != "xhigh" {
			t.Fatal("Responses effort lost its source or provider mapping")
		}
	}
	body := []byte(`{"reasoning":{"effort":"high"},"input":"fixture"}`)
	for _, source := range [][]byte{nil, []byte(`{"input":"fixture"}`), []byte("invalid-json")} {
		got, err := thinking.ApplyThinkingWithModelInfo(body, source, info.ID, "codex", "codex", "codex", info)
		if err != nil || gjson.GetBytes(got, "reasoning.effort").String() != "high" {
			t.Fatal("missing source effort removed translated configuration")
		}
	}
	got, err := thinking.ApplyThinkingWithModelInfo(body, body, "without-thinking", "codex", "codex", "codex", &registry.ModelInfo{ID: "without-thinking", Type: "codex"})
	if err != nil || gjson.GetBytes(got, "reasoning.effort").Exists() || gjson.GetBytes(got, "input").String() != "fixture" {
		t.Fatal("non-thinking model bypassed canonical stripping")
	}
}

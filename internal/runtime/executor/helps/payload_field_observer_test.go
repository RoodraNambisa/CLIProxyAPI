package helps

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/tidwall/gjson"
)

func TestPayloadFieldObserverReportsAppliedRulesAndKeepsLegacyOutput(t *testing.T) {
	match := []config.PayloadModelRule{{Name: "alias", Protocol: "openai"}}
	for _, tc := range []struct {
		name, input, original string
		rules                 config.PayloadConfig
		want                  []string
	}{
		{"default", `{}`, `{}`, config.PayloadConfig{Default: []config.PayloadRule{{Models: match, Params: map[string]any{"reasoning_effort": "high"}}}}, []string{"request.reasoning_effort"}},
		{"default-already-present", `{}`, `{"request":{"reasoning_effort":"low"}}`, config.PayloadConfig{Default: []config.PayloadRule{{Models: match, Params: map[string]any{"reasoning_effort": "high"}}}}, nil},
		{"default-first-wins", `{}`, `{}`, config.PayloadConfig{Default: []config.PayloadRule{{Models: match, Params: map[string]any{"reasoning_effort": "high"}}, {Models: match, Params: map[string]any{"reasoning_effort": "low"}}}}, []string{"request.reasoning_effort"}},
		{"default-raw", `{}`, `{}`, config.PayloadConfig{DefaultRaw: []config.PayloadRule{{Models: match, Params: map[string]any{"reasoning_effort": `"high"`}}}}, []string{"request.reasoning_effort"}},
		{"override-same-value", `{"request":{"reasoning_effort":"high"}}`, `{}`, config.PayloadConfig{Override: []config.PayloadRule{{Models: match, Params: map[string]any{"reasoning_effort": "high"}}}}, []string{"request.reasoning_effort"}},
		{"override-last-wins", `{}`, `{}`, config.PayloadConfig{Override: []config.PayloadRule{{Models: match, Params: map[string]any{"reasoning_effort": "high"}}, {Models: match, Params: map[string]any{"reasoning_effort": "low"}}}}, []string{"request.reasoning_effort", "request.reasoning_effort"}},
		{"override-raw-parent", `{}`, `{}`, config.PayloadConfig{OverrideRaw: []config.PayloadRule{{Models: match, Params: map[string]any{"reasoning": `{"effort":"high"}`}}}}, []string{"request.reasoning"}},
		{"raw-string-preserves-legacy-write", `{}`, `{}`, config.PayloadConfig{OverrideRaw: []config.PayloadRule{{Models: match, Params: map[string]any{"reasoning": `{invalid`}}}}, []string{"request.reasoning"}},
		{"unserializable-raw", `{}`, `{}`, config.PayloadConfig{OverrideRaw: []config.PayloadRule{{Models: match, Params: map[string]any{"reasoning": make(chan int)}}}}, nil},
		{"invalid-value", `{}`, `{}`, config.PayloadConfig{Override: []config.PayloadRule{{Models: match, Params: map[string]any{"reasoning": make(chan int)}}}}, nil},
		{"filter-present", `{"request":{"reasoning_effort":"high"}}`, `{}`, config.PayloadConfig{Filter: []config.PayloadFilterRule{{Models: match, Params: []string{"reasoning_effort"}}}}, []string{"request.reasoning_effort"}},
		{"filter-absent", `{}`, `{}`, config.PayloadConfig{Filter: []config.PayloadFilterRule{{Models: match, Params: []string{"reasoning_effort"}}}}, []string{"request.reasoning_effort"}},
		{"protocol-mismatch", `{}`, `{}`, config.PayloadConfig{Override: []config.PayloadRule{{Models: []config.PayloadModelRule{{Name: "alias", Protocol: "claude"}}, Params: map[string]any{"reasoning_effort": "high"}}}}, nil},
		{"model-mismatch", `{}`, `{}`, config.PayloadConfig{Override: []config.PayloadRule{{Models: []config.PayloadModelRule{{Name: "other"}}, Params: map[string]any{"reasoning_effort": "high"}}}}, nil},
		{"empty", `{}`, `{}`, config.PayloadConfig{}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{Payload: tc.rules}
			payload, original := []byte(tc.input), []byte(tc.original)
			var fields []string
			got := ApplyPayloadConfigWithFieldObserver(cfg, "upstream", "openai", "request", payload, original, "alias", func(path string, before, after []byte) {
				fields = append(fields, path)
				if len(before) == 0 || len(after) == 0 {
					t.Fatal("observer lost pre/post rule views")
				}
			})
			legacy := ApplyPayloadConfigWithRoot(cfg, "upstream", "openai", "request", payload, original, "alias")
			if !bytes.Equal(got, legacy) || !reflect.DeepEqual(fields, tc.want) {
				t.Fatalf("output changed or applied fields = %v, want %v", fields, tc.want)
			}
			if string(payload) != tc.input || string(original) != tc.original {
				t.Fatal("observer processing changed input buffers")
			}
		})
	}
}

func TestPayloadFieldObserverReceivesViewsInRuleOrder(t *testing.T) {
	models := []config.PayloadModelRule{{Name: "upstream"}}
	cfg := &config.Config{Payload: config.PayloadConfig{
		Default:  []config.PayloadRule{{Models: models, Params: map[string]any{"reasoning_effort": "high"}}},
		Override: []config.PayloadRule{{Models: models, Params: map[string]any{"reasoning_effort": "low"}}},
		Filter:   []config.PayloadFilterRule{{Models: models, Params: []string{"reasoning_effort"}}},
	}}
	var transitions []string
	body := ApplyPayloadConfigWithFieldObserver(cfg, "upstream", "openai", "", []byte(`{"keep":true}`), nil, "", func(path string, before, after []byte) {
		if path != "reasoning_effort" {
			t.Fatal("observer lost the applied path")
		}
		transitions = append(transitions, gjson.GetBytes(before, path).String()+"|"+gjson.GetBytes(after, path).String())
	})
	if !reflect.DeepEqual(transitions, []string{"|high", "high|low", "low|"}) || string(body) != `{"keep":true}` {
		t.Fatalf("observer received wrong rule views: %v", transitions)
	}
}

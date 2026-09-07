package registry

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func modifiedClientCatalog(t *testing.T, modify func(*codexClientModelsPayload)) []byte {
	t.Helper()
	var payload codexClientModelsPayload
	if err := json.Unmarshal(codexClientModelsJSON, &payload); err != nil {
		t.Fatal(err)
	}
	modify(&payload)
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestCodexClientCatalogValidationAndCanonicalRevision(t *testing.T) {
	for name, modify := range map[string]func(*codexClientModelsPayload){
		"empty":             func(p *codexClientModelsPayload) { p.Models = nil },
		"duplicate":         func(p *codexClientModelsPayload) { p.Models = append(p.Models, p.Models[0]) },
		"missing fallback":  func(p *codexClientModelsPayload) { p.Models = p.Models[:1] },
		"negative":          func(p *codexClientModelsPayload) { p.Models[0]["context_window"] = -1 },
		"overflow":          func(p *codexClientModelsPayload) { p.Models[0]["context_window"] = json.Number("9223372036854775808") },
		"fraction":          func(p *codexClientModelsPayload) { p.Models[0]["context_window"] = 1.2 },
		"max below context": func(p *codexClientModelsPayload) { p.Models[0]["max_context_window"] = 1 },
		"boolean":           func(p *codexClientModelsPayload) { p.Models[0]["use_responses_lite"] = "true" },
		"invalid template": func(p *codexClientModelsPayload) {
			p.Models[0]["model_messages"] = map[string]any{"instructions_template": 1}
		},
		"priority overflow":        func(p *codexClientModelsPayload) { p.Models[0]["priority"] = int64(1 << 31) },
		"priority underflow":       func(p *codexClientModelsPayload) { p.Models[0]["priority"] = int64(-1<<31 - 1) },
		"missing shell":            func(p *codexClientModelsPayload) { delete(p.Models[0], "shell_type") },
		"invalid shell":            func(p *codexClientModelsPayload) { p.Models[0]["shell_type"] = "future_shell" },
		"invalid visibility":       func(p *codexClientModelsPayload) { p.Models[0]["visibility"] = "visible" },
		"missing supported in api": func(p *codexClientModelsPayload) { delete(p.Models[0], "supported_in_api") },
		"missing verbosity":        func(p *codexClientModelsPayload) { delete(p.Models[0], "support_verbosity") },
		"missing tools":            func(p *codexClientModelsPayload) { delete(p.Models[0], "experimental_supported_tools") },
		"missing truncation":       func(p *codexClientModelsPayload) { delete(p.Models[0], "truncation_policy") },
		"invalid truncation": func(p *codexClientModelsPayload) {
			p.Models[0]["truncation_policy"] = map[string]any{"mode": "future", "limit": 10}
		},
		"fractional truncation": func(p *codexClientModelsPayload) {
			p.Models[0]["truncation_policy"] = map[string]any{"mode": "tokens", "limit": 1.5}
		},
		"invalid description":   func(p *codexClientModelsPayload) { p.Models[0]["description"] = true },
		"invalid selector type": func(p *codexClientModelsPayload) { p.Models[0]["tool_mode"] = true },
		"missing effort description": func(p *codexClientModelsPayload) {
			p.Models[0]["supported_reasoning_levels"] = []any{map[string]any{"effort": "low"}}
			p.Models[0]["default_reasoning_level"] = "low"
		},
		"invalid level": func(p *codexClientModelsPayload) {
			p.Models[0]["supported_reasoning_levels"] = []any{map[string]any{"effort": "future"}}
		},
		"invalid default type": func(p *codexClientModelsPayload) { p.Models[0]["default_reasoning_level"] = 1 },
		"null levels":          func(p *codexClientModelsPayload) { p.Models[0]["supported_reasoning_levels"] = nil },
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateCodexClientModelsJSON(modifiedClientCatalog(t, modify)); err == nil {
				t.Fatal("invalid catalog accepted")
			}
		})
	}
	emptyLevels := modifiedClientCatalog(t, func(p *codexClientModelsPayload) {
		p.Models[0]["supported_reasoning_levels"] = []any{}
		delete(p.Models[0], "default_reasoning_level")
	})
	if err := ValidateCodexClientModelsJSON(emptyLevels); err != nil {
		t.Fatalf("valid empty reasoning array rejected: %v", err)
	}
	if err := ValidateCodexClientModelsJSON(bytes.Repeat([]byte(" "), maxModelsCatalogBytes+1)); err == nil {
		t.Fatal("oversized catalog accepted")
	}
	canonical, err := prepareCodexClientModels(codexClientModelsJSON)
	if err != nil {
		t.Fatal(err)
	}
	var pretty bytes.Buffer
	if err = json.Indent(&pretty, canonical, "", "\t"); err != nil {
		t.Fatal(err)
	}
	reformatted, err := prepareCodexClientModels(pretty.Bytes())
	if err != nil || !bytes.Equal(canonical, reformatted) {
		t.Fatal("whitespace changed canonical catalog")
	}
	var store codexClientModelsStore
	if !store.publish(canonical) || store.publish(reformatted) || store.revision != 1 {
		t.Fatal("revision changed without content change")
	}
	canonical[0] = ' '
	if store.data[0] != '{' {
		t.Fatal("published catalog retained caller's mutable buffer")
	}
}

func TestCodexClientCatalogRetainsOptionalAndUnknownFields(t *testing.T) {
	for name, modify := range map[string]func(map[string]any){
		"null optional": func(m map[string]any) {
			for _, key := range []string{"description", "model_messages", "context_window", "max_context_window", "default_reasoning_level", "auto_compact_token_limit", "tool_mode", "multi_agent_version"} {
				m[key] = nil
			}
		},
		"missing optional": func(m map[string]any) {
			for _, key := range []string{"description", "model_messages", "context_window", "max_context_window", "default_reasoning_level"} {
				delete(m, key)
			}
		},
		"empty instructions": func(m map[string]any) {
			m["description"] = ""
			m["model_messages"] = map[string]any{"instructions_template": "", "persistent_instructions": ""}
		},
		"negative display order": func(m map[string]any) { m["priority"] = -1 << 31 },
		"shell alias":            func(m map[string]any) { m["shell_type"] = "shell_command" },
		"future selector":        func(m map[string]any) { m["tool_mode"], m["multi_agent_version"] = "future", "v3" },
		"clamped compaction":     func(m map[string]any) { m["auto_compact_token_limit"] = 1000000 },
	} {
		t.Run(name, func(t *testing.T) {
			raw := modifiedClientCatalog(t, func(p *codexClientModelsPayload) { modify(p.Models[0]) })
			if err := ValidateCodexClientModelsJSON(raw); err != nil {
				t.Fatalf("valid client metadata rejected: %v", err)
			}
		})
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(codexClientModelsJSON, &root); err != nil {
		t.Fatal(err)
	}
	root["future_metadata"] = json.RawMessage(`{"revision":9007199254740993,"optional":null}`)
	raw, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := prepareCodexClientModels(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(prepared, []byte(`"revision":9007199254740993`)) || !bytes.Contains(prepared, []byte(`"optional":null`)) {
		t.Fatal("unknown top-level metadata was lost or rounded")
	}
}

func TestCodexClientCatalogCorrectsOnlyKnownAstraTemplate(t *testing.T) {
	var reviewed, obsoleteTemplate string
	old := modifiedClientCatalog(t, func(p *codexClientModelsPayload) {
		model := p.Models[0]
		messages := model["model_messages"].(map[string]any)
		reviewed = messages["instructions_template"].(string)
		obsoleteTemplate = strings.Replace(reviewed,
			"When available, you can use the `functions.request_user_input_async` tool",
			"You can use the `functions.send_user_message_async` or `functions.request_user_input_async` tool (depending on which is available)", 1)
		obsoleteTemplate = strings.Replace(obsoleteTemplate,
			"You can ask multiple questions in a single tool call. Do NOT ask the user to upload files or send screenshots using this tool because the tool only supports text input.",
			"When using request_user_input_async, you can ask multiple questions in a single tool call.", 1)
		obsoleteTemplate = strings.Replace(obsoleteTemplate, "60 seconds for a simple multi-choice question and longer for complex and bundled questions", "30 seconds for a simple multi-choice question and longer for complex and bundled questions ones", 1)
		hash := sha256.Sum256([]byte(obsoleteTemplate))
		if hex.EncodeToString(hash[:]) != "408a3625ee04de5c3e8aadfca916fa0d729efb7565fa304df3d1f583186b4856" {
			t.Fatal("test did not reconstruct the reviewed obsolete upstream template")
		}
		messages["instructions_template"], model["base_instructions"] = obsoleteTemplate, obsoleteTemplate
		messages["persistent_instructions"] = "unrelated functions.send_user_message_async"
		model["new_remote_capability"] = true
	})
	corrected, err := prepareCodexClientModels(old)
	if err != nil {
		t.Fatal(err)
	}
	var payload codexClientModelsPayload
	if err = json.Unmarshal(corrected, &payload); err != nil {
		t.Fatal(err)
	}
	astra := payload.Models[0]
	messages := astra["model_messages"].(map[string]any)
	if messages["instructions_template"] != reviewed || astra["base_instructions"] != reviewed || messages["persistent_instructions"] != "unrelated functions.send_user_message_async" || astra["new_remote_capability"] != true {
		t.Fatal("compatibility correction altered unrelated fields or retained the obsolete template")
	}
	future := obsoleteTemplate + "\nA legitimate future upstream update."
	old = modifiedClientCatalog(t, func(p *codexClientModelsPayload) {
		p.Models[0]["base_instructions"] = future
		p.Models[0]["model_messages"].(map[string]any)["instructions_template"] = future
	})
	corrected, err = prepareCodexClientModels(old)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(corrected, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Models[0]["base_instructions"] != future || payload.Models[0]["model_messages"].(map[string]any)["instructions_template"] != future {
		t.Fatal("unknown future template was overwritten")
	}
}

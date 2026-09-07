package executor

import (
	"bytes"
	"fmt"
	"net/http"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestXAICodexToolCompatibilityBoundaries(t *testing.T) {
	tools := `[{"type":"namespace","name":"codex_app","tools":[{"type":"function","name":"automation_update","strict":true,"parameters":{"oneOf":[{"type":"object"}]}}]},{"type":"namespace","name":"other","tools":[{"type":"function","name":"automation_update","strict":true,"parameters":{"oneOf":[{"type":"object"}]}}]},{"type":"custom","name":"apply_patch"},{"type":"tool_search"},{"type":"image_generation"}]`
	for _, format := range []sdktranslator.Format{sdktranslator.FormatCodex, sdktranslator.FormatOpenAIResponse} {
		for _, additional := range []bool{false, true} {
			for _, enabled := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/additional=%t/enabled=%t", format, additional, enabled), func(t *testing.T) {
					payload := []byte(`{"input":[{"role":"user","content":"fixture"}],"tools":` + tools + `,"tool_choice":{"type":"custom","name":"apply_patch"}}`)
					if additional {
						payload = []byte(`{"input":[{"type":"additional_tools","tools":` + tools + `},{"role":"user","content":"fixture"}],"tool_choice":{"type":"custom","name":"apply_patch"}}`)
					}
					original := bytes.Clone(payload)
					executor := NewXAIExecutor(&config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: enabled}})
					prepared, err := executor.prepareResponsesRequest(t.Context(), nil, core.Request{Model: "grok-4", Payload: payload}, core.Options{SourceFormat: format, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}}, true)
					if err != nil {
						t.Fatal(err)
					}
					body := prepared.body
					if gjson.GetBytes(body, "tools.#").Int() != 2 || gjson.GetBytes(body, "tools.0.name").String() != "automation_update" || gjson.GetBytes(body, "tools.0.parameters.oneOf").Exists() || gjson.GetBytes(body, "tools.0.strict").Bool() || !gjson.GetBytes(body, "tools.1.parameters.oneOf").Exists() || !gjson.GetBytes(body, "tools.1.strict").Bool() || gjson.GetBytes(body, "tool_choice").Exists() {
						t.Fatal("xAI local filtering or exact schema exception changed")
					}
					if gjson.GetBytes(body, "input.#").Int() != 1 || gjson.GetBytes(body, "input.0.role").String() != "user" || !bytes.Equal(payload, original) {
						t.Fatal("tool metadata remained in history or source was changed")
					}
				})
			}
		}
	}
}

package helps

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

func TestCodexWebSearchHistoryReplayPreservesState(t *testing.T) {
	credential := &coreauth.Auth{Metadata: map[string]any{"access_token": "fixture"}}
	history := `{"type":"web_search_call","id":"ws_fixture","action":{"type":"search","query":"fixture"}}, {"type":"reasoning","encrypted_content":"opaque","summary":[],"id":"rs_fixture"}`
	for _, lite := range []bool{false, true} {
		for _, tools := range []string{"", `,"tools":[]`, `,"tools":null`} {
			for _, choice := range []string{"", `null`, `"auto"`, `"none"`, `""`, `"required"`, `{"type":"function","name":"lookup"}`} {
				t.Run(fmt.Sprintf("lite=%t/tools=%s/choice=%s", lite, tools, choice), func(t *testing.T) {
					fields := tools
					if choice != "" {
						fields += `,"tool_choice":` + choice
					}
					body := []byte(`{"input":[` + history + `,{"type":"compaction_trigger"}],"opaque_number":9007199254740993,"previous_response_id":"resp_prior","instructions":"keep"` + fields + `}`)
					original := bytes.Clone(body)
					out, err := EnsureCodexWebSearchHistoryTool(body, credential, lite)
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(body, original) || !gjson.ValidBytes(out) {
						t.Fatal("modified caller memory or produced invalid JSON")
					}
					for _, path := range []string{"input.0", "input.1", "opaque_number", "previous_response_id", "instructions"} {
						if gjson.GetBytes(out, path).Raw != gjson.GetBytes(original, path).Raw {
							t.Fatalf("changed %s", path)
						}
					}
					toolPath, triggerPath := "tools.0", "input.2"
					if lite {
						toolPath, triggerPath = "input.2.tools.0", "input.3"
						if gjson.GetBytes(out, "tools").Raw != gjson.GetBytes(original, "tools").Raw || gjson.GetBytes(out, "input.2.role").String() != "developer" {
							t.Fatal("Lite root tools or additional-tools role changed")
						}
					} else if gjson.GetBytes(out, "input").Raw != gjson.GetBytes(original, "input").Raw {
						t.Fatal("ordinary replay changed input formatting")
					}
					if gjson.GetBytes(out, toolPath).Raw != codexCachedSearchTool || gjson.GetBytes(out, triggerPath+".type").String() != "compaction_trigger" {
						t.Fatalf("missing declaration or misplaced trigger: %s", out)
					}
					wantChoice := `"none"`
					if choice == `"required"` || strings.HasPrefix(choice, "{") {
						wantChoice = choice
					}
					if gjson.GetBytes(out, "tool_choice").Raw != wantChoice {
						t.Fatal("explicit choice changed or no-tools replay gained tool calls")
					}
					again, err := EnsureCodexWebSearchHistoryTool(out, credential, lite)
					if err != nil || !bytes.Equal(out, again) {
						t.Fatal("repeated preparation was not idempotent")
					}
				})
			}
		}
	}
}

func TestCodexWebSearchHistoryExistingToolsAndEscapes(t *testing.T) {
	credential := &coreauth.Auth{}
	for _, tc := range []struct {
		name, body, addedPath, preservedPath string
		lite                                 bool
	}{
		{"function-named-search", `{"input":[{"type":"web_search_call"}],"tools":[{"type":"function","name":"web_search"}],"tool_choice":"auto"}`, "tools.1", "tools.0", false},
		{"additional", `{"input":[{"type":"additional_tools","role":"developer","tools":[{"type":"custom","name":"exec"}],"extra":1},{"type":"web_search_call"}],"tool_choice":"auto"}`, "input.0.tools.1", "input.0.tools.0", true},
		{"empty-additional", `{"input":[{"type":"additional_tools","tools":null},{"type":"web_search_call"}]}`, "input.0.tools.0", "input.1", true},
		{"multiple-additional", `{"input":[{"type":"additional_tools","tools":[]},{"type":"additional_tools","tools":[{"type":"custom","name":"exec"}]},{"type":"web_search_call"}],"tool_choice":"auto"}`, "input.0.tools.0", "input.1", true},
		{"no-trigger", `{"input":[{"type":"web_search_call"}],"tools":[{"type":"function","name":"lookup"}],"tool_choice":"auto"}`, "input.1.tools.0", "tools", true},
		{"escaped", `{"in\u0070ut":[{"ty\u0070e":"web_se\u0061rch_call"}],"to\u006fls":[],"tool_cho\u0069ce":"auto"}`, "tools.0", "input", false},
		{"escaped-lite", `{"in\u0070ut":[{"type":"web_se\u0061rch_call"}]}`, "input.1.tools.0", "input.0", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(tc.body)
			out, err := EnsureCodexWebSearchHistoryTool(body, credential, tc.lite)
			if err != nil || gjson.GetBytes(out, tc.addedPath).Raw != codexCachedSearchTool {
				t.Fatalf("declaration missing: %s, err=%v", out, err)
			}
			if gjson.GetBytes(out, tc.preservedPath).Raw != gjson.GetBytes(body, tc.preservedPath).Raw {
				t.Fatal("existing declaration or history changed")
			}
			if HasCodexToolDeclarations(body) && gjson.GetBytes(out, "tool_choice").Raw != gjson.GetBytes(body, "tool_choice").Raw {
				t.Fatal("existing tool selection changed")
			}
		})
	}
}

func TestCodexWebSearchHistoryNoOpBoundaries(t *testing.T) {
	credential := &coreauth.Auth{}
	for _, body := range []string{
		``, `null`, `{"input":[{"type":"web_search_call"}]`,
		`{"input":"web_search_call"}`, `{"input":{"type":"web_search_call"}}`,
		`{"input":[{"type":"message","content":"web_search_call"}]}`,
		`{"input":[{"type":"function_call_output","output":{"type":"web_search_call"}}]}`,
		`{"input":[{"type":"web_search_call"}],"tools":[{"type":"web_search_preview","search_context_size":"high"}],"tool_choice":"auto"}`,
		`{"input":[{"type":"web_search_call"},{"type":"additional_tools","tools":[{"type":"web_search","external_web_access":true}]}]}`,
		`{"input":[{"type":"web_search_call"}],"tools":{"type":"function","name":"lookup"}}`,
		`{"input":[{"type":"web_search_call"},{"type":"additional_tools","tools":"invalid"}]}`,
	} {
		for _, lite := range []bool{false, true} {
			out, err := EnsureCodexWebSearchHistoryTool([]byte(body), credential, lite)
			if err != nil || string(out) != body {
				t.Fatalf("out-of-scope input changed: %s, err=%v", body, err)
			}
		}
	}
	body := []byte(`{"input":[{"type":"web_search_call"}]}`)
	for _, auth := range []*coreauth.Auth{
		nil,
		{Attributes: map[string]string{"api_key": "fixture"}, Metadata: map[string]any{"email": "fixture", "access_token": "fixture"}},
		{Attributes: map[string]string{"auth_kind": " APIKEY "}},
		{Metadata: map[string]any{"auth_kind": "api_key"}},
	} {
		out, err := EnsureCodexWebSearchHistoryTool(body, auth, true)
		if err != nil || !bytes.Equal(out, body) {
			t.Fatal("API-key or absent credential was rewritten")
		}
	}
}

func TestCodexWebSearchHistoryPlainRequestAllocations(t *testing.T) {
	body := []byte(`{"input":[{"role":"user","content":"` + strings.Repeat("x", 1<<20) + `"}]}`)
	credential := &coreauth.Auth{}
	allocations := testing.AllocsPerRun(5, func() {
		_, _ = EnsureCodexWebSearchHistoryTool(body, credential, false)
	})
	if allocations != 0 {
		t.Fatalf("plain request allocated %.0f times", allocations)
	}
}

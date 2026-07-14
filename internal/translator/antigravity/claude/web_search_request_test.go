package claude

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/tidwall/gjson"
)

func registerAntigravityWebSearchTestModel(t *testing.T, id string) {
	t.Helper()
	clientID := "test-antigravity-web-search-" + id
	registry.GetGlobalRegistry().RegisterClient(clientID, "antigravity", []*registry.ModelInfo{
		{ID: id, SupportsWebSearch: true},
	})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(clientID) })
}

func TestConvertClaudeRequestToAntigravityBuildsIndependentWebSearch(t *testing.T) {
	registerAntigravityWebSearchTestModel(t, "gemini-web-search-test")
	input := []byte(`{
		"messages":[{"role":"user","content":"old"},{"role":"assistant","content":"ok"},{"role":"user","content":[{"type":"text","text":"current query"}]}],
		"tools":[{"type":"web_search_20250305","name":"web_search","max_uses":8,"allowed_domains":["example.com"]}]
	}`)

	output := ConvertClaudeRequestToAntigravity("gemini-web-search-test", WithAntigravityWebSearchCapability(input, true), true)
	if got := gjson.GetBytes(output, "requestType").String(); got != "web_search" {
		t.Fatalf("requestType = %q, want web_search: %s", got, output)
	}
	if got := gjson.GetBytes(output, "request.contents.0.parts.0.text").String(); got != "current query" {
		t.Fatalf("query = %q, want current query: %s", got, output)
	}
	if got := gjson.GetBytes(output, "request.tools.0.googleSearch.enhancedContent.imageSearch.maxResultCount").Int(); got != 8 {
		t.Fatalf("maxResultCount = %d, want 8: %s", got, output)
	}
	if got := gjson.GetBytes(output, "request.tools.0.googleSearch.includedDomains.0").String(); got != "example.com" {
		t.Fatalf("included domain = %q, want example.com: %s", got, output)
	}
}

func TestConvertClaudeRequestToAntigravityWebSearchMixedToolsFallback(t *testing.T) {
	registerAntigravityWebSearchTestModel(t, "gemini-web-search-mixed-test")
	input := []byte(`{
		"messages":[{"role":"user","content":"query"}],
		"tools":[
			{"type":"web_search_20260209","name":"web_search"},
			{"name":"lookup","description":"lookup","input_schema":{"type":"object","properties":{}}}
		]
	}`)

	output := ConvertClaudeRequestToAntigravity("gemini-web-search-mixed-test", WithAntigravityWebSearchCapability(input, true), true)
	if gjson.GetBytes(output, "requestType").String() == "web_search" {
		t.Fatalf("mixed tools unexpectedly enabled independent web search: %s", output)
	}
	if gjson.GetBytes(output, "request.tools.#(googleSearch)").Exists() {
		t.Fatalf("mixed tools unexpectedly injected googleSearch: %s", output)
	}
	if got := gjson.GetBytes(output, `request.tools.0.functionDeclarations.0.name`).String(); got != "lookup" {
		t.Fatalf("custom tool = %q, want lookup: %s", got, output)
	}
}

func TestConvertClaudeRequestToAntigravityMixedWebSearchAnyKeepsCustomAny(t *testing.T) {
	registerAntigravityWebSearchTestModel(t, "gemini-web-search-any-test")
	input := []byte(`{
		"messages":[{"role":"user","content":"query"}],
		"tools":[
			{"type":"web_search_20260209","name":"web_search"},
			{"name":"lookup","input_schema":{"type":"object","properties":{}}}
		],
		"tool_choice":{"type":"any"}
	}`)

	output := ConvertClaudeRequestToAntigravity("gemini-web-search-any-test", WithAntigravityWebSearchCapability(input, true), true)
	if got := gjson.GetBytes(output, "requestType").String(); got == "web_search" {
		t.Fatalf("mixed any unexpectedly forced web search: %s", output)
	}
	if got := gjson.GetBytes(output, "request.tools.0.functionDeclarations.0.name").String(); got != "lookup" {
		t.Fatalf("custom tool = %q, want lookup: %s", got, output)
	}
	if got := gjson.GetBytes(output, "request.toolConfig.functionCallingConfig.mode").String(); got != "ANY" {
		t.Fatalf("mixed tool choice mode = %q, want ANY: %s", got, output)
	}
}

func TestConvertClaudeRequestToAntigravityWebSearchRequiresCapabilityAndToolChoice(t *testing.T) {
	registerAntigravityWebSearchTestModel(t, "gemini-web-search-choice-test")
	tool := `"tools":[{"type":"web_search_20250305","name":"web_search"}]`

	unsupported := ConvertClaudeRequestToAntigravity("gemini-no-web-search", []byte(`{"messages":[{"role":"user","content":"query"}],`+tool+`}`), true)
	if gjson.GetBytes(unsupported, "request.tools.#(googleSearch)").Exists() {
		t.Fatalf("unsupported model unexpectedly received googleSearch: %s", unsupported)
	}

	disabledInput := WithAntigravityWebSearchCapability([]byte(`{"messages":[{"role":"user","content":"query"}],"tool_choice":{"type":"none"},`+tool+`}`), true)
	disabled := ConvertClaudeRequestToAntigravity("gemini-web-search-choice-test", disabledInput, true)
	if gjson.GetBytes(disabled, "requestType").String() == "web_search" {
		t.Fatalf("tool_choice none unexpectedly enabled web search: %s", disabled)
	}
}

func TestConvertClaudeRequestToAntigravityUsesExplicitWebSearchChoiceWithMixedTools(t *testing.T) {
	registerAntigravityWebSearchTestModel(t, "gemini-web-search-filtered-choice-test")
	mixed := []byte(`{
		"messages":[{"role":"user","content":"query"}],
		"tools":[
			{"type":"web_search_20250305","name":"web_search"},
			{"name":"lookup","input_schema":{"type":"object","properties":{}}}
		],
		"tool_choice":{"type":"tool","name":"web_search"}
	}`)

	output := ConvertClaudeRequestToAntigravity("gemini-web-search-filtered-choice-test", WithAntigravityWebSearchCapability(mixed, true), true)
	if got := gjson.GetBytes(output, "requestType").String(); got != "web_search" {
		t.Fatalf("requestType = %q, want web_search: %s", got, output)
	}
	if gjson.GetBytes(output, "request.tools.0.functionDeclarations").Exists() {
		t.Fatalf("explicit web search retained unrelated function tools: %s", output)
	}
}

func TestConvertClaudeRequestToAntigravityDropsToolsWhenSelectedWebSearchIsUnsupported(t *testing.T) {
	mixed := []byte(`{
		"messages":[{"role":"user","content":"query"}],
		"tools":[
			{"type":"web_search_20250305","name":"web_search"},
			{"name":"lookup","input_schema":{"type":"object","properties":{}}}
		],
		"tool_choice":{"type":"tool","name":"web_search"}
	}`)

	output := ConvertClaudeRequestToAntigravity("gemini-no-web-search", mixed, true)
	if gjson.GetBytes(output, "request.tools").Exists() || gjson.GetBytes(output, "request.toolConfig").Exists() {
		t.Fatalf("unsupported selected web search exposed unrelated tools: %s", output)
	}
}

func TestConvertClaudeRequestToAntigravityDoesNotForceAnyWithoutDeclaredTools(t *testing.T) {
	input := []byte(`{
		"messages":[{"role":"user","content":"query"}],
		"tools":[{"type":"web_search_20250305","name":"web_search"}],
		"tool_choice":{"type":"any"}
	}`)

	output := ConvertClaudeRequestToAntigravity("gemini-no-web-search", input, true)
	if gjson.GetBytes(output, "request.tools").Exists() || gjson.GetBytes(output, "request.toolConfig").Exists() {
		t.Fatalf("unsupported typed-only request emitted empty tool controls: %s", output)
	}
}

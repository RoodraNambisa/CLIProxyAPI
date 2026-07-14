package executor

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestGeminiToAntigravityPreservesIndependentWebSearchRequest(t *testing.T) {
	payload := []byte(`{"requestType":"web_search","request":{"contents":[{"role":"user","parts":[{"text":"query"}]}],"tools":[{"googleSearch":{}}]}}`)
	output := geminiToAntigravity("gemini-web-search-test", payload, "project-1")

	if got := gjson.GetBytes(output, "requestType").String(); got != "web_search" {
		t.Fatalf("requestType = %q, want web_search: %s", got, output)
	}
	if gjson.GetBytes(output, "requestId").Exists() || gjson.GetBytes(output, "request.sessionId").Exists() {
		t.Fatalf("independent web search must not get agent identifiers: %s", output)
	}
	if got := gjson.GetBytes(output, "project").String(); got != "project-1" {
		t.Fatalf("project = %q, want project-1: %s", got, output)
	}
}

func TestPrepareAntigravityWebSearchTranslationPayloadUsesSelectedAuth(t *testing.T) {
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient("web-search-capable-auth", "antigravity", []*registry.ModelInfo{{
		ID: "team-a/search", UpstreamID: "gemini-web-search-test", SupportsWebSearch: true,
	}})
	reg.RegisterClient("web-search-unsupported-auth", "antigravity", []*registry.ModelInfo{{
		ID: "team-b/search", UpstreamID: "gemini-web-search-test",
	}})
	t.Cleanup(func() {
		reg.UnregisterClient("web-search-capable-auth")
		reg.UnregisterClient("web-search-unsupported-auth")
	})

	payload := []byte(`{"_cliproxy_antigravity_web_search":true,"tools":[{"type":"web_search_20250305","name":"web_search"}]}`)
	capable := prepareAntigravityWebSearchTranslationPayload(&cliproxyauth.Auth{ID: "web-search-capable-auth"}, "gemini-web-search-test(high)", payload)
	if !gjson.GetBytes(capable, "_cliproxy_antigravity_web_search").Bool() {
		t.Fatal("selected capable auth did not enable the translation marker")
	}
	unsupported := prepareAntigravityWebSearchTranslationPayload(&cliproxyauth.Auth{ID: "web-search-unsupported-auth"}, "gemini-web-search-test", payload)
	if gjson.GetBytes(unsupported, "_cliproxy_antigravity_web_search").Exists() {
		t.Fatal("selected unsupported auth retained a spoofed translation marker")
	}
}

func TestShouldResolveAntigravityWebSearchGroundingURLs(t *testing.T) {
	original := []byte(`{"tools":[{"type":"web_search_20250305","name":"web_search"}]}`)
	withGoogleSearch := []byte(`{"request":{"tools":[{"googleSearch":{}}]}}`)
	withoutGoogleSearch := []byte(`{"request":{"contents":[]}}`)

	if !shouldResolveAntigravityWebSearchGroundingURLs(sdktranslator.FormatClaude, original, withGoogleSearch) {
		t.Fatal("Claude typed web search with googleSearch should resolve grounding URLs")
	}
	if shouldResolveAntigravityWebSearchGroundingURLs(sdktranslator.FormatClaude, original, withoutGoogleSearch) {
		t.Fatal("request without googleSearch should not resolve grounding URLs")
	}
	if shouldResolveAntigravityWebSearchGroundingURLs(sdktranslator.FormatOpenAI, original, withGoogleSearch) {
		t.Fatal("non-Claude request should not resolve grounding URLs")
	}
}

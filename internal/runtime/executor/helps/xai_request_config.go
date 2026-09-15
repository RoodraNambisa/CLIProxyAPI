package helps

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const XAIClientVersion = "0.2.120"

// ApplyXAIRequestParameters restores Grok-supported client parameters which a
// shared Codex translator may discard. Defaults never override explicit values.
func ApplyXAIRequestParameters(body, original []byte, defaults map[string]any, sourceFormat string) []byte {
	paths := map[string][]string{
		"max_output_tokens": {"max_output_tokens", "max_completion_tokens", "max_tokens"},
		"temperature":       {"temperature"}, "top_p": {"top_p"},
		"parallel_tool_calls": {"parallel_tool_calls"}, "stream_tool_calls": {"stream_tool_calls"},
		"reasoning.effort": {"reasoning.effort", "reasoning_effort"},
	}
	for destination, sources := range paths {
		var explicit gjson.Result
		for _, source := range sources {
			if value := gjson.GetBytes(original, source); value.Exists() {
				explicit = value
				break
			}
		}
		if explicit.Exists() {
			body, _ = sjson.SetRawBytes(body, destination, []byte(explicit.Raw))
			continue
		}
		if xaiHasTranslatedParameter(original, destination, sourceFormat) {
			continue
		}
		key := strings.Split(destination, ".")[0]
		// Codex translators supply these defaults even when a client
		// omitted them. Grok accepts omission and owns its model defaults.
		if (sourceFormat == "openai" || sourceFormat == "openai-response" || sourceFormat == "codex" || sourceFormat == "claude" || sourceFormat == "gemini" || sourceFormat == "antigravity") &&
			(destination == "reasoning.effort" || destination == "parallel_tool_calls") {
			body, _ = sjson.DeleteBytes(body, destination)
		}
		if _, configured := defaults[key]; configured {
			body, _ = sjson.DeleteBytes(body, destination)
		}
	}
	if !gjson.GetBytes(original, "tool_choice").Exists() {
		if _, configured := defaults["tool_choice"]; configured {
			body, _ = sjson.DeleteBytes(body, "tool_choice")
		}
	}
	body = ApplyXAIDefaults(body, defaults)
	if reasoning := gjson.GetBytes(body, "reasoning"); reasoning.IsObject() && len(reasoning.Map()) == 0 {
		body, _ = sjson.DeleteBytes(body, "reasoning")
	}
	return body
}

func xaiHasTranslatedParameter(original []byte, destination, source string) bool {
	if source == "claude" {
		if destination == "parallel_tool_calls" {
			return gjson.GetBytes(original, "tool_choice.disable_parallel_tool_use").Exists()
		}
		return destination == "reasoning.effort" && gjson.GetBytes(original, "thinking.type").Exists()
	}
	if destination == "reasoning.effort" && (source == "gemini" || source == "antigravity") {
		if source == "antigravity" && gjson.GetBytes(original, "request").IsObject() {
			original = []byte(gjson.GetBytes(original, "request").Raw)
		}
		for _, path := range []string{"generationConfig.thinkingLevel", "generationConfig.thinking_level", "generationConfig.thinkingConfig.thinkingLevel", "generationConfig.thinkingConfig.thinking_level", "generationConfig.thinkingConfig.thinkingBudget", "generationConfig.thinkingConfig.thinking_budget"} {
			if gjson.GetBytes(original, path).Exists() {
				return true
			}
		}
	}
	return false
}

// ApplyXAIChatDefaults uses Chat field names without translating messages or tools.
// Existing max_tokens and max_completion_tokens both suppress the output default.
func ApplyXAIChatDefaults(body []byte, defaults map[string]any) []byte {
	for key, value := range defaults {
		path := key
		switch key {
		case "tool_choice":
			value = xaiChatToolChoiceDefault(value)
		case "max_output_tokens":
			if gjson.GetBytes(body, "max_tokens").Exists() || gjson.GetBytes(body, "max_completion_tokens").Exists() {
				continue
			}
			path = "max_completion_tokens"
		case "reasoning":
			path = "reasoning_effort"
			raw, err := json.Marshal(value)
			if err != nil || !gjson.GetBytes(raw, "effort").Exists() {
				continue
			}
			value = gjson.GetBytes(raw, "effort").Value()
		}
		if !gjson.GetBytes(body, path).Exists() {
			body, _ = sjson.SetBytes(body, path, value)
		}
	}
	return body
}

// XAIWebsocketHeaderDigest freezes connection-scoped headers. Per-turn tracing
// is emitted by the request logger; a reused websocket has no new handshake.
func XAIWebsocketHeaderDigest(headers http.Header) string {
	stable := headers.Clone()
	stable.Del("x-grok-req-id")
	stable.Del("x-grok-turn-idx")
	stable.Del("x-grok-transient-retry")
	raw, _ := json.Marshal(stable)
	return XAIIdentityDigest(string(raw))
}

// ApplyXAIResourceHeaders is shared by inference and authenticated management
// resource calls. OAuth acquisition never invokes this helper.
func ApplyXAIResourceHeaders(req *http.Request, auth *coreauth.Auth, cfg *config.Config) {
	if req == nil {
		return
	}
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	var settings config.XAIConfig
	if cfg != nil {
		settings = cfg.XAI
	}
	version := strings.TrimSpace(settings.HeaderDefaults.ClientVersion)
	if version == "" {
		version = XAIClientVersion
	}
	userAgent := strings.TrimSpace(settings.HeaderDefaults.UserAgent)
	if userAgent == "" {
		userAgent = "xai-grok-workspace/" + version
	}
	identifier := strings.TrimSpace(settings.HeaderDefaults.ClientIdentifier)
	if identifier == "" {
		identifier = "grok-shell"
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("x-grok-client-version", version)
	req.Header.Set("x-grok-client-identifier", identifier)
	if req.URL != nil && strings.EqualFold(req.URL.Hostname(), "cli-chat-proxy.grok.com") {
		req.Header.Set("X-XAI-Token-Auth", "xai-grok-cli")
		req.Header.Set("x-authenticateresponse", "authenticate-response")
	}
	attrs := make(map[string]string, len(settings.Headers))
	for key, value := range settings.Headers {
		attrs["header:"+key] = value
	}
	util.ApplyCustomHeadersFromAttrs(req, attrs)
	if auth != nil {
		util.ApplyCustomHeadersFromAttrs(req, auth.Attributes)
	}
}

// ApplyXAIDefaults fills absent translated fields. Explicit values, including
// false and zero, survive; force-overrides run later through the payload pipeline.
func ApplyXAIDefaults(body []byte, defaults map[string]any) []byte {
	keys := make([]string, 0, len(defaults))
	for key := range defaults {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		path, value := key, defaults[key]
		if key == "reasoning" {
			path = "reasoning.effort"
			if fields, ok := value.(map[string]any); ok {
				value = fields["effort"]
			} else if fields, ok := value.(map[string]string); ok {
				value = fields["effort"]
			} else {
				continue
			}
		}
		if !gjson.GetBytes(body, path).Exists() {
			body, _ = sjson.SetBytes(body, path, value)
		}
	}
	return body
}

func InjectXAISearchTools(body []byte, web, x bool) []byte {
	if !web && !x {
		return body
	}
	tools := gjson.GetBytes(body, "tools")
	if tools.Exists() && !tools.IsArray() {
		return body
	}
	for _, entry := range []struct {
		enabled bool
		kind    string
	}{{web, "web_search"}, {x, "x_search"}} {
		if !entry.enabled {
			continue
		}
		found := false
		for _, tool := range gjson.GetBytes(body, "tools").Array() {
			if tool.Get("type").String() == entry.kind || tool.Get("name").String() == entry.kind {
				found = true
				break
			}
		}
		if !found {
			body, _ = sjson.SetRawBytes(body, "tools.-1", []byte(`{"type":"`+entry.kind+`"}`))
		}
	}
	return body
}

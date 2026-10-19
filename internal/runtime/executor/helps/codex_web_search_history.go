package helps

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"unsafe"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const codexCachedSearchTool = `{"type":"web_search","external_web_access":false}`

// EnsureCodexWebSearchHistoryTool declares the hosted tool required to replay
// web_search_call history on Codex OAuth. Missing declarations can produce
// "response protection is unavailable" (Sub2API #7939). Lite carries hosted
// declarations in additional_tools; ordinary Responses uses the root tools.
// Call after payload rules and before tool selection normalization, excluding
// the dedicated /responses/compact endpoint.
func EnsureCodexWebSearchHistoryTool(body []byte, auth *coreauth.Auth, responsesLite bool) ([]byte, error) {
	if auth == nil || codexUsesAPIKeyAuth(auth) {
		return body, nil
	}
	// Escaped JSON identifiers must still reach the parser. Plain requests
	// without search history do not allocate a second copy of their body.
	if !bytes.Contains(body, []byte("web_search_call")) && !bytes.Contains(body, []byte(`\u`)) {
		return body, nil
	}
	if !gjson.ValidBytes(body) {
		return body, nil
	}
	// This view is read-only and local; sjson mutations below allocate their
	// output instead of modifying the caller's request or retaining this view.
	bodyJSON := unsafe.String(unsafe.SliceData(body), len(body))
	input := gjson.Get(bodyJSON, "input")
	if !input.IsArray() {
		return body, nil
	}
	tools := gjson.Get(bodyJSON, "tools")
	if !codexSearchToolsShapeValid(tools) || codexToolsContainHostedSearch(tools) {
		return body, nil
	}
	items := input.Array()
	hasSearchHistory := false
	hadTools := len(tools.Array()) > 0
	additionalIndex := -1
	for i, item := range items {
		switch item.Get("type").String() {
		case "web_search_call":
			hasSearchHistory = true
		case "additional_tools":
			additional := item.Get("tools")
			if !codexSearchToolsShapeValid(additional) || codexToolsContainHostedSearch(additional) {
				return body, nil
			}
			hadTools = hadTools || len(additional.Array()) > 0
			if additionalIndex < 0 {
				additionalIndex = i
			}
		}
	}
	if !hasSearchHistory {
		return body, nil
	}

	var next []byte
	var err error
	switch {
	case !responsesLite:
		next, err = appendCodexCachedSearchTool(body, "tools", tools)
	case additionalIndex >= 0:
		path := "input." + strconv.Itoa(additionalIndex) + ".tools"
		next, err = appendCodexCachedSearchTool(body, path, items[additionalIndex].Get("tools"))
	default:
		// Keep a trailing compaction trigger last and preserve each original
		// item verbatim, including opaque state and large numeric identifiers.
		at := len(items)
		if items[at-1].Get("type").String() == "compaction_trigger" {
			at--
		}
		rawItems := make([]string, 0, len(items)+1)
		additional := `{"type":"additional_tools","role":"developer","tools":[` + codexCachedSearchTool + `]}`
		for i, item := range items {
			if i == at {
				rawItems = append(rawItems, additional)
			}
			rawItems = append(rawItems, item.Raw)
		}
		if at == len(items) {
			rawItems = append(rawItems, additional)
		}
		next, err = sjson.SetRawBytes(body, "input", []byte("["+strings.Join(rawItems, ",")+"]"))
	}
	if err != nil {
		return body, fmt.Errorf("codex: declare search tool for history: %w", err)
	}
	if !hadTools {
		choice := gjson.Get(bodyJSON, "tool_choice")
		pinNone := !choice.Exists() || choice.Type == gjson.Null
		if choice.Type == gjson.String {
			switch strings.ToLower(strings.TrimSpace(choice.String())) {
			case "", "auto", "none":
				pinNone = true
			}
		}
		if pinNone {
			next, err = sjson.SetBytes(next, "tool_choice", "none")
			if err != nil {
				return body, fmt.Errorf("codex: preserve no-tools history replay: %w", err)
			}
		}
	}
	return next, nil
}

func codexSearchToolsShapeValid(tools gjson.Result) bool {
	return !tools.Exists() || tools.Type == gjson.Null || tools.IsArray()
}

func codexToolsContainHostedSearch(tools gjson.Result) bool {
	for _, tool := range tools.Array() {
		if strings.HasPrefix(strings.TrimSpace(tool.Get("type").String()), "web_search") {
			return true
		}
	}
	return false
}

func appendCodexCachedSearchTool(body []byte, path string, existing gjson.Result) ([]byte, error) {
	if existing.IsArray() {
		return sjson.SetRawBytes(body, path+".-1", []byte(codexCachedSearchTool))
	}
	return sjson.SetRawBytes(body, path, []byte("["+codexCachedSearchTool+"]"))
}

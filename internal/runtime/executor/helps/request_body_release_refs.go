package helps

import (
	"context"
	"encoding/json"
	"strings"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// RequestBodyRefs keeps executor-local request body references releasable while
// preserving the small metadata translators need after a timed release.
func RequestBodyRefs(ctx context.Context, opts cliproxyexecutor.Options, original, translated []byte) (*cliproxyexecutor.ReleasableBytes, *cliproxyexecutor.ReleasableBytes, func()) {
	originalRef := cliproxyexecutor.NewReleasableBytes(original)
	translatedRef := cliproxyexecutor.NewReleasableBytes(translated)
	unregister := cliproxyexecutor.RegisterRequestBodyReleaseCallback(ctx, opts, func([]byte) {
		originalRef.Replace(SlimRequestBodyForTranslation(original))
		translatedRef.Replace(SlimRequestBodyForTranslation(translated))
	})
	return originalRef, translatedRef, unregister
}

// RequestBodyReplayable reports whether the request body may still be replayed.
func RequestBodyReplayable(ctx context.Context, opts cliproxyexecutor.Options) bool {
	if ctrl := cliproxyexecutor.RequestBodyReleaseControllerFromOptions(opts); ctrl != nil {
		return ctrl.Replayable()
	}
	if ctrl := cliproxyexecutor.RequestBodyReleaseControllerFromContext(ctx); ctrl != nil {
		return ctrl.Replayable()
	}
	return true
}

// ReleaseRequestBodyAfterStreamEstablished releases request body references once
// an upstream stream is established and later replay is no longer needed.
func ReleaseRequestBodyAfterStreamEstablished(ctx context.Context, opts cliproxyexecutor.Options) bool {
	if ctrl := cliproxyexecutor.RequestBodyReleaseControllerFromOptions(opts); ctrl != nil {
		return ctrl.ReleaseWithPlaceholder(cliproxyexecutor.RequestBodyReleaseStreamPlaceholder(ctrl.OriginalSize(), ctrl.LogOnly()))
	}
	if ctrl := cliproxyexecutor.RequestBodyReleaseControllerFromContext(ctx); ctrl != nil {
		return ctrl.ReleaseWithPlaceholder(cliproxyexecutor.RequestBodyReleaseStreamPlaceholder(ctrl.OriginalSize(), ctrl.LogOnly()))
	}
	return false
}

// SlimRequestBodyForTranslation drops prompt/content payloads and keeps only
// request metadata used by response translators.
func SlimRequestBodyForTranslation(body []byte) []byte {
	if len(body) == 0 || !gjson.ValidBytes(body) {
		return nil
	}
	out := []byte(`{}`)
	copied := false
	for _, path := range []string{
		"model",
		"stream",
		"parallel_tool_calls",
		"tool_choice",
		"response_format",
		"reasoning",
		"include",
		"thinking",
	} {
		if result := gjson.GetBytes(body, path); result.Exists() {
			out, _ = sjson.SetRawBytes(out, path, []byte(result.Raw))
			copied = true
		}
	}
	if tools := slimRequestTools(gjson.GetBytes(body, "tools")); len(tools) > 0 {
		out, _ = sjson.SetRawBytes(out, "tools", tools)
		copied = true
	}
	if nested := gjson.GetBytes(body, "request"); nested.IsObject() {
		if slim := SlimRequestBodyForTranslation([]byte(nested.Raw)); len(slim) > 0 {
			out, _ = sjson.SetRawBytes(out, "request", slim)
			copied = true
		}
	}
	if !copied {
		return nil
	}
	return out
}

func slimRequestTools(tools gjson.Result) []byte {
	if !tools.IsArray() {
		return nil
	}
	type slimDeclaration struct {
		Name string `json:"name,omitempty"`
	}
	type slimFunction struct {
		Name string `json:"name,omitempty"`
	}
	type slimTool struct {
		Type                 string            `json:"type,omitempty"`
		Name                 string            `json:"name,omitempty"`
		Function             *slimFunction     `json:"function,omitempty"`
		FunctionDeclarations []slimDeclaration `json:"functionDeclarations,omitempty"`
	}
	out := make([]slimTool, 0, len(tools.Array()))
	tools.ForEach(func(_, tool gjson.Result) bool {
		item := slimTool{
			Type: strings.TrimSpace(tool.Get("type").String()),
			Name: strings.TrimSpace(tool.Get("name").String()),
		}
		if name := strings.TrimSpace(tool.Get("function.name").String()); name != "" {
			item.Function = &slimFunction{Name: name}
		}
		if declarations := tool.Get("functionDeclarations"); declarations.IsArray() {
			for _, declaration := range declarations.Array() {
				if name := strings.TrimSpace(declaration.Get("name").String()); name != "" {
					item.FunctionDeclarations = append(item.FunctionDeclarations, slimDeclaration{Name: name})
				}
			}
		}
		if item.Type != "" || item.Name != "" || item.Function != nil || len(item.FunctionDeclarations) > 0 {
			out = append(out, item)
		}
		return true
	})
	if len(out) == 0 {
		return nil
	}
	data, err := json.Marshal(out)
	if err != nil {
		return nil
	}
	return data
}

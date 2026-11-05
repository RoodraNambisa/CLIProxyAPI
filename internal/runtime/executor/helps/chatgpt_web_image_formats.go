package helps

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type ChatGPTWebImageOutputFormatError struct {
	Model, Format string
	Allowed       []string
}

func (e *ChatGPTWebImageOutputFormatError) Error() string {
	b, _ := json.Marshal(map[string]any{"error": map[string]any{"type": "invalid_request_error", "code": "unsupported_parameter", "param": "output_format", "message": fmt.Sprintf("ChatGPT Web image alias %q does not support output_format %q; allowed formats: %s", e.Model, e.Format, strings.Join(e.Allowed, ", "))}})
	return string(b)
}
func (*ChatGPTWebImageOutputFormatError) StatusCode() int      { return 400 }
func (*ChatGPTWebImageOutputFormatError) SkipAuthResult() bool { return true }
func (*ChatGPTWebImageOutputFormatError) RetryOtherAuth() bool { return false }
func (*ChatGPTWebImageOutputFormatError) ExecutionResultErrorCode() string {
	return "unsupported_parameter"
}

func ValidateChatGPTWebImageOutputFormat(snapshot core.ChatGPTWebImageConfigSnapshot, outerModel, executionModel, toolModel, format string) *ChatGPTWebImageOutputFormatError {
	model := ""
	for _, candidate := range []string{outerModel, executionModel} {
		candidate = strings.TrimSpace(thinking.ParseSuffix(candidate).ModelName)
		_, registered := snapshot.ModelOutputFormats[strings.ToLower(candidate)]
		if registered || ChatGPTWebModelUsesImageGeneration(candidate) {
			model = candidate
			break
		}
	}
	if model == "" {
		model = strings.TrimSpace(toolModel)
	}
	if model == "" {
		model = snapshot.DefaultImageModel
	}
	if model == "" {
		model = "gpt-image-2"
	}
	allowed, exists := snapshot.ModelOutputFormats[strings.ToLower(model)]
	if !exists {
		allowed = config.DefaultChatGPTWebImageOutputFormats(model)
	}
	format = strings.ToLower(strings.TrimSpace(format))
	if format == "" {
		format = "png"
	}
	if format == "jpg" {
		format = "jpeg"
	}
	// Unknown provider parameters still follow the existing ignore policy.
	if !slices.Contains(config.ChatGPTWebImageOutputFormats(), format) {
		return nil
	}
	if !slices.Contains(allowed, format) {
		return &ChatGPTWebImageOutputFormatError{Model: model, Format: format, Allowed: append([]string(nil), allowed...)}
	}
	return nil
}

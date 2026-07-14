package translator

import (
	"errors"
	"strings"
)

// ErrGeminiCLIFormatNotSupported reports use of the retired Gemini CLI translation format.
var ErrGeminiCLIFormatNotSupported = errors.New("translator: Gemini CLI format is no longer supported")

// Common format identifiers exposed for SDK users.
const (
	FormatOpenAI         Format = "openai"
	FormatOpenAIResponse Format = "openai-response"
	FormatClaude         Format = "claude"
	FormatGemini         Format = "gemini"
	// FormatGeminiCLI is retained for v6 source compatibility. No translator is registered for it.
	FormatGeminiCLI   Format = "gemini-cli"
	FormatCodex       Format = "codex"
	FormatAntigravity Format = "antigravity"
)

func usesRetiredGeminiCLIFormat(from, to Format) bool {
	return strings.EqualFold(strings.TrimSpace(string(from)), string(FormatGeminiCLI)) ||
		strings.EqualFold(strings.TrimSpace(string(to)), string(FormatGeminiCLI))
}

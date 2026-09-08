package registry

const (
	CodexRealtimeModelType = "codex-realtime"
	CodexLiveModelID       = "gpt-live-1-codex"
	CodexRealtimeModelID   = "gpt-realtime"
)

// GetCodexRealtimeModels supplies routes for enabled native realtime endpoints.
// These are not Responses models and must not inherit Codex prompt templates.
func GetCodexRealtimeModels() []*ModelInfo {
	return []*ModelInfo{
		{ID: CodexLiveModelID, Object: "model", OwnedBy: "openai", Type: CodexRealtimeModelType, DisplayName: "Codex Live", Description: "Native Codex Live and Realtime endpoints only"},
		{ID: CodexRealtimeModelID, Object: "model", OwnedBy: "openai", Type: CodexRealtimeModelType, DisplayName: "GPT Realtime", Description: "Native Realtime endpoints only"},
	}
}

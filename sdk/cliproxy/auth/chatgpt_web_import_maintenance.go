package auth

import (
	"context"
	"strings"
)

const (
	ChatGPTWebImportSessionIntent     = "import_session_refresh_pending"
	ChatGPTWebImportModelsIntent      = "import_model_validation_pending"
	ChatGPTWebImportAccountInfoIntent = "import_account_info_refresh_pending"
)

// ChatGPTWebImportIntent reports whether a persisted upload-maintenance intent
// is present on the supplied credential.
func ChatGPTWebImportIntent(auth *Auth, key string) bool {
	if auth == nil || auth.Metadata == nil {
		return false
	}
	value, _ := auth.Metadata[strings.TrimSpace(key)].(bool)
	return value
}

// ClearChatGPTWebImportIntentIfCurrent durably clears one upload-maintenance
// intent without starting another remote model synchronization. Instance
// replacement makes the operation a no-op.
func (m *Manager) ClearChatGPTWebImportIntentIfCurrent(ctx context.Context, expected *Auth, key string) (bool, error) {
	if m == nil || expected == nil {
		return false, nil
	}
	key = strings.TrimSpace(key)
	switch key {
	case ChatGPTWebImportSessionIntent, ChatGPTWebImportModelsIntent, ChatGPTWebImportAccountInfoIntent:
	default:
		return false, nil
	}
	ctx = WithChatGPTWebImportPolicy(ctx, ChatGPTWebImportPolicy{})
	_, current, errMutate := m.MutateRuntimeMetadataIfCurrent(ctx, expected, func(auth *Auth) {
		if auth != nil && auth.Metadata != nil {
			delete(auth.Metadata, key)
		}
	})
	return current, errMutate
}

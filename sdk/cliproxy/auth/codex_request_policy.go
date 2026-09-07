package auth

import "context"

// CodexMultiAgentV2RequestSetting exposes the scalar policy captured at logical
// request entry. Executors still validate the original client identity.
func CodexMultiAgentV2RequestSetting(ctx context.Context) (bool, bool) {
	if ctx != nil {
		if captured, _ := ctx.Value(retrySettingsContextKey{}).(*requestRetrySettings); captured != nil {
			return captured.codexOptimizeMultiAgentV2, true
		}
	}
	return false, false
}

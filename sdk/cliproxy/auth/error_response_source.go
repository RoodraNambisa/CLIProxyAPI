package auth

import (
	"strings"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func errorResponseSourceForAuth(auth *Auth, fallbackProvider string) cliproxyexecutor.ErrorResponseSourceSnapshot {
	if auth == nil {
		return cliproxyexecutor.LocalErrorResponseSource()
	}
	provider := strings.ToLower(strings.TrimSpace(fallbackProvider))
	if authProvider := strings.ToLower(strings.TrimSpace(auth.Provider)); authProvider != "" {
		provider = authProvider
	}
	if provider == "" {
		return cliproxyexecutor.LocalErrorResponseSource()
	}
	return cliproxyexecutor.CredentialErrorResponseSource(provider, authPriority(auth))
}

func withAuthErrorResponseSource(err error, auth *Auth, fallbackProvider string) error {
	return cliproxyexecutor.WithErrorResponseSource(err, errorResponseSourceForAuth(auth, fallbackProvider))
}

func withInheritedErrorResponseSource(err, sourceErr error) error {
	if source, ok := cliproxyexecutor.ErrorResponseSourceOf(sourceErr); ok {
		return cliproxyexecutor.WithErrorResponseSource(err, source)
	}
	return err
}

func publishErrorResponseSourceMetadata(meta map[string]any, source cliproxyexecutor.ErrorResponseSourceSnapshot) {
	if len(meta) == 0 {
		return
	}
	if callback, ok := meta[cliproxyexecutor.SelectedAuthSourceCallbackMetadataKey].(func(cliproxyexecutor.ErrorResponseSourceSnapshot)); ok && callback != nil {
		callback(source)
	}
}

func finalizeErrorResponseSource(meta map[string]any, err error) error {
	if err == nil {
		return nil
	}
	if source, ok := cliproxyexecutor.ErrorResponseSourceOf(err); ok {
		publishErrorResponseSourceMetadata(meta, source)
	}
	return cliproxyexecutor.WithoutErrorResponseSource(err)
}

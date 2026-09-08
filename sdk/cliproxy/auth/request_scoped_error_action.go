package auth

import (
	"context"
	"errors"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type requestScopedActionError struct {
	error
	action config.RequestScopedErrorAction
}

func (err *requestScopedActionError) Unwrap() error { return err.error }
func (err *requestScopedActionError) IsRequestStop() bool {
	return err.action == config.RequestScopedActionStop || err.action == config.RequestScopedActionStopAndCooldown
}

func isRequestScopedStopError(err error) bool {
	var stop interface{ IsRequestStop() bool }
	return errors.As(err, &stop) && stop.IsRequestStop()
}

func requestScopedErrorBody(err error) string {
	var source interface{ ResponseBody() []byte }
	if errors.As(err, &source) {
		if body := source.ResponseBody(); len(body) != 0 {
			return string(body)
		}
	}
	var authError *Error
	if errors.As(err, &authError) && authError != nil && authError.Message != "" {
		return authError.Message
	}
	return err.Error()
}

func (m *Manager) matchRequestScopedErrorAction(ctx context.Context, auth *Auth, opts core.Options, err error) (config.RequestScopedErrorAction, bool) {
	if m == nil || auth == nil || err == nil {
		return "", false
	}
	var rules *config.CompiledRequestScopedErrors
	if captured := auth.requestScopedErrorRules; captured != nil {
		rules = captured.rules
	}
	if rules == nil {
		// File projection explicitly distinguishes OAuth from API-key entries.
		// A configured API key never inherits OAuth rules merely due to an email.
		kind := strings.ToLower(strings.TrimSpace(auth.Attributes["auth_kind"]))
		if kind == "apikey" || kind == "api_key" || strings.TrimSpace(auth.Attributes["api_key"]) != "" {
			return "", false
		}
		channel := modelAliasChannel(auth)
		if channel == "" {
			return "", false
		}
		rules = m.requestOAuthErrorRules(ctx, channel)
	}
	if rules == nil {
		return "", false
	}
	// Matching may still control availability after a body was released.
	// Callers retain their output-commit, replay and retry-budget checks.
	if (ctx != nil && ctx.Err() != nil) || isRuntimeAuthInstanceRetiredError(err) ||
		runtimeAuthInstanceRetiredContext(ctx) || skipAuthResultForError(err) ||
		m.isRequestInvalidError(err, ctx) || isResponsesCompactRequestFaultError(opts, err) ||
		core.IsImageExecutionCapacityError(err) {
		return "", false
	}
	return rules.Match(statusCodeFromError(err), requestScopedErrorBody(err))
}

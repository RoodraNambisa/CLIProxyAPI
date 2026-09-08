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

func requestScopedActionFromError(err error) (config.RequestScopedErrorAction, bool) {
	var handled *requestScopedActionError
	if errors.As(err, &handled) && handled != nil {
		return handled.action, true
	}
	return "", false
}

func wrapRequestScopedAction(err error, action config.RequestScopedErrorAction, matched bool) error {
	if err == nil || !matched {
		return err
	}
	return &requestScopedActionError{error: err, action: action}
}

func applyRequestScopedActionToResult(action config.RequestScopedErrorAction, matched bool, result *executionResult) {
	if !matched || result == nil || result.Error == nil {
		return
	}
	result.Error = cloneError(result.Error)
	result.Error.requestScopedAction = action
}

func requestScopedActionSuppressesCooldown(err *Error) bool {
	return err != nil && (err.requestScopedAction == config.RequestScopedActionStop || err.requestScopedAction == config.RequestScopedActionContinue)
}

func requestScopedActionNeedsFallbackCooldown(err *Error) bool {
	if err == nil || (err.requestScopedAction != config.RequestScopedActionStopAndCooldown && err.requestScopedAction != config.RequestScopedActionContinueAndCooldown) {
		return false
	}
	// Existing status-specific durations and Retry-After handling remain intact.
	switch err.HTTPStatus {
	case 401, 402, 403, 404, 408, 429, 500, 502, 503, 504:
		return false
	default:
		return true
	}
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

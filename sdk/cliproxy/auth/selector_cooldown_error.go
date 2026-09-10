package auth

import (
	"errors"
	"net/http"
)

func (e *modelCooldownError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// Restore only presentation metadata; keep the duration, provider and original failure chain.
func restoreModelCooldownErrorModel(err error, requestedModel string) error {
	if err == nil || requestedModel == "" || statusCodeFromError(err) != http.StatusTooManyRequests {
		return err
	}
	var cooldown *modelCooldownError
	if !errors.As(err, &cooldown) || cooldown == nil || cooldown.model != "" {
		return err
	}
	restored := *cooldown
	restored.model = requestedModel
	restored.cause = err
	return &restored
}

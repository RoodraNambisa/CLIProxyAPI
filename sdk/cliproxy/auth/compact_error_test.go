package auth

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestCompactRequestFaultStopsWithoutCoolingOrdinaryResponses(t *testing.T) {
	for _, status := range []int{400, 404, 405, 409, 413, 422, 501} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			manager, exec := newCredentialRetryLimitTestManagerWithAuthCount(t, 0, 2)
			manager.SetRetryConfig(3, 0, 0)
			original := &Error{HTTPStatus: status, Message: "compact endpoint or parameter unavailable"}
			exec.err = original
			_, err := manager.Execute(t.Context(), []string{"claude"}, executor.Request{Model: "test-model"}, executor.Options{Alt: "responses/compact"})
			if !errors.Is(err, original) || exec.Calls() != 1 {
				t.Fatal("compact request fault retried or lost original error")
			}
			for _, auth := range manager.List() {
				if auth.Unavailable || auth.LastError != nil || len(auth.ModelStates) > 0 {
					t.Fatal("compact endpoint fault changed normal model availability")
				}
			}
			_, err = manager.ExecuteStream(t.Context(), []string{"claude"}, executor.Request{Model: "test-model"}, executor.Options{Alt: "responses/compact"})
			if !errors.Is(err, original) || exec.Calls() != 2 {
				t.Fatal("compact SDK stream request fault retried")
			}
			for _, auth := range manager.List() {
				if auth.Unavailable || len(auth.ModelStates) > 0 {
					t.Fatal("unsupported compact stream changed normal availability")
				}
			}
		})
	}
}

func TestCompactTransientErrorsKeepExistingAttemptBudgetAndDoNotCool(t *testing.T) {
	manager, exec := newCredentialRetryLimitTestManagerWithAuthCount(t, 0, 2)
	original := &Error{HTTPStatus: 503, Message: "temporary compact failure"}
	exec.err = original
	_, err := manager.Execute(t.Context(), []string{"claude"}, executor.Request{Model: "test-model"}, executor.Options{Alt: "responses/compact"})
	if !errors.Is(err, original) || exec.Calls() != 2 {
		t.Fatal("compact transient failure changed the existing attempt budget")
	}
	for _, auth := range manager.List() {
		if auth.Unavailable || len(auth.ModelStates) > 0 {
			t.Fatal("compact transient failure cooled normal Responses")
		}
	}
	_, _ = manager.Execute(t.Context(), []string{"claude"}, executor.Request{Model: "test-model"}, executor.Options{})
	cooled := false
	for _, auth := range manager.List() {
		cooled = cooled || auth.Unavailable || len(auth.ModelStates) > 0
	}
	if !cooled {
		t.Fatal("normal endpoint inherited compact availability exemption")
	}
}

func TestCompactKeepsCredentialFaultsAndCancellationAuthoritative(t *testing.T) {
	for _, status := range []int{401, 402, 403, 429} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			manager, exec := newCredentialRetryLimitTestManagerWithAuthCount(t, 0, 2)
			exec.err = &Error{HTTPStatus: status, Message: "credential rejected"}
			_, err := manager.Execute(t.Context(), []string{"claude"}, executor.Request{Model: "test-model"}, executor.Options{Alt: "responses/compact"})
			if err == nil || exec.Calls() != 2 {
				t.Fatal("compact credential error stopped normal rotation")
			}
			for _, auth := range manager.List() {
				if !auth.Unavailable {
					t.Fatal("compact credential error bypassed normal cooling")
				}
			}
		})
	}
	opts := executor.Options{Alt: "responses/compact"}
	for _, err := range []error{&Error{HTTPStatus: 400, Message: `{"error":"invalid_grant"}`}, &Error{HTTPStatus: 400, Message: `{"error":{"type":"authentication_error"}}`}} {
		if isResponsesCompactAvailabilityNeutralError(opts, err) || isResponsesCompactRequestFaultError(opts, err) {
			t.Fatal("credential evidence was reclassified as compact request fault")
		}
	}
	manager, exec := newCredentialRetryLimitTestManagerWithAuthCount(t, 0, 2)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := manager.Execute(ctx, []string{"claude"}, executor.Request{Model: "test-model"}, opts)
	if !errors.Is(err, context.Canceled) || exec.Calls() != 0 {
		t.Fatal("canceled compact request executed upstream")
	}
}

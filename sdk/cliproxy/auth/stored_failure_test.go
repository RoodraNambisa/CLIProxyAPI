package auth

import (
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestStoredAuthFailurePreservesCurrentClassification(t *testing.T) {
	current := &Error{Code: "auth_unavailable", Message: "no auth available", HTTPStatus: 503}
	previous := &Error{Code: "misalignment_policy_violation", Message: "private fixture message", HTTPStatus: 400, Diagnostic: &ErrorDiagnostic{Stage: "request"}}
	wrapped := WithStoredAuthFailure(current, previous)
	if wrapped.Error() != current.Error() || !errors.Is(wrapped, current) || errors.Is(wrapped, previous) {
		t.Fatal("stored failure changed the current error")
	}
	var typed *Error
	if !errors.As(wrapped, &typed) || typed != current || statusCodeFromError(wrapped) != 503 {
		t.Fatal("stored failure changed classification")
	}
	if isRequestInvalidErrorWithConfig(wrapped, nil) || cliproxyexecutor.IsUpstreamAttemptError(wrapped) {
		t.Fatal("stored failure became current request evidence")
	}
	encoded, errMarshal := json.Marshal(wrapped)
	expected, _ := json.Marshal(current)
	if errMarshal != nil || string(encoded) != string(expected) {
		t.Fatal("stored private data was serialized")
	}
	previous.Message = "changed"
	previous.Diagnostic.Stage = "changed"
	copied := StoredAuthFailureOf(wrapped)
	if copied.Message != "private fixture message" || copied.Diagnostic.Stage != "request" {
		t.Fatal("failure snapshot was shared")
	}
	copied.Diagnostic.Stage = "changed again"
	if StoredAuthFailureOf(wrapped).Diagnostic.Stage != "request" {
		t.Fatal("returned failure snapshot was shared")
	}
	if WithStoredAuthFailure(nil, previous) != nil || WithStoredAuthFailure(current, nil) != current || StoredAuthFailureOf(nil) != nil {
		t.Fatal("nil behavior changed")
	}
}

func TestStoredAuthFailureSurvivesCooldownFinalization(t *testing.T) {
	previous := &Error{Message: "private fixture body", HTTPStatus: 429, Diagnostic: &ErrorDiagnostic{Stage: "responses"}}
	base := newModelCooldownError("fixture-model", "codex", 30*time.Second)
	source := cliproxyexecutor.CredentialErrorResponseSource("codex", 2)
	original := cliproxyexecutor.WithErrorResponseSource(WithStoredAuthFailure(base, previous), source)
	finalized := finalAuthSelectionError(original)
	var selection *Error
	var cooldown *modelCooldownError
	if !errors.As(finalized, &selection) || selection.Code != "auth_unavailable" || errors.As(finalized, &cooldown) {
		t.Fatal("final selection contract changed")
	}
	if !reflect.DeepEqual(StoredAuthFailureOf(finalized), previous) {
		t.Fatal("finalization discarded stored failure")
	}
	var headers interface{ Headers() http.Header }
	if !errors.As(finalized, &headers) || headers.Headers().Get("Retry-After") != "30" {
		t.Fatal("retry hint was lost")
	}
	resultHeaders := headers.Headers()
	resultHeaders.Set("Retry-After", "99")
	if headers.Headers().Get("Retry-After") != "30" {
		t.Fatal("retry hint was shared")
	}
	if got, ok := cliproxyexecutor.ErrorResponseSourceOf(finalized); !ok || got != source {
		t.Fatal("response source was lost")
	}
}

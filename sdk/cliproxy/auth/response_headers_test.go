package auth

import (
	"errors"
	"net/http"
	"testing"
	"time"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestResponseHeaderErrorKeepsIdentityAndIndependentSnapshots(t *testing.T) {
	base := &Error{Code: "auth_unavailable", Message: "no auth available", HTTPStatus: 503}
	headers := http.Header{"Retry-After": {"30"}}
	wrapped := WithResponseHeaders(base, headers)
	headers.Set("Retry-After", "99")
	var typed *Error
	var accessor interface{ Headers() http.Header }
	if !errors.Is(wrapped, base) || !errors.As(wrapped, &typed) || typed != base || statusCodeFromError(wrapped) != 503 || !errors.As(wrapped, &accessor) {
		t.Fatal("response hints changed the underlying error")
	}
	if accessor.Headers().Get("Retry-After") != "30" {
		t.Fatal("caller mutation changed the captured headers")
	}
	accessor.Headers().Set("Retry-After", "77")
	if accessor.Headers().Get("Retry-After") != "30" {
		t.Fatal("returned headers exposed internal state")
	}
	if WithResponseHeaders(nil, headers) != nil || WithResponseHeaders(base, nil) != base {
		t.Fatal("empty response hints changed the old error path")
	}
}

func TestFinalSelectionCooldownKeepsHintAndUnavailableContract(t *testing.T) {
	source := core.CredentialErrorResponseSource("claude", 3)
	cooldown := newModelCooldownError("test-model", "claude", time.Minute)
	converted := finalAuthSelectionError(core.WithErrorResponseSource(cooldown, source))
	var base *Error
	var exposed *modelCooldownError
	var headers interface{ Headers() http.Header }
	if !errors.As(converted, &base) || base.Code != "auth_unavailable" || errors.As(converted, &exposed) {
		t.Fatal("final selection conversion changed the existing public error contract")
	}
	if !errors.As(converted, &headers) || headers.Headers().Get("Retry-After") != "60" {
		t.Fatal("final selection conversion discarded its response hint")
	}
	if got, ok := core.ErrorResponseSourceOf(converted); !ok || got != source {
		t.Fatal("conversion changed error-source filtering")
	}
	stripped := core.WithoutErrorResponseSource(converted)
	if !errors.As(stripped, &headers) || headers.Headers().Get("Retry-After") != "60" {
		t.Fatal("source finalization discarded response hints")
	}
}

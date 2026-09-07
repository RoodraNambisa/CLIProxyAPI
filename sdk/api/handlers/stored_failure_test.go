package handlers

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestStoredAuthFailureEnrichmentKeepsPrivateDetailsInternal(t *testing.T) {
	for _, status := range []int{0, 200, 401, 429, 503, 700} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			current := &coreauth.Error{Code: "auth_unavailable", Message: "no auth available"}
			previous := &coreauth.Error{
				Code: "test-private-code", Message: "test-private-body", HTTPStatus: status,
				Diagnostic: &coreauth.ErrorDiagnostic{ResponseBody: "test-private-diagnostic", Stage: "upstream"},
			}
			source := coreexecutor.CredentialErrorResponseSource("codex", 2)
			wrapped := coreexecutor.WithErrorResponseSource(coreauth.WithStoredAuthFailure(
				coreauth.WithResponseHeaders(current, http.Header{"Retry-After": {"30"}}), previous,
			), source)
			message := executionErrorMessage(wrapped, []string{"codex"}, "fixture-model")
			if message.StatusCode != 503 || message.Addon.Get("Retry-After") != "30" {
				t.Fatal("stored failure changed public status or retry hint")
			}
			var selection *coreauth.Error
			if !errors.As(message.Error, &selection) || selection.Code != "auth_unavailable" {
				t.Fatal("selection type was lost")
			}
			if !reflect.DeepEqual(coreauth.StoredAuthFailureOf(message.Error), previous) {
				t.Fatal("stored error details were lost")
			}
			if got, ok := coreexecutor.ErrorResponseSourceOf(message.Error); !ok || got != source {
				t.Fatal("response source was lost")
			}
			if coreexecutor.IsUpstreamAttemptError(message.Error) {
				t.Fatal("historical failure was counted as an upstream attempt")
			}
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			NewBaseAPIHandlers(nil, nil).WriteErrorResponse(ctx, message)
			body := recorder.Body.String()
			if recorder.Code != 503 || recorder.Header().Get("Retry-After") != "30" || strings.Contains(body, "test-private") {
				t.Fatal("response leaked history or changed classification")
			}
			if strings.Contains(body, "previous credential failure") != (status >= 400 && status <= 599) {
				t.Fatal("invalid historical status summary")
			}
			if current.Message != "no auth available" || current.HTTPStatus != 0 {
				t.Fatal("enrichment changed its input")
			}
		})
	}
}

package auth

import (
	"errors"
	"net/http"
	"testing"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
)

type diagnosticExecutionTestError struct {
	diagnostic *ErrorDiagnostic
}

func TestNewProviderErrorConvertsChatGPTWebAuthenticationDiagnostic(t *testing.T) {
	auth := &Auth{ID: "passkey.json", Provider: chatgptwebauth.Provider}
	err := &chatgptwebauth.AuthError{
		Code:           "passkey_verification_failed",
		DiagnosticCode: "invalid_passkey_response",
		FailureStage:   "passkey_verify",
		Attempts:       2,
		StatusCode:     http.StatusBadRequest,
		ResponseType:   "json",
		ContentType:    "application/json",
		CFRay:          "safe-ray-SJC",
		TargetHost:     "auth.openai.com",
		TargetPath:     "/api/accounts/passkey/verify",
		ResponseBytes:  48,
		ResponseBody:   `{"error":{"code":"invalid_passkey_response"}}`,
		Retryable:      false,
		Message:        "Passkey credential was rejected",
	}
	result := NewProviderError(auth, err)
	if result == nil || result.Diagnostic == nil {
		t.Fatalf("result = %#v", result)
	}
	if result.Code != "" || result.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("business error changed = %#v", result)
	}
	diagnostic := result.Diagnostic
	if diagnostic.Code != "invalid_passkey_response" || diagnostic.Stage != "passkey_verify" || diagnostic.Attempts != 2 ||
		diagnostic.TargetHost != "auth.openai.com" || diagnostic.TargetPath != "/api/accounts/passkey/verify" || diagnostic.AuthIndex != auth.EnsureIndex() ||
		diagnostic.ResponseBody != err.ResponseBody {
		t.Fatalf("diagnostic = %#v", diagnostic)
	}
}

func (err diagnosticExecutionTestError) Error() string { return "opaque upstream failure" }

func (err diagnosticExecutionTestError) StatusCode() int { return 403 }

func (err diagnosticExecutionTestError) AuthErrorDiagnostic() *ErrorDiagnostic {
	return err.diagnostic.Clone()
}

func TestExecutionResultErrorEnrichesDiagnosticWithoutChangingBusinessCode(t *testing.T) {
	auth := &Auth{
		ID:       "diagnostic-chatgpt-web.json",
		Provider: "chatgpt-web",
		Metadata: map[string]any{
			"type": "chatgpt-web",
			"persona": map[string]any{
				"profile":    "chrome_146",
				"user_agent": "Mozilla/5.0 Chrome/146.0.0.0 Safari/537.36",
				"platform":   "MacIntel",
			},
		},
	}
	source := &ErrorDiagnostic{
		Provider:   "chatgpt-web",
		Stage:      "file_sign",
		Code:       "cloudflare_challenge",
		HTTPStatus: 403,
		Cloudflare: true,
		Retryable:  true,
	}

	result := executionResultError(auth, diagnosticExecutionTestError{diagnostic: source})
	if result == nil || result.Diagnostic == nil {
		t.Fatalf("result = %#v", result)
	}
	if result.Code != "" {
		t.Fatalf("diagnostic changed business error code to %q", result.Code)
	}
	if result.HTTPStatus != 403 || !result.Retryable {
		t.Fatalf("result classification = %#v", result)
	}
	if result.Diagnostic.AuthIndex != auth.EnsureIndex() || result.Diagnostic.Persona != "chrome_146" ||
		result.Diagnostic.CatalogVersion != "v2" || result.Diagnostic.CatalogID == "" ||
		result.Diagnostic.TransportPersonaID == "" || result.Diagnostic.BrowserEnvironmentID == "" ||
		result.Diagnostic.TLSProfile != "chrome_146" || result.Diagnostic.UAMajor != "146" ||
		(result.Diagnostic.Platform != "MacIntel" && result.Diagnostic.Platform != "Win32") {
		t.Fatalf("diagnostic was not enriched: %#v", result.Diagnostic)
	}

	result.Diagnostic.Code = "changed"
	if source.Code != "cloudflare_challenge" {
		t.Fatal("diagnostic provider value was aliased")
	}
	cloned := cloneError(result)
	cloned.Diagnostic.Stage = "changed"
	if result.Diagnostic.Stage != "file_sign" {
		t.Fatal("cloned diagnostic was aliased")
	}
}

func TestExecutionResultErrorHandlesWrappedDiagnostic(t *testing.T) {
	err := errors.Join(errors.New("outer"), diagnosticExecutionTestError{diagnostic: &ErrorDiagnostic{Stage: "models"}})
	result := executionResultError(&Auth{ID: "wrapped", Provider: "chatgpt-web"}, err)
	if result.Diagnostic == nil || result.Diagnostic.Stage != "models" {
		t.Fatalf("wrapped diagnostic missing: %#v", result)
	}
}

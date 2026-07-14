package auth

import (
	"errors"
	"testing"
)

func TestGeminiAuthenticatorCompatibilityShell(t *testing.T) {
	authenticator := NewGeminiAuthenticator()
	if authenticator.Provider() != "gemini" {
		t.Fatalf("Provider() = %q, want gemini", authenticator.Provider())
	}
	if auth, errLogin := authenticator.Login(t.Context(), nil, nil); auth != nil || !errors.Is(errLogin, ErrGeminiCLINotSupported) {
		t.Fatalf("Login() = (%#v, %v), want retired provider error", auth, errLogin)
	}
}

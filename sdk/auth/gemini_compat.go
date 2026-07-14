package auth

import (
	"context"
	"errors"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

// ErrGeminiCLINotSupported reports use of the retired Gemini CLI login flow.
var ErrGeminiCLINotSupported = errors.New("cliproxy auth: Gemini CLI authentication is no longer supported")

// GeminiAuthenticator is retained for v6 source compatibility and cannot authenticate.
type GeminiAuthenticator struct{}

// NewGeminiAuthenticator returns a compatibility authenticator for the retired provider.
func NewGeminiAuthenticator() *GeminiAuthenticator {
	return &GeminiAuthenticator{}
}

// Provider returns the legacy provider identifier.
func (a *GeminiAuthenticator) Provider() string {
	return "gemini"
}

// RefreshLead returns no refresh schedule for the retired provider.
func (a *GeminiAuthenticator) RefreshLead() *time.Duration {
	return nil
}

// Login always reports that Gemini CLI authentication has been removed.
func (a *GeminiAuthenticator) Login(context.Context, *config.Config, *LoginOptions) (*coreauth.Auth, error) {
	return nil, ErrGeminiCLINotSupported
}

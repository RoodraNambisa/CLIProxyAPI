package executor

import (
	"context"
	"net/url"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func codexFingerprintJA3Enabled(cfg *config.Config) bool {
	return cfg != nil && cfg.CodexFingerprint.JA3
}

func codexFingerprintForceHTTP1Enabled(cfg *config.Config) bool {
	return cfg != nil && cfg.CodexFingerprint.ForceHTTP1
}

func codexFingerprintImagesForceHTTP1Enabled(cfg *config.Config) bool {
	return cfg != nil && cfg.CodexFingerprint.ImagesForceHTTP1
}

func codexFingerprintShouldForceHTTP1(cfg *config.Config, imageRequest bool) bool {
	if codexFingerprintJA3Enabled(cfg) {
		return false
	}
	if codexFingerprintForceHTTP1Enabled(cfg) {
		return true
	}
	return imageRequest && codexFingerprintImagesForceHTTP1Enabled(cfg)
}

func contextWithCodexFingerprintPersona(ctx context.Context, _ *config.Config, _ *cliproxyauth.Auth) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func parsedURLOrNil(raw string) *url.URL {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil
	}
	return parsed
}

package executor

import (
	"context"
	"crypto/sha256"
	"net/url"
	"strings"

	"github.com/google/uuid"
	codexauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

type codexConvergedFingerprint struct {
	mode           codexauth.FingerprintMode
	installationID string
	sessionID      string
	threadID       string
	turnID         string
	windowID       string
}

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

func resolveCodexConvergedFingerprint(auth *cliproxyauth.Auth, prepared codexPreparedSessionIdentity, clientSessionID string) codexConvergedFingerprint {
	if auth == nil || codexAuthUsesAPIKey(auth) {
		return codexConvergedFingerprint{mode: codexauth.FingerprintModeOff}
	}
	mode := codexauth.EffectiveFingerprintMode(auth.Metadata)
	if mode == codexauth.FingerprintModeOff {
		return codexConvergedFingerprint{mode: mode}
	}

	seed := strings.TrimSpace(codexauth.EffectiveRequestAccountID(auth.Metadata))
	if seed == "" {
		seed = strings.TrimSpace(auth.ID)
	}
	if seed == "" {
		return codexConvergedFingerprint{mode: codexauth.FingerprintModeOff}
	}
	installationID := codexMetadataString(auth.Metadata, "openai_device_id")
	if parsedInstallationID, err := uuid.Parse(installationID); err == nil {
		installationID = parsedInstallationID.String()
	} else {
		installationID = deriveStableCodexFingerprintUUID("installation", seed)
	}
	resolved := codexConvergedFingerprint{mode: mode, installationID: installationID}
	if mode == codexauth.FingerprintModeDevice {
		return resolved
	}

	resolved.sessionID = deriveStableCodexFingerprintUUID("session", seed)
	if mode == codexauth.FingerprintModeFull {
		resolved.threadID = resolved.sessionID
	} else if clientSessionID != "" {
		resolved.threadID = deriveStableCodexFingerprintUUID("thread", seed+"\x00"+clientSessionID)
	} else {
		resolved.threadID = resolved.sessionID
	}
	resolved.turnID = strings.TrimSpace(prepared.TurnID)
	if resolved.turnID == "" {
		resolved.turnID = uuid.NewString()
	}
	resolved.windowID = resolved.threadID + ":0"
	return resolved
}

func deriveStableCodexFingerprintUUID(kind, seed string) string {
	sum := sha256.Sum256([]byte("cli-proxy-api:codex-fingerprint:v1:" + kind + ":" + seed))
	var id uuid.UUID
	copy(id[:], sum[:16])
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	return id.String()
}

func codexMetadataString(metadata map[string]any, key string) string {
	if len(metadata) == 0 {
		return ""
	}
	value, _ := metadata[key].(string)
	return strings.TrimSpace(value)
}

func originalCodexClientSessionID(sessionID string, rawJSON []byte) string {
	if sessionID = strings.TrimSpace(sessionID); sessionID != "" {
		return sessionID
	}
	if sessionID := strings.TrimSpace(gjson.GetBytes(rawJSON, "client_metadata.session_id").String()); sessionID != "" {
		return sessionID
	}
	rawTurnMetadata := strings.TrimSpace(gjson.GetBytes(rawJSON, "client_metadata.x-codex-turn-metadata").String())
	if rawTurnMetadata == "" {
		return ""
	}
	return strings.TrimSpace(gjson.Get(rawTurnMetadata, "session_id").String())
}

func parsedURLOrNil(raw string) *url.URL {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil
	}
	return parsed
}

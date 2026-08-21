package executor

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/google/uuid"
	codexauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

const codexSessionIdentityPoolMetadataKey = "codex_session_identity_pool"

var errCodexSessionIdentityPoolNotPrepared = errors.New("codex session identity pool is not prepared")

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

func resolveCodexConvergedFingerprint(auth *cliproxyauth.Auth, prepared codexPreparedSessionIdentity, clientThreadID string) (codexConvergedFingerprint, error) {
	if auth == nil || codexAuthUsesAPIKey(auth) {
		return codexConvergedFingerprint{mode: codexauth.FingerprintModeOff}, nil
	}
	mode := codexauth.EffectiveFingerprintMode(auth.Metadata)
	if mode == codexauth.FingerprintModeOff {
		return codexConvergedFingerprint{mode: mode}, nil
	}

	installationID := expectedCodexInstallationID(auth)
	if installationID == "" {
		return codexConvergedFingerprint{mode: codexauth.FingerprintModeOff}, nil
	}
	resolved := codexConvergedFingerprint{mode: mode, installationID: installationID}
	if mode == codexauth.FingerprintModeDevice {
		return resolved, nil
	}

	turnID := strings.TrimSpace(prepared.TurnID)
	if turnID == "" {
		var err error
		turnID, err = helps.NewCodexUUIDv7()
		if err != nil {
			return codexConvergedFingerprint{}, fmt.Errorf("generate Codex turn ID: %w", err)
		}
	}
	pool, ok := codexSessionIdentityPool(auth.Metadata)
	if !ok || len(pool) == 0 {
		return codexConvergedFingerprint{}, errCodexSessionIdentityPoolNotPrepared
	}
	for _, identity := range pool {
		if !isCodexUUIDVersion(identity, uuid.Version(7)) {
			return codexConvergedFingerprint{}, errCodexSessionIdentityPoolNotPrepared
		}
	}

	resolved.sessionID = pool[codexSessionIdentityPoolIndex(auth, prepared, turnID, len(pool))]
	if mode == codexauth.FingerprintModeFull {
		resolved.threadID = resolved.sessionID
	} else {
		resolved.threadID = codexSessionThreadID(auth, clientThreadID, resolved.sessionID)
	}
	resolved.turnID = turnID
	resolved.windowID = resolved.threadID + ":0"
	return resolved, nil
}

func codexSessionIdentityPoolIndex(auth *cliproxyauth.Auth, prepared codexPreparedSessionIdentity, turnID string, poolSize int) int {
	if poolSize <= 1 {
		return 0
	}
	affinityKind := strings.TrimSpace(prepared.AffinityKind)
	affinityDigest := strings.TrimSpace(prepared.AffinityDigest)
	if affinityDigest == "" {
		affinityKind = "turn"
		affinityDigest = codexFingerprintDigest(turnID)
	}
	tenantDigest := strings.TrimSpace(prepared.TenantDigest)
	if tenantDigest == "" {
		tenantDigest = "anonymous"
	}
	scope := codexFingerprintAccountScope(auth)
	sum := sha256.Sum256([]byte("cli-proxy-api:codex-session-pool:v1:" + tenantDigest + "\x00" + scope + "\x00" + affinityKind + "\x00" + affinityDigest))
	return int(binary.BigEndian.Uint64(sum[:8]) % uint64(poolSize))
}

func codexSessionThreadID(auth *cliproxyauth.Auth, clientThreadID, fallback string) string {
	parsed, err := uuid.Parse(strings.TrimSpace(clientThreadID))
	if err != nil || parsed == uuid.Nil {
		return fallback
	}
	if parsed.Version() != uuid.Version(7) {
		return parsed.String()
	}
	sum := sha256.Sum256([]byte("cli-proxy-api:codex-thread:v1:" + codexFingerprintAccountScope(auth) + "\x00" + parsed.String()))
	var mapped uuid.UUID
	copy(mapped[:6], parsed[:6])
	copy(mapped[6:], sum[:10])
	mapped[6] = (mapped[6] & 0x0f) | 0x70
	mapped[8] = (mapped[8] & 0x3f) | 0x80
	return mapped.String()
}

func codexFingerprintAccountScope(auth *cliproxyauth.Auth) string {
	if auth == nil {
		return "unknown"
	}
	if accountID := strings.TrimSpace(codexauth.EffectiveRequestAccountID(auth.Metadata)); accountID != "" {
		return accountID
	}
	if installationID := expectedCodexInstallationID(auth); installationID != "" {
		return installationID
	}
	if authID := strings.TrimSpace(auth.ID); authID != "" {
		return authID
	}
	return "unknown"
}

func codexFingerprintDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", sum[:])
}

func codexSessionIdentityPoolNeedsPreparation(auth *cliproxyauth.Auth, cfg *config.Config) bool {
	if auth == nil || codexAuthUsesAPIKey(auth) || !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
		return false
	}
	mode := codexauth.EffectiveFingerprintMode(auth.Metadata)
	if mode != codexauth.FingerprintModeSession && mode != codexauth.FingerprintModeFull {
		return false
	}
	desired := config.DefaultCodexSessionIdentityPoolSize
	if cfg != nil {
		desired = cfg.CodexFingerprint.ResolvedSessionIdentityPoolSize()
	}
	pool, ok := codexSessionIdentityPool(auth.Metadata)
	if !ok || len(pool) != desired {
		return true
	}
	for _, identity := range pool {
		if !isCodexUUIDVersion(identity, uuid.Version(7)) {
			return true
		}
	}
	return false
}

func prepareCodexSessionIdentityPool(auth *cliproxyauth.Auth, cfg *config.Config) (*cliproxyauth.Auth, bool, error) {
	if !codexSessionIdentityPoolNeedsPreparation(auth, cfg) {
		return auth, false, nil
	}
	desired := config.DefaultCodexSessionIdentityPoolSize
	if cfg != nil {
		desired = cfg.CodexFingerprint.ResolvedSessionIdentityPoolSize()
	}
	existing, _ := codexSessionIdentityPool(auth.Metadata)
	pool := make([]string, desired)
	for index := range pool {
		if index < len(existing) && isCodexUUIDVersion(existing[index], uuid.Version(7)) {
			parsed, _ := uuid.Parse(existing[index])
			pool[index] = parsed.String()
			continue
		}
		identity, err := helps.NewCodexUUIDv7()
		if err != nil {
			return nil, false, fmt.Errorf("generate Codex session identity pool member: %w", err)
		}
		pool[index] = identity
	}
	updated := auth.Clone()
	if updated.Metadata == nil {
		updated.Metadata = make(map[string]any)
	}
	updated.Metadata[codexSessionIdentityPoolMetadataKey] = pool
	return updated, true, nil
}

func codexSessionIdentityPool(metadata map[string]any) ([]string, bool) {
	if len(metadata) == 0 {
		return nil, false
	}
	raw, exists := metadata[codexSessionIdentityPoolMetadataKey]
	if !exists || raw == nil {
		return nil, false
	}
	switch values := raw.(type) {
	case []string:
		return append([]string(nil), values...), true
	case []any:
		pool := make([]string, len(values))
		for index, value := range values {
			identity, ok := value.(string)
			if !ok {
				return nil, false
			}
			pool[index] = strings.TrimSpace(identity)
		}
		return pool, true
	default:
		return nil, false
	}
}

func codexInstallationIDNeedsPreparation(auth *cliproxyauth.Auth) bool {
	if auth == nil || codexAuthUsesAPIKey(auth) ||
		!strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") ||
		codexauth.EffectiveFingerprintMode(auth.Metadata) == codexauth.FingerprintModeOff {
		return false
	}
	installationID := canonicalCodexInstallationID(codexMetadataString(auth.Metadata, "openai_device_id"))
	expected := derivedCodexAccountInstallationID(auth)
	if expected != "" {
		return installationID != expected
	}
	return installationID == ""
}

func prepareCodexInstallationID(auth *cliproxyauth.Auth) (*cliproxyauth.Auth, bool) {
	if !codexInstallationIDNeedsPreparation(auth) {
		return auth, false
	}
	updated := auth.Clone()
	if updated.Metadata == nil {
		updated.Metadata = make(map[string]any)
	}
	installationID := derivedCodexAccountInstallationID(updated)
	if installationID == "" {
		installationID = uuid.NewString()
	}
	updated.Metadata["openai_device_id"] = installationID
	return updated, true
}

func expectedCodexInstallationID(auth *cliproxyauth.Auth) string {
	if installationID := derivedCodexAccountInstallationID(auth); installationID != "" {
		return installationID
	}
	if auth == nil {
		return ""
	}
	return canonicalCodexInstallationID(codexMetadataString(auth.Metadata, "openai_device_id"))
}

func derivedCodexAccountInstallationID(auth *cliproxyauth.Auth) string {
	if auth == nil {
		return ""
	}
	accountID := strings.TrimSpace(codexauth.EffectiveRequestAccountID(auth.Metadata))
	if accountID == "" {
		return ""
	}
	return deriveStableCodexFingerprintUUID("installation", accountID)
}

func canonicalCodexInstallationID(value string) string {
	parsed, err := uuid.Parse(strings.TrimSpace(value))
	if err != nil || parsed == uuid.Nil {
		return ""
	}
	return parsed.String()
}

func isCodexUUIDVersion(value string, version uuid.Version) bool {
	parsed, err := uuid.Parse(strings.TrimSpace(value))
	return err == nil && parsed != uuid.Nil && parsed.Version() == version
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

func originalCodexClientThreadID(threadID string, rawJSON []byte) string {
	rawTurnMetadata := strings.TrimSpace(gjson.GetBytes(rawJSON, "client_metadata.x-codex-turn-metadata").String())
	if rawTurnMetadata != "" {
		if turnThreadID := strings.TrimSpace(gjson.Get(rawTurnMetadata, "thread_id").String()); turnThreadID != "" {
			return turnThreadID
		}
	}
	if bodyThreadID := strings.TrimSpace(gjson.GetBytes(rawJSON, "client_metadata.thread_id").String()); bodyThreadID != "" {
		return bodyThreadID
	}
	return strings.TrimSpace(threadID)
}

func parsedURLOrNil(raw string) *url.URL {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil
	}
	return parsed
}

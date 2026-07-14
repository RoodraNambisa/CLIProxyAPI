package auth

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	log "github.com/sirupsen/logrus"
)

var retiredGeminiCLIWarning sync.Once

// ErrRetiredGeminiCLIAuthReadOnly marks an attempted mutation of a historical Gemini CLI credential.
var ErrRetiredGeminiCLIAuthReadOnly = errors.New("Gemini CLI credentials are read-only and may only be viewed, downloaded, or deleted")

func retiredGeminiCLIAuthError() *Error {
	return &Error{
		Code:       "provider_not_supported",
		Message:    "Gemini CLI credentials are no longer supported",
		HTTPStatus: http.StatusGone,
	}
}

func usesRetiredGeminiCLIExecutionFormat(req cliproxyexecutor.Request, opts cliproxyexecutor.Options) bool {
	for _, format := range []string{req.Format.String(), opts.SourceFormat.String(), opts.ResponseFormat.String()} {
		if strings.EqualFold(strings.TrimSpace(format), "gemini-cli") {
			return true
		}
	}
	return false
}

func usesRetiredGeminiCLIProvider(providers []string) bool {
	for _, provider := range providers {
		if strings.EqualFold(strings.TrimSpace(provider), "gemini-cli") {
			return true
		}
	}
	return false
}

// IsRetiredGeminiCLIAuth reports whether an auth represents a retired file-backed Gemini CLI credential.
// Gemini API key credentials remain eligible because they carry the api_key attribute.
func IsRetiredGeminiCLIAuth(auth *Auth) bool {
	if auth == nil {
		return false
	}
	provider := strings.ToLower(strings.TrimSpace(auth.Provider))
	metadataType := ""
	if auth.Metadata != nil {
		metadataType, _ = auth.Metadata["type"].(string)
		metadataType = strings.ToLower(strings.TrimSpace(metadataType))
	}
	if provider == "gemini-cli" || metadataType == "gemini-cli" {
		return true
	}
	apiKey := ""
	authKind := ""
	if auth.Attributes != nil {
		apiKey = strings.TrimSpace(auth.Attributes["api_key"])
		authKind = strings.ToLower(strings.TrimSpace(auth.Attributes["auth_kind"]))
	}
	if auth.Metadata != nil && authKind == "" {
		metadataAuthKind, _ := auth.Metadata["auth_kind"].(string)
		authKind = strings.ToLower(strings.TrimSpace(metadataAuthKind))
	}
	if (provider == "gemini" || metadataType == "gemini") && (authKind == "oauth" || hasGeminiCLITokenMetadata(auth.Metadata)) {
		return true
	}
	if apiKey == "" && auth.Metadata != nil && provider != "gemini-cli" && metadataType != "gemini-cli" {
		apiKey, _ = auth.Metadata["api_key"].(string)
		apiKey = strings.TrimSpace(apiKey)
	}
	if apiKey != "" {
		return false
	}
	if metadataType == "gemini" {
		return true
	}
	if provider != "gemini" || auth.Metadata == nil {
		return false
	}
	for _, key := range []string{"email", "project_id", "token", "access_token", "refresh_token"} {
		switch value := auth.Metadata[key].(type) {
		case string:
			if strings.TrimSpace(value) != "" {
				return true
			}
		case map[string]any:
			if len(value) > 0 {
				return true
			}
		}
	}
	return false
}

// ApplyFileBackedGeminiAPIKey maps an explicit Gemini API key file to the
// attributes consumed by the Gemini executor. Explicit gemini-cli files remain retired.
func ApplyFileBackedGeminiAPIKey(auth *Auth) {
	if auth == nil || auth.Metadata == nil {
		return
	}
	provider := strings.ToLower(strings.TrimSpace(auth.Provider))
	metadataType, _ := auth.Metadata["type"].(string)
	metadataType = strings.ToLower(strings.TrimSpace(metadataType))
	if provider == "gemini-cli" || metadataType == "gemini-cli" || (provider != "gemini" && metadataType != "gemini") {
		return
	}
	metadataAuthKind, _ := auth.Metadata["auth_kind"].(string)
	if strings.EqualFold(strings.TrimSpace(metadataAuthKind), "oauth") || hasGeminiCLITokenMetadata(auth.Metadata) {
		return
	}
	apiKey, _ := auth.Metadata["api_key"].(string)
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return
	}
	if auth.Attributes == nil {
		auth.Attributes = make(map[string]string)
	}
	auth.Attributes["api_key"] = apiKey
	auth.Attributes["auth_kind"] = "apikey"
}

// IsRetiredGeminiCLIAuthFileData reports whether JSON data belongs to a historical Gemini CLI credential.
func IsRetiredGeminiCLIAuthFileData(data []byte) bool {
	var metadata map[string]any
	if len(data) == 0 || json.Unmarshal(data, &metadata) != nil {
		return false
	}
	provider, _ := metadata["type"].(string)
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider == "gemini-cli" {
		return true
	}
	if provider != "gemini" {
		return false
	}
	metadataAuthKind, _ := metadata["auth_kind"].(string)
	if strings.EqualFold(strings.TrimSpace(metadataAuthKind), "oauth") || hasGeminiCLITokenMetadata(metadata) {
		return true
	}
	apiKey, _ := metadata["api_key"].(string)
	return strings.TrimSpace(apiKey) == ""
}

func hasGeminiCLITokenMetadata(metadata map[string]any) bool {
	for _, key := range []string{"token", "access_token", "refresh_token"} {
		switch value := metadata[key].(type) {
		case string:
			if strings.TrimSpace(value) != "" {
				return true
			}
		case map[string]any:
			if len(value) > 0 {
				return true
			}
		}
	}
	return false
}

// RejectRetiredGeminiCLIAuthFileMutation rejects writes to historical Gemini CLI credential data.
func RejectRetiredGeminiCLIAuthFileMutation(data []byte) error {
	if IsRetiredGeminiCLIAuthFileData(data) {
		return ErrRetiredGeminiCLIAuthReadOnly
	}
	return nil
}

// WarnRetiredGeminiCLIAuthIgnored emits the process-wide retirement warning once.
func WarnRetiredGeminiCLIAuthIgnored() {
	retiredGeminiCLIWarning.Do(func() {
		log.Warn("Gemini CLI credentials are no longer supported; legacy files remain available for management only")
	})
}

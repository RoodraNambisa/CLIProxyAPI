package cliproxy

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/misc"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
	log "github.com/sirupsen/logrus"
)

const (
	antigravityModelBaseURLProd         = "https://cloudcode-pa.googleapis.com"
	antigravityModelBaseURLDaily        = "https://daily-cloudcode-pa.googleapis.com"
	antigravityModelBaseURLDailySandbox = "https://daily-cloudcode-pa.sandbox.googleapis.com"
	antigravityModelsPath               = "/v1internal:fetchAvailableModels"
)

type antigravityFetchAvailableModelsResponse struct {
	WebSearchModelIDs []string `json:"webSearchModelIds"`
}

type antigravityModelCapabilityHints struct {
	WebSearchModelIDs map[string]struct{}
}

type antigravityModelCapabilityCacheEntry struct {
	RuntimeInstanceID string
	Hints             antigravityModelCapabilityHints
}

type modelSyncHookSuppressedContextKey struct{}

func (s *Service) fetchAntigravityModelCapabilityHintsForAuth(ctx context.Context, auth *coreauth.Auth) (antigravityModelCapabilityHints, bool) {
	hints, _, ok := s.fetchAntigravityModelCapabilityHintsWithSource(ctx, auth)
	return hints, ok
}

func (s *Service) fetchAntigravityModelCapabilityHintsWithSource(ctx context.Context, auth *coreauth.Auth) (antigravityModelCapabilityHints, *coreauth.Auth, bool) {
	if auth == nil || auth.Metadata == nil {
		return antigravityModelCapabilityHints{}, auth, false
	}

	current := auth
	for refreshAttempted := false; ; {
		accessToken, _ := current.Metadata["access_token"].(string)
		accessToken = strings.TrimSpace(accessToken)
		if accessToken == "" {
			return antigravityModelCapabilityHints{}, current, false
		}

		hints, ok, unauthorized := s.fetchAntigravityModelCapabilityHints(ctx, current, accessToken)
		if ok {
			return hints, current, true
		}
		if !unauthorized || refreshAttempted || s == nil || s.coreManager == nil {
			return antigravityModelCapabilityHints{}, current, false
		}

		refreshCtx := context.WithValue(ctx, modelSyncHookSuppressedContextKey{}, true)
		refreshed, errRefresh := s.coreManager.RefreshAntigravityAfterUnauthorized(refreshCtx, current.ID, accessToken)
		if errRefresh != nil || refreshed == nil {
			if errRefresh != nil {
				log.Debugf("antigravity model fetch: refresh rejected access token: %v", errRefresh)
			}
			return antigravityModelCapabilityHints{}, current, false
		}
		current = refreshed
		refreshAttempted = true
	}
}

func (s *Service) fetchAntigravityModelCapabilityHints(ctx context.Context, auth *coreauth.Auth, accessToken string) (antigravityModelCapabilityHints, bool, bool) {
	client := &http.Client{}
	transport, _, errProxy := proxyutil.BuildHTTPTransport(s.antigravityModelFetchProxyURL(auth))
	if errProxy != nil {
		log.Debug("antigravity model fetch: invalid proxy configuration")
		return antigravityModelCapabilityHints{}, false, false
	}
	if transport != nil {
		client.Transport = transport
	}

	for _, baseURL := range antigravityModelBaseURLs(auth) {
		req, errReq := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+antigravityModelsPath, strings.NewReader(`{}`))
		if errReq != nil {
			continue
		}
		req.Close = true
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+accessToken)
		req.Header.Set("User-Agent", misc.AntigravityUserAgent())

		resp, errDo := client.Do(req)
		if errDo != nil {
			continue
		}
		body, errRead := io.ReadAll(resp.Body)
		if errClose := resp.Body.Close(); errClose != nil {
			log.Debugf("antigravity model fetch: close response body: %v", errClose)
		}
		if errRead != nil {
			continue
		}
		if resp.StatusCode == http.StatusUnauthorized {
			return antigravityModelCapabilityHints{}, false, true
		}
		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			continue
		}
		hints, okParse := parseAntigravityModelCapabilityHints(body)
		if okParse {
			return hints, true, false
		}
	}
	return antigravityModelCapabilityHints{}, false, false
}

func (s *Service) antigravityModelFetchProxyURL(auth *coreauth.Auth) string {
	if auth != nil {
		if proxyURL := strings.TrimSpace(auth.ProxyURL); proxyURL != "" {
			return proxyURL
		}
	}
	if cfg := s.currentConfig(); cfg != nil {
		return strings.TrimSpace(cfg.ProxyURL)
	}
	return ""
}

func antigravityModelBaseURLs(auth *coreauth.Auth) []string {
	if baseURL := resolveAntigravityModelBaseURL(auth); baseURL != "" {
		return []string{baseURL}
	}
	return []string{antigravityModelBaseURLProd, antigravityModelBaseURLDaily, antigravityModelBaseURLDailySandbox}
}

func resolveAntigravityModelBaseURL(auth *coreauth.Auth) string {
	if auth == nil {
		return ""
	}
	if auth.Attributes != nil {
		if value := strings.TrimSpace(auth.Attributes["base_url"]); value != "" {
			return strings.TrimRight(value, "/")
		}
	}
	if auth.Metadata != nil {
		if value, ok := auth.Metadata["base_url"].(string); ok {
			value = strings.TrimSpace(value)
			if value != "" {
				return strings.TrimRight(value, "/")
			}
		}
	}
	return ""
}

func parseAntigravityModelCapabilityHints(body []byte) (antigravityModelCapabilityHints, bool) {
	var parsed antigravityFetchAvailableModelsResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return antigravityModelCapabilityHints{}, false
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return antigravityModelCapabilityHints{}, false
	}
	if _, exists := fields["webSearchModelIds"]; !exists || parsed.WebSearchModelIDs == nil {
		return antigravityModelCapabilityHints{}, false
	}
	webSearchModels := make(map[string]struct{}, len(parsed.WebSearchModelIDs))
	for _, modelID := range parsed.WebSearchModelIDs {
		modelID = normalizeAntigravityFetchedModelID(modelID)
		if modelID != "" {
			webSearchModels[modelID] = struct{}{}
		}
	}
	return antigravityModelCapabilityHints{WebSearchModelIDs: webSearchModels}, true
}

func applyAntigravityFetchedModelCapabilities(models []*ModelInfo, hints antigravityModelCapabilityHints) []*ModelInfo {
	if len(models) == 0 {
		return models
	}

	result := make([]*ModelInfo, len(models))
	for i, model := range models {
		if model == nil {
			continue
		}
		modelCopy := *model
		modelID := normalizeAntigravityFetchedModelID(modelCopy.ID)
		modelCopy.UpstreamID = modelID
		modelCopy.SupportsWebSearch = false
		if _, ok := hints.WebSearchModelIDs[modelID]; ok {
			modelCopy.SupportsWebSearch = true
		}
		result[i] = &modelCopy
	}
	return result
}

func normalizeAntigravityFetchedModelID(modelID string) string {
	return strings.ToLower(strings.TrimSpace(modelID))
}

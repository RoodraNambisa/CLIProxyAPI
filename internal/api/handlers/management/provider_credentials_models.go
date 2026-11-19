package management

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/constant"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxypool"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/watcher/synthesizer"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
)

// DiscoverProviderCredentialModels delegates transport and cancellation to APICall.
func (h *Handler) DiscoverProviderCredentialModels(c *gin.Context) {
	var input struct {
		Kind      string                     `json:"kind"`
		Values    map[string]json.RawMessage `json:"values"`
		PageToken string                     `json:"page_token"`
		AfterID   string                     `json:"after_id"`
	}
	if err := c.ShouldBindJSON(&input); err != nil && !errors.Is(err, io.EOF) {
		c.JSON(400, gin.H{"error": "invalid discovery request"})
		return
	}
	ref := config.APICredentialRef{Kind: input.Kind, Management: &config.APIManagement{}}
	ok := input.Values != nil
	if id := c.Param("id"); id != "" {
		ref, ok = findProviderCredential(h.currentConfig(), id)
	}
	if !ok {
		c.JSON(404, gin.H{"error": "API credential not found"})
		return
	}
	fields, auth, err := providerCatalogDraft(ref, input.Values)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	endpoint := fields.ModelsEndpoint
	base := strings.TrimRight(strings.TrimSpace(fields.BaseURL), "/")
	if endpoint == "" {
		switch ref.Kind {
		case "gemini", "interactions":
			if base == "" {
				base = "https://generativelanguage.googleapis.com"
			}
			if !strings.HasSuffix(base, "/v1beta") && !strings.HasSuffix(base, "/v1") {
				base += "/v1beta"
			}
			endpoint = base + "/models"
		case "claude":
			if base == "" {
				base = "https://api.anthropic.com"
			}
			if !strings.HasSuffix(base, "/v1") {
				base += "/v1"
			}
			endpoint = base + "/models"
		case "xai":
			if base == "" {
				base = "https://api.x.ai/v1"
			}
			if !strings.HasSuffix(base, "/v1") {
				base += "/v1"
			}
			endpoint = base + "/models"
		case "vertex":
			c.JSON(400, gin.H{"error": "configure models-endpoint for this Vertex-compatible upstream"})
			return
		default:
			if base == "" {
				c.JSON(400, gin.H{"error": "base-url is required"})
				return
			}
			if !strings.HasSuffix(base, "/v1") {
				base += "/v1"
			}
			endpoint = base + "/models"
		}
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		c.JSON(400, gin.H{"error": "invalid models endpoint"})
		return
	}
	query := u.Query()
	if input.PageToken != "" {
		query.Set("pageToken", input.PageToken)
	}
	if input.AfterID != "" {
		query.Set("after_id", input.AfterID)
	}
	u.RawQuery = query.Encode()
	headers := fields.Headers
	if headers == nil {
		headers = map[string]string{}
	}
	has := func(name string) bool {
		for k := range headers {
			if strings.EqualFold(k, name) {
				return true
			}
		}
		return false
	}
	switch ref.Kind {
	case "gemini", "interactions":
		if !has("x-goog-api-key") {
			headers["x-goog-api-key"] = "$TOKEN$"
		}
	case "claude":
		if !has("x-api-key") {
			headers["x-api-key"] = "$TOKEN$"
		}
		if !has("anthropic-version") {
			headers["anthropic-version"] = "2023-06-01"
		}
	default:
		if !has("authorization") && fields.APIKey != "" {
			headers["Authorization"] = "Bearer $TOKEN$"
		}
	}
	h.executeAPICall(c, apiCallRequest{Method: http.MethodGet, URL: u.String(), Header: headers}, auth)
}

type catalogFields struct {
	BaseURL        string            `json:"base-url"`
	Headers        map[string]string `json:"headers"`
	ModelsEndpoint string            `json:"models-endpoint"`
	APIKey         string            `json:"api-key"`
	ProxyURL       string            `json:"proxy-url"`
	Priority       int               `json:"priority"`
}

func providerCatalogDraft(ref config.APICredentialRef, values map[string]json.RawMessage) (catalogFields, *coreauth.Auth, error) {
	var fields catalogFields
	value := ref.Value
	owner := ref.Management
	provider := ref.Kind
	if ref.Parent != nil {
		value = ref.Parent
	}
	if raw, err := json.Marshal(value); err != nil || json.Unmarshal(raw, &fields) != nil {
		return fields, nil, errors.New("invalid catalog configuration")
	}
	switch ref.Kind {
	case "openai":
		name := ""
		if ref.Parent != nil {
			name = ref.Parent.Name
		}
		provider = synthesizer.OpenAICompatRuntimeProviderName(name)
	case "interactions":
		provider = constant.GeminiInteractions
	case "codex", "claude", "gemini", "xai", "vertex":
	default:
		return fields, nil, errors.New("unsupported API credential type")
	}

	applyChild := func(key *config.OpenAICompatibilityAPIKey) {
		owner = &key.APIManagement
		fields.APIKey, fields.ProxyURL = key.APIKey, key.ProxyURL
		if key.Priority != nil {
			fields.Priority = *key.Priority
		}
		if key.ModelsEndpoint != "" {
			fields.ModelsEndpoint = key.ModelsEndpoint
		}
	}
	isChild := ref.Parent != nil && ref.KeyPosition >= 0
	if isChild {
		applyChild(&ref.Parent.APIKeyEntries[ref.KeyPosition])
	}
	// Omitted fields retain their saved values. An empty secret retains the
	// selected key; an empty proxy explicitly restores routing inheritance.
	currentKey, currentProxy := fields.APIKey, fields.ProxyURL
	if _, changed := values["headers"]; changed {
		fields.Headers = nil
	}
	patch, err := json.Marshal(values)
	if err != nil || json.Unmarshal(patch, &fields) != nil {
		return fields, nil, errors.New("invalid catalog connection settings")
	}
	if strings.TrimSpace(fields.APIKey) == "" {
		fields.APIKey = currentKey
	}
	fields.ProxyURL, err = applyProxyURLPatch(currentProxy, &fields.ProxyURL, false)
	if err != nil {
		return fields, nil, err
	}
	if isChild {
		if strings.TrimSpace(string(values["priority"])) == "null" {
			fields.Priority = ref.Parent.Priority
		}
		if fields.ModelsEndpoint == "" {
			fields.ModelsEndpoint = ref.Parent.ModelsEndpoint
		}
	} else if ref.IsParent() {
		for i := range ref.Parent.APIKeyEntries {
			key := &ref.Parent.APIKeyEntries[i]
			if !key.Disabled && strings.TrimSpace(key.APIKey) != "" {
				applyChild(key)
				break
			}
		}
	}
	if _, err := proxyutil.Parse(fields.ProxyURL); err != nil {
		return fields, nil, errors.New("invalid_proxy_url")
	}
	id := owner.RuntimeID
	if id == "" {
		id = owner.CredentialID
	}
	auth := &coreauth.Auth{ID: id, Provider: provider, ProxyURL: fields.ProxyURL, Attributes: map[string]string{
		"api_key": fields.APIKey, "base_url": fields.BaseURL, "priority": strconv.Itoa(fields.Priority), "runtime_only": "true",
	}}
	return fields, auth, nil
}

func (h *Handler) providerCatalogTransport(ctx context.Context, auth *coreauth.Auth) (*http.Transport, error) {
	var route coreauth.ResolvedProxy
	var err error
	if pool := h.proxyPoolRuntimeManager(); pool != nil {
		route, err = pool.ResolveForProbe(ctx, auth)
	} else {
		route, err = h.coreAuthRuntimeManager().ResolveProxyForProbe(ctx, auth)
	}
	if err != nil {
		return nil, err
	}
	if route.URL == "" && route.Source == "" {
		if cfg := h.currentConfig(); cfg != nil {
			priority, _ := strconv.Atoi(auth.Attributes["priority"])
			if _, matched := config.MatchProxyRuleTargets(cfg.ProxyRules, auth.Provider, priority); matched {
				return nil, &proxypool.UnavailableError{}
			}
			route.URL = cfg.ProxyURL
		}
	}
	transport, _, err := proxyutil.BuildHTTPTransport(route.URL)
	if err != nil {
		return nil, &proxypool.UnavailableError{Cause: err}
	}
	if transport == nil {
		transport = proxyutil.NewDirectTransport()
	}
	return transport, nil
}

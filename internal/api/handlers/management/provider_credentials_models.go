package management

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
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
	ref, ok := findProviderCredential(h.currentConfig(), c.Param("id"))
	if c.Param("id") == "" {
		ref = config.APICredentialRef{Kind: input.Kind, Value: input.Values, Management: &config.APIManagement{}}
		ok = input.Values != nil
	}
	if !ok {
		c.JSON(404, gin.H{"error": "API credential not found"})
		return
	}
	value := ref.Value
	if ref.Parent != nil {
		value = ref.Parent
	}
	raw, _ := json.Marshal(value)
	combined := map[string]json.RawMessage{}
	_ = json.Unmarshal(raw, &combined)
	if ref.Parent != nil && ref.KeyPosition >= 0 {
		keyRaw, _ := json.Marshal(ref.Value)
		child := map[string]json.RawMessage{}
		_ = json.Unmarshal(keyRaw, &child)
		for _, key := range []string{"api-key", "proxy-url", "models-endpoint"} {
			if v, ok := child[key]; ok {
				combined[key] = v
			}
		}
	} else if ref.IsParent() {
		for _, key := range ref.Parent.APIKeyEntries {
			if !key.Disabled && key.APIKey != "" {
				combined["api-key"], _ = json.Marshal(key.APIKey)
				combined["proxy-url"], _ = json.Marshal(key.ProxyURL)
				break
			}
		}
	}
	for _, key := range []string{"api-key", "base-url", "proxy-url", "headers", "models-endpoint"} {
		v, ok := input.Values[key]
		if !ok {
			continue
		}
		if key == "api-key" {
			var k string
			_ = json.Unmarshal(v, &k)
			if strings.TrimSpace(k) == "" {
				continue
			}
		}
		if key == "proxy-url" {
			var old, next string
			_ = json.Unmarshal(combined[key], &old)
			_ = json.Unmarshal(v, &next)
			restored, err := applyProxyURLPatch(old, &next, false)
			if err != nil {
				c.JSON(400, gin.H{"error": err.Error()})
				return
			}
			v, _ = json.Marshal(restored)
		}
		combined[key] = v
	}
	b, _ := json.Marshal(combined)
	var fields struct {
		BaseURL        string            `json:"base-url"`
		Headers        map[string]string `json:"headers"`
		ModelsEndpoint string            `json:"models-endpoint"`
		APIKey         string            `json:"api-key"`
		ProxyURL       string            `json:"proxy-url"`
	}
	_ = json.Unmarshal(b, &fields)
	transient := &coreauth.Auth{Provider: ref.Kind, ProxyURL: fields.ProxyURL, Attributes: map[string]string{"api_key": fields.APIKey, "base_url": fields.BaseURL, "runtime_only": "true"}}
	if ref.Management.CredentialIndex != "" {
		if existing := h.authByIndex(ref.Management.CredentialIndex); existing != nil {
			transient = existing.Clone()
			transient.ProxyURL = fields.ProxyURL
			transient.Attributes["api_key"] = fields.APIKey
			transient.Attributes["base_url"] = fields.BaseURL
		}
	}
	c.Set("provider_models_draft_auth", transient)
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
		if !has("authorization") && (ref.Management.CredentialIndex != "" || fields.APIKey != "") {
			headers["Authorization"] = "Bearer $TOKEN$"
		}
	}
	payload, _ := json.Marshal(map[string]any{"auth_index": ref.Management.CredentialIndex, "method": "GET", "url": u.String(), "header": headers})
	c.Request = c.Request.Clone(c.Request.Context())
	c.Request.Body = io.NopCloser(bytes.NewReader(payload))
	c.Request.ContentLength = int64(len(payload))
	h.APICall(c)
}

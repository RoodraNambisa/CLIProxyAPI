package management

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/watcher/synthesizer"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
)

type providerCredentialMutation struct {
	Kind     string                     `json:"kind"`
	ParentID string                     `json:"parent_id"`
	Revision string                     `json:"revision"`
	Values   map[string]json.RawMessage `json:"values"`
}

func findProviderCredential(cfg *config.Config, id string) (config.APICredentialRef, bool) {
	if ref, ok := cfg.APICredentialByID(id); ok {
		return ref, true
	}
	for _, ref := range cfg.APICredentialRefs() {
		if ref.Management.CredentialID == id {
			return ref, true
		}
	}
	return config.APICredentialRef{}, false
}

func providerCredentialValues(value any) map[string]any {
	b, _ := json.Marshal(value)
	out := map[string]any{}
	_ = json.Unmarshal(b, &out)
	_, hasKey := out["api-key"]
	out["has_api_key"] = hasKey && out["api-key"] != ""
	delete(out, "api-key")
	delete(out, "api-key-entries")
	delete(out, "credential-id")
	delete(out, "credential-runtime-id")
	delete(out, "credential-index")
	delete(out, "fill-order-scope")
	if proxy, ok := out["proxy-url"].(string); ok {
		out["proxy-url"] = proxyutil.MaskProxyURL(proxy)
	}
	return out
}

func (h *Handler) GetProviderCredential(c *gin.Context) {
	ref, ok := findProviderCredential(h.currentConfig(), c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "API credential not found"})
		return
	}
	result := gin.H{"id": ref.Management.CredentialID, "kind": ref.Kind, "revision": ref.Revision(), "values": providerCredentialValues(ref.Value), "is_parent": ref.IsParent()}
	if ref.Parent != nil && ref.KeyPosition >= 0 {
		result["parent"] = providerCredentialValues(ref.Parent)
		result["parent_id"] = ref.Parent.CredentialID
	}
	if ref.IsParent() {
		children := make([]gin.H, 0, len(ref.Parent.APIKeyEntries))
		for i, key := range ref.Parent.APIKeyEntries {
			name := key.ChannelName
			if name == "" {
				name = fmt.Sprintf("Key %d", i+1)
			}
			children = append(children, gin.H{"id": key.CredentialID, "name": name, "disabled": key.Disabled})
		}
		result["keys"] = children
	}
	c.JSON(http.StatusOK, result)
}

var providerCredentialEditableFields = map[string]bool{
	"channel-name": true, "api-key": true, "base-url": true, "proxy-url": true, "proxy-region": true, "priority": true, "weight": true, "disabled": true,
	"prefix": true, "headers": true, "models": true, "models-mode": true, "models-endpoint": true, "excluded-models": true, "websockets": true, "alpha-search": true,
	"request-retry": true, "request-scoped-errors": true, "time-context": true, "cloak": true,
	"name": true, "api-key-entries": true, "test-model": true,
}

func applyProviderCredentialValues(ref config.APICredentialRef, values map[string]json.RawMessage) error {
	b, err := json.Marshal(ref.Value)
	if err != nil {
		return err
	}
	var merged map[string]json.RawMessage
	if err = json.Unmarshal(b, &merged); err != nil {
		return err
	}
	for key, value := range values {
		if !providerCredentialEditableFields[key] {
			return fmt.Errorf("unsupported field: %s", key)
		}
		if key == "name" || key == "api-key-entries" || key == "test-model" {
			if ref.Kind != "openai" || ref.KeyPosition >= 0 {
				return fmt.Errorf("field %s belongs to the parent channel", key)
			}
		}
		if key == "api-key-entries" {
			var entries []config.OpenAICompatibilityAPIKey
			if err := json.Unmarshal(value, &entries); err != nil {
				return fmt.Errorf("invalid channel keys: %w", err)
			}
			existing := make(map[string]config.OpenAICompatibilityAPIKey, len(ref.Parent.APIKeyEntries))
			for _, entry := range ref.Parent.APIKeyEntries {
				existing[entry.CredentialID] = entry
			}
			for i := range entries {
				entry := &entries[i]
				if entry.CredentialID != "" {
					old, found := existing[entry.CredentialID]
					if !found {
						return fmt.Errorf("channel key identity does not belong to this channel")
					}
					entry.RuntimeID, entry.CredentialIndex, entry.FillOrderScope = old.RuntimeID, old.CredentialIndex, old.FillOrderScope
					if strings.TrimSpace(entry.APIKey) == "" {
						entry.APIKey = old.APIKey
					}
					proxy, err := applyProxyURLPatch(old.ProxyURL, &entry.ProxyURL, false)
					if err != nil {
						return err
					}
					entry.ProxyURL = proxy
				} else {
					identity := newProviderCredentialIdentity()
					entry.CredentialID, entry.CredentialIndex, entry.RuntimeID, entry.FillOrderScope = identity.CredentialID, identity.CredentialIndex, "", ""
				}
				if strings.TrimSpace(entry.APIKey) == "" {
					return fmt.Errorf("api-key is required")
				}
			}
			value, _ = json.Marshal(entries)
		}
		if ref.Parent != nil && ref.KeyPosition >= 0 {
			switch key {
			case "channel-name", "api-key", "proxy-url", "priority", "weight", "disabled", "models", "models-mode", "models-endpoint":
			default:
				return fmt.Errorf("field %s belongs to the parent channel", key)
			}
		}
		if key == "api-key" {
			var v string
			if json.Unmarshal(value, &v) != nil {
				return fmt.Errorf("invalid api-key")
			}
			if strings.TrimSpace(v) == "" {
				continue
			}
		}
		if key == "proxy-url" {
			var proxy string
			if json.Unmarshal(value, &proxy) != nil {
				return fmt.Errorf("invalid proxy-url")
			}
			var current string
			_ = json.Unmarshal(merged[key], &current)
			next, errProxy := applyProxyURLPatch(current, &proxy, false)
			if errProxy != nil {
				return errProxy
			}
			value, _ = json.Marshal(next)
		}
		merged[key] = value
	}
	b, err = json.Marshal(merged)
	if err != nil {
		return err
	}
	replacement := reflect.New(reflect.TypeOf(ref.Value).Elem())
	if errDecode := json.Unmarshal(b, replacement.Interface()); errDecode != nil {
		return errDecode
	}
	reflect.ValueOf(ref.Value).Elem().Set(replacement.Elem())
	return nil
}

func (h *Handler) PatchProviderCredential(c *gin.Context)  { h.mutateProviderCredential(c, false) }
func (h *Handler) DeleteProviderCredential(c *gin.Context) { h.mutateProviderCredential(c, true) }
func (h *Handler) mutateProviderCredential(c *gin.Context, remove bool) {
	var body providerCredentialMutation
	if c.ShouldBindJSON(&body) != nil || body.Revision == "" {
		c.JSON(400, gin.H{"error": "revision is required"})
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	candidate, err := config.Clone(h.cfg)
	if err != nil {
		c.JSON(500, gin.H{"error": "unable to snapshot configuration"})
		return
	}
	ref, ok := findProviderCredential(candidate, c.Param("id"))
	if !ok {
		c.JSON(404, gin.H{"error": "API credential not found"})
		return
	}
	if ref.Revision() != body.Revision {
		c.JSON(409, gin.H{"error": "configuration changed; refresh before saving"})
		return
	}
	if remove {
		removeProviderCredential(candidate, ref)
	} else if err = applyProviderCredentialValues(ref, body.Values); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if !remove {
		if err = validateProviderCredential(ref); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
	}
	if err = candidate.ValidateAPICredentials(); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	previous := h.cfg
	h.cfg = candidate
	if !h.persistLocked(c) {
		h.cfg = previous
	}
}

func removeProviderCredential(cfg *config.Config, r config.APICredentialRef) {
	i := r.Position
	switch r.Kind {
	case "gemini":
		cfg.GeminiKey = append(cfg.GeminiKey[:i], cfg.GeminiKey[i+1:]...)
	case "interactions":
		cfg.InteractionsKey = append(cfg.InteractionsKey[:i], cfg.InteractionsKey[i+1:]...)
	case "claude":
		cfg.ClaudeKey = append(cfg.ClaudeKey[:i], cfg.ClaudeKey[i+1:]...)
	case "codex":
		cfg.CodexKey = append(cfg.CodexKey[:i], cfg.CodexKey[i+1:]...)
	case "xai":
		cfg.XAIKey = append(cfg.XAIKey[:i], cfg.XAIKey[i+1:]...)
	case "vertex":
		cfg.VertexCompatAPIKey = append(cfg.VertexCompatAPIKey[:i], cfg.VertexCompatAPIKey[i+1:]...)
	case "openai":
		if r.KeyPosition < 0 {
			cfg.OpenAICompatibility = append(cfg.OpenAICompatibility[:i], cfg.OpenAICompatibility[i+1:]...)
		} else {
			p := &cfg.OpenAICompatibility[i]
			j := r.KeyPosition
			p.APIKeyEntries = append(p.APIKeyEntries[:j], p.APIKeyEntries[j+1:]...)
			if len(p.APIKeyEntries) == 0 {
				p.Disabled = true
			}
		}
	}
}

func newProviderCredentialIdentity() config.APIManagement {
	id := "api-credential:" + uuid.NewString()
	sum := sha256.Sum256([]byte(id))
	return config.APIManagement{CredentialID: id, CredentialIndex: hex.EncodeToString(sum[:8])}
}

func (h *Handler) CreateProviderCredential(c *gin.Context) {
	var body providerCredentialMutation
	if c.ShouldBindJSON(&body) != nil || body.Values == nil {
		c.JSON(400, gin.H{"error": "kind and values are required"})
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	candidate, err := config.Clone(h.cfg)
	if err != nil {
		c.JSON(500, gin.H{"error": "unable to snapshot configuration"})
		return
	}
	m := newProviderCredentialIdentity()
	switch body.Kind {
	case "gemini":
		candidate.GeminiKey = append(candidate.GeminiKey, config.GeminiKey{APIManagement: m})
	case "interactions":
		candidate.InteractionsKey = append(candidate.InteractionsKey, config.GeminiKey{APIManagement: m})
	case "claude":
		candidate.ClaudeKey = append(candidate.ClaudeKey, config.ClaudeKey{APIManagement: m})
	case "codex":
		candidate.CodexKey = append(candidate.CodexKey, config.CodexKey{APIManagement: m})
	case "xai":
		candidate.XAIKey = append(candidate.XAIKey, config.XAIKey{APIManagement: m})
	case "vertex":
		candidate.VertexCompatAPIKey = append(candidate.VertexCompatAPIKey, config.VertexCompatKey{APIManagement: m})
	case "openai":
		if body.ParentID != "" {
			parent, ok := findProviderCredential(candidate, body.ParentID)
			if !ok || parent.Kind != "openai" || parent.KeyPosition >= 0 {
				c.JSON(400, gin.H{"error": "parent channel not found"})
				return
			}
			if parent.Revision() != body.Revision {
				c.JSON(409, gin.H{"error": "parent channel changed"})
				return
			}
			parent.Parent.APIKeyEntries = append(parent.Parent.APIKeyEntries, config.OpenAICompatibilityAPIKey{APIManagement: m})
		} else {
			candidate.OpenAICompatibility = append(candidate.OpenAICompatibility, config.OpenAICompatibility{APIManagement: m, Name: "channel-" + uuid.NewString()})
		}
	default:
		c.JSON(400, gin.H{"error": "unsupported API credential type"})
		return
	}
	candidate.IndexAPICredentials()
	ref, _ := findProviderCredential(candidate, m.CredentialID)
	if err = applyProviderCredentialValues(ref, body.Values); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if body.Kind == "openai" && body.ParentID == "" {
		var key string
		_ = json.Unmarshal(body.Values["api-key"], &key)
		if strings.TrimSpace(key) != "" {
			child := newProviderCredentialIdentity()
			ref.Parent.APIKeyEntries = []config.OpenAICompatibilityAPIKey{{APIManagement: child, APIKey: strings.TrimSpace(key)}}
			m = child
		}
	}
	if err = validateProviderCredential(ref); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if _, err = synthesizer.PrepareManagedCredentials(candidate); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	ref, _ = findProviderCredential(candidate, m.CredentialID)
	c.Set(managementResponseFieldsKey, gin.H{"id": m.CredentialID, "revision": ref.Revision()})
	previous := h.cfg
	h.cfg = candidate
	if !h.persistLocked(c) {
		h.cfg = previous
	}
}

func validateProviderCredential(ref config.APICredentialRef) error {
	raw, _ := json.Marshal(ref.Value)
	var fields struct {
		APIKey  string `json:"api-key"`
		BaseURL string `json:"base-url"`
		Models  []struct {
			Name string `json:"name"`
		} `json:"models"`
	}
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	if (ref.Kind != "openai" || ref.KeyPosition >= 0) && strings.TrimSpace(fields.APIKey) == "" {
		return fmt.Errorf("api-key is required")
	}
	if (ref.Kind == "codex" || ref.Kind == "openai" && ref.KeyPosition < 0) && strings.TrimSpace(fields.BaseURL) == "" {
		return fmt.Errorf("base-url is required")
	}
	for _, m := range fields.Models {
		if strings.TrimSpace(m.Name) == "" {
			return fmt.Errorf("model name is required")
		}
	}
	return nil
}

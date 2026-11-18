package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// APIManagement gives a configured upstream credential a durable management identity.
// Existing runtime identifiers are frozen during migration to retain usage and order.
type APIManagement struct {
	RuntimeID       string `yaml:"credential-runtime-id,omitempty" json:"credential-runtime-id,omitempty"`
	CredentialID    string `yaml:"credential-id,omitempty" json:"credential-id,omitempty"`
	CredentialIndex string `yaml:"credential-index,omitempty" json:"credential-index,omitempty"`
	FillOrderScope  string `yaml:"fill-order-scope,omitempty" json:"fill-order-scope,omitempty"`
	ChannelName     string `yaml:"channel-name,omitempty" json:"channel-name,omitempty"`
	ModelsMode      string `yaml:"models-mode,omitempty" json:"models-mode,omitempty"`
	ModelsEndpoint  string `yaml:"models-endpoint,omitempty" json:"models-endpoint,omitempty"`
}

// APICredentialRef is used only while loading or editing configuration.
type APICredentialRef struct {
	Kind        string
	Position    int
	KeyPosition int
	Management  *APIManagement
	Value       any
	Parent      *OpenAICompatibility
	Disabled    *bool
}

func (c *Config) APICredentialRefs() []APICredentialRef {
	if c == nil {
		return nil
	}
	out := make([]APICredentialRef, 0, len(c.GeminiKey)+len(c.InteractionsKey)+len(c.ClaudeKey)+len(c.CodexKey)+len(c.XAIKey)+len(c.VertexCompatAPIKey)+len(c.OpenAICompatibility))
	for i := range c.GeminiKey {
		v := &c.GeminiKey[i]
		out = append(out, APICredentialRef{"gemini", i, -1, &v.APIManagement, v, nil, &v.Disabled})
	}
	for i := range c.InteractionsKey {
		v := &c.InteractionsKey[i]
		out = append(out, APICredentialRef{"interactions", i, -1, &v.APIManagement, v, nil, &v.Disabled})
	}
	for i := range c.ClaudeKey {
		v := &c.ClaudeKey[i]
		out = append(out, APICredentialRef{"claude", i, -1, &v.APIManagement, v, nil, &v.Disabled})
	}
	for i := range c.CodexKey {
		v := &c.CodexKey[i]
		out = append(out, APICredentialRef{"codex", i, -1, &v.APIManagement, v, nil, &v.Disabled})
	}
	for i := range c.XAIKey {
		v := &c.XAIKey[i]
		out = append(out, APICredentialRef{"xai", i, -1, &v.APIManagement, v, nil, &v.Disabled})
	}
	for i := range c.VertexCompatAPIKey {
		v := &c.VertexCompatAPIKey[i]
		out = append(out, APICredentialRef{"vertex", i, -1, &v.APIManagement, v, nil, &v.Disabled})
	}
	for i := range c.OpenAICompatibility {
		p := &c.OpenAICompatibility[i]
		out = append(out, APICredentialRef{"openai", i, -1, &p.APIManagement, p, p, &p.Disabled})
		for j := range p.APIKeyEntries {
			v := &p.APIKeyEntries[j]
			out = append(out, APICredentialRef{"openai", i, j, &v.APIManagement, v, p, &v.Disabled})
		}
	}
	return out
}

func (r APICredentialRef) IsParent() bool {
	return r.Parent != nil && r.KeyPosition < 0 && len(r.Parent.APIKeyEntries) > 0
}
func (r APICredentialRef) Revision() string {
	b, _ := yaml.Marshal(r.Value)
	if r.Parent != nil && r.KeyPosition >= 0 {
		parent := *r.Parent
		parent.APIKeyEntries = nil
		p, _ := yaml.Marshal(parent)
		b = append(b, p...)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:16])
}

func (c *Config) ValidateAPICredentials() error {
	ids := make(map[string]bool)
	runtimeIDs := make(map[string]bool)
	indexes := make(map[string]bool)
	for _, r := range c.APICredentialRefs() {
		m := r.Management
		if m.CredentialID != "" {
			if len(m.CredentialID) > 256 || strings.ContainsAny(m.CredentialID, "/\\\x00\r\n") || ids[m.CredentialID] {
				return fmt.Errorf("invalid or duplicate credential-id")
			}
			ids[m.CredentialID] = true
		}
		if m.CredentialID != "" && !r.IsParent() {
			runtimeID := m.RuntimeID
			if runtimeID == "" {
				runtimeID = m.CredentialID
			}
			if runtimeIDs[runtimeID] {
				return fmt.Errorf("duplicate credential runtime identity")
			}
			runtimeIDs[runtimeID] = true
		}
		if m.CredentialIndex != "" && !r.IsParent() {
			if len(m.CredentialIndex) != 16 || indexes[m.CredentialIndex] {
				return fmt.Errorf("invalid or duplicate credential-index")
			}
			if _, err := hex.DecodeString(m.CredentialIndex); err != nil {
				return fmt.Errorf("invalid credential-index")
			}
			indexes[m.CredentialIndex] = true
		}
		if m.ModelsMode != "" && m.ModelsMode != "default" && m.ModelsMode != "selected" {
			return fmt.Errorf("invalid models-mode")
		}
		if len(m.ChannelName) > 256 {
			return fmt.Errorf("channel-name exceeds 256 bytes")
		}
		if m.ModelsMode == "selected" {
			b, _ := json.Marshal(r.Value)
			var v struct {
				Models []json.RawMessage `json:"models"`
			}
			_ = json.Unmarshal(b, &v)
			if len(v.Models) == 0 {
				return fmt.Errorf("selected model mode requires at least one model")
			}
		}
	}
	return nil
}

// EffectiveOpenAIKey snapshots the independently selected credential's model policy.
func EffectiveOpenAIKey(parent *OpenAICompatibility, id, key string) *OpenAICompatibility {
	if parent == nil {
		return nil
	}
	for i := range parent.APIKeyEntries {
		entry := &parent.APIKeyEntries[i]
		matches := id != "" && entry.CredentialID == id
		if id == "" || entry.CredentialID == "" {
			matches = strings.TrimSpace(entry.APIKey) == strings.TrimSpace(key)
		}
		if !matches {
			continue
		}
		return EffectiveOpenAIEntry(parent, entry)
	}
	return parent
}

func EffectiveOpenAIEntry(parent *OpenAICompatibility, entry *OpenAICompatibilityAPIKey) *OpenAICompatibility {
	if parent == nil || entry == nil {
		return parent
	}
	out := *parent
	if entry.Priority != nil {
		out.Priority = *entry.Priority
	}
	if entry.Models != nil {
		out.Models = *entry.Models
	}
	if entry.ModelsMode != "" {
		out.ModelsMode = entry.ModelsMode
	}
	out.Disabled = parent.Disabled || entry.Disabled
	return &out
}

// IndexAPICredentials runs before publication; lookups never initialize shared state.
func (c *Config) IndexAPICredentials() {
	if c == nil {
		return
	}
	var index map[string]APICredentialRef
	for _, ref := range c.APICredentialRefs() {
		if ref.Management.CredentialID != "" {
			if index == nil {
				index = make(map[string]APICredentialRef)
			}
			index[ref.Management.CredentialID] = ref
		}
	}
	c.apiCredentialIndex = index
}

func (c *Config) APICredentialByID(id string) (APICredentialRef, bool) {
	if c == nil || id == "" {
		return APICredentialRef{}, false
	}
	ref, ok := c.apiCredentialIndex[id]
	return ref, ok
}

func (m APIManagement) GetCredentialID() string { return m.CredentialID }

// RetainAPIManagement preserves durable fields omitted by older list editors.
// Ambiguous matches are deliberately not inherited across configuration entries.
func RetainAPIManagement(next, previous *Config) {
	identity := func(r APICredentialRef) string {
		raw, _ := json.Marshal(r.Value)
		var v struct {
			APIKey  string `json:"api-key"`
			BaseURL string `json:"base-url"`
		}
		_ = json.Unmarshal(raw, &v)
		parent := ""
		if r.Parent != nil {
			parent = r.Parent.Name
			v.BaseURL = r.Parent.BaseURL
		}
		return r.Kind + "\x00" + parent + "\x00" + v.APIKey + "\x00" + v.BaseURL
	}
	old := make(map[string][]APIManagement)
	for _, r := range previous.APICredentialRefs() {
		key := identity(r)
		old[key] = append(old[key], *r.Management)
	}
	for _, r := range next.APICredentialRefs() {
		if r.Management.CredentialID != "" {
			continue
		}
		if matches := old[identity(r)]; len(matches) == 1 {
			m := r.Management
			m.CredentialID, m.CredentialIndex, m.FillOrderScope = matches[0].CredentialID, matches[0].CredentialIndex, matches[0].FillOrderScope
			m.RuntimeID = matches[0].RuntimeID
			if m.ChannelName == "" {
				m.ChannelName = matches[0].ChannelName
			}
			if m.ModelsMode == "" {
				m.ModelsMode = matches[0].ModelsMode
			}
			if m.ModelsEndpoint == "" {
				m.ModelsEndpoint = matches[0].ModelsEndpoint
			}
		}
	}
}

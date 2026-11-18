package synthesizer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func applyManagedCredential(a *coreauth.Auth, m *config.APIManagement, disabled bool, kind string, position, keyPosition int, parent *config.OpenAICompatibility) {
	if a == nil || m == nil {
		return
	}
	if m.CredentialID != "" {
		a.ID = m.CredentialID
		a.Attributes["source"] = fmt.Sprintf("config:%s[%s]", kind, m.CredentialID)
	}
	if m.RuntimeID != "" {
		a.ID = m.RuntimeID
	}
	a.Attributes["config_id"] = m.CredentialID
	if m.CredentialID == "" {
		a.Attributes["config_id"] = a.ID
	}
	a.Attributes["config_kind"] = kind
	a.Attributes["config_slot"] = fmt.Sprintf("%s:%d:%d", kind, position, keyPosition)
	if m.CredentialIndex != "" {
		a.Attributes["config_usage_index"] = m.CredentialIndex
	}
	if m.FillOrderScope != "" {
		a.Attributes["config_fill_scope"] = m.FillOrderScope
	}
	if m.ChannelName != "" {
		a.Label = m.ChannelName
	}
	if m.ModelsMode != "" {
		a.Attributes["config_models_mode"] = m.ModelsMode
	}
	if parent != nil {
		a.Attributes["config_parent_id"] = parent.CredentialID
		a.Attributes["config_parent_name"] = parent.ChannelName
		if parent.ChannelName == "" {
			a.Attributes["config_parent_name"] = parent.Name
		}
		if m.ChannelName == "" {
			a.Label = a.Attributes["config_parent_name"] + " / " + strconv.Itoa(keyPosition+1)
		}
		if m.ModelsMode == "" {
			a.Attributes["config_models_mode"] = parent.ModelsMode
		}
	}
	if disabled {
		a.Disabled = true
		a.Status = coreauth.StatusDisabled
	}
}

// PrepareManagedCredentials must run on an unpublished configuration candidate.
// It captures the legacy IDs before a key or endpoint is changed.
func PrepareManagedCredentials(cfg *config.Config) (bool, error) {
	if cfg == nil {
		return false, nil
	}
	auths, err := NewConfigSynthesizer().Synthesize(&SynthesisContext{Config: cfg, Now: time.Now(), IDGenerator: NewStableIDGenerator()})
	if err != nil {
		return false, err
	}
	bySlot := make(map[string]*coreauth.Auth, len(auths))
	for _, a := range auths {
		bySlot[a.Attributes["config_slot"]] = a
	}
	changed := false
	completeInactive := false
	for _, r := range cfg.APICredentialRefs() {
		m := r.Management
		a := bySlot[fmt.Sprintf("%s:%d:%d", r.Kind, r.Position, r.KeyPosition)]
		if m.CredentialID == "" {
			if a != nil {
				m.CredentialID = a.ID
				if strings.ContainsAny(a.ID, "/\\") {
					m.CredentialID = "api-credential:" + uuid.NewString()
					m.RuntimeID = a.ID
				}
			} else {
				m.CredentialID = "api-channel:" + uuid.NewString()
			}
			changed = true
		}
		if a == nil {
			if r.Kind == "openai" && r.KeyPosition >= 0 {
				completeInactive = true
			}
			continue
		}
		if m.CredentialIndex == "" {
			m.CredentialIndex = a.EnsureIndex()
			changed = true
		}
		if m.FillOrderScope == "" {
			base := strings.TrimRight(a.Attributes["base_url"], "/")
			if a.Provider == "codex" {
				base = coreauth.ResolveCodexUpstream(a, cfg).BaseURL
			}
			sum := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(a.Provider)) + "\x00" + base))
			m.FillOrderScope = hex.EncodeToString(sum[:])
			changed = true
		}
	}
	if completeInactive {
		_, err := PrepareManagedCredentials(cfg)
		if err != nil {
			return changed, err
		}
	}
	cfg.IndexAPICredentials()
	return changed, cfg.ValidateAPICredentials()
}

func finalizeManagedCredentials(cfg *config.Config, auths []*coreauth.Auth) {
	refs := make(map[string]config.APICredentialRef)
	for _, r := range cfg.APICredentialRefs() {
		refs[fmt.Sprintf("%s:%d:%d", r.Kind, r.Position, r.KeyPosition)] = r
	}
	for _, a := range auths {
		r, ok := refs[a.Attributes["config_slot"]]
		if !ok {
			continue
		}
		a.Attributes["config_revision"] = r.Revision()
		value := r.Value
		if r.Parent != nil && r.KeyPosition >= 0 {
			value = config.EffectiveOpenAIEntry(r.Parent, &r.Parent.APIKeyEntries[r.KeyPosition])
		}
		raw, _ := json.Marshal(value)
		var fields struct {
			Models []struct {
				Name  string `json:"name"`
				Alias string `json:"alias"`
			} `json:"models"`
		}
		_ = json.Unmarshal(raw, &fields)
		names := make([]string, 0, len(fields.Models))
		for _, model := range fields.Models {
			name := model.Alias
			if name == "" {
				name = model.Name
			}
			names = append(names, name)
		}
		encoded, _ := json.Marshal(names)
		a.Attributes["config_models"] = string(encoded)
		a.Attributes["config_models_count"] = strconv.Itoa(len(names))
	}
}

// PrepareManagedCredentialsWithPrevious carries durable metadata through legacy
// configuration editors before assigning identities to newly added entries.
func PrepareManagedCredentialsWithPrevious(cfg, previous *config.Config) (bool, error) {
	refs := cfg.APICredentialRefs()
	before := make([]config.APIManagement, len(refs))
	for i, ref := range refs {
		before[i] = *ref.Management
	}
	config.RetainAPIManagement(cfg, previous)
	changed, err := PrepareManagedCredentials(cfg)
	if err != nil {
		return changed, err
	}
	for i, ref := range refs {
		changed = changed || before[i] != *ref.Management
	}
	return changed, nil
}

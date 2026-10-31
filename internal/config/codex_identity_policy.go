package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"golang.org/x/net/http/httpguts"
)

// CodexIdentityOverride contains only runtime identity controls. Nil means inherit.
type CodexIdentityOverride struct {
	IdentityConfuse           *bool                 `yaml:"identity-confuse,omitempty" json:"identity-confuse,omitempty"`
	RequireSessionAffinity    *bool                 `yaml:"identity-confuse-require-session-affinity,omitempty" json:"identity-confuse-require-session-affinity,omitempty"`
	RestoreResponse           *bool                 `yaml:"identity-confuse-restore-response,omitempty" json:"identity-confuse-restore-response,omitempty"`
	SpoofSessionIdentity      *bool                 `yaml:"spoof-session-identity,omitempty" json:"spoof-session-identity,omitempty"`
	FingerprintMode           *string               `yaml:"fingerprint-mode,omitempty" json:"fingerprint-mode,omitempty"`
	SessionIdentityPoolSize   *int                  `yaml:"session-identity-pool-size,omitempty" json:"session-identity-pool-size,omitempty"`
	EnforceSoftwareIdentity   *bool                 `yaml:"enforce-software-identity,omitempty" json:"enforce-software-identity,omitempty"`
	UserAgent                 *string               `yaml:"user-agent,omitempty" json:"user-agent,omitempty"`
	TurnStatePolicy           *CodexTurnStatePolicy `yaml:"turn-state-policy,omitempty" json:"turn-state-policy,omitempty"`
	PassthroughPromptCacheKey *bool                 `yaml:"passthrough-prompt-cache-key,omitempty" json:"passthrough-prompt-cache-key,omitempty"`
}

func (o *CodexIdentityOverride) Validate() error {
	if o == nil {
		return nil
	}
	if o.FingerprintMode != nil {
		mode, ok := NormalizeFingerprintIdentityMode(*o.FingerprintMode)
		if !ok {
			return fmt.Errorf("fingerprint-mode must be off, device, session, or full")
		}
		*o.FingerprintMode = mode
	}
	if o.SessionIdentityPoolSize != nil && (*o.SessionIdentityPoolSize < 1 || *o.SessionIdentityPoolSize > MaxCodexSessionIdentityPoolSize) {
		return fmt.Errorf("session-identity-pool-size must be between 1 and 64")
	}
	if o.TurnStatePolicy != nil {
		value, err := NormalizeCodexTurnStatePolicy(*o.TurnStatePolicy)
		if err != nil {
			return err
		}
		*o.TurnStatePolicy = value
	}
	if o.UserAgent != nil && (!httpguts.ValidHeaderFieldValue(*o.UserAgent) || strings.TrimSpace(*o.UserAgent) != *o.UserAgent) {
		return fmt.Errorf("user-agent must be a valid HTTP header without surrounding whitespace")
	}
	return nil
}

func NormalizeFingerprintIdentityMode(value string) (string, bool) {
	value = strings.ToLower(strings.TrimSpace(value))
	switch value {
	case "off", "device", "session", "full":
		return value, true
	}
	return "", false
}

// CodexIdentityPolicy is an immutable, allocation-free runtime value.
type CodexIdentityPolicy struct {
	IdentityConfuse           bool                 `json:"identity-confuse"`
	RequireSessionAffinity    bool                 `json:"identity-confuse-require-session-affinity"`
	RestoreResponse           bool                 `json:"identity-confuse-restore-response"`
	SpoofSessionIdentity      bool                 `json:"spoof-session-identity"`
	FingerprintMode           string               `json:"fingerprint-mode"`
	SessionIdentityPoolSize   int                  `json:"session-identity-pool-size"`
	EnforceSoftwareIdentity   bool                 `json:"enforce-software-identity"`
	UserAgent                 string               `json:"user-agent"`
	TurnStatePolicy           CodexTurnStatePolicy `json:"turn-state-policy"`
	PassthroughPromptCacheKey bool                 `json:"passthrough-prompt-cache-key"`
	SessionAffinity           bool                 `json:"session-affinity"`
	ConfuseEffective          bool                 `json:"identity-confuse-effective"`
	priorityUA                bool
}

// CodexIdentityPolicySet is compiled once per configuration snapshot.
type CodexIdentityPolicySet struct {
	base                CodexIdentityPolicy
	priorities          map[int]CodexIdentityPolicy
	sources             map[int]map[string]bool
	needsSpoof          bool
	needsCacheSource    bool
	needsIdentitySource bool
	Version             string
}

func CompileCodexIdentityPolicies(cfg *Config) *CodexIdentityPolicySet {
	if cfg == nil {
		cfg = &Config{}
	}
	p := &CodexIdentityPolicySet{base: CodexIdentityPolicy{
		IdentityConfuse:           cfg.Codex.IdentityConfuse,
		RequireSessionAffinity:    cfg.Codex.IdentityConfuseRequireSessionAffinity == nil || *cfg.Codex.IdentityConfuseRequireSessionAffinity,
		RestoreResponse:           cfg.Codex.IdentityConfuseRestoreResponse == nil || *cfg.Codex.IdentityConfuseRestoreResponse,
		SpoofSessionIdentity:      cfg.Codex.SpoofSessionIdentity,
		SessionIdentityPoolSize:   cfg.CodexFingerprint.ResolvedSessionIdentityPoolSize(),
		EnforceSoftwareIdentity:   cfg.Codex.ResolvedEnforceSoftwareIdentity(),
		UserAgent:                 cfg.CodexHeaderDefaults.UserAgent,
		TurnStatePolicy:           cfg.Codex.ResolvedTurnStatePolicy(),
		PassthroughPromptCacheKey: cfg.Codex.PassthroughPromptCacheKey,
		SessionAffinity:           cfg.Routing.SessionAffinity || cfg.Routing.ClaudeCodeSessionAffinity,
	}, priorities: make(map[int]CodexIdentityPolicy), sources: make(map[int]map[string]bool)}
	p.needsSpoof = p.base.SpoofSessionIdentity
	p.needsIdentitySource = p.base.IdentityConfuse || p.base.SpoofSessionIdentity
	p.needsCacheSource = p.base.PassthroughPromptCacheKey
	for _, r := range cfg.Routing.PriorityOverrides {
		if r.Codex == nil {
			continue
		}
		o := r.Codex
		v := p.base
		if o.IdentityConfuse != nil {
			v.IdentityConfuse = *o.IdentityConfuse
		}
		if o.RequireSessionAffinity != nil {
			v.RequireSessionAffinity = *o.RequireSessionAffinity
		}
		if o.RestoreResponse != nil {
			v.RestoreResponse = *o.RestoreResponse
		}
		if o.SpoofSessionIdentity != nil {
			v.SpoofSessionIdentity = *o.SpoofSessionIdentity
		}
		if o.FingerprintMode != nil {
			v.FingerprintMode = *o.FingerprintMode
		}
		if o.SessionIdentityPoolSize != nil {
			v.SessionIdentityPoolSize = *o.SessionIdentityPoolSize
		}
		if o.EnforceSoftwareIdentity != nil {
			v.EnforceSoftwareIdentity = *o.EnforceSoftwareIdentity
		}
		if o.UserAgent != nil {
			v.UserAgent = *o.UserAgent
			v.priorityUA = true
		}
		if o.TurnStatePolicy != nil {
			v.TurnStatePolicy = *o.TurnStatePolicy
		}
		if o.PassthroughPromptCacheKey != nil {
			v.PassthroughPromptCacheKey = *o.PassthroughPromptCacheKey
		}
		p.needsSpoof = p.needsSpoof || v.SpoofSessionIdentity
		p.needsIdentitySource = p.needsIdentitySource || v.IdentityConfuse || v.SpoofSessionIdentity
		p.needsCacheSource = p.needsCacheSource || v.PassthroughPromptCacheKey
		p.priorities[r.Priority] = v
		raw, _ := json.Marshal(o)
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(raw, &fields)
		p.sources[r.Priority] = make(map[string]bool, len(fields))
		for key := range fields {
			p.sources[r.Priority][key] = true
		}
	}
	raw, _ := json.Marshal(struct {
		Base       CodexIdentityPolicy
		Priorities map[int]CodexIdentityPolicy
		Sources    map[int]map[string]bool
	}{p.base, p.priorities, p.sources})
	sum := sha256.Sum256(raw)
	p.Version = hex.EncodeToString(sum[:8])
	return p
}

func (p *CodexIdentityPolicySet) Resolve(priority int, credentialMode, credentialUA string) CodexIdentityPolicy {
	v := p.base
	if o, ok := p.priorities[priority]; ok {
		v = o
	}
	if v.FingerprintMode == "" {
		v.FingerprintMode = credentialMode
	}
	if v.FingerprintMode == "" {
		v.FingerprintMode = "device"
	}
	if !v.priorityUA && credentialUA != "" {
		v.UserAgent = credentialUA
	}
	v.ConfuseEffective = v.IdentityConfuse && (!v.RequireSessionAffinity || v.SessionAffinity)
	return v
}

// Sources is used only by management previews, never on the execution hot path.
func (p *CodexIdentityPolicySet) Sources(priority int, credentialMode, credentialUA bool) map[string]string {
	raw, _ := json.Marshal(p.base)
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	result := make(map[string]string, len(fields))
	for key := range fields {
		result[key] = "global"
	}
	if credentialMode {
		result["fingerprint-mode"] = "credential"
	} else {
		result["fingerprint-mode"] = "default"
	}
	if credentialUA {
		result["user-agent"] = "credential"
	}
	for key := range p.sources[priority] {
		result[key] = "priority"
	}
	result["identity-confuse-effective"] = "derived"
	return result
}

func (p *CodexIdentityPolicySet) NeedsSpoof() bool       { return p.needsSpoof }
func (p *CodexIdentityPolicySet) NeedsCacheSource() bool { return p.needsCacheSource }

func (o *CodexIdentityOverride) Clone() *CodexIdentityOverride {
	if o == nil {
		return nil
	}
	out := *o
	out.IdentityConfuse = cloneIdentityOptional(o.IdentityConfuse)
	out.RequireSessionAffinity = cloneIdentityOptional(o.RequireSessionAffinity)
	out.RestoreResponse = cloneIdentityOptional(o.RestoreResponse)
	out.SpoofSessionIdentity = cloneIdentityOptional(o.SpoofSessionIdentity)
	out.FingerprintMode = cloneIdentityOptional(o.FingerprintMode)
	out.SessionIdentityPoolSize = cloneIdentityOptional(o.SessionIdentityPoolSize)
	out.EnforceSoftwareIdentity = cloneIdentityOptional(o.EnforceSoftwareIdentity)
	out.UserAgent = cloneIdentityOptional(o.UserAgent)
	out.TurnStatePolicy = cloneIdentityOptional(o.TurnStatePolicy)
	out.PassthroughPromptCacheKey = cloneIdentityOptional(o.PassthroughPromptCacheKey)
	return &out
}
func cloneIdentityOptional[T any](v *T) *T {
	if v == nil {
		return nil
	}
	copy := *v
	return &copy
}

func (p CodexIdentityPolicy) HasUserAgentOverride() bool { return p.priorityUA }

func (p *CodexIdentityPolicySet) NeedsIdentitySource() bool { return p.needsIdentitySource }

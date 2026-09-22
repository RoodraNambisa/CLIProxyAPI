package config

import (
	"fmt"
	"time"
)

// CodexStateStrategySettings preserves explicit zero and false overrides.
type CodexStateStrategySettings struct {
	Strategy                   *string `yaml:"strategy,omitempty" json:"strategy,omitempty"`
	CookieVerifyAfterAcquire   *bool   `yaml:"cookie-verify-after-acquire,omitempty" json:"cookie-verify-after-acquire,omitempty"`
	CookieBackupCount          *int    `yaml:"cookie-backup-count,omitempty" json:"cookie-backup-count,omitempty"`
	CookieMaxAgeSeconds        *int    `yaml:"cookie-max-age-seconds,omitempty" json:"cookie-max-age-seconds,omitempty"`
	CookieRefreshBeforeSeconds *int    `yaml:"cookie-refresh-before-seconds,omitempty" json:"cookie-refresh-before-seconds,omitempty"`
	TTLSeconds                 *int    `yaml:"ttl-seconds,omitempty" json:"ttl-seconds,omitempty"`
	RefreshBeforeSeconds       *int    `yaml:"refresh-before-seconds,omitempty" json:"refresh-before-seconds,omitempty"`
	MissingReturnedState       *string `yaml:"missing-returned-state,omitempty" json:"missing-returned-state,omitempty"`
}

func cloneStateValue[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

func (s CodexStateStrategySettings) clone() CodexStateStrategySettings {
	s.Strategy = cloneStateValue(s.Strategy)
	s.CookieVerifyAfterAcquire = cloneStateValue(s.CookieVerifyAfterAcquire)
	s.CookieBackupCount = cloneStateValue(s.CookieBackupCount)
	s.CookieMaxAgeSeconds = cloneStateValue(s.CookieMaxAgeSeconds)
	s.CookieRefreshBeforeSeconds = cloneStateValue(s.CookieRefreshBeforeSeconds)
	s.TTLSeconds = cloneStateValue(s.TTLSeconds)
	s.RefreshBeforeSeconds = cloneStateValue(s.RefreshBeforeSeconds)
	s.MissingReturnedState = cloneStateValue(s.MissingReturnedState)
	return s
}

func (s CodexStateStrategySettings) validateExplicit() error {
	if s.Strategy != nil && *s.Strategy != "state" && *s.Strategy != "cookie-only" {
		return fmt.Errorf("invalid strategy override")
	}
	if s.CookieBackupCount != nil && (*s.CookieBackupCount < 0 || *s.CookieBackupCount > 10) {
		return fmt.Errorf("cookie-backup-count must be between 0 and 10")
	}
	if s.MissingReturnedState != nil && *s.MissingReturnedState != "ignore" && *s.MissingReturnedState != "reject" {
		return fmt.Errorf("invalid missing-returned-state override")
	}
	return nil
}

func applyStateStrategySettings(c *CodexStateOverrideConfig, s CodexStateStrategySettings, sources map[string]string, layer string) {
	mark := func(key string, set bool) {
		if sources != nil {
			if set {
				sources[key] = layer
			} else if sources[key] == "" {
				sources[key] = "default"
			}
		}
	}
	if s.Strategy != nil {
		c.Strategy = *s.Strategy
	}
	mark("strategy", s.Strategy != nil)
	if s.CookieVerifyAfterAcquire != nil {
		c.CookieVerifyAfterAcquire = *s.CookieVerifyAfterAcquire
	}
	mark("cookie-verify-after-acquire", s.CookieVerifyAfterAcquire != nil)
	if s.CookieBackupCount != nil {
		c.CookieBackupCount = *s.CookieBackupCount
	}
	mark("cookie-backup-count", s.CookieBackupCount != nil)
	if s.CookieMaxAgeSeconds != nil {
		c.CookieMaxAgeSeconds = *s.CookieMaxAgeSeconds
	}
	mark("cookie-max-age-seconds", s.CookieMaxAgeSeconds != nil)
	if s.CookieRefreshBeforeSeconds != nil {
		c.CookieRefreshBeforeSeconds = *s.CookieRefreshBeforeSeconds
	}
	mark("cookie-refresh-before-seconds", s.CookieRefreshBeforeSeconds != nil)
	if s.TTLSeconds != nil {
		c.TTLSeconds = cloneStateValue(s.TTLSeconds)
	}
	mark("ttl-seconds", s.TTLSeconds != nil)
	if s.RefreshBeforeSeconds != nil {
		c.RefreshBeforeSeconds = cloneStateValue(s.RefreshBeforeSeconds)
	}
	mark("refresh-before-seconds", s.RefreshBeforeSeconds != nil)
	if s.MissingReturnedState != nil {
		c.MissingReturnedState = *s.MissingReturnedState
	}
	mark("missing-returned-state", s.MissingReturnedState != nil)
}

func (c CodexStateOverrideConfig) CookieOnly() bool { return c.Strategy == "cookie-only" }
func (c CodexStateOverrideConfig) StateTTL() time.Duration {
	if c.TTLSeconds != nil {
		return time.Duration(*c.TTLSeconds) * time.Second
	}
	n := c.TTLMinutes
	if n == 0 {
		n = 60
	}
	return time.Duration(n) * time.Minute
}
func (c CodexStateOverrideConfig) StateRefreshBefore() time.Duration {
	if c.RefreshBeforeSeconds != nil {
		return time.Duration(*c.RefreshBeforeSeconds) * time.Second
	}
	n := c.RefreshBeforeMinutes
	if n == 0 {
		n = 5
	}
	return time.Duration(n) * time.Minute
}

func (c CodexStateOverrideConfig) validateStateStrategy() error {
	if c.Strategy != "state" && c.Strategy != "cookie-only" {
		return fmt.Errorf("invalid strategy")
	}
	if c.MissingReturnedState != "ignore" && c.MissingReturnedState != "reject" {
		return fmt.Errorf("invalid missing-returned-state policy")
	}
	if c.CookieBackupCount < 0 || c.CookieBackupCount > 10 {
		return fmt.Errorf("cookie-backup-count must be between 0 and 10")
	}
	if c.CookieMaxAgeSeconds < 0 || c.CookieMaxAgeSeconds > 86400 || c.CookieRefreshBeforeSeconds < 0 || c.CookieRefreshBeforeSeconds > 86400 || c.CookieMaxAgeSeconds > 0 && c.CookieRefreshBeforeSeconds >= c.CookieMaxAgeSeconds {
		return fmt.Errorf("invalid Cookie lifetime or refresh interval")
	}
	if c.TTLSeconds != nil && (*c.TTLSeconds < 1 || *c.TTLSeconds > 86400) || c.RefreshBeforeSeconds != nil && (*c.RefreshBeforeSeconds < 0 || *c.RefreshBeforeSeconds > 86400) {
		return fmt.Errorf("invalid State seconds")
	}
	if !c.CookieOnly() && c.StateRefreshBefore() >= c.StateTTL() {
		return fmt.Errorf("State refresh lead time must be less than lifetime")
	}
	return nil
}

// HasStateStrategy allows strip with exclusively Cookie-only rules.
func (c CodexStateOverrideConfig) HasStateStrategy() bool {
	if c.Rules == nil {
		if !c.CookieOnly() {
			return true
		}
		for _, o := range c.ModelOverrides {
			if o.Strategy != nil && *o.Strategy == "state" {
				return true
			}
		}
		return false
	}
	for _, r := range *c.Rules {
		if r.Action == "skip" || r.Enabled != nil && !*r.Enabled {
			continue
		}
		p := c
		applyStateStrategySettings(&p, r.Settings.CodexStateStrategySettings, nil, "")
		if !p.CookieOnly() {
			return true
		}
		for _, legacy := range c.ModelOverrides {
			inherited := c.ForModel(legacy.Model)
			applyStateStrategySettings(&inherited, r.Settings.CodexStateStrategySettings, nil, "")
			// Explicit per-rule model settings still have the last word.
			for _, item := range r.ModelOverrides {
				if item.Enabled != nil && !*item.Enabled {
					continue
				}
				for _, model := range item.Models {
					if model == legacy.Model {
						applyStateStrategySettings(&inherited, item.Settings.CodexStateStrategySettings, nil, "")
					}
				}
			}
			if !inherited.CookieOnly() {
				return true
			}
		}
		for _, o := range r.ModelOverrides {
			if o.Enabled != nil && !*o.Enabled {
				continue
			}
			q := p
			applyStateStrategySettings(&q, o.Settings.CodexStateStrategySettings, nil, "")
			if !q.CookieOnly() {
				return true
			}
		}
	}
	return false
}

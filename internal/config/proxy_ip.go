package config

import (
	"fmt"
	"strings"
)

// ProxyIPConfig owns only IP metadata settings. Proxy health owns exit observations.
type ProxyIPConfig struct {
	LookupConcurrency      *int   `yaml:"lookup-concurrency,omitempty" json:"lookup-concurrency,omitempty"`
	LookupURL              string `yaml:"lookup-url,omitempty" json:"lookup-url"`
	LookupTimeoutSeconds   *int   `yaml:"lookup-timeout-seconds,omitempty" json:"lookup-timeout-seconds,omitempty"`
	IPCacheTTLSeconds      *int   `yaml:"ip-cache-ttl-seconds,omitempty" json:"ip-cache-ttl-seconds,omitempty"`
	IPCacheSize            *int   `yaml:"ip-cache-size,omitempty" json:"ip-cache-size,omitempty"`
	RefreshMode            string `yaml:"refresh-mode,omitempty" json:"refresh-mode"`
	ColdCacheMode          string `yaml:"cold-cache-mode,omitempty" json:"cold-cache-mode"`
	RefreshBatchSize       *int   `yaml:"refresh-batch-size,omitempty" json:"refresh-batch-size,omitempty"`
	RefreshIntervalSeconds *int   `yaml:"refresh-interval-seconds,omitempty" json:"refresh-interval-seconds,omitempty"`
}
type ResolvedProxyIP struct {
	LookupConcurrency      int    `json:"lookup-concurrency"`
	LookupURL              string `json:"lookup-url"`
	LookupTimeoutSeconds   int    `json:"lookup-timeout-seconds"`
	IPCacheTTLSeconds      int    `json:"ip-cache-ttl-seconds"`
	IPCacheSize            int    `json:"ip-cache-size"`
	RefreshMode            string `json:"refresh-mode"`
	ColdCacheMode          string `json:"cold-cache-mode"`
	RefreshBatchSize       int    `json:"refresh-batch-size"`
	RefreshIntervalSeconds int    `json:"refresh-interval-seconds"`
}

func (c ProxyIPConfig) Resolved() ResolvedProxyIP {
	r := ResolvedProxyIP{LookupURL: strings.TrimSpace(c.LookupURL), LookupTimeoutSeconds: 8, LookupConcurrency: 8, IPCacheTTLSeconds: 86400, IPCacheSize: 100000, RefreshMode: c.RefreshMode, ColdCacheMode: c.ColdCacheMode, RefreshBatchSize: 64, RefreshIntervalSeconds: 5}
	if r.LookupURL == "" {
		r.LookupURL = "https://ipwho.is/{ip}"
	}
	if r.RefreshMode == "" {
		r.RefreshMode = "lazy"
	}
	if r.ColdCacheMode == "" {
		r.ColdCacheMode = "fallback"
	}
	if c.LookupConcurrency != nil {
		r.LookupConcurrency = *c.LookupConcurrency
	}
	if c.LookupTimeoutSeconds != nil {
		r.LookupTimeoutSeconds = *c.LookupTimeoutSeconds
	}
	if c.IPCacheTTLSeconds != nil {
		r.IPCacheTTLSeconds = *c.IPCacheTTLSeconds
	}
	if c.IPCacheSize != nil {
		r.IPCacheSize = *c.IPCacheSize
	}
	if c.RefreshBatchSize != nil {
		r.RefreshBatchSize = *c.RefreshBatchSize
	}
	if c.RefreshIntervalSeconds != nil {
		r.RefreshIntervalSeconds = *c.RefreshIntervalSeconds
	}
	return r
}
func (c ProxyIPConfig) Validate() error {
	r := c.Resolved()
	if r.LookupConcurrency < 1 || r.LookupConcurrency > 256 {
		return fmt.Errorf("proxy-ip.lookup-concurrency must be between 1 and 256")
	}
	if r.RefreshMode != "lazy" && r.RefreshMode != "scheduled" {
		return fmt.Errorf("proxy-ip.refresh-mode must be lazy or scheduled")
	}
	if r.ColdCacheMode != "fallback" && r.ColdCacheMode != "wait" {
		return fmt.Errorf("proxy-ip.cold-cache-mode must be fallback or wait")
	}
	if r.RefreshBatchSize < 1 || r.RefreshBatchSize > 4096 || r.RefreshIntervalSeconds < 1 || r.RefreshIntervalSeconds > 86400 {
		return fmt.Errorf("invalid proxy-ip refresh batch or interval")
	}
	v := ChatGPTWebBrowserRegionConfig{LookupURL: c.LookupURL, LookupTimeoutSeconds: c.LookupTimeoutSeconds, CacheTTLSeconds: c.IPCacheTTLSeconds, IPCacheSize: c.IPCacheSize}
	if err := v.Validate(); err != nil {
		return fmt.Errorf("%s", strings.ReplaceAll(strings.ReplaceAll(err.Error(), "chatgpt-web.browser-region", "proxy-ip"), "cache-ttl-seconds", "ip-cache-ttl-seconds"))
	}
	return nil
}

// EffectiveProxyIP migrates only metadata parameters, never proxy health policy.
func (c *Config) EffectiveProxyIP() ProxyIPConfig {
	if c == nil {
		return ProxyIPConfig{}
	}
	if c.ProxyIP != nil {
		return *c.ProxyIP
	}
	v := c.ChatGPTWeb.BrowserRegion
	return ProxyIPConfig{LookupURL: v.LookupURL, LookupTimeoutSeconds: v.LookupTimeoutSeconds, IPCacheSize: v.IPCacheSize}
}
func (c *Config) ValidateProxyIP() error {
	if c != nil {
		if err := c.ProxyRegion.Validate(); err != nil {
			return err
		}
		for _, k := range c.CodexKey {
			if err := k.ProxyRegion.Validate(); err != nil {
				return err
			}
		}
	}
	return c.EffectiveProxyIP().Validate()
}

package config

import (
	"fmt"
	"math"
	"net/url"
	"strings"
	"time"

	"golang.org/x/text/language"
)

// ChatGPTWebBrowserRegionConfig controls only ChatGPT Web environment overlays.
type ChatGPTWebBrowserRegionConfig struct {
	AutoTimezone         bool   `yaml:"auto-timezone" json:"auto-timezone"`
	AutoLanguage         bool   `yaml:"auto-language" json:"auto-language"`
	Language             string `yaml:"language,omitempty" json:"language"`
	LookupURL            string `yaml:"lookup-url,omitempty" json:"lookup-url"`
	LookupTimeoutSeconds *int   `yaml:"lookup-timeout-seconds,omitempty" json:"lookup-timeout-seconds,omitempty"`
	CacheTTLSeconds      *int   `yaml:"cache-ttl-seconds,omitempty" json:"cache-ttl-seconds,omitempty"`
	RouteCacheSize       *int   `yaml:"route-cache-size,omitempty" json:"route-cache-size,omitempty"`
	IPCacheSize          *int   `yaml:"ip-cache-size,omitempty" json:"ip-cache-size,omitempty"`
}

type ResolvedChatGPTWebBrowserRegion struct {
	AutoTimezone         bool   `json:"auto-timezone"`
	AutoLanguage         bool   `json:"auto-language"`
	Language             string `json:"language"`
	LookupURL            string `json:"lookup-url"`
	LookupTimeoutSeconds int    `json:"lookup-timeout-seconds"`
	CacheTTLSeconds      int    `json:"cache-ttl-seconds"`
	RouteCacheSize       int    `json:"route-cache-size"`
	IPCacheSize          int    `json:"ip-cache-size"`
}

func (cfg ChatGPTWebBrowserRegionConfig) Resolved() ResolvedChatGPTWebBrowserRegion {
	r := ResolvedChatGPTWebBrowserRegion{AutoTimezone: cfg.AutoTimezone, AutoLanguage: cfg.AutoLanguage,
		Language: strings.TrimSpace(cfg.Language), LookupURL: strings.TrimSpace(cfg.LookupURL),
		LookupTimeoutSeconds: 8, CacheTTLSeconds: 21600, RouteCacheSize: 100000, IPCacheSize: 100000}
	if r.LookupURL == "" {
		r.LookupURL = "https://ipwho.is/{ip}"
	}
	if r.Language != "" {
		if tag, err := language.Parse(r.Language); err == nil {
			r.Language = tag.String()
		}
	}
	if cfg.LookupTimeoutSeconds != nil {
		r.LookupTimeoutSeconds = *cfg.LookupTimeoutSeconds
	}
	if cfg.CacheTTLSeconds != nil {
		r.CacheTTLSeconds = *cfg.CacheTTLSeconds
	}
	if cfg.RouteCacheSize != nil {
		r.RouteCacheSize = *cfg.RouteCacheSize
	}
	if cfg.IPCacheSize != nil {
		r.IPCacheSize = *cfg.IPCacheSize
	}
	return r
}

func (cfg ChatGPTWebBrowserRegionConfig) Validate() error {
	r := cfg.Resolved()
	if len(r.Language) > 128 {
		return fmt.Errorf("chatgpt-web.browser-region.language is too long")
	}
	if r.Language != "" {
		tag, err := language.Parse(r.Language)
		if err != nil || tag == language.Und {
			return fmt.Errorf("chatgpt-web.browser-region.language must be a language tag")
		}
	}
	raw := strings.ReplaceAll(r.LookupURL, "{ip}", "8.8.8.8")
	u, err := url.Parse(raw)
	if err != nil || len(raw) > 4096 || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" || strings.ContainsAny(raw, "{}\r\n\t") {
		return fmt.Errorf("chatgpt-web.browser-region.lookup-url must be an HTTPS URL with optional {ip}")
	}
	// Placeholder expansion must never change the destination authority.
	authority := strings.TrimPrefix(r.LookupURL, "https://")
	if end := strings.IndexAny(authority, "/?#"); end >= 0 {
		authority = authority[:end]
	}
	if strings.Contains(authority, "{ip}") {
		return fmt.Errorf("chatgpt-web.browser-region.lookup-url cannot place {ip} in the authority")
	}
	for name, value := range map[string]int{"lookup-timeout-seconds": r.LookupTimeoutSeconds, "cache-ttl-seconds": r.CacheTTLSeconds, "route-cache-size": r.RouteCacheSize, "ip-cache-size": r.IPCacheSize} {
		if value < 1 {
			return fmt.Errorf("chatgpt-web.browser-region.%s must be a positive integer", name)
		}
	}
	// Leave room for the 20 percent refresh jitter without duration overflow.
	maxSeconds := int64(math.MaxInt64 / int64(time.Second) / 2)
	if int64(r.LookupTimeoutSeconds) > maxSeconds || int64(r.CacheTTLSeconds) > maxSeconds {
		return fmt.Errorf("chatgpt-web.browser-region duration is not representable")
	}
	return nil
}

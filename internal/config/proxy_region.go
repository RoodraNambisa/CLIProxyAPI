package config

import (
	"encoding/json"
	"fmt"
	"strings"

	"golang.org/x/text/language"
)

const ProxyRegionMetadataKey = "proxy_region"

// ProxyRegionConfig describes a route, independently of consumer policy.
type ProxyRegionConfig struct {
	Timezone string `yaml:"timezone,omitempty" json:"timezone,omitempty"`
	Language string `yaml:"language,omitempty" json:"language,omitempty"`
}

func (r ProxyRegionConfig) Validate() error {
	if r.Timezone != "" && !ValidIANATimezone(r.Timezone) {
		return fmt.Errorf("invalid proxy region timezone")
	}
	if r.Language != "" {
		if _, err := language.Parse(r.Language); err != nil {
			return fmt.Errorf("invalid proxy region language")
		}
	}
	return nil
}
func (r ProxyRegionConfig) Overlay(other ProxyRegionConfig) ProxyRegionConfig {
	if strings.TrimSpace(other.Timezone) != "" {
		r.Timezone = other.Timezone
	}
	if strings.TrimSpace(other.Language) != "" {
		r.Language = other.Language
	}
	return r
}
func ReadProxyRegion(raw any) (ProxyRegionConfig, error) {
	var r ProxyRegionConfig
	if raw == nil {
		return r, nil
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return r, err
	}
	if err = json.Unmarshal(b, &r); err != nil {
		return r, err
	}
	return r, r.Validate()
}

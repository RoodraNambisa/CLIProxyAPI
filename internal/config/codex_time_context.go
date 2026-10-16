package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	_ "time/tzdata"
)

const CodexTimeContextMetadataKey = "codex_time_context"
const WebRegionMetadataKey = "chatgpt_web_browser_region"

type CodexTimeContextConfig struct {
	Enabled          bool   `yaml:"enabled" json:"enabled"`
	Mode             string `yaml:"mode,omitempty" json:"mode"`
	Timezone         string `yaml:"timezone,omitempty" json:"timezone"`
	LookupFailure    string `yaml:"lookup-failure,omitempty" json:"lookup-failure"`
	FallbackTimezone string `yaml:"fallback-timezone,omitempty" json:"fallback-timezone"`
}

func (c CodexTimeContextConfig) Resolved() CodexTimeContextConfig {
	c.Mode = strings.TrimSpace(c.Mode)
	if c.Mode == "" {
		c.Mode = "proxy"
	}
	c.Timezone = strings.TrimSpace(c.Timezone)
	if c.LookupFailure == "" {
		c.LookupFailure = "preserve"
	}
	return c
}
func ValidIANATimezone(value string) bool {
	if value == "" || value == "Local" || len(value) > 128 {
		return false
	}
	_, err := time.LoadLocation(value)
	return err == nil
}
func (c CodexTimeContextConfig) Validate() error {
	c = c.Resolved()
	if c.Mode != "proxy" && c.Mode != "manual" && c.Mode != "inherit" && c.Mode != "off" {
		return fmt.Errorf("invalid time-context mode")
	}
	if c.Enabled && c.Mode == "manual" && !ValidIANATimezone(c.Timezone) {
		return fmt.Errorf("manual time-context requires a valid IANA timezone")
	}
	if c.Timezone != "" && !ValidIANATimezone(c.Timezone) {
		return fmt.Errorf("invalid time-context timezone")
	}
	if c.LookupFailure != "preserve" && c.LookupFailure != "fallback" {
		return fmt.Errorf("invalid time-context lookup-failure")
	}
	if c.LookupFailure == "fallback" && !ValidIANATimezone(c.FallbackTimezone) {
		return fmt.Errorf("time-context fallback requires a valid IANA timezone")
	}
	return nil
}
func (c *Config) ValidateTimeContext() error {
	if c == nil {
		return nil
	}
	if err := c.Codex.TimeContext.Validate(); err != nil {
		return err
	}
	for _, k := range c.CodexKey {
		if k.TimeContext != nil {
			if _, err := EffectiveCodexTimeContext(CodexTimeContextConfig{}, *k.TimeContext); err != nil {
				return err
			}
		}
	}
	return nil
}
func EffectiveCodexTimeContext(base CodexTimeContextConfig, raw any) (CodexTimeContextConfig, error) {
	if raw == nil {
		return base.Resolved(), base.Validate()
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return base, err
	}
	var override CodexTimeContextConfig
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err = d.Decode(&override); err != nil {
		return base, err
	}
	if override.Mode == "inherit" {
		return base.Resolved(), base.Validate()
	}
	override = override.Resolved()
	override.Enabled = override.Mode != "off"
	return override, override.Validate()
}

// ApplyWebRegionOverride changes consumer policy only, never shared lookup settings.
func ApplyWebRegionOverride(base ChatGPTWebConfig, raw any) (ChatGPTWebConfig, error) {
	if raw == nil {
		return base, nil
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return base, err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(b, &fields); err != nil {
		return base, err
	}
	for key, value := range fields {
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) && key != "timezone-offset-minutes" {
			return base, fmt.Errorf("%s cannot be null", key)
		}
		switch key {
		case "auto-timezone":
			err = json.Unmarshal(value, &base.BrowserRegion.AutoTimezone)
		case "auto-language":
			err = json.Unmarshal(value, &base.BrowserRegion.AutoLanguage)
		case "timezone":
			err = json.Unmarshal(value, &base.Timezone)
		case "language":
			err = json.Unmarshal(value, &base.BrowserRegion.Language)
		case "timezone-offset-minutes":
			if string(value) == "null" {
				base.TimezoneOffsetMinutes = nil
			} else {
				var v int
				err = json.Unmarshal(value, &v)
				base.TimezoneOffsetMinutes = &v
			}
		default:
			return base, fmt.Errorf("unsupported credential browser region field %s", key)
		}
		if err != nil {
			return base, err
		}
	}
	return base, base.Validate()
}

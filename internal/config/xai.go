package config

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"

	"golang.org/x/net/http/httpguts"
)

const DefaultXAISessionIdentityPoolSize = 4

type XAIHeaderDefaults struct {
	UserAgent        string `yaml:"user-agent" json:"user-agent"`
	ClientVersion    string `yaml:"client-version" json:"client-version"`
	ClientIdentifier string `yaml:"client-identifier" json:"client-identifier"`
}

type XAIConfig struct {
	HeaderDefaults             XAIHeaderDefaults `yaml:"header-defaults" json:"header-defaults"`
	Headers                    map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
	PassthroughClientIdentity  bool              `yaml:"passthrough-client-identity" json:"passthrough-client-identity"`
	SpoofSessionIdentity       bool              `yaml:"spoof-session-identity" json:"spoof-session-identity"`
	SessionIdentityConvergence bool              `yaml:"session-identity-convergence" json:"session-identity-convergence"`
	SessionIdentityPoolSize    int               `yaml:"session-identity-pool-size,omitempty" json:"session-identity-pool-size,omitempty"`
	IdentityConfuse            bool              `yaml:"identity-confuse" json:"identity-confuse"`
	RequestDefaults            map[string]any    `yaml:"request-defaults,omitempty" json:"request-defaults,omitempty"`
	InjectWebSearch            bool              `yaml:"inject-web-search" json:"inject-web-search"`
	InjectXSearch              bool              `yaml:"inject-x-search" json:"inject-x-search"`
}

func (c XAIConfig) IdentityEnabled() bool {
	return c.PassthroughClientIdentity || c.SpoofSessionIdentity || c.SessionIdentityConvergence || c.IdentityConfuse
}

func (c XAIConfig) NeedsIdentitySeed() bool {
	return c.SpoofSessionIdentity || c.SessionIdentityConvergence || c.IdentityConfuse
}

func (c XAIConfig) PoolSize() int {
	if c.SessionIdentityPoolSize == 0 {
		return DefaultXAISessionIdentityPoolSize
	}
	return c.SessionIdentityPoolSize
}

// Clone owns nested user configuration before a request crosses a retry boundary.
func (c XAIConfig) Clone() XAIConfig {
	data, err := json.Marshal(c)
	if err != nil {
		return c
	}
	var out XAIConfig
	if json.Unmarshal(data, &out) != nil {
		return c
	}
	return out
}

func (c *XAIConfig) Validate() error {
	if c.SessionIdentityPoolSize < 1 || c.SessionIdentityPoolSize > 64 {
		return fmt.Errorf("xai.session-identity-pool-size must be between 1 and 64")
	}
	for _, value := range []string{c.HeaderDefaults.UserAgent, c.HeaderDefaults.ClientVersion, c.HeaderDefaults.ClientIdentifier} {
		if len(value) > 1024 || !httpguts.ValidHeaderFieldValue(value) {
			return fmt.Errorf("xai.header-defaults contains an invalid HTTP header value")
		}
	}
	c.HeaderDefaults.UserAgent = strings.TrimSpace(c.HeaderDefaults.UserAgent)
	c.HeaderDefaults.ClientVersion = strings.TrimSpace(c.HeaderDefaults.ClientVersion)
	c.HeaderDefaults.ClientIdentifier = strings.TrimSpace(c.HeaderDefaults.ClientIdentifier)
	if len(c.Headers) > 64 {
		return fmt.Errorf("xai.headers supports at most 64 headers")
	}
	headers := make(map[string]string, len(c.Headers))
	for key, value := range c.Headers {
		if !httpguts.ValidHeaderFieldName(key) || !httpguts.ValidHeaderFieldValue(value) || len(value) > 8192 {
			return fmt.Errorf("xai.headers contains an invalid HTTP header")
		}
		canonical := http.CanonicalHeaderKey(key)
		if _, exists := headers[canonical]; exists {
			return fmt.Errorf("xai.headers contains duplicate header %q", canonical)
		}
		headers[canonical] = value
	}
	if c.Headers != nil {
		c.Headers = headers
	}
	raw, err := json.Marshal(c.RequestDefaults)
	if err != nil || len(raw) > 16*1024 {
		return fmt.Errorf("xai.request-defaults must be valid JSON within 16 KiB")
	}
	for key, value := range c.RequestDefaults {
		switch key {
		case "max_output_tokens", "temperature", "top_p":
			var n float64
			data, _ := json.Marshal(value)
			if json.Unmarshal(data, &n) != nil || math.IsNaN(n) || math.IsInf(n, 0) || value == nil {
				return fmt.Errorf("xai.request-defaults.%s must be a number", key)
			}
			if (key == "max_output_tokens" && (n < 1 || n != math.Trunc(n) || n > 2147483647)) || (key == "temperature" && (n < 0 || n > 2)) || (key == "top_p" && (n < 0 || n > 1)) {
				return fmt.Errorf("xai.request-defaults.%s is out of range", key)
			}
		case "parallel_tool_calls", "stream_tool_calls":
			if _, ok := value.(bool); !ok {
				return fmt.Errorf("xai.request-defaults.%s must be a boolean", key)
			}
		case "reasoning":
			data, _ := json.Marshal(value)
			var fields map[string]string
			if json.Unmarshal(data, &fields) != nil || len(fields) != 1 || fields["effort"] == "" {
				return fmt.Errorf("xai.request-defaults.reasoning must contain only effort")
			}
		case "tool_choice":
			data, _ := json.Marshal(value)
			var choice string
			if json.Unmarshal(data, &choice) == nil {
				if choice != "auto" && choice != "none" && choice != "required" {
					return fmt.Errorf("xai.request-defaults.tool_choice must be auto, none, required or an object")
				}
			} else {
				var object map[string]any
				if json.Unmarshal(data, &object) != nil || len(object) == 0 {
					return fmt.Errorf("xai.request-defaults.tool_choice must be auto, none, required or an object")
				}
			}
		default:
			return fmt.Errorf("xai.request-defaults.%s is not a supported default; use payload rules for advanced parameters", key)
		}
	}
	return nil
}

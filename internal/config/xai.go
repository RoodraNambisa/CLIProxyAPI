package config

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"

	"golang.org/x/net/http/httpguts"
)

const DefaultXAISessionIdentityPoolSize = 4

const DefaultXAIBaseURLMode = "cli"

// XAIBaseURLForMode resolves the built-in upstream choices shared by management
// and execution. An empty mode keeps the historical OAuth default.
func XAIBaseURLForMode(mode string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "cli":
		return "https://cli-chat-proxy.grok.com/v1", true
	case "api":
		return "https://api.x.ai/v1", true
	case "us-east-1", "us-west-2", "eu-west-1":
		return "https://" + strings.ToLower(strings.TrimSpace(mode)) + ".api.x.ai/v1", true
	default:
		return "", false
	}
}

// NormalizeXAIBaseURL validates an explicitly edited credential endpoint. Empty
// values remove the override; credentials and query parameters belong elsewhere.
func NormalizeXAIBaseURL(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", nil
	}
	u, err := url.Parse(value)
	if u != nil {
		u.Scheme = strings.ToLower(u.Scheme)
	}
	if err != nil || u.Hostname() == "" || (u.Scheme != "https" && u.Scheme != "http") ||
		u.User != nil || strings.ContainsAny(value, "?#\\\r\n\t") {
		return "", fmt.Errorf("base_url must be an HTTP(S) base URL without user information, query or fragment")
	}
	u.Host = strings.ToLower(u.Host)
	if (u.Scheme == "https" && u.Port() == "443") || (u.Scheme == "http" && u.Port() == "80") {
		u.Host = strings.TrimSuffix(u.Host, ":"+u.Port())
	}
	return strings.TrimRight(u.String(), "/"), nil
}

type XAIHeaderDefaults struct {
	UserAgent        string `yaml:"user-agent" json:"user-agent"`
	ClientVersion    string `yaml:"client-version" json:"client-version"`
	ClientIdentifier string `yaml:"client-identifier" json:"client-identifier"`
}

type XAIConfig struct {
	ImageGenerationToolPolicy  string            `yaml:"image-generation-tool-policy,omitempty" json:"image-generation-tool-policy,omitempty"`
	DynamicHeaders             bool              `yaml:"dynamic-headers,omitempty" json:"dynamic-headers,omitempty"`
	ChatCompletionsMode        string            `yaml:"chat-completions-mode,omitempty" json:"chat-completions-mode,omitempty"`
	DefaultBaseURLMode         string            `yaml:"default-base-url-mode,omitempty" json:"default-base-url-mode,omitempty"`
	ModelCatalogSources        []string          `yaml:"model-catalog-sources,omitempty" json:"model-catalog-sources,omitempty"`
	ModelRoutes                []XAIModelRoute   `yaml:"model-routes,omitempty" json:"model-routes,omitempty"`
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
	if c != nil {
		switch c.ImageGenerationToolPolicy {
		case "", "remove", "error", "allow":
		default:
			return fmt.Errorf("xai.image-generation-tool-policy must be remove, error or allow")
		}
	}
	c.ChatCompletionsMode = strings.ToLower(strings.TrimSpace(c.ChatCompletionsMode))
	if c.ChatCompletionsMode != "" && c.ChatCompletionsMode != "responses" && c.ChatCompletionsMode != "direct" {
		return fmt.Errorf("xai.chat-completions-mode must be responses or direct")
	}
	var errRouting error
	c.ModelCatalogSources, errRouting = NormalizeXAICatalogSources(c.ModelCatalogSources)
	if errRouting != nil {
		return fmt.Errorf("xai.model-catalog-sources: %w", errRouting)
	}
	c.ModelRoutes, errRouting = NormalizeXAIModelRoutes(c.ModelRoutes)
	if errRouting != nil {
		return fmt.Errorf("xai.model-routes: %w", errRouting)
	}
	c.DefaultBaseURLMode = strings.ToLower(strings.TrimSpace(c.DefaultBaseURLMode))
	if c.DefaultBaseURLMode == "" {
		c.DefaultBaseURLMode = DefaultXAIBaseURLMode
	}
	if _, ok := XAIBaseURLForMode(c.DefaultBaseURLMode); !ok {
		return fmt.Errorf("xai.default-base-url-mode must be cli, api, us-east-1, us-west-2 or eu-west-1")
	}
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

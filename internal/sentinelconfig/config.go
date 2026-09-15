// Package sentinelconfig defines portable configuration for Sentinel computation.
package sentinelconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/netip"
	"net/url"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelcompat"
	"gopkg.in/yaml.v3"
)

type Node struct {
	Name   string `json:"name" yaml:"name"`
	URL    string `json:"url" yaml:"url"`
	APIKey string `json:"api-key" yaml:"api-key"`
}

type Remote struct {
	Scopes        *[]string `json:"scopes,omitempty" yaml:"scopes,omitempty"`
	Nodes         []Node    `json:"nodes,omitempty" yaml:"nodes,omitempty"`
	BudgetSeconds *int      `json:"budget-seconds,omitempty" yaml:"budget-seconds,omitempty"`
}

func decodeSettings(data []byte, target any, ignoredFields ...string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil || fields == nil {
		return fmt.Errorf("Sentinel settings must be an object")
	}
	filtered := false
	for _, name := range ignoredFields {
		if _, exists := fields[name]; exists {
			delete(fields, name)
			filtered = true
		}
	}
	if filtered {
		var err error
		data, err = json.Marshal(fields)
		if err != nil {
			return err
		}
	}
	for name, value := range fields {
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("Sentinel setting %s must not be null", name)
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return fmt.Errorf("unexpected trailing Sentinel settings")
	}
	return nil
}
func settingsYAML(node *yaml.Node, target any) error {
	if node == nil || (node.Kind != yaml.MappingNode && node.Kind != yaml.AliasNode) {
		return fmt.Errorf("Sentinel settings must be an object")
	}
	var fields map[string]any
	if err := node.Decode(&fields); err != nil {
		return err
	}
	data, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}
func (cfg *Remote) UnmarshalJSON(data []byte) error {
	type plain Remote
	var next plain
	seed, err := json.Marshal(plain(*cfg))
	if err != nil {
		return err
	}
	if err = json.Unmarshal(seed, &next); err != nil {
		return err
	}
	if err := decodeSettings(data, &next); err != nil {
		return err
	}
	*cfg = Remote(next)
	return nil
}
func (cfg *Remote) UnmarshalYAML(node *yaml.Node) error { return settingsYAML(node, cfg) }

func (cfg Remote) ScopeList() []string {
	if cfg.Scopes == nil {
		return []string{"images"}
	}
	return append([]string{}, (*cfg.Scopes)...)
}

func (cfg Remote) Budget() int { return value(cfg.BudgetSeconds, 30) }

func (cfg Remote) Enabled(mode, scope string) bool {
	if Mode(mode) != "remote" {
		return false
	}
	for _, candidate := range cfg.ScopeList() {
		if candidate == scope {
			return true
		}
	}
	return false
}

func Mode(mode string) string {
	if mode == "" {
		return "local"
	}
	return mode
}

func (cfg Remote) Validate(mode string) error {
	if Mode(mode) != "local" && Mode(mode) != "remote" {
		return fmt.Errorf("sentinel mode must be local or remote")
	}
	if cfg.Budget() < 1 || cfg.Budget() > 3600 {
		return fmt.Errorf("sentinel remote budget-seconds must be between 1 and 3600")
	}
	seen := map[string]bool{}
	for _, scope := range cfg.ScopeList() {
		if scope != "images" && scope != "chat" && scope != "login" {
			return fmt.Errorf("unsupported sentinel remote scope")
		}
		if seen[scope] {
			return fmt.Errorf("duplicate sentinel remote scope")
		}
		seen[scope] = true
	}
	seen = map[string]bool{}
	for _, node := range cfg.Nodes {
		if strings.TrimSpace(node.Name) == "" || len(node.Name) > 128 || seen[node.Name] {
			return fmt.Errorf("sentinel node names must be nonempty and unique")
		}
		seen[node.Name] = true
		if len(strings.TrimSpace(node.APIKey)) == 0 || len(node.APIKey) > 4096 || strings.ContainsAny(node.APIKey, "\r\n") {
			return fmt.Errorf("invalid sentinel node api-key")
		}
		if _, err := ParseNodeURL(node.URL); err != nil {
			return err
		}
	}
	if Mode(mode) == "remote" && len(cfg.ScopeList()) > 0 && len(cfg.Nodes) == 0 {
		return fmt.Errorf("sentinel remote mode requires at least one node")
	}
	return nil
}

// ParseNodeURL limits cleartext credentials to explicit local/private addresses.
func ParseNodeURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || u.RawPath != "" {
		return nil, fmt.Errorf("invalid sentinel node URL")
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return nil, fmt.Errorf("sentinel node URL must use HTTP(S)")
	}
	if u.Scheme == "http" && !PrivateHost(u.Hostname()) {
		return nil, fmt.Errorf("public sentinel nodes require HTTPS; HTTP requires a private IP or localhost")
	}
	if u.Path != "" && u.Path != "/" {
		if _, err := NormalizeAccessPath(strings.TrimRight(u.Path, "/")); err != nil {
			return nil, fmt.Errorf("invalid sentinel node URL path")
		}
	}
	return u, nil
}

// NodeBaseURL accepts the complete solver endpoint. Bare origins use the default path.
func NodeBaseURL(raw string) (string, error) {
	u, err := ParseNodeURL(raw)
	if err != nil {
		return "", err
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = DefaultAccessPath
	}
	u.Path = strings.TrimRight(u.Path, "/")
	return u.String(), nil
}

func PrivateHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip, err := netip.ParseAddr(host)
	return err == nil && (ip.Unmap().IsLoopback() || ip.Unmap().IsPrivate())
}

type Server struct {
	Enabled             bool                  `json:"enabled" yaml:"enabled"`
	AccessPath          string                `json:"access-path,omitempty" yaml:"access-path,omitempty"`
	APIKeys             []string              `json:"api-keys,omitempty" yaml:"api-keys,omitempty"`
	SDKFallbackEnabled  bool                  `json:"sdk-fallback-enabled" yaml:"sdk-fallback-enabled"`
	GoVMCompatibility   sentinelcompat.Config `json:"go-vm-compatibility" yaml:"go-vm-compatibility,omitempty"`
	GoWorkers           *int                  `json:"go-workers,omitempty" yaml:"go-workers,omitempty"`
	QueueSize           *int                  `json:"queue-size,omitempty" yaml:"queue-size,omitempty"`
	MaxSessions         *int                  `json:"max-sessions,omitempty" yaml:"max-sessions,omitempty"`
	MemoryBudgetMiB     *int                  `json:"memory-budget-mib,omitempty" yaml:"memory-budget-mib,omitempty"`
	SDKWorkers          *int                  `json:"sdk-workers,omitempty" yaml:"sdk-workers,omitempty"`
	SDKQueueSize        *int                  `json:"sdk-queue-size,omitempty" yaml:"sdk-queue-size,omitempty"`
	SDKCacheVersions    *int                  `json:"sdk-cache-versions,omitempty" yaml:"sdk-cache-versions,omitempty"`
	SessionIdleSeconds  *int                  `json:"session-idle-seconds,omitempty" yaml:"session-idle-seconds,omitempty"`
	DrainTimeoutSeconds *int                  `json:"drain-timeout-seconds,omitempty" yaml:"drain-timeout-seconds,omitempty"`
}

func (cfg *Server) UnmarshalJSON(data []byte) error {
	type plain Server
	var next plain
	seed, err := json.Marshal(plain(*cfg))
	if err != nil {
		return err
	}
	if err = json.Unmarshal(seed, &next); err != nil {
		return err
	}
	// Retired listener settings are accepted but never stored or used at runtime.
	if err := decodeSettings(data, &next, "listen", "tls"); err != nil {
		return err
	}
	*cfg = Server(next)
	return nil
}
func (cfg *Server) UnmarshalYAML(node *yaml.Node) error { return settingsYAML(node, cfg) }

type Limits struct {
	GoWorkers           int `json:"go_workers"`
	QueueSize           int `json:"queue_size"`
	MaxSessions         int `json:"max_sessions"`
	MemoryBudgetMiB     int `json:"memory_budget_mib"`
	SDKWorkers          int `json:"sdk_workers"`
	SDKQueueSize        int `json:"sdk_queue_size"`
	SDKCacheVersions    int `json:"sdk_cache_versions"`
	SessionIdleSeconds  int `json:"session_idle_seconds"`
	DrainTimeoutSeconds int `json:"drain_timeout_seconds"`
}

func value(p *int, fallback int) int {
	if p == nil {
		return fallback
	}
	return *p
}

const DefaultAccessPath = "/v1/sentinel"

func NormalizeAccessPath(raw string) (string, error) {
	if raw == "" {
		return DefaultAccessPath, nil
	}
	if len(raw) > 256 || !strings.HasPrefix(raw, "/") || strings.HasSuffix(raw, "/") {
		return "", fmt.Errorf("sentinel-solver.access-path must be an absolute path without a trailing slash, up to 256 characters")
	}
	for _, segment := range strings.Split(raw[1:], "/") {
		if segment == "" || segment == "." || segment == ".." {
			return "", fmt.Errorf("invalid sentinel-solver.access-path segment")
		}
		for _, ch := range segment {
			if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_' || ch == '.') {
				return "", fmt.Errorf("sentinel-solver.access-path allows letters, digits, '/', '-', '_' and '.' only")
			}
		}
	}
	return raw, nil
}

func (cfg Server) Path() string {
	if cfg.AccessPath == "" {
		return DefaultAccessPath
	}
	return cfg.AccessPath
}
func (cfg Server) Limits() Limits {
	return Limits{value(cfg.GoWorkers, 0), value(cfg.QueueSize, 64), value(cfg.MaxSessions, 128), value(cfg.MemoryBudgetMiB, 512), value(cfg.SDKWorkers, 0), value(cfg.SDKQueueSize, 32), value(cfg.SDKCacheVersions, 3), value(cfg.SessionIdleSeconds, 120), value(cfg.DrainTimeoutSeconds, 120)}
}
func (cfg Server) Validate() error {
	if _, err := NormalizeAccessPath(cfg.AccessPath); err != nil {
		return err
	}
	// Keep the configurable mount from hiding proxy or management endpoints.
	if cfg.Path() != DefaultAccessPath {
		for _, reserved := range []string{"/v1", "/v1beta", "/v0", "/api", "/backend-api", "/debug", "/healthz", "/readyz", "/keep-alive", "/management.html"} {
			if cfg.Path() == reserved || strings.HasPrefix(cfg.Path(), reserved+"/") {
				return fmt.Errorf("sentinel-solver.access-path conflicts with a reserved API path")
			}
		}
	}
	if cfg.Enabled && len(cfg.APIKeys) == 0 {
		return fmt.Errorf("sentinel-solver requires api-keys")
	}
	seen := map[string]bool{}
	for _, key := range cfg.APIKeys {
		if strings.TrimSpace(key) == "" || len(key) > 4096 || strings.ContainsAny(key, "\r\n") || seen[key] {
			return fmt.Errorf("invalid or duplicate sentinel-solver api-key")
		}
		seen[key] = true
	}
	if _, err := sentinelcompat.Compile(cfg.GoVMCompatibility); err != nil {
		return fmt.Errorf("sentinel-solver.go-vm-compatibility: %w", err)
	}
	l := cfg.Limits()
	if l.GoWorkers < 0 || l.GoWorkers > 4096 || l.QueueSize < 0 || l.QueueSize > 65536 || l.MaxSessions < 1 || l.MaxSessions > 65536 || l.MemoryBudgetMiB < 256 || l.MemoryBudgetMiB > 1048576 {
		return fmt.Errorf("invalid sentinel-solver resource limits")
	}
	if l.SDKWorkers < 0 || l.SDKWorkers > 4096 || l.SDKQueueSize < 0 || l.SDKQueueSize > 1024 || l.SDKCacheVersions < 1 || l.SDKCacheVersions > 5 {
		return fmt.Errorf("invalid sentinel-solver SDK limits")
	}
	if l.SessionIdleSeconds < 60 || l.SessionIdleSeconds > 3600 || l.DrainTimeoutSeconds < 1 || l.DrainTimeoutSeconds > 3600 {
		return fmt.Errorf("invalid sentinel-solver lifetime limits")
	}
	return nil
}

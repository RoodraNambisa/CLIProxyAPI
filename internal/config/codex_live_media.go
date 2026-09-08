package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const DefaultCodexLiveMediaMaxSessions = 32
const MaxCodexLiveMediaSessions = math.MaxInt32

// CodexLiveMediaRelayConfig applies to newly created WebRTC media sessions.
type CodexLiveMediaRelayConfig struct {
	Enabled                 bool                 `yaml:"enabled" json:"enabled"`
	MaxSessions             int                  `yaml:"max-sessions" json:"max-sessions"`
	DisablePrivateRemoteIPs bool                 `yaml:"disable-private-remote-ips" json:"disable-private-remote-ips"`
	PublicIP                string               `yaml:"public-ip" json:"public-ip"`
	UDPPortMin              uint16               `yaml:"udp-port-min" json:"udp-port-min"`
	UDPPortMax              uint16               `yaml:"udp-port-max" json:"udp-port-max"`
	ICEServers              []CodexLiveICEServer `yaml:"ice-servers" json:"ice-servers"`
}

// ICE authentication is persisted in YAML but omitted from public config JSON.
type CodexLiveICEServer struct {
	URLs       []string `yaml:"urls" json:"urls"`
	Username   string   `yaml:"username" json:"-"`
	Credential string   `yaml:"credential" json:"-"`
}

func (c CodexLiveMediaRelayConfig) EffectiveMaxSessions() int {
	if c.MaxSessions > 0 {
		return c.MaxSessions
	}
	return DefaultCodexLiveMediaMaxSessions
}

func (c CodexLiveMediaRelayConfig) Validate() error {
	return c.validate(true)
}

func (c CodexLiveMediaRelayConfig) validate(requireICECredentials bool) error {
	const path = "codex.live-media-relay"
	if c.MaxSessions < 0 || c.MaxSessions > MaxCodexLiveMediaSessions {
		return fmt.Errorf("%s.max-sessions must be an integer between 0 and %d", path, MaxCodexLiveMediaSessions)
	}
	if ip := strings.TrimSpace(c.PublicIP); ip != "" && net.ParseIP(ip) == nil {
		return fmt.Errorf("%s.public-ip must be an IP address or empty", path)
	}
	if (c.UDPPortMin == 0) != (c.UDPPortMax == 0) || c.UDPPortMin > c.UDPPortMax {
		return fmt.Errorf("%s UDP ports must both be zero, or ordered values between 1 and 65535", path)
	}
	if c.UDPPortMin != 0 && uint64(c.UDPPortMax)-uint64(c.UDPPortMin)+1 < uint64(c.EffectiveMaxSessions())*2 {
		return fmt.Errorf("%s UDP range must provide at least two ports per session", path)
	}
	for index, server := range c.ICEServers {
		if len(server.URLs) == 0 {
			return fmt.Errorf("%s.ice-servers[%d].urls must not be empty", path, index)
		}
		for _, raw := range server.URLs {
			turn, valid := validCodexLiveICEURL(raw)
			if !valid {
				return fmt.Errorf("%s.ice-servers[%d] contains an invalid STUN or TURN URL", path, index)
			}
			if turn && (requireICECredentials || server.Username != "" || server.Credential != "") && (strings.TrimSpace(server.Username) == "" || server.Credential == "") {
				return fmt.Errorf("%s.ice-servers[%d] requires TURN username and credential", path, index)
			}
		}
	}
	return nil
}

// Validate the opaque RFC 7064/7065 authority without resolving hosts or making
// network requests. Never include the supplied address or secret in an error.
func validCodexLiveICEURL(raw string) (bool, bool) {
	u, errURL := url.Parse(strings.TrimSpace(raw))
	if errURL != nil || u.Fragment != "" || u.Opaque == "" {
		return false, false
	}
	turn := u.Scheme == "turn" || u.Scheme == "turns"
	if !turn && u.Scheme != "stun" && u.Scheme != "stuns" {
		return false, false
	}
	authority := u.Opaque
	host, port, errHost := net.SplitHostPort(authority)
	if errHost != nil {
		var addressError *net.AddrError
		if !errors.As(errHost, &addressError) || addressError.Err != "missing port in address" {
			return false, false
		}
		host, port, errHost = net.SplitHostPort(authority + ":3478")
	}
	if errHost != nil || host == "" || strings.ContainsAny(host, " \t\r\n/@?#\\%") {
		return false, false
	}
	if strings.Contains(host, ":") && net.ParseIP(host) == nil {
		return false, false
	}
	numericPort, errPort := strconv.ParseUint(port, 10, 16)
	if errPort != nil || numericPort == 0 {
		return false, false
	}
	query, errQuery := url.ParseQuery(u.RawQuery)
	if errQuery != nil {
		return false, false
	}
	if len(query) == 0 {
		return turn, true
	}
	transport := query["transport"]
	return turn, turn && len(query) == 1 && len(transport) == 1 && (transport[0] == "tcp" || transport[0] == "udp")
}

func (c *CodexLiveMediaRelayConfig) UnmarshalJSON(data []byte) error {
	type plain CodexLiveMediaRelayConfig
	var decoded plain
	if err := json.Unmarshal(data, &decoded); err != nil {
		return errors.New("codex.live-media-relay contains an invalid JSON field type")
	}
	result := CodexLiveMediaRelayConfig(decoded)
	// Management JSON snapshots intentionally omit both ICE credential fields.
	// Full configuration save, YAML load and runtime cloning still require them.
	if err := result.validate(false); err != nil {
		return err
	}
	*c = result
	return nil
}

func (s *CodexLiveICEServer) UnmarshalJSON(data []byte) error {
	var decoded struct {
		URLs       []string `json:"urls"`
		Username   string   `json:"username"`
		Credential string   `json:"credential"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return errors.New("codex.live-media-relay ICE server contains an invalid JSON field type")
	}
	*s = CodexLiveICEServer(decoded)
	return nil
}

func (c *CodexLiveMediaRelayConfig) UnmarshalYAML(node *yaml.Node) error {
	if err := validateCodexLiveMediaMapping(node); err != nil {
		return err
	}
	type plain CodexLiveMediaRelayConfig
	var decoded plain
	if err := node.Decode(&decoded); err != nil {
		return errors.New("codex.live-media-relay contains an invalid YAML value")
	}
	result := CodexLiveMediaRelayConfig(decoded)
	if err := result.Validate(); err != nil {
		return err
	}
	*c = result
	return nil
}

func validateCodexLiveMediaMapping(node *yaml.Node) error {
	if node == nil || node.Tag == "!!null" {
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return errors.New("codex.live-media-relay must be an object")
	}
	for _, field := range []struct{ name, tag string }{
		{"enabled", "!!bool"}, {"disable-private-remote-ips", "!!bool"},
		{"max-sessions", "!!int"}, {"udp-port-min", "!!int"}, {"udp-port-max", "!!int"}, {"public-ip", "!!str"},
	} {
		value, err := credentialYAMLField(node, field.name, make(map[*yaml.Node]bool))
		if err != nil {
			return err
		}
		if value != nil && value.Tag != "!!null" && (value.Kind != yaml.ScalarNode || value.Tag != field.tag) {
			return fmt.Errorf("codex.live-media-relay.%s has an invalid type", field.name)
		}
	}
	servers, err := credentialYAMLField(node, "ice-servers", make(map[*yaml.Node]bool))
	if err != nil {
		return err
	}
	if servers == nil || servers.Tag == "!!null" {
		return nil
	}
	if servers.Kind != yaml.SequenceNode {
		return errors.New("codex.live-media-relay.ice-servers must be an array")
	}
	for _, server := range servers.Content {
		if server.Kind == yaml.AliasNode {
			server = server.Alias
		}
		if server == nil || server.Kind != yaml.MappingNode {
			return errors.New("codex.live-media-relay ICE server must be an object")
		}
		for _, name := range []string{"username", "credential", "urls"} {
			value, err := credentialYAMLField(server, name, make(map[*yaml.Node]bool))
			if err != nil {
				return err
			}
			if value == nil || value.Tag == "!!null" {
				continue
			}
			if name == "urls" {
				if value.Kind != yaml.SequenceNode {
					return errors.New("codex.live-media-relay ICE URLs must be an array")
				}
				for _, item := range value.Content {
					if item.Kind == yaml.AliasNode {
						item = item.Alias
					}
					if item == nil || item.Kind != yaml.ScalarNode || item.Tag != "!!str" {
						return errors.New("codex.live-media-relay ICE URL must be a string")
					}
				}
			} else if value.Kind != yaml.ScalarNode || value.Tag != "!!str" {
				return fmt.Errorf("codex.live-media-relay ICE %s must be a string", name)
			}
		}
	}
	return nil
}

func validateCodexLiveMediaYAML(data []byte) error {
	var document yaml.Node
	if err := yaml.Unmarshal(data, &document); err != nil || len(document.Content) == 0 {
		return nil
	}
	codex, err := credentialYAMLField(document.Content[0], "codex", make(map[*yaml.Node]bool))
	if err != nil {
		return err
	}
	media, err := credentialYAMLField(codex, "live-media-relay", make(map[*yaml.Node]bool))
	if err != nil {
		return err
	}
	if media == nil || media.Tag == "!!null" {
		return nil
	}
	var decoded CodexLiveMediaRelayConfig
	return media.Decode(&decoded)
}

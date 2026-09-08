package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestCodexLiveMediaDefaultsAndRangeValidation(t *testing.T) {
	if c := (CodexLiveMediaRelayConfig{}); c.Enabled || c.EffectiveMaxSessions() != 32 || c.Validate() != nil {
		t.Fatal("media defaults changed")
	}
	for _, enabled := range []bool{false, true} {
		for _, test := range []struct {
			name   string
			mutate func(*CodexLiveMediaRelayConfig)
			valid  bool
		}{
			{"default", func(*CodexLiveMediaRelayConfig) {}, true},
			{"capacity upper", func(c *CodexLiveMediaRelayConfig) { c.MaxSessions = MaxCodexLiveMediaSessions }, true},
			{"capacity negative", func(c *CodexLiveMediaRelayConfig) { c.MaxSessions = -1 }, false},
			{"capacity overflow", func(c *CodexLiveMediaRelayConfig) {
				oversized := int64(MaxCodexLiveMediaSessions) + 1
				c.MaxSessions = int(oversized)
			}, false},
			{"ports default boundary", func(c *CodexLiveMediaRelayConfig) { c.UDPPortMin = 50000; c.UDPPortMax = 50063 }, true},
			{"ports short", func(c *CodexLiveMediaRelayConfig) { c.UDPPortMin = 50000; c.UDPPortMax = 50062 }, false},
			{"ports partial", func(c *CodexLiveMediaRelayConfig) { c.UDPPortMax = 65535 }, false},
			{"ports reversed", func(c *CodexLiveMediaRelayConfig) { c.UDPPortMin = 50001; c.UDPPortMax = 50000 }, false},
			{"multiplication overflow", func(c *CodexLiveMediaRelayConfig) {
				c.MaxSessions = MaxCodexLiveMediaSessions
				c.UDPPortMin = 1
				c.UDPPortMax = 65535
			}, false},
			{"IPv4", func(c *CodexLiveMediaRelayConfig) { c.PublicIP = " 203.0.113.7 " }, true},
			{"IPv6", func(c *CodexLiveMediaRelayConfig) { c.PublicIP = "2001:db8::1" }, true},
			{"invalid IP", func(c *CodexLiveMediaRelayConfig) { c.PublicIP = "private-fixture.invalid" }, false},
		} {
			t.Run(fmt.Sprintf("%s/%v", test.name, enabled), func(t *testing.T) {
				c := CodexLiveMediaRelayConfig{Enabled: enabled}
				test.mutate(&c)
				if err := c.Validate(); (err == nil) != test.valid {
					t.Fatalf("valid=%v err=%v", test.valid, err)
				}
			})
		}
	}
}

func TestCodexLiveMediaICEValidationDoesNotResolveOrExposeSecrets(t *testing.T) {
	for _, valid := range []bool{true, false} {
		urls := []string{"stun:fixture.invalid", "stuns:fixture.invalid:5349", "stun:[2001:db8::1]", "turn:fixture.invalid:3478?transport=udp", "turns:fixture.invalid?transport=tcp"}
		if !valid {
			urls = []string{"", "https://fixture.invalid", "stun:", "stun://fixture.invalid", "stun:fixture.invalid:0", "stun:fixture.invalid:65536", "turn:fixture.invalid:-1", "stun:fixture.invalid?transport=udp", "turn:fixture.invalid?transport=quic", "turn:fixture.invalid?transport=tcp&transport=udp", "stun:fixture.invalid#fragment", "stun:user:password@fixture.invalid", "stun:fixture.invalid/path", "stun:bad host"}
		}
		for index, raw := range urls {
			c := CodexLiveMediaRelayConfig{ICEServers: []CodexLiveICEServer{{URLs: []string{raw}, Username: "user-fixture", Credential: "secret-fixture"}}}
			err := c.Validate()
			if (err == nil) != valid {
				t.Fatalf("URL case %v/%d unexpected validation result", valid, index)
			}
			if err != nil && (strings.Contains(err.Error(), "secret-fixture") || strings.Contains(err.Error(), "fixture.invalid")) {
				t.Fatal("validation exposed ICE material")
			}
		}
	}
	for _, server := range []CodexLiveICEServer{{}, {URLs: []string{"turn:fixture.invalid"}}, {URLs: []string{"turn:fixture.invalid"}, Username: "user"}, {URLs: []string{"turn:fixture.invalid"}, Credential: "secret"}} {
		if err := (CodexLiveMediaRelayConfig{ICEServers: []CodexLiveICEServer{server}}).Validate(); err == nil {
			t.Fatal("incomplete ICE server was accepted")
		}
	}
}

func TestCodexLiveMediaStrictYAMLAndJSON(t *testing.T) {
	for _, field := range []struct {
		name      string
		good, bad []string
	}{
		{"enabled", []string{"true", "false", "null"}, []string{`"true"`, "yes", "1", "[]"}},
		{"disable-private-remote-ips", []string{"false", "true"}, []string{`"false"`, "no", "1"}},
		{"max-sessions", []string{"0", "32", "2147483647", "null"}, []string{"-1", "2147483648", "9223372036854775808", "1.5", `"32"`}},
		{"public-ip", []string{`""`, `"203.0.113.1"`, "null"}, []string{"32", "false", "[]", `"not-an-ip"`}},
		{"udp-port-min", []string{"0", "null"}, []string{"1", "-1", "65536", "1.0", `"0"`}},
		{"ice-servers", []string{"[]", "null"}, []string{"{}", "[null]", "[{urls: [23]}]", "[{urls: [stun:fixture.invalid], credential: 23}]"}},
	} {
		for _, valid := range []bool{true, false} {
			values := field.bad
			if valid {
				values = field.good
			}
			for _, value := range values {
				path := filepath.Join(t.TempDir(), "config.yaml")
				body := "codex:\n  live-media-relay:\n    " + field.name + ": " + value + "\n"
				if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
					t.Fatal(err)
				}
				for _, optional := range []bool{false, true} {
					if _, err := LoadConfigOptional(path, optional); (err == nil) != valid {
						t.Fatalf("%s valid=%v optional=%v failed: %v", field.name, valid, optional, err)
					}
				}
				if json.Valid([]byte(value)) {
					var c CodexLiveMediaRelayConfig
					if err := json.Unmarshal([]byte(`{"`+field.name+`":`+value+`}`), &c); (err == nil) != valid {
						t.Fatalf("JSON %s valid=%v failed", field.name, valid)
					}
				}
			}
		}
	}
}

func TestCodexLiveMediaCloneSaveInheritanceAndSecretRedaction(t *testing.T) {
	for _, shape := range []string{"nested merge", "alias", "root merge"} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		fields := "    enabled: true\n    max-sessions: 32\n    future-field: keep\n    ice-servers:\n      - urls: [turn:fixture.invalid]\n        username: user-fixture\n        credential: secret-fixture\n        future-ice: keep\n"
		body := "defaults: &media\n" + fields + "codex:\n  live-media-relay:\n    <<: *media\n"
		if shape == "alias" {
			body = "defaults: &media\n" + fields + "codex:\n  live-media-relay: *media\n"
		}
		if shape == "root merge" {
			body = "defaults: &root\n  codex:\n    live-media-relay:\n    " + strings.ReplaceAll(strings.TrimSuffix(fields, "\n"), "\n", "\n    ") + "\n<<: *root\n"
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := LoadConfig(path)
		if err != nil || !cfg.Codex.LiveMediaRelay.Enabled {
			t.Fatalf("%s did not load: %v", shape, err)
		}
		cloned, errClone := Clone(cfg)
		if errClone != nil || cloned.Codex.LiveMediaRelay.ICEServers[0].Credential != "secret-fixture" {
			t.Fatal("runtime clone lost ICE secret")
		}
		cloned.Codex.LiveMediaRelay.ICEServers[0].URLs[0] = "stun:another.invalid"
		if cfg.Codex.LiveMediaRelay.ICEServers[0].URLs[0] != "turn:fixture.invalid" {
			t.Fatal("runtime clone retained shared ICE slice")
		}
		public, errJSON := json.Marshal(cfg)
		if errJSON != nil || strings.Contains(string(public), "secret-fixture") || strings.Contains(string(public), "user-fixture") {
			t.Fatal("public JSON exposed ICE authentication")
		}
		cfg.Codex.LiveMediaRelay.Enabled = false
		cfg.Codex.LiveMediaRelay.MaxSessions = 0
		if errSave := SaveConfigPreserveComments(path, cfg); errSave != nil {
			t.Fatal(errSave)
		}
		got, errLoad := LoadConfig(path)
		if errLoad != nil || got.Codex.LiveMediaRelay.Enabled || got.Codex.LiveMediaRelay.MaxSessions != 0 || got.Codex.LiveMediaRelay.ICEServers[0].Credential != "secret-fixture" {
			t.Fatalf("%s lost explicit values/secret after reload", shape)
		}
		saved, errRead := os.ReadFile(path)
		if errRead != nil || !strings.Contains(string(saved), "future-field: keep") || !strings.Contains(string(saved), "future-ice: keep") || !strings.Contains(string(saved), "enabled: true") {
			t.Fatal("media save changed defaults or unknown YAML")
		}
		cfg.Codex.LiveMediaRelay.MaxSessions = -1
		if errSave := SaveConfigPreserveComments(path, cfg); errSave == nil {
			t.Fatal("saving invalid media config succeeded")
		}
		after, _ := os.ReadFile(path)
		if string(after) != string(saved) {
			t.Fatal("failed save mutated old config")
		}
	}
	var decoded CodexLiveMediaRelayConfig
	if err := json.Unmarshal([]byte(`{"ice-servers":[{"urls":["turn:fixture.invalid"],"username":"user-fixture","credential":"secret-fixture"}]}`), &decoded); err != nil || decoded.ICEServers[0].Credential != "secret-fixture" {
		t.Fatal("explicit JSON ICE input lost secret")
	}
	var inherited CodexLiveMediaRelayConfig
	if err := yaml.Unmarshal([]byte("defaults: &defaults {enabled: wrong}\n<<: *defaults\nenabled: false\n"), &inherited); err != nil {
		t.Fatal("explicit override did not take precedence")
	}
}

package sentinelcompat

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestConfigWireRoundTrip(t *testing.T) {
	var cfg Config
	raw := `{"enabled":true,"environment-properties":[{"path":"window.__flag","type":"boolean","value":false},{"path":"window.__count","type":"number","value":0},{"path":"window.__empty","type":"string","value":""},{"path":"window.__null","type":"null","value":null},{"path":"window.__missing","type":"undefined"}]}`
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatal(err)
	}
	baseline, err := Compile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, codec := range []string{"yaml", "json"} {
		t.Run(codec, func(t *testing.T) {
			var data []byte
			var err error
			if codec == "yaml" {
				data, err = yaml.Marshal(cfg)
			} else {
				data, err = json.Marshal(cfg)
			}
			if err != nil {
				t.Fatal(err)
			}
			var restored Config
			if codec == "yaml" {
				err = yaml.Unmarshal(data, &restored)
			} else {
				err = json.Unmarshal(data, &restored)
			}
			if err != nil {
				t.Fatalf("round trip: %v", err)
			}
			policy, err := Compile(restored)
			if err != nil || policy.Version() != baseline.Version() {
				t.Fatalf("policy changed: %v", err)
			}
		})
	}
	for _, zero := range []Config{{}, {Enabled: true}} {
		data, err := json.Marshal(zero)
		if err != nil {
			t.Fatal(err)
		}
		var restored Config
		if err = json.Unmarshal(data, &restored); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPolicyCanonicalAndImmutable(t *testing.T) {
	cfg := Config{Enabled: true, WritableWindowProperties: []string{"__extra_b", "__extra_a", "__extra_b"}, EnvironmentProperties: []Property{{Path: "window.__flag", Type: "boolean", Value: true}}}
	p, err := Compile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg.WritableWindowProperties[0] = "fetch"
	cfg.EnvironmentProperties[0].Value = false
	if !p.AllowsWrite("__extra_b") || p.AllowsWrite("fetch") {
		t.Fatal("mutable config changed policy")
	}
	props := p.Properties()
	props[0].Value = false
	if got, _ := p.Property("window.__flag"); got.Value != true {
		t.Fatal("mutable output changed policy")
	}
	other, err := Compile(Config{Enabled: true, WritableWindowProperties: []string{"__extra_a", "__extra_b"}, EnvironmentProperties: []Property{{Path: "window.__flag", Type: "boolean", Value: true}}})
	if err != nil || p.Version() != other.Version() {
		t.Fatal("equivalent rules have different digests")
	}
	if !p.AllowsWrite("__oai_so_next") || p.AllowsWrite("__oai_so_") || p.AllowsWrite("__oai_so_a.b") {
		t.Fatal("namespace boundary")
	}
	if p, err := Compile(Config{}); err != nil || p != nil {
		t.Fatal("default policy not disabled")
	}
}

func TestConfigRejectsInvalidRules(t *testing.T) {
	cases := []string{
		`null`, `[]`, `{"enabled":null}`, `{"enabled":"true"}`, `{"observer-state-auto-extend":null}`, `{"unknown":1}`,
		`{"writable-window-properties":null}`, `{"writable-window-properties":["__proto__"]}`, `{"writable-window-properties":["fetch"]}`, `{"writable-window-properties":["__secret"]}`,
		`{"environment-properties":[{"path":"window.navigator.userAgent","type":"string","value":"x"}]}`,
		`{"environment-properties":[{"path":"window.__oai_so_owner","type":"string","value":"x"}]}`,
		`{"environment-properties":[{"path":"window.__x","type":"boolean","value":0}]}`,
		`{"environment-properties":[{"path":"window.__x","type":"number","value":9007199254740992}]}`,
		`{"environment-properties":[{"path":"window.__x","type":"undefined","value":null}]}`,
		`{"environment-properties":[{"path":"window.__x","type":"null"}]}`,
		`{"environment-properties":[{"path":"window.__x","type":"string","value":"x","enumerable":null}]}`,
		`{"environment-properties":[{"path":"window.__x","type":"object","value":{}}]}`,
		`{"environment-properties":[{"path":"window.__x","type":"null","value":null},{"path":"window.__x","type":"null","value":null}]}`,
		`{"writable-window-properties":["__x"],"environment-properties":[{"path":"window.__x","type":"null","value":null}]}`,
	}
	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			var c Config
			err := json.Unmarshal([]byte(raw), &c)
			if err == nil {
				_, err = Compile(c)
			}
			if err == nil {
				t.Fatal("accepted invalid rules")
			}
		})
	}
	for _, raw := range []string{"enabled: yes\n", "enabled: false\nenabled: true\n", "environment-properties: null\n"} {
		var c Config
		if err := yaml.Unmarshal([]byte(raw), &c); err == nil {
			t.Fatalf("accepted YAML %s", raw)
		}
	}
	_, err := Compile(Config{EnvironmentProperties: []Property{{Path: "window.__long", Type: "string", Value: strings.Repeat("x", MaxStringBytes+1)}}})
	if err == nil {
		t.Fatal("oversized value")
	}
}

func TestMergePreservesOmittedAndClearsExplicitLists(t *testing.T) {
	current := Config{Enabled: true, WritableWindowProperties: []string{"__extra"}}
	merged, err := Merge(current, []byte(`{"enabled":false}`))
	if err != nil {
		t.Fatal(err)
	}
	if merged.Enabled || !reflect.DeepEqual(merged.WritableWindowProperties, current.WritableWindowProperties) {
		t.Fatal("partial update lost rules")
	}
	merged, err = Merge(current, []byte(`{"writable-window-properties":[]}`))
	if err != nil || len(merged.WritableWindowProperties) != 0 {
		t.Fatal("list not cleared")
	}
	if _, err = Merge(current, []byte(`{"unknown":true}`)); err == nil {
		t.Fatal("unknown patch key accepted")
	}
}

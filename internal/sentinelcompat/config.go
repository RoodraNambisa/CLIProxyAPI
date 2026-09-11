// Package sentinelcompat defines data-only extensions to the Sentinel Go VM.
package sentinelcompat

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"unicode/utf16"

	"gopkg.in/yaml.v3"
)

const (
	MaxProperties  = 64
	MaxNameBytes   = 128
	MaxStringBytes = 4096
	MaxStateBytes  = 64 << 10
)

type Config struct {
	Enabled                  bool       `json:"enabled" yaml:"enabled"`
	ObserverStateAutoExtend  *bool      `json:"observer-state-auto-extend,omitempty" yaml:"observer-state-auto-extend,omitempty"`
	WritableWindowProperties []string   `json:"writable-window-properties" yaml:"writable-window-properties"`
	EnvironmentProperties    []Property `json:"environment-properties" yaml:"environment-properties"`
}

type Property struct {
	bytes      int
	Path       string `json:"path" yaml:"path"`
	Type       string `json:"type" yaml:"type"`
	Value      any    `json:"value,omitempty" yaml:"value,omitempty"`
	Enumerable bool   `json:"enumerable" yaml:"enumerable"`
}

// BudgetBytes is precomputed while compiling the immutable policy.
func (p Property) BudgetBytes() int { return p.bytes }

func (c Config) MarshalJSON() ([]byte, error) {
	type plain Config
	return json.Marshal(plain(c.Resolved()))
}
func (c Config) MarshalYAML() (any, error) { type plain Config; return plain(c.Resolved()), nil }

func (p Property) wire() map[string]any {
	out := map[string]any{"path": p.Path, "type": p.Type, "enumerable": p.Enumerable}
	if p.Type != "undefined" {
		out["value"] = p.Value
	}
	return out
}

func (p Property) MarshalJSON() ([]byte, error) { return json.Marshal(p.wire()) }
func (p Property) MarshalYAML() (any, error) {
	out := p.wire()
	if n, ok := p.Value.(json.Number); ok {
		value, err := n.Float64()
		if err != nil {
			return nil, err
		}
		out["value"] = value
	}
	return out, nil
}

func decodeObject(data []byte, out any) error {
	if !bytes.HasPrefix(bytes.TrimSpace(data), []byte("{")) {
		return fmt.Errorf("object required")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return fmt.Errorf("unexpected trailing data")
	}
	return nil
}

func (p *Property) UnmarshalJSON(data []byte) error {
	var raw struct {
		Path       string          `json:"path"`
		Type       string          `json:"type"`
		Value      json.RawMessage `json:"value"`
		Enumerable json.RawMessage `json:"enumerable"`
	}
	if err := decodeObject(data, &raw); err != nil {
		return err
	}
	*p = Property{Path: raw.Path, Type: raw.Type}
	if len(raw.Enumerable) > 0 {
		if bytes.Equal(bytes.TrimSpace(raw.Enumerable), []byte("null")) {
			return fmt.Errorf("enumerable must be boolean")
		}
		if err := json.Unmarshal(raw.Enumerable, &p.Enumerable); err != nil {
			return fmt.Errorf("enumerable must be boolean")
		}
	}
	if raw.Type == "undefined" {
		if len(raw.Value) != 0 {
			return fmt.Errorf("undefined property must omit value")
		}
	} else {
		if len(raw.Value) == 0 {
			return fmt.Errorf("property value is required")
		}
		decoder := json.NewDecoder(bytes.NewReader(raw.Value))
		decoder.UseNumber()
		if err := decoder.Decode(&p.Value); err != nil {
			return fmt.Errorf("invalid property value")
		}
	}
	return nil
}

func yamlObjectJSON(node *yaml.Node) ([]byte, error) {
	var value map[string]any
	if node.Kind != yaml.MappingNode && node.Kind != yaml.AliasNode {
		return nil, fmt.Errorf("object required")
	}
	if err := node.Decode(&value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func (p *Property) UnmarshalYAML(node *yaml.Node) error {
	data, err := yamlObjectJSON(node)
	if err != nil {
		return err
	}
	return p.UnmarshalJSON(data)
}

func (c *Config) UnmarshalJSON(data []byte) error {
	type plain Config
	var raw plain
	if err := decodeObject(data, &raw); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for key, value := range fields {
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("%s must not be null", key)
		}
	}
	*c = Config(raw)
	return nil
}

func (c *Config) UnmarshalYAML(node *yaml.Node) error {
	data, err := yamlObjectJSON(node)
	if err != nil {
		return err
	}
	return c.UnmarshalJSON(data)
}

func (c Config) Resolved() Config {
	auto := c.ObserverStateAutoExtend == nil || *c.ObserverStateAutoExtend
	c.ObserverStateAutoExtend = &auto
	c.WritableWindowProperties = append([]string{}, c.WritableWindowProperties...)
	c.EnvironmentProperties = append([]Property{}, c.EnvironmentProperties...)
	return c
}

// Policy owns immutable, validated rules; mutable challenge values belong to a VM.
type Policy struct {
	propertyBytes int
	version       string
	auto          bool
	writable      map[string]struct{}
	properties    []Property
	byPath        map[string]Property
}

func (p *Policy) PropertyCount() int {
	if p == nil {
		return 0
	}
	return len(p.properties)
}
func (p *Policy) PropertyBytes() int {
	if p == nil {
		return 0
	}
	return p.propertyBytes
}

func Merge(c Config, data []byte) (Config, error) {
	var patch map[string]json.RawMessage
	if err := decodeObject(data, &patch); err != nil {
		return Config{}, err
	}
	previous, err := json.Marshal(c)
	if err != nil {
		return Config{}, err
	}
	var merged map[string]json.RawMessage
	if err = json.Unmarshal(previous, &merged); err != nil {
		return Config{}, err
	}
	for k, v := range patch {
		merged[k] = v
	}
	data, err = json.Marshal(merged)
	if err != nil {
		return Config{}, err
	}
	var result Config
	err = json.Unmarshal(data, &result)
	return result, err
}

func (p *Policy) Version() string {
	if p == nil {
		return ""
	}
	return p.version
}
func (p *Policy) Property(path string) (Property, bool) {
	if p == nil {
		return Property{}, false
	}
	v, ok := p.byPath[path]
	return v, ok
}
func (p *Policy) Properties() []Property {
	if p == nil {
		return nil
	}
	return append([]Property(nil), p.properties...)
}
func (p *Policy) ForEachProperty(f func(Property) error) error {
	if p != nil {
		for _, prop := range p.properties {
			if err := f(prop); err != nil {
				return err
			}
		}
	}
	return nil
}
func (p *Policy) RuleCount() int {
	if p == nil {
		return 0
	}
	n := len(p.writable) + len(p.properties)
	if p.auto {
		n++
	}
	return n
}
func (p *Policy) AllowsWrite(key string) bool {
	if p == nil || !ValidName(key) || protectedName(key) {
		return false
	}
	if _, ok := p.writable[key]; ok {
		return true
	}
	return p.auto && strings.HasPrefix(key, "__oai_so_") && len(key) > len("__oai_so_")
}

func ValidName(key string) bool {
	if len(key) == 0 || len(key) > MaxNameBytes {
		return false
	}
	for i, c := range []byte(key) {
		if c == '_' || c == '$' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || i > 0 && c >= '0' && c <= '9' {
			continue
		}
		return false
	}
	return true
}

func protectedName(name string) bool {
	switch name {
	case "__proto__", "prototype", "constructor", "__defineGetter__", "__defineSetter__", "__lookupGetter__", "__lookupSetter__", "toString", "toLocaleString", "valueOf", "hasOwnProperty", "isPrototypeOf", "propertyIsEnumerable":
		return true
	}
	lower := strings.ToLower(name)
	for _, part := range []string{"cookie", "token", "password", "secret", "credential", "storage", "authorization"} {
		if strings.Contains(lower, part) {
			return true
		}
	}
	return strings.HasPrefix(name, "__sentinel") || strings.HasPrefix(name, "__reactContainer$")
}

var reserved = map[string]string{
	"window":           "window self top parent frames opener document navigator screen location history performance Array Date Object String Math Reflect JSON Number Boolean Function Symbol Promise RegExp Error TypeError Map Set WeakMap WeakSet Proxy BigInt Intl crypto console fetch XMLHttpRequest Worker WebSocket eval atob btoa TextEncoder TextDecoder URL URLSearchParams Uint8Array ArrayBuffer SharedArrayBuffer Atomics setTimeout clearTimeout setInterval clearInterval queueMicrotask requestAnimationFrame cancelAnimationFrame requestIdleCallback addEventListener removeEventListener dispatchEvent postMessage innerWidth innerHeight outerWidth outerHeight devicePixelRatio localStorage sessionStorage __reactRouterContext __oai_so_owner __oai_so_uk __oai_so_uin __oai_so_ui Element HTMLElement HTMLCanvasElement Document Navigator Screen Storage CanvasRenderingContext2D WebGLRenderingContext std os undefined NaN Infinity",
	"window.document":  "URL location referrer readyState hidden visibilityState body head documentElement currentScript scripts cookie createElement createElementNS querySelector querySelectorAll getElementById getElementsByTagName addEventListener removeEventListener dispatchEvent write writeln open close defaultView implementation",
	"window.navigator": "userAgent appVersion appCodeName appName language languages hardwareConcurrency platform vendor vendorSub webdriver cookieEnabled onLine deviceMemory maxTouchPoints pdfViewerEnabled product productSub plugins mimeTypes userAgentData geolocation credentials serviceWorker sendBeacon",
	"window.screen":    "width height availLeft availTop availWidth availHeight colorDepth pixelDepth orientation",
}

func ValidatePath(path string) error {
	parent, name, ok := splitPath(path)
	if !ok || !ValidName(name) || protectedName(name) {
		return fmt.Errorf("unsupported environment property path")
	}
	list, ok := reserved[parent]
	if !ok {
		return fmt.Errorf("unsupported environment property parent")
	}
	if parent == "window" {
		list += " InternalError DOMException gc scriptArgs print bjson QJS_PROXY_VALUE SentinelSDK"
		list += " globalThis Event CustomEvent MessageChannel MessagePort cancelIdleCallback matchMedia getComputedStyle chrome CSS indexedDB Float16Array Float32Array Float64Array Int8Array Int16Array Int32Array Uint8ClampedArray Uint16Array Uint32Array BigInt64Array BigUint64Array DataView WeakRef FinalizationRegistry EvalError RangeError ReferenceError SyntaxError URIError AggregateError Iterator AsyncIterator escape unescape decodeURI decodeURIComponent encodeURI encodeURIComponent parseInt parseFloat isNaN isFinite"
	}
	for _, item := range strings.Fields(list) {
		if item == name {
			return fmt.Errorf("cannot override built-in environment property")
		}
	}
	if parent == "window" && strings.HasPrefix(name, "__oai_so_") {
		return fmt.Errorf("observer state must be written by its collector")
	}
	return nil
}

func splitPath(path string) (string, string, bool) {
	at := strings.LastIndexByte(path, '.')
	if at < 0 {
		return "", "", false
	}
	return path[:at], path[at+1:], true
}

func NormalizeValue(kind string, value any) (any, int, error) {
	switch kind {
	case "undefined", "null":
		if value == nil {
			return nil, 16, nil
		}
	case "string":
		if s, ok := value.(string); ok {
			size := max(len(s), len(utf16.Encode([]rune(s)))*2)
			if size <= MaxStringBytes {
				return s, size + 16, nil
			}
		}
	case "boolean":
		if b, ok := value.(bool); ok {
			return b, 16, nil
		}
	case "number":
		var n float64
		switch v := value.(type) {
		case json.Number:
			var err error
			n, err = v.Float64()
			if err != nil {
				return nil, 0, fmt.Errorf("invalid number")
			}
		case float64:
			n = v
		case int:
			n = float64(v)
		default:
			return nil, 0, fmt.Errorf("number required")
		}
		if !math.IsNaN(n) && !math.IsInf(n, 0) && (math.Trunc(n) != n || math.Abs(n) <= 9007199254740991) {
			return n, 16, nil
		}
	}
	return nil, 0, fmt.Errorf("invalid or oversized %s property value", kind)
}

func Compile(c Config) (*Policy, error) {
	c = c.Resolved()
	writable := make(map[string]struct{}, len(c.WritableWindowProperties))
	for _, name := range c.WritableWindowProperties {
		if !ValidName(name) || protectedName(name) {
			return nil, fmt.Errorf("invalid writable window property")
		}
		if err := ValidatePath("window." + name); err != nil && !(strings.HasPrefix(name, "__oai_so_") && len(name) > len("__oai_so_")) {
			return nil, err
		}
		writable[name] = struct{}{}
	}
	if len(writable)+len(c.EnvironmentProperties) > MaxProperties {
		return nil, fmt.Errorf("at most %d compatibility properties are allowed", MaxProperties)
	}
	byPath := make(map[string]Property, len(c.EnvironmentProperties))
	total := 0
	for _, prop := range c.EnvironmentProperties {
		if err := ValidatePath(prop.Path); err != nil {
			return nil, err
		}
		if _, exists := byPath[prop.Path]; exists {
			return nil, fmt.Errorf("duplicate environment property")
		}
		if parent, name, _ := splitPath(prop.Path); parent == "window" {
			if _, exists := writable[name]; exists {
				return nil, fmt.Errorf("environment property conflicts with writable window property")
			}
		}
		value, size, err := NormalizeValue(prop.Type, prop.Value)
		if err != nil {
			return nil, err
		}
		prop.Value = value
		prop.bytes = len(prop.Path) + size + 64
		total += prop.bytes
		if total > MaxStateBytes {
			return nil, fmt.Errorf("compatibility properties exceed memory limit")
		}
		byPath[prop.Path] = prop
	}
	if !c.Enabled {
		return nil, nil
	}
	c.WritableWindowProperties = c.WritableWindowProperties[:0]
	for name := range writable {
		c.WritableWindowProperties = append(c.WritableWindowProperties, name)
	}
	sort.Strings(c.WritableWindowProperties)
	c.EnvironmentProperties = c.EnvironmentProperties[:0]
	for _, prop := range byPath {
		c.EnvironmentProperties = append(c.EnvironmentProperties, prop)
	}
	sort.Slice(c.EnvironmentProperties, func(i, j int) bool { return c.EnvironmentProperties[i].Path < c.EnvironmentProperties[j].Path })
	if !*c.ObserverStateAutoExtend && len(writable) == 0 && len(byPath) == 0 {
		return nil, nil
	}
	data, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(data)
	return &Policy{version: hex.EncodeToString(hash[:]), auto: *c.ObserverStateAutoExtend, writable: writable, properties: c.EnvironmentProperties, byPath: byPath, propertyBytes: total}, nil
}

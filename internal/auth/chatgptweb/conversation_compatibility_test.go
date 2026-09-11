package chatgptweb

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelcompat"
)

func compileCompatibility(t testing.TB, cfg sentinelcompat.Config) *sentinelcompat.Policy {
	t.Helper()
	p, err := sentinelcompat.Compile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func compatibilityPrograms(key string, value any) ([]any, []any) {
	return []any{[]any{2, 40, "Reflect"}, []any{6, 41, 10, 40}, []any{2, 42, "set"}, []any{6, 43, 41, 42}, []any{2, 44, key}, []any{2, 45, value}, []any{7, 43, 10, 44, 45}},
		[]any{[]any{2, 46, key}, []any{6, 47, 10, 46}, []any{7, 3, 47}}
}

func TestCompatibilityObserverStateAndIsolation(t *testing.T) {
	policy := compileCompatibility(t, sentinelcompat.Config{Enabled: true})
	for _, key := range []string{"__oai_so_new_owner", "__manual_owner"} {
		selected := policy
		if key == "__manual_owner" {
			selected = compileCompatibility(t, sentinelcompat.Config{Enabled: true, WritableWindowProperties: []string{key}})
		}
		for _, owner := range []string{"first-request-owner", "second-request-owner"} {
			collector, snapshot := compatibilityPrograms(key, owner)
			observer, err := newConversationSentinelObserverVM(t.Context(), encodeConversationTurnstileProgram(t, "requirements", collector), encodeConversationTurnstileProgram(t, "requirements", snapshot), "requirements", ConversationTurnstileEnvironment{Persona: DefaultPersona(), Compatibility: selected}, zeroReader{}, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			token, err := observer.Snapshot(t.Context())
			observer.Close()
			if err != nil || token != base64.StdEncoding.EncodeToString([]byte(owner)) {
				t.Fatalf("owner not preserved: %q %v", token, err)
			}
		}
	}
	_, snapshot := compatibilityPrograms("__oai_so_new_owner", "unused")
	observer, err := newConversationSentinelObserverVM(t.Context(), encodeConversationTurnstileProgram(t, "requirements", []any{}), encodeConversationTurnstileProgram(t, "requirements", snapshot), "requirements", ConversationTurnstileEnvironment{Compatibility: policy}, zeroReader{}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	_, err = observer.Snapshot(t.Context())
	var compat *SentinelCompatibilityError
	if !errors.As(err, &compat) || compat.Kind != SentinelCompatibilityMissingEnvironment {
		t.Fatal("unwritten state leaked or was synthesized")
	}
}

func newCompatibilityVM(t *testing.T, cfg sentinelcompat.Config) *conversationTurnstileVM {
	t.Helper()
	prepared, err := prepareConversationTurnstileProgram(t.Context(), encodeConversationTurnstileProgram(t, "requirements", []any{}), "requirements", nil)
	if err != nil {
		t.Fatal(err)
	}
	vm, err := newConversationTurnstileVM(t.Context(), prepared, ConversationTurnstileEnvironment{Compatibility: compileCompatibility(t, cfg)}, zeroReader{}, time.Now, 0, 0, nil, true, SentinelProgramObserverCollect)
	if err != nil {
		t.Fatal(err)
	}
	return vm
}

func TestCompatibilityPrimitiveSemanticsAndLimits(t *testing.T) {
	vm := newCompatibilityVM(t, sentinelcompat.Config{Enabled: true})
	for _, value := range []any{false, 0, "", conversationTurnstileUndefined, conversationTurnstileExplicitNull, conversationTurnstileJSString{units: []uint16{0xd800}}} {
		if ok, err := vm.setCompatibilityWindowProperty("__oai_so_value", value); err != nil || !ok {
			t.Fatalf("value %T: %v", value, err)
		}
		got, err := vm.property(conversationTurnstileObjectRef{path: "window"}, "__oai_so_value")
		if _, null := value.(conversationTurnstileExplicitNullValue); null {
			value = nil
		}
		if err != nil || !conversationTurnstileStrictEqual(got, value) {
			t.Fatalf("type lost %T -> %T", value, got)
		}
	}
	for _, value := range []any{map[string]any{}, strings.Repeat("x", sentinelcompat.MaxStringBytes+1)} {
		_, err := vm.setCompatibilityWindowProperty("__oai_so_invalid", value)
		var compat *SentinelCompatibilityError
		if !errors.As(err, &compat) {
			t.Fatalf("expected compatibility failure, got %v", err)
		}
	}
	for i := 1; i < sentinelcompat.MaxProperties; i++ {
		if _, err := vm.setCompatibilityWindowProperty(fmt.Sprintf("__oai_so_%d", i), i); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := vm.setCompatibilityWindowProperty("__oai_so_overflow", 0); err == nil {
		t.Fatal("field count not bounded")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	vm.ctx = ctx
	if _, err := vm.setCompatibilityWindowProperty("__oai_so_value", 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
}

func TestCompatibilityConstantsReadonlyAndEnumerationFallback(t *testing.T) {
	vm := newCompatibilityVM(t, sentinelcompat.Config{Enabled: true, EnvironmentProperties: []sentinelcompat.Property{
		{Path: "window.__constant", Type: "null"}, {Path: "window.__undefined", Type: "undefined"},
		{Path: "window.navigator.exampleFlag", Type: "boolean", Value: false},
	}})
	for _, key := range []string{"__constant", "__undefined"} {
		if !vm.knownEnvironmentProperty(conversationTurnstileObjectRef{path: "window"}, key) {
			t.Fatal("declared null/undefined not modeled")
		}
		if ok, err := vm.reflectSetObjectRefProperty(conversationTurnstileObjectRef{path: "window"}, key, "overwrite"); err != nil || ok {
			t.Fatal("readonly property modified")
		}
	}
	if v, _ := vm.property(conversationTurnstileObjectRef{path: "window"}, "__constant"); v != nil {
		t.Fatal("null became undefined")
	}
	if v, _ := vm.property(conversationTurnstileObjectRef{path: "window.navigator"}, "exampleFlag"); v != false {
		t.Fatal("nested constant missing")
	}
	_, err := vm.call(conversationTurnstileObjectRef{path: "window.Object.keys"}, []any{conversationTurnstileObjectRef{path: "window"}})
	var compat *SentinelCompatibilityError
	if !errors.As(err, &compat) {
		t.Fatalf("incomplete host enumeration must fall back: %v", err)
	}
}

func TestCompatibilityStateBudgetAndProtectedWrites(t *testing.T) {
	vm := newCompatibilityVM(t, sentinelcompat.Config{Enabled: true})
	for _, key := range []string{"constructor", "__proto__", "fetch", "__oai_so_access_token"} {
		if ok, err := vm.setCompatibilityWindowProperty(key, "value"); ok || err != nil {
			t.Fatalf("protected property %s was extended", key)
		}
	}
	vm.programKind = SentinelProgramTurnstile
	if ok, err := vm.setCompatibilityWindowProperty("__oai_so_new", "value"); ok || err != nil {
		t.Fatal("Turnstile acquired Observer state")
	}
	vm.programKind = SentinelProgramObserverCollect
	for i := 0; i < 15; i++ {
		if _, err := vm.setCompatibilityWindowProperty(fmt.Sprintf("__oai_so_large%d", i), strings.Repeat("a", 2048)); err != nil {
			t.Fatal(err)
		}
	}
	_, err := vm.setCompatibilityWindowProperty("__oai_so_over_budget", strings.Repeat("a", 2048))
	var compatibility *SentinelCompatibilityError
	if !errors.As(err, &compatibility) || compatibility.Operation != "compatibility_state_limit" {
		t.Fatalf("state limit bypassed: %v", err)
	}
	vm = newCompatibilityVM(t, sentinelcompat.Config{Enabled: true})
	vm.memoryBudget.used = conversationTurnstileMaxRuntimeBytes
	_, err = vm.setCompatibilityWindowProperty("__oai_so_new", "value")
	if err == nil || errors.As(err, &compatibility) {
		t.Fatalf("hard VM budget became compatibility fallback: %v", err)
	}
}

func BenchmarkCompatibilityObserver(b *testing.B) {
	maxRules := sentinelcompat.Config{Enabled: true}
	for i := 0; i < sentinelcompat.MaxProperties; i++ {
		maxRules.EnvironmentProperties = append(maxRules.EnvironmentProperties, sentinelcompat.Property{Path: fmt.Sprintf("window.__constant%d", i), Type: "number", Value: i})
	}
	for _, tc := range []struct {
		name, key string
		cfg       sentinelcompat.Config
	}{
		{"disabled", "__oai_so_owner", sentinelcompat.Config{}},
		{"enabled_builtin", "__oai_so_owner", sentinelcompat.Config{Enabled: true}},
		{"auto_extended", "__oai_so_new_owner", sentinelcompat.Config{Enabled: true}},
		{"maximum_rules", "__oai_so_owner", maxRules},
	} {
		b.Run(tc.name, func(b *testing.B) {
			policy := compileCompatibility(b, tc.cfg)
			collector, snapshot := compatibilityPrograms(tc.key, "11111111-2222-4333-8444-555555555555")
			// Repeated writes and reads model a nontrivial collector without account data.
			step := append([]any(nil), collector...)
			for range 31 {
				collector = append(collector, step...)
			}
			dx := encodeConversationTurnstileProgram(b, "requirements", collector)
			sx := encodeConversationTurnstileProgram(b, "requirements", snapshot)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				vm, err := newConversationSentinelObserverVM(b.Context(), dx, sx, "requirements", ConversationTurnstileEnvironment{Compatibility: policy}, zeroReader{}, time.Now)
				if err != nil {
					b.Fatal(err)
				}
				_, err = vm.Snapshot(b.Context())
				vm.Close()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

package auth

import (
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCodexQuotaObservationFiltersAndDetachesHeaders(t *testing.T) {
	headers := http.Header{
		"X-Codex-Plan-Type":                   {" pro "},
		"X-Codex-Primary-Used-Percent":        {"10", "20"},
		"X-Codex-Primary-Reset-After-Seconds": {"0"},
		"X-Codex-Credits-Unlimited":           {"false"},
		"X-Codex-Bengalfox-Used-Percent":      {"30"},
		"X-Ratelimit-Remaining-Tokens":        {"40"},
		"Retry-After":                         {"60"},
		"Authorization":                       {"fixture-secret"},
		"Set-Cookie":                          {"fixture-cookie"},
		"Anthropic-Ratelimit-Unified-Status":  {"allowed"},
		"X-Codex-Credits-Token":               {"fixture-secret"},
		"X-Codex-Used-Percent-Secret":         {"fixture-secret"},
	}
	original := headers.Clone()
	at := time.Unix(123, 0).UTC()
	observation := newCodexQuotaObservation(headers, "http", at)
	if observation == nil || len(observation.Signals) != 7 || observation.Signals["X-Codex-Primary-Used-Percent"] != "20" || observation.Signals["X-Codex-Plan-Type"] != "pro" || !observation.ObservedAt.Equal(at) || observation.Source != "http" {
		t.Fatal("quota observation lost accepted signals or retained private fields")
	}
	if !reflect.DeepEqual(headers, original) {
		t.Fatal("collecting quota signals modified the upstream headers")
	}
	copy := observation.Clone()
	copy.Signals["X-Codex-Plan-Type"] = "changed"
	headers.Set("X-Codex-Primary-Used-Percent", "changed")
	if observation.Signals["X-Codex-Plan-Type"] != "pro" || observation.Signals["X-Codex-Primary-Used-Percent"] != "20" || original.Get("X-Codex-Primary-Used-Percent") != "10" {
		t.Fatal("quota snapshots share mutable source or returned maps")
	}
	if (*CodexQuotaObservation)(nil).Clone() != nil || newCodexQuotaObservation(headers, "unknown", at) != nil || newCodexQuotaObservation(http.Header{"Authorization": {"fixture"}}, "http", at) != nil {
		t.Fatal("unsupported or empty observations must not replace a usable snapshot")
	}
}

func TestCodexQuotaSignalBoundsRejectControlCharacters(t *testing.T) {
	for _, invalid := range []string{"", " ", "\npro", "pro\r", "a\tb", "a\x00b", "a\x7fb", "\xff", strings.Repeat("x", maxCodexQuotaSignalText+1)} {
		if got := collectCodexQuotaSignals(http.Header{"X-Codex-Plan-Type": {invalid}}); len(got) != 0 {
			t.Fatal("invalid quota value was retained")
		}
	}
	for _, invalid := range []string{"X-Codex-\r\nPlan-Type", "X-Codex- bad-used-percent", "X-Codex-" + strings.Repeat("x", 256) + "-used-percent"} {
		if got := collectCodexQuotaSignals(http.Header{invalid: {"1"}}); len(got) != 0 {
			t.Fatal("invalid quota key was retained")
		}
	}
	value := strings.Repeat("x", maxCodexQuotaSignalText)
	if got := collectCodexQuotaSignals(http.Header{"X-Codex-Plan-Type": {value}}); got["X-Codex-Plan-Type"] != value {
		t.Fatal("maximum-sized valid quota value was rejected")
	}
}

func TestCodexQuotaSignalsAreBoundedAndDeterministic(t *testing.T) {
	headers := http.Header{"x-codex-plan-type": {"lower"}, "X-Codex-Plan-Type": {"canonical"}, "X-CODEX-PLAN-TYPE": {"upper"}, "X-Codex-Primary-Used-Percent": {"15"}, "Retry-After": {"1"}}
	for index := range 100 {
		headers[fmt.Sprintf("X-Codex-Additional-%03d-Used-Percent", index)] = []string{"20"}
	}
	want := collectCodexQuotaSignals(headers)
	if len(want) != maxCodexQuotaSignals || want["X-Codex-Plan-Type"] != "canonical" || want["X-Codex-Primary-Used-Percent"] != "15" || want["Retry-After"] != "1" {
		t.Fatal("bounded snapshot dropped primary signals or chose a conflicting header nondeterministically")
	}
	for range 100 {
		if got := collectCodexQuotaSignals(headers); !reflect.DeepEqual(got, want) {
			t.Fatal("header selection depends on map iteration order")
		}
	}
}

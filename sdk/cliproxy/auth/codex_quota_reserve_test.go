package auth

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func codexQuotaHeadersWithReserve() http.Header {
	return http.Header{
		"X-Codex-Active-Limit":                                        {"premium"},
		"X-Codex-Plan-Type":                                           {"pro"},
		"X-Codex-Primary-Used-Percent":                                {"13"},
		"X-Codex-Primary-Window-Minutes":                              {"10080"},
		"X-Codex-Primary-Reset-After-Seconds":                         {"404639"},
		"X-Codex-Primary-Reset-At":                                    {"1789805476"},
		"X-Codex-Secondary-Used-Percent":                              {"0"},
		"X-Codex-Secondary-Window-Minutes":                            {"0"},
		"X-Codex-Secondary-Reset-After-Seconds":                       {"0"},
		"X-Codex-Secondary-Reset-At":                                  {""},
		"X-Codex-Bengalfox-Limit-Name":                                {"GPT-5.3-Codex-Spark"},
		"X-Codex-Bengalfox-Primary-Used-Percent":                      {"0"},
		"X-Codex-Bengalfox-Primary-Window-Minutes":                    {"300"},
		"X-Codex-Bengalfox-Primary-Reset-After-Seconds":               {"18000"},
		"X-Codex-Bengalfox-Primary-Reset-At":                          {"1789418838"},
		"X-Codex-Bengalfox-Secondary-Used-Percent":                    {"0"},
		"X-Codex-Bengalfox-Secondary-Window-Minutes":                  {"10080"},
		"X-Codex-Bengalfox-Secondary-Reset-After-Seconds":             {"604800"},
		"X-Codex-Bengalfox-Secondary-Reset-At":                        {"1790005638"},
		"X-Base-Model-Inference-Limit-Name":                           {"gpt-reserve"},
		"X-Base-Model-Inference-Primary-Used-Percent":                 {"0"},
		"X-Base-Model-Inference-Primary-Window-Minutes":               {"10080"},
		"X-Base-Model-Inference-Primary-Over-Secondary-Limit-Percent": {"0"},
		"X-Base-Model-Inference-Primary-Reset-After-Seconds":          {"604800"},
		"X-Base-Model-Inference-Primary-Reset-At":                     {"1790005638"},
		"X-Base-Model-Inference-Secondary-Used-Percent":               {"0"},
		"X-Base-Model-Inference-Secondary-Window-Minutes":             {"0"},
		"X-Base-Model-Inference-Secondary-Reset-After-Seconds":        {"0"},
		"X-Base-Model-Inference-Secondary-Reset-At":                   {""},
	}
}

func TestCodexQuotaSignalsAcceptReserveWithoutPrivateHeaders(t *testing.T) {
	headers := codexQuotaHeadersWithReserve()
	for _, key := range []string{"Authorization", "Set-Cookie", "X-Base-Model-Inference-Token", "X-Base-Model-Inference-Turn-State", "X-Other-Primary-Used-Percent"} {
		headers.Set(key, "private")
	}
	// Additional families must not crowd the observed reserve pool out of the bound.
	for index := range 70 {
		headers.Set(fmt.Sprintf("X-Codex-Additional-%03d-Primary-Used-Percent", index), "1")
	}
	signals := CollectCodexQuotaSignals(headers)
	for key, values := range headers {
		if !strings.HasPrefix(key, "X-Base-Model-Inference-") || values[0] == "" || values[0] == "private" {
			continue
		}
		if signals[key] != values[0] {
			t.Errorf("reserve signal %s = %q, want %q", key, signals[key], values[0])
		}
	}
	for key, value := range signals {
		if value == "private" || value == "" {
			t.Errorf("invalid quota signal retained: %s", key)
		}
	}
}

func TestCodexQuotaPoolsKeepReserveOptionalAndCredentialScoped(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	at := time.Unix(1789400838, 0)
	withReserve := codexQuotaHeadersWithReserve()
	withoutReserve := withReserve.Clone()
	for key := range withoutReserve {
		if strings.HasPrefix(key, "X-Base-Model-Inference-") {
			delete(withoutReserve, key)
		}
	}
	for _, tc := range []struct {
		name    string
		headers http.Header
		want    int
	}{
		{"with-reserve", withReserve, 3},
		{"without-reserve", withoutReserve, 2},
	} {
		installed, err := manager.Register(t.Context(), &Auth{ID: tc.name, Provider: "codex"})
		if err != nil {
			t.Fatal(err)
		}
		manager.recordCodexQuotaObservation(installed.ID, installed.RuntimeInstanceID(), "http", tc.headers, at)
		// An omitted family is not a new observation or evidence of lost entitlement.
		manager.recordCodexQuotaObservation(installed.ID, installed.RuntimeInstanceID(), "http", withoutReserve, at.Add(time.Minute))
		current, _ := manager.GetByID(installed.ID)
		pools := current.CodexQuotaSnapshot().Pools
		if len(pools) != tc.want {
			t.Fatalf("%s: got %d pools, want %d", tc.name, len(pools), tc.want)
		}
		foundReserve := false
		for _, pool := range pools {
			if pool.ID != "base_model_inference" {
				continue
			}
			foundReserve = true
			if tc.want == 2 || pool.Name != "gpt-reserve" || !pool.ObservedAt.Equal(at) || pool.Signals["X-Codex-Primary-Window-Minutes"] != "10080" || pool.Signals["X-Codex-Primary-Used-Percent"] != "0" || pool.Signals["X-Codex-Primary-Reset-At"] != "1790005638" {
				t.Fatalf("reserve was fabricated, renamed, or redated: %#v", pool)
			}
		}
		if foundReserve != (tc.want == 3) {
			t.Fatalf("%s: optional reserve pool presence is incorrect", tc.name)
		}
	}
}

func TestCodexQuotaPoolsDoNotInventReserveFromNameAlone(t *testing.T) {
	observation := newCodexQuotaObservation(http.Header{"X-Base-Model-Inference-Limit-Name": {"gpt-reserve"}}, "http", time.Unix(100, 0))
	if observation == nil {
		t.Fatal("upstream reserve name was discarded")
	}
	pools, _ := mergeCodexQuotaPools(nil, observation)
	if len(pools) != 0 {
		t.Fatal("a display name without quota windows invented a reserve balance")
	}
}

func TestCodexQuotaPoolsPreferNamedReserveOverActiveAlias(t *testing.T) {
	headers := codexQuotaHeadersWithReserve()
	headers.Set("X-Codex-Active-Limit", "base_model_inference")
	headers.Set("X-Codex-Primary-Used-Percent", "9")
	observation := newCodexQuotaObservation(headers, "http", time.Unix(100, 0))
	pools, _ := mergeCodexQuotaPools(nil, observation)
	if len(pools) != 2 {
		t.Fatalf("reserve active alias was not deduplicated: %#v", pools)
	}
	var reserve CodexQuotaPool
	for _, pool := range pools {
		if pool.ID == "base_model_inference" {
			reserve = pool
		}
	}
	if reserve.Name != "gpt-reserve" || reserve.Signals["X-Codex-Primary-Used-Percent"] != "0" {
		t.Fatalf("active alias replaced the explicit reserve family: %#v", reserve)
	}
}

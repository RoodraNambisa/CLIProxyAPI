package helps

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestCodexQuotaEventsPreserveWindowsCreditsAndNamespaces(t *testing.T) {
	for _, additional := range []string{
		`{"GPT-5.3-Codex-Spark":{"primary":{"usedPercent":15,"windowMinutes":300,"resetAfterSeconds":0}}}`,
		`[{"limitName":"GPT-5.3-Codex-Spark","rateLimit":{"primary":{"usedPercent":15,"windowMinutes":300,"resetAfterSeconds":0}}}]`,
	} {
		payload := []byte(`{"type":"codex.rate_limits","planType":"pro","meteredLimitName":"codex_bengalfox","rateLimit":{"allowed":true,"limitReached":false,"primary":{"used_percent":2.5,"window_minutes":10080,"reset_at":1782951970}},"codeReviewRateLimits":{"secondary":{"used_percent":10,"window_minutes":300,"reset_after_seconds":1}},"credits":{"hasCredits":false,"unlimited":false,"balance":"0"},"additionalRateLimits":` + additional + `}`)
		original := bytes.Clone(payload)
		headers := ParseCodexQuotaEventHeaders(payload)
		for key, want := range map[string]string{
			"X-Codex-Plan-Type": "pro", "X-Codex-Active-Limit": "codex_bengalfox", "X-Codex-Allowed": "true", "X-Codex-Limit-Reached": "false",
			"X-Codex-Primary-Used-Percent": "2.5", "X-Codex-Primary-Reset-At": "1782951970", "X-Codex-Code-Review-Secondary-Used-Percent": "10",
			"X-Codex-Additional-GPT-5.3-Codex-Spark-Primary-Used-Percent": "15", "X-Codex-Additional-GPT-5.3-Codex-Spark-Primary-Reset-After-Seconds": "0",
			"X-Codex-Credits-Has-Credits": "false", "X-Codex-Credits-Unlimited": "false", "X-Codex-Credits-Balance": "0",
		} {
			if headers.Get(key) != want {
				t.Fatalf("quota field %s was lost or changed", key)
			}
		}
		if !bytes.Equal(payload, original) {
			t.Fatal("quota observation mutated a forwarded frame")
		}
	}
}

func TestCodexQuotaEventsRejectMalformedAndUnrelatedPayloads(t *testing.T) {
	for _, raw := range []string{
		`{`, `{"type":"codex.rate_limits"}`, `{"type":"codex.rate_limits","rate_limits":{"primary":{"used_percent":1}}}`,
		`{"type":"response.output_text.delta","item":{"type":"codex.rate_limits"},"rate_limits":{"allowed":true}}`,
		`{"type":"codex.rate_limits","rate_limits":{"allowed":true}} garbage`,
		`{"type":"codex.rate_limits","rate_limits":{"allowed":true},"padding":"` + strings.Repeat("x", maxCodexQuotaEventBytes) + `"}`,
	} {
		if ParseCodexQuotaEventHeaders([]byte(raw)) != nil {
			t.Fatal("malformed, oversized or unrelated frame became a quota observation")
		}
	}
	for _, window := range []string{
		`{"used_percent":"NaN","window_minutes":1,"reset_at":1}`,
		`{"used_percent":101,"window_minutes":1,"reset_at":1}`,
		`{"used_percent":-1,"window_minutes":1,"reset_at":1}`,
		`{"used_percent":50,"window_minutes":0,"reset_at":1}`,
		`{"used_percent":50,"window_minutes":1.5,"reset_at":1}`,
		`{"used_percent":50,"window_minutes":1,"reset_at":9223372036854775808}`,
		`{"used_percent":false,"window_minutes":1,"reset_at":1}`,
	} {
		if ParseCodexQuotaEventHeaders([]byte(`{"type":"codex.rate_limits","rate_limits":{"primary":`+window+`}}`)) != nil {
			t.Fatal("invalid quota window was accepted")
		}
	}
}

func TestCodexQuotaErrorFramesFilterPrivateHeaders(t *testing.T) {
	headers := ParseCodexQuotaEventHeaders([]byte(`{"type":"error","status_code":429,"headers":{"X-Codex-Primary-Used-Percent":"100","Retry-After":30,"Set-Cookie":"fixture","Authorization":"fixture","X-Codex-Turn-State":"fixture","X-Codex-Plan-Type":"pro\nforged"}}`))
	if len(headers) != 2 || headers.Get("Retry-After") != "30" || headers.Get("X-Codex-Primary-Used-Percent") != "100" {
		t.Fatal("quota error headers leaked private values or lost accepted signals")
	}
}

func TestCodexQuotaErrorFramesPreserveOptionalReserveHeaders(t *testing.T) {
	headers := ParseCodexQuotaEventHeaders([]byte(`{"type":"error","status_code":429,"headers":{"x-base-model-inference-limit-name":"gpt-reserve","x-base-model-inference-primary-used-percent":"100","x-base-model-inference-primary-window-minutes":"10080","x-base-model-inference-token":"private","Authorization":"private"}}`))
	if len(headers) != 3 || headers.Get("X-Base-Model-Inference-Limit-Name") != "gpt-reserve" || headers.Get("X-Base-Model-Inference-Primary-Used-Percent") != "100" || headers.Get("X-Base-Model-Inference-Primary-Window-Minutes") != "10080" {
		t.Fatal("reserve quota was discarded or private headers were retained")
	}
}

func TestCodexQuotaEventsBoundAdditionalLimitsAndAvoidNamespaceMerging(t *testing.T) {
	var additional []string
	for index := range 20 {
		additional = append(additional, fmt.Sprintf(`"model-%d":{"allowed":true}`, index))
	}
	headers := ParseCodexQuotaEventHeaders([]byte(`{"type":"codex.rate_limits","additional_rate_limits":{` + strings.Join(additional, ",") + `}}`))
	if len(headers) != 16 || headers.Get("X-Codex-Additional-Model-8-Allowed") != "" {
		t.Fatal("additional quota windows exceeded the eight-limit bound")
	}
	headers = ParseCodexQuotaEventHeaders([]byte(`{"type":"codex.rate_limits","metered_limit_name":"bad/name","rate_limits":{"allowed":true},"additional_rate_limits":{"a b":{"primary":{"used_percent":1,"window_minutes":300,"reset_at":1}},"a/b":{"secondary":{"used_percent":2,"window_minutes":300,"reset_at":1}}}}`))
	if headers.Get("X-Codex-Active-Limit") != "" || headers.Get("X-Codex-Allowed") != "true" || headers.Get("X-Codex-Additional-A-B-Primary-Used-Percent") != "1" || headers.Get("X-Codex-Additional-A-B-Secondary-Used-Percent") != "" {
		t.Fatal("malformed active name erased valid data or additional namespaces were merged")
	}
}

func TestCodexQuotaEventsPreserveExplicitPoolIdentity(t *testing.T) {
	headers := ParseCodexQuotaEventHeaders([]byte(`{"type":"codex.rate_limits","metered_limit_name":"codex_bengalfox","limit_name":"GPT-5.3-Codex-Spark","rate_limits":{"primary":{"used_percent":0,"window_minutes":300,"reset_at":1789331215}},"additional_rate_limits":[{"limit_name":"GPT-5.3-Codex-Spark","metered_feature":"codex_bengalfox","rate_limit":{"secondary":{"used_percent":2,"window_minutes":10080,"reset_at":1789806595}}}]}`))
	if headers.Get("X-Codex-Active-Limit") != "codex_bengalfox" || headers.Get("X-Codex-Limit-Name") != "GPT-5.3-Codex-Spark" || headers.Get("X-Codex-Additional-GPT-5.3-Codex-Spark-Limit-Id") != "codex_bengalfox" {
		t.Fatal("an explicit pool id or display name was discarded")
	}
}

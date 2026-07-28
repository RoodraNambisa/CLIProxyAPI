package chatgptweb

import (
	"encoding/json"
	"testing"
)

func TestCredentialQuotaRoundTripPreservesZero(t *testing.T) {
	zero := 0
	credential := &Credential{
		Type:                Provider,
		ImageQuotaRemaining: &zero,
		ImageQuotaResetAt:   "2026-07-28T00:00:00Z",
		QuotaState:          QuotaStateExhausted,
		QuotaUpdatedAt:      "2026-07-27T00:00:00Z",
		QuotaStale:          true,
		QuotaLastError:      "rate_limited",
	}
	metadata := map[string]any{}
	credential.ApplyToMetadata(metadata)
	payload, errMarshal := json.Marshal(metadata)
	if errMarshal != nil {
		t.Fatalf("marshal metadata: %v", errMarshal)
	}
	decoded, errDecode := DecodeCredential(payload)
	if errDecode != nil {
		t.Fatalf("DecodeCredential() error = %v", errDecode)
	}
	if decoded.ImageQuotaRemaining == nil || *decoded.ImageQuotaRemaining != 0 {
		t.Fatalf("ImageQuotaRemaining = %#v, want zero", decoded.ImageQuotaRemaining)
	}
	if decoded.QuotaState != QuotaStateExhausted || !decoded.QuotaStale || decoded.QuotaLastError != "rate_limited" {
		t.Fatalf("decoded quota = %+v", decoded)
	}
}

func TestNormalizeQuotaStateKeepsMissingQuotaUnknown(t *testing.T) {
	if got := NormalizeQuotaState("", nil); got != QuotaStateUnknown {
		t.Fatalf("NormalizeQuotaState() = %q, want unknown", got)
	}
	zero := 0
	if got := NormalizeQuotaState("", &zero); got != QuotaStateExhausted {
		t.Fatalf("zero NormalizeQuotaState() = %q, want exhausted", got)
	}
	remaining := 4
	if got := NormalizeQuotaState("", &remaining); got != QuotaStateAvailable {
		t.Fatalf("positive NormalizeQuotaState() = %q, want available", got)
	}
	if got := NormalizeQuotaState(QuotaStateAvailable, &zero); got != QuotaStateExhausted {
		t.Fatalf("inconsistent zero NormalizeQuotaState() = %q, want exhausted", got)
	}
	if got := NormalizeQuotaState(QuotaStateExhausted, &remaining); got != QuotaStateAvailable {
		t.Fatalf("inconsistent positive NormalizeQuotaState() = %q, want available", got)
	}
}

func TestSafeQuotaErrorRedactsUnknownMessages(t *testing.T) {
	if got := SafeQuotaError("proxy password secret"); got != "refresh_failed" {
		t.Fatalf("SafeQuotaError() = %q, want refresh_failed", got)
	}
	if got := SafeQuotaError("RATE_LIMITED"); got != "rate_limited" {
		t.Fatalf("SafeQuotaError() = %q, want rate_limited", got)
	}
}

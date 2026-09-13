package auth

import (
	"net/http"
	"testing"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestSessionAffinityFingerprintOnlyUsesCapturedIdentity(t *testing.T) {
	selector := NewSessionAffinitySelector(nil)
	t.Cleanup(selector.Stop)
	headers := http.Header{"Session-Id": {"original"}}
	body := []byte(`{"input":"initial user message"}`)
	opts := core.Options{Headers: headers, OriginalRequest: body}
	if SessionAffinityFingerprint(opts) != "" {
		t.Fatal("uncaptured request enabled shared pool affinity")
	}
	captured := selector.withBasicAffinityIdentity(t.Context(), core.Request{}, opts)
	want := SessionAffinityFingerprint(captured)
	if len(want) != 64 {
		t.Fatal("captured session did not produce a bounded digest")
	}
	headers.Set("Session-Id", "changed")
	clear(body)
	captured.OriginalRequest = nil
	if got := SessionAffinityFingerprint(captured); got != want {
		t.Fatal("released body or changed headers replaced the snapshot")
	}
	absent := selector.withBasicAffinityIdentity(t.Context(), core.Request{}, core.Options{})
	absent.OriginalRequest = []byte(`{"prompt_cache_key":"automatic"}`)
	if SessionAffinityFingerprint(absent) != "" {
		t.Fatal("automatic provider identity replaced an empty routing snapshot")
	}
}

func TestSessionAffinityFingerprintPreservesExactCacheKeysAndUserInput(t *testing.T) {
	selector := NewSessionAffinitySelector(nil)
	t.Cleanup(selector.Stop)
	seen := map[string]bool{}
	for _, body := range []string{
		`{"prompt_cache_key":"cache"}`, `{"prompt_cache_key":" cache "}`,
		`{"instructions":"shared","input":"first user"}`, `{"instructions":"shared","input":"different user"}`,
	} {
		opts := selector.withBasicAffinityIdentity(t.Context(), core.Request{Payload: []byte(body)}, core.Options{})
		digest := SessionAffinityFingerprint(opts)
		if len(digest) != 64 || seen[digest] {
			t.Fatal("different routing identities were merged")
		}
		seen[digest] = true
	}
	if got := SessionAffinityFingerprint(selector.withBasicAffinityIdentity(t.Context(), core.Request{Payload: []byte(`{"instructions":"shared"}`)}, core.Options{})); got != "" {
		t.Fatal("system-only request established pool affinity")
	}
}

func TestSessionAffinityFingerprintSharesExplicitSlotsWithoutSharingScopedBindings(t *testing.T) {
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Subagents: true})
	t.Cleanup(selector.Stop)
	firstCtx := affinityCallerContext(t, "caller-a", "codex")
	secondCtx := affinityCallerContext(t, "caller-b", "codex")
	opts := subagentOptions("child", "parent", false)
	first := withAffinityIdentity(firstCtx, core.Request{}, opts)
	second := withAffinityIdentity(secondCtx, core.Request{}, opts)
	if SessionAffinityFingerprint(first) == "" || SessionAffinityFingerprint(first) != SessionAffinityFingerprint(second) {
		t.Fatal("same explicit session did not share the provider slot seed")
	}
	firstID, _ := selector.sessionIDs(firstCtx, first)
	secondID, _ := selector.sessionIDs(secondCtx, second)
	if firstID == secondID {
		t.Fatal("pool sharing removed the scoped routing authorization boundary")
	}
	parent := withAffinityIdentity(firstCtx, core.Request{}, subagentOptions("parent", "", false))
	if SessionAffinityFingerprint(first) == SessionAffinityFingerprint(parent) {
		t.Fatal("child preference replaced its own identity with the parent")
	}
}

func TestSessionAffinityFingerprintReusesScopedHistoryAnchor(t *testing.T) {
	ctx := affinityCallerContext(t, "caller-a", "codex")
	opts := core.Options{Headers: make(http.Header), SourceFormat: sdktranslator.FormatCodex}
	first := withAffinityIdentity(ctx, core.Request{Payload: []byte(`{"instructions":"system","input":"first"}`)}, opts, true)
	continuedBody := []byte(`{"instructions":"system","input":[{"role":"user","content":"first"},{"role":"assistant","content":"answer"},{"role":"user","content":"next"}]}`)
	next := withAffinityIdentity(ctx, core.Request{Payload: continuedBody}, opts, true)
	otherCaller := withAffinityIdentity(affinityCallerContext(t, "caller-b", "codex"), core.Request{Payload: continuedBody}, opts, true)
	want := SessionAffinityFingerprint(first)
	clear(continuedBody)
	if want == "" || SessionAffinityFingerprint(next) != want || SessionAffinityFingerprint(otherCaller) == want {
		t.Fatal("history growth, release or caller scope changed the pool anchor incorrectly")
	}
	for _, body := range []string{`{"instructions":"system"}`, `{"previous_response_id":"opaque","input":"next"}`} {
		captured := withAffinityIdentity(ctx, core.Request{Payload: []byte(body)}, opts, true)
		if SessionAffinityFingerprint(captured) != "" {
			t.Fatal("unusable history established shared pool affinity")
		}
	}
}

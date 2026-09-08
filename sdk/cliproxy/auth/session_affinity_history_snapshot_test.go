package auth

import (
	"net/http"
	"testing"
	"time"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/session"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestAffinityHistoryCaptureIsOptionalAndDetached(t *testing.T) {
	ctx := affinityCallerContext(t, "caller-a", "codex")
	body := []byte(`{"input":[{"role":"user","content":"question"},{"role":"assistant","content":"answer"}]}`)
	opts := core.Options{Headers: make(http.Header), SourceFormat: sdktranslator.FormatCodex}
	req := core.Request{Payload: body}
	without := withAffinityIdentity(ctx, req, opts)
	if captured := captureAffinityIdentity(ctx, req, without, true); captured.history != nil || captured.legacyPrimary == "" {
		t.Fatal("disabled history capture changed or upgraded an existing snapshot")
	}
	frozen := withAffinityIdentity(ctx, req, opts, true)
	captured := captureAffinityIdentity(ctx, req, frozen)
	if captured.history == nil || !captured.history.Usable() || captured.identity.SessionID != "" {
		t.Fatal("logical request did not capture complete history")
	}
	matcher := session.NewHistoryMatcher(time.Hour)
	matcher.Bind("scope", *captured.history, "credential")
	clear(body)
	if next := captureAffinityIdentity(ctx, core.Request{}, withAffinityIdentity(ctx, core.Request{}, frozen, false)); next.history != captured.history {
		t.Fatal("release or a later policy replaced the snapshot")
	}
	nextHistory := session.FingerprintHistory(sdktranslator.FormatCodex, []byte(`{"input":[{"role":"user","content":"question"},{"role":"assistant","content":"answer"},{"role":"user","content":"next"}]}`))
	if match, ok := matcher.Match("scope", nextHistory); !ok || match.AuthID != "credential" {
		t.Fatal("released request changed the retained history")
	}
}

func TestAffinityHistoryNeverOverridesExplicitIdentityOrInventsCallerScope(t *testing.T) {
	body := []byte(`{"input":"question"}`)
	ctx := affinityCallerContext(t, "caller-a", "codex")
	opts := subagentOptions("explicit", "", false)
	opts.SourceFormat = sdktranslator.FormatCodex
	if captured := captureAffinityIdentity(ctx, core.Request{Payload: body}, opts, true); captured.history != nil || captured.identity.SessionID != "codex:explicit" {
		t.Fatal("history replaced a reliable explicit identity")
	}
	opts.Headers = make(http.Header)
	if captured := captureAffinityIdentity(t.Context(), core.Request{Payload: body}, opts, true); captured.history != nil || captured.scope != "" {
		t.Fatal("anonymous history created shared inference state")
	}
	opts.Metadata = map[string]any{core.ExecutionSessionMetadataKey: "generated-connection"}
	if captured := captureAffinityIdentity(ctx, core.Request{Payload: body}, opts, true); captured.history == nil || !captured.history.Usable() || captured.identity.SessionID != "" {
		t.Fatal("generated transport identity masked complete history")
	}
	if captured := captureAffinityIdentity(ctx, core.Request{Payload: body}, opts, false); captured.history != nil || captured.identity.SessionID != "execution:generated-connection" {
		t.Fatal("disabled mode changed execution-session fallback")
	}
	partial := []byte(`{"previous_response_id":"opaque","input":"continuation"}`)
	if captured := captureAffinityIdentity(ctx, core.Request{Payload: partial}, opts, true); captured.history == nil || captured.history.Usable() || captured.identity.SessionID != "execution:generated-connection" {
		t.Fatal("partial history replaced the transport fallback")
	}
}

package helps

import (
	"bytes"
	"encoding/base64"
	"net/http"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func xaiIdentityTestAuth(seed byte) *coreauth.Auth {
	return &coreauth.Auth{ID: "fixture", Provider: "xai", Metadata: map[string]any{XAIIdentitySeedKey: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{seed}, 32))}}
}

func TestXAIIdentityCombinationsProtectClientValuesAndContent(t *testing.T) {
	body := []byte(`{"prompt_cache_key":"cache-A","input":[{"role":"user","content":"session-A"}],"tools":[{"type":"function","name":"session-A","parameters":{"const":"cache-A"}}]}`)
	original := bytes.Clone(body)
	for flags := range 16 {
		cfg := &config.Config{XAI: config.XAIConfig{PassthroughClientIdentity: flags&1 != 0, SpoofSessionIdentity: flags&2 != 0, SessionIdentityConvergence: flags&4 != 0, IdentityConfuse: flags&8 != 0}}
		opts := core.Options{SourceFormat: sdktranslator.FormatCodex, Headers: http.Header{"X-Grok-Session-Id": {"session-A"}, "X-Grok-Conv-Id": {"side-call"}, "X-Grok-Req-Id": {"request-A"}}}
		plan, err := NewXAIRequestPlan(t.Context(), cfg, core.Request{Payload: body}, opts)
		if err != nil {
			t.Fatal(err)
		}
		projected, identity, err := plan.Project(xaiIdentityTestAuth(1), body, nil, "")
		if err != nil {
			t.Fatal(err)
		}
		if gjson.GetBytes(projected, "input").Raw != gjson.GetBytes(body, "input").Raw || gjson.GetBytes(projected, "tools").Raw != gjson.GetBytes(body, "tools").Raw {
			t.Fatalf("flags %d modified user content", flags)
		}
		if flags&1 != 0 && (identity.Session != "session-A" || identity.Conversation != "side-call" || identity.CacheKey != "cache-A") {
			t.Fatalf("flags %d lost a protected identity: %#v", flags, identity)
		}
		if flags == 2 && (identity.Session != "session-A" || identity.Conversation != "side-call" || identity.CacheKey != "cache-A") {
			t.Fatal("fill-missing overwrote existing fields")
		}
		if flags == 8 && (identity.Session == identity.Conversation || identity.Conversation == identity.CacheKey || identity.CacheKey == "cache-A") {
			t.Fatal("stable obfuscation collapsed distinct explicit identities")
		}
		if !bytes.Equal(body, original) {
			t.Fatal("projection mutated caller payload")
		}
	}
}

func TestXAIIdentitySlotsStableAcrossCallersModelsAndRestarts(t *testing.T) {
	cfg := &config.Config{XAI: config.XAIConfig{SessionIdentityConvergence: true, SessionIdentityPoolSize: 4}}
	auth := xaiIdentityTestAuth(2)
	var first XAIIdentityProjection
	for index := range 4 {
		opts := core.Options{Headers: http.Header{"X-Grok-Session-Id": {"shared-session"}}, Metadata: map[string]any{core.CallerScopeMetadataKey: string(rune('a' + index))}}
		plan, err := NewXAIRequestPlan(t.Context(), cfg, core.Request{Model: string(rune('a' + index)), Payload: []byte(`{}`)}, opts)
		if err != nil {
			t.Fatal(err)
		}
		_, identity, err := plan.Project(auth.Clone(), []byte(`{}`), nil, "")
		if err != nil {
			t.Fatal(err)
		}
		if index == 0 {
			first = identity
		} else if identity.Session != first.Session || identity.Slot != first.Slot || identity.Agent != first.Agent {
			t.Fatal("caller, model or clone changed the slot")
		}
		if identity.Slot < 0 || identity.Slot >= 4 || identity.Session != identity.Conversation || identity.Session != identity.CacheKey {
			t.Fatal("invalid converged identity")
		}
	}
	plan, _ := NewXAIRequestPlan(t.Context(), cfg, core.Request{Payload: []byte(`{}`)}, core.Options{Headers: http.Header{"X-Grok-Session-Id": {"shared-session"}}})
	_, other, _ := plan.Project(xaiIdentityTestAuth(3), []byte(`{}`), nil, "")
	if other.Session == first.Session || other.Agent == first.Agent {
		t.Fatal("different credentials share an identity")
	}
}

func TestXAIIdentityNoHistoryOrSeedWorkWhenOffAndRetryIsFrozen(t *testing.T) {
	request := core.Request{Payload: []byte(`{"messages":[{"role":"user","content":"history"}]}`)}
	plan, err := NewXAIRequestPlan(t.Context(), &config.Config{}, request, core.Options{})
	if err != nil || plan.Nonce != "" || plan.Basis != "" || plan.ShouldPrepareRequestAuth(&coreauth.Auth{}) {
		t.Fatal("disabled policies prepared an identity")
	}
	noHistory := false
	cfg := &config.Config{Routing: config.RoutingConfig{SessionAffinityUseHistory: &noHistory}, XAI: config.XAIConfig{SessionIdentityConvergence: true}}
	plan, err = NewXAIRequestPlan(t.Context(), cfg, request, core.Options{Metadata: map[string]any{core.CallerScopeMetadataKey: "caller"}})
	if err != nil || plan.Source != "request" {
		t.Fatalf("history was inferred despite the switch: %v", err)
	}
	_, first, _ := plan.Project(xaiIdentityTestAuth(4), []byte(`{}`), nil, "")
	_, retry, _ := plan.Project(xaiIdentityTestAuth(4), []byte(`{}`), nil, "")
	_, failover, _ := plan.Project(xaiIdentityTestAuth(5), []byte(`{}`), nil, "")
	if failover.Slot != first.Slot || failover.Request != first.Request {
		t.Fatal("anonymous failover changed the request-owned slot or request ID")
	}
	if first.Session != retry.Session || first.Request != retry.Request {
		t.Fatal("retry changed a request-owned identity")
	}
	headers := make(http.Header)
	if plan.ApplyAttemptHeader(headers) != 0 || headers.Get("x-grok-transient-retry") != "" || plan.ApplyAttemptHeader(headers) != 1 || headers.Get("x-grok-transient-retry") != "1" {
		t.Fatal("retry count is incorrect")
	}
}

func TestXAIGlobalHeadersAndRequestSnapshotAreDetached(t *testing.T) {
	cfg := &config.Config{XAI: config.XAIConfig{HeaderDefaults: config.XAIHeaderDefaults{ClientVersion: "1.2.3"}, Headers: map[string]string{"X-Test": "global"}, RequestDefaults: map[string]any{"reasoning": map[string]any{"effort": "high"}}}}
	plan, err := NewXAIRequestPlan(t.Context(), cfg, core.Request{}, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	cfg.XAI.Headers["X-Test"] = "changed"
	cfg.XAI.RequestDefaults["reasoning"].(map[string]any)["effort"] = "low"
	req, _ := http.NewRequest("GET", "https://cli-chat-proxy.grok.com/v1/models", nil)
	ApplyXAIResourceHeaders(req, &coreauth.Auth{Attributes: map[string]string{"header:X-Test": "credential"}}, plan.Config)
	if req.Header.Get("X-Test") != "credential" || req.Header.Get("User-Agent") != "xai-grok-workspace/1.2.3" || req.Header.Get("X-XAI-Token-Auth") != "xai-grok-cli" {
		t.Fatal("global headers or credential precedence are incorrect")
	}
	body := ApplyXAIDefaults([]byte(`{"stream_tool_calls":false,"temperature":0}`), plan.Config.XAI.RequestDefaults)
	if gjson.GetBytes(body, "reasoning.effort").String() != "high" {
		t.Fatal("request defaults changed after capture")
	}
	body = ApplyXAIDefaults(body, map[string]any{"stream_tool_calls": true, "temperature": 1})
	if gjson.GetBytes(body, "stream_tool_calls").Bool() || gjson.GetBytes(body, "temperature").Float() != 0 {
		t.Fatal("defaults overwrote explicit zero or false")
	}
}

func TestXAISeedPreparationOnlyChangesOwnedMetadata(t *testing.T) {
	p := &XAIRequestPlan{Config: &config.Config{XAI: config.XAIConfig{SpoofSessionIdentity: true}}}
	auth := &coreauth.Auth{ID: "fixture", Metadata: map[string]any{"headers": map[string]any{"X-Test": "value"}}}
	prepared, err := p.PrepareRequestAuth(t.Context(), auth)
	if err != nil || len(XAIIdentitySeed(prepared)) != 32 || len(XAIIdentitySeed(auth)) != 0 {
		t.Fatal("seed preparation was not detached")
	}
	if p.ShouldPrepareRequestAuth(prepared) {
		t.Fatal("a valid persisted seed would be regenerated")
	}
}

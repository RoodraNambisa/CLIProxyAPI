package auth

import (
	"context"
	"testing"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

func TestSanitizeDownstreamWebsocketFallbackRequestStripsGenerateForHTTPFallback(t *testing.T) {
	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
	req := cliproxyexecutor.Request{Payload: []byte(`{"model":"test-model","generate":true,"input":[{"type":"message"}]}`)}

	got := sanitizeDownstreamWebsocketFallbackRequest(ctx, &Auth{ID: "auth-1"}, req)

	if gjson.GetBytes(got.Payload, "generate").Exists() {
		t.Fatalf("generate was not stripped from HTTP fallback payload: %s", got.Payload)
	}
	if !gjson.GetBytes(req.Payload, "generate").Exists() {
		t.Fatalf("original request payload should not be mutated: %s", req.Payload)
	}
}

func TestSanitizeDownstreamWebsocketFallbackRequestKeepsGenerateForWebsocketAuth(t *testing.T) {
	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
	req := cliproxyexecutor.Request{Payload: []byte(`{"model":"test-model","generate":true}`)}
	auth := &Auth{ID: "auth-1", Attributes: map[string]string{"websockets": "true"}}

	got := sanitizeDownstreamWebsocketFallbackRequest(ctx, auth, req)

	if !gjson.GetBytes(got.Payload, "generate").Bool() {
		t.Fatalf("generate should be preserved for websocket auth payload: %s", got.Payload)
	}
}

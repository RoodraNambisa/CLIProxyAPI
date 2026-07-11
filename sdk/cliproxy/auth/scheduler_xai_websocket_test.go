package auth

import (
	"context"
	"testing"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestSchedulerPickXAIWebsocketPrefersEnabledCredentials(t *testing.T) {
	scheduler := newSchedulerForTest(
		&RoundRobinSelector{},
		&Auth{ID: "xai-http", Provider: "xai"},
		&Auth{ID: "xai-ws-a", Provider: "xai", Attributes: map[string]string{"websockets": "true"}},
		&Auth{ID: "xai-ws-b", Provider: "xai", Attributes: map[string]string{"websockets": "true"}},
	)

	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
	for index, wantID := range []string{"xai-ws-a", "xai-ws-b", "xai-ws-a"} {
		got, err := scheduler.pickSingle(ctx, "xai", "", cliproxyexecutor.Options{}, nil)
		if err != nil {
			t.Fatalf("pickSingle() #%d error = %v", index, err)
		}
		if got == nil || got.ID != wantID {
			t.Fatalf("pickSingle() #%d = %v, want %s", index, got, wantID)
		}
	}
}

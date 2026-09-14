package executor

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelconfig"
)

func TestChatGPTWebRemoteComputePreservesOfficialHTTPFlow(t *testing.T) {
	compute, err := chatgptweb.NewSentinelComputeServer(sentinelconfig.Server{Enabled: true, APIKeys: []string{"solver-key"}})
	if err != nil {
		t.Fatal(err)
	}
	defer compute.Close()
	computeHTTP := httptest.NewServer(compute)
	defer computeHTTP.Close()
	var prepare, finalize atomic.Int32
	official := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			_, _ = io.WriteString(w, `<html><script src="/backend-api/sentinel/20260219f9f6/sdk.js"></script></html>`)
		case "/backend-api/sentinel/chat-requirements/prepare":
			prepare.Add(1)
			var input map[string]string
			_ = json.NewDecoder(r.Body).Decode(&input)
			if input["p"] == "" {
				t.Error("missing requirements token")
			}
			_, _ = io.WriteString(w, `{"prepare_token":"prepared"}`)
		case "/backend-api/sentinel/chat-requirements/finalize":
			finalize.Add(1)
			_, _ = io.WriteString(w, `{"token":"finalized"}`)
		default:
			t.Errorf("unexpected official request: %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer official.Close()
	cfg := &config.Config{}
	cfg.ChatGPTWeb.Sentinel.Mode = "remote"
	cfg.ChatGPTWeb.Sentinel.Remote = sentinelconfig.Remote{Nodes: []sentinelconfig.Node{{Name: "one", URL: computeHTTP.URL, APIKey: "solver-key"}}}
	executor := NewChatGPTWebExecutor(cfg, nil)
	defer func() { _ = executor.Close() }()
	executor.runtimeBaseURL = official.URL
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	for _, scope := range []string{"images", "chat"} {
		before := compute.Snapshot().GoSuccess
		result, err := executor.chatGPTWebRequirements(chatgptweb.WithSentinelComputeScope(context.Background(), scope), client, credential)
		if err != nil || result.Token != "finalized" {
			t.Fatalf("requirements: %v %q", err, result.Token)
		}
		after := compute.Snapshot().GoSuccess
		if scope == "images" && after-before != 2 || scope == "chat" && after != before {
			t.Fatal("remote scopes not respected")
		}
	}
	if prepare.Load() != 2 || finalize.Load() != 2 {
		t.Fatal("official requests replayed")
	}
}

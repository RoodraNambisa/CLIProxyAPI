package executor

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
)

func TestChatGPTWebImageAliasesUseAutoPictureConversation(t *testing.T) {
	var mu sync.Mutex
	var bodies []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			_, _ = io.WriteString(w, "<html></html>")
		case "/backend-api/sentinel/chat-requirements/prepare":
			_, _ = io.WriteString(w, `{"prepare_token":"prepare"}`)
		case "/backend-api/sentinel/chat-requirements/finalize":
			_, _ = io.WriteString(w, `{"token":"requirements"}`)
		case "/backend-api/f/conversation/prepare", "/backend-api/f/conversation":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			mu.Lock()
			bodies = append(bodies, body)
			mu.Unlock()
			if r.URL.Path == "/backend-api/f/conversation/prepare" {
				_, _ = io.WriteString(w, `{"conduit_token":"conduit"}`)
			} else {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: [DONE]\n\n")
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	executor.runtimeBaseURL = server.URL
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	aliases := []string{"gpt-image-2", "gpt-image-2.5", "gpt-image-2.5-flare", "gpt-image-2.5-sunburst", "my-web-image"}
	for _, alias := range aliases {
		execution, errBegin := executor.beginChatGPTWebImage(context.Background(), client, credential, &chatGPTWebPreparedRequest{
			routeModel: alias,
			request:    helps.ChatGPTWebRequest{Image: &helps.ChatGPTWebImageRequest{Model: alias, Prompt: "draw"}},
		})
		if errBegin != nil {
			t.Fatal(errBegin)
		}
		if errClose := execution.response.Body.Close(); errClose != nil {
			t.Fatal(errClose)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != len(aliases)*2 {
		t.Fatalf("conversation requests=%d", len(bodies))
	}
	for _, body := range bodies {
		if body["model"] != "auto" {
			t.Fatalf("alias changed conversation model: %v", body["model"])
		}
		hints, ok := body["system_hints"].([]any)
		if !ok || len(hints) != 1 || hints[0] != "picture_v2" {
			t.Fatalf("picture hints = %v", body["system_hints"])
		}
		if _, exists := body["image_model"]; exists {
			t.Fatal("alias leaked as an upstream image selector")
		}
	}
}

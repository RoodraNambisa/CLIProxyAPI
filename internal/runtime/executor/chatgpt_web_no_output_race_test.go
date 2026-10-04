package executor

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
)

func TestPollChatGPTWebImageEmptyTerminalWaitsForTaskEvidence(t *testing.T) {
	testChatGPTWebImageEmptyTerminalWaitsForTaskEvidence(t, false)
}

func TestChatGPTWebEmptyTerminalLookupRemainsBounded(t *testing.T) {
	for _, mode := range []string{"empty tasks", "unsupported tasks", "pending task", "retryable task error"} {
		t.Run(mode, func(t *testing.T) {
			var snapshots atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/backend-api/tasks":
					if mode == "unsupported tasks" {
						w.WriteHeader(http.StatusMethodNotAllowed)
						return
					}
					if mode == "retryable task error" {
						http.NotFound(w, r)
						return
					}
					if mode == "pending task" {
						_, _ = io.WriteString(w, `{"tasks":[{"conversation_id":"genuine-empty","status":"running","type":"image_gen"}]}`)
					} else {
						_, _ = io.WriteString(w, `{"tasks":[]}`)
					}
				case "/backend-api/conversation/genuine-empty":
					snapshots.Add(1)
					_, _ = io.WriteString(w, `{"mapping":{"done":{"message":{"author":{"role":"tool"},"status":"finished_successfully","metadata":{"async_task_type":"image_gen","is_complete":true},"content":{"parts":[]}}}}}`)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			e := NewChatGPTWebExecutor(nil, nil)
			e.runtimeBaseURL = server.URL
			disableChatGPTWebImagePollWaits(e)
			e.imagePollInterval = 5 * time.Millisecond
			e.imageMaxPolls = 20
			client, credential, err := e.newRuntimeClient(chatGPTWebRuntimeAuth())
			if err != nil {
				t.Fatal(err)
			}
			defer client.CloseIdleConnections()
			budget := 2 * time.Second
			waiting := mode == "pending task" || mode == "retryable task error"
			if waiting {
				budget = 60 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(t.Context(), budget)
			defer cancel()
			err = e.pollChatGPTWebImageConversation(ctx, client, credential, &helps.ChatGPTWebImageAccumulator{ConversationID: "genuine-empty"}, nil, false)
			if waiting {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("pending task treated as finished: %v", err)
				}
			} else {
				var noOutput *chatGPTWebImageNoOutputResultError
				if !errors.As(err, &noOutput) || snapshots.Load() < 2 {
					t.Fatalf("empty terminal not confirmed: %v, snapshots=%d", err, snapshots.Load())
				}
			}
		})
	}
}

func TestConsumeChatGPTWebImageEmptyTerminalWaitsForTaskEvidence(t *testing.T) {
	testChatGPTWebImageEmptyTerminalWaitsForTaskEvidence(t, true)
}

func testChatGPTWebImageEmptyTerminalWaitsForTaskEvidence(t *testing.T, stream bool) {
	var conversations atomic.Int32
	var delivered atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/backend-api/tasks":
			// The conversation completes before the task endpoint reveals its output.
			timer := time.NewTimer(30 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-r.Context().Done():
				return
			case <-timer.C:
			}
			delivered.Store(true)
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
				"conversation_id": "empty-terminal", "status": "completed",
				"image_gen_message": map[string]any{
					"author": map[string]any{"role": "tool"}, "status": "finished_successfully",
					"metadata": map[string]any{"async_task_type": "image_gen"},
					"content":  map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://generated"}}},
				},
			}}})
		case "/backend-api/conversation/empty-terminal":
			conversations.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{
				"done": map[string]any{"message": map[string]any{
					"author": map[string]any{"role": "tool"}, "status": "finished_successfully",
					"metadata": map[string]any{"async_task_type": "image_gen", "is_complete": true},
					"content":  map[string]any{"parts": []any{}},
				}},
			}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	e := NewChatGPTWebExecutor(nil, nil)
	e.runtimeBaseURL = server.URL
	disableChatGPTWebImagePollWaits(e)
	e.imagePollInterval = 10 * time.Millisecond
	e.imageMaxPolls = 12
	client, credential, err := e.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	acc := &helps.ChatGPTWebImageAccumulator{ConversationID: "empty-terminal"}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if stream {
		body := newChatGPTWebBlockingBody("data: {\"conversation_id\":\"empty-terminal\"}\n\n")
		defer func() { _ = body.Close() }()
		acc, err = e.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: body})
	} else {
		err = e.pollChatGPTWebImageConversation(ctx, client, credential, acc, nil, false)
	}
	if err != nil {
		t.Fatalf("premature empty result: %v (task delivered=%v, snapshots=%d)", err, delivered.Load(), conversations.Load())
	}
	if len(acc.FileIDs) != 1 || acc.FileIDs[0] != "generated" {
		t.Fatalf("lost task output: %v", acc.FileIDs)
	}
}

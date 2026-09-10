package executor

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestClaudeExecutorKeepsResponsesRefusalHistory(t *testing.T) {
	requests := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requests <- body
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"fixture","type":"message","role":"assistant","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":1}}`)
	}))
	defer server.Close()
	exec := NewClaudeExecutor(&config.Config{})
	auth := &coreauth.Auth{Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}
	body := []byte(`{"input":[{"role":"user","content":"first"},{"type":"message","role":"assistant","content":[{"type":"refusal","refusal":"cannot help with that"}]},{"role":"user","content":"next"}]}`)
	_, err := exec.Execute(t.Context(), auth, core.Request{Model: "claude-sonnet-4-5", Payload: body}, core.Options{SourceFormat: translator.FormatOpenAIResponse})
	if err != nil {
		t.Fatal(err)
	}
	out := <-requests
	if gjson.GetBytes(out, "messages.1.role").String() != "assistant" || gjson.GetBytes(out, "messages.1.content").String() != "cannot help with that" {
		t.Fatal("actual upstream request lost the refusal history")
	}
}

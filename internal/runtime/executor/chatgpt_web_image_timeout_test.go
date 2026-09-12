package executor

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestImageRequestTimeoutWebReleasesLifecycle(t *testing.T) {
	core.ConfigureChatGPTWebImageAdmissions(1, 0, 1)
	t.Cleanup(func() { core.ConfigureChatGPTWebImageAdmissions(64, 64, 8) })
	stopped := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			_, _ = io.WriteString(w, `<html><script src="/c/build/_next/a.js"></script></html>`)
			return
		}
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		stopped <- struct{}{}
	}))
	defer server.Close()
	e := NewChatGPTWebExecutor(nil, nil)
	defer e.Close()
	e.runtimeBaseURL = server.URL
	b := core.NewImageRequestBudget(t.Context(), time.Now(), 0, time.Second)
	defer b.Close()
	ctx, cancel := b.Bind(t.Context())
	defer cancel()
	if err := b.Select("chatgpt-web"); err != nil {
		t.Fatal(err)
	}
	_, err := e.Execute(ctx, chatGPTWebRuntimeAuth(), core.Request{Model: "gpt-image-2", Payload: []byte(`{"model":"gpt-image-2","input":"draw","tools":[{"type":"image_generation"}]}`)}, core.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex})
	if !core.IsImageRequestTimeout(core.ImageRequestContextError(ctx, err)) {
		t.Fatalf("deadline lost: %v", err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("response socket remained active")
	}
	if s := core.ChatGPTWebImageExecutionAdmissionSnapshot(); s.Active != 0 || s.Queued != 0 {
		t.Fatalf("admission leaked: %#v", s)
	}
	if s := ChatGPTWebImageTasksSnapshot(); s.Active != 0 {
		t.Fatalf("task leaked: %#v", s)
	}
}

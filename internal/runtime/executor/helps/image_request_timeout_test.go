package helps

import (
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestImageRequestTimeoutHTTP2BodyAndIsolation(t *testing.T) {
	stopped := make(chan struct{}, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 {
			t.Errorf("want HTTP/2, got %s", r.Proto)
		}
		if r.URL.Path == "/healthy" {
			_, _ = io.WriteString(w, "ok")
			return
		}
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		stopped <- struct{}{}
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	b := core.NewImageRequestBudget(t.Context(), time.Now(), time.Second, 0)
	defer b.Close()
	ctx, cancel := b.Bind(t.Context())
	defer cancel()
	_ = b.Select("codex")
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/blocked", nil)
	resp, err := DoUpstreamHTTPRequest(server.Client(), req)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if !core.IsImageRequestTimeout(err) {
		t.Fatalf("body error: %v", err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("HTTP/2 stream not reset")
	}
	healthy, err := server.Client().Get(server.URL + "/healthy")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(healthy.Body)
	_ = healthy.Body.Close()
	if err != nil || string(data) != "ok" {
		t.Fatalf("other stream affected: %s %v", data, err)
	}
}

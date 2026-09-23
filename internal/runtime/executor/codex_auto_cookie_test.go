package executor

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexcookie"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

type autoCookieExecutorTransport func(*http.Request) (*http.Response, error)

func (f autoCookieExecutorTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestAutoCookieExecutorHTTPModesAndGuard(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "http", true: "sse"}[stream], func(t *testing.T) {
			cfg := &config.Config{Codex: config.CodexConfig{AutoCookie: true, EnforceSoftwareIdentity: new(false)}}
			a := &auth.Auth{ID: t.Name(), Provider: "codex", Attributes: map[string]string{"header:Cookie": "__oailb=custom"}, Metadata: map[string]any{"account_id": "owner", "access_token": "fixture"}}
			codexcookie.Default.Sync(true, map[string]string{a.ID: helps.StateCredential(a, "").Owner})
			t.Cleanup(func() { codexcookie.Default.Sync(false, nil) })
			var sent []string
			ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", autoCookieExecutorTransport(func(r *http.Request) (*http.Response, error) {
				sent = append(sent, r.Header.Get("Cookie"))
				body := `data: {"type":"response.created","response":{"id":"resp_fixture","model":"gpt-5.5","status":"in_progress"}}` + "\n\n" + `data: {"type":"response.completed","response":{"id":"resp_fixture","model":"gpt-5.5","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1}}}` + "\n\n"
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}, "Set-Cookie": {"__oailb=server; Path=/; Max-Age=120"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			}))
			e := NewCodexExecutor(cfg)
			for range 2 {
				req := core.Request{Model: "gpt-5.5", Payload: []byte(`{"model":"gpt-5.5","input":"hi"}`)}
				opts := core.Options{SourceFormat: translator.FormatOpenAIResponse}
				if stream {
					result, err := e.ExecuteStream(ctx, a, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
					}
				} else {
					if _, err := e.Execute(ctx, a, req, opts); err != nil {
						t.Fatal(err)
					}
				}
			}
			if len(sent) != 2 || sent[0] != "" || sent[1] != "__oailb=server" {
				t.Fatal(sent)
			}
			cfg.Codex.ResponseGuard = config.CodexResponseGuardConfig{Enabled: true, CodexResponseGuardSettings: config.CodexResponseGuardSettings{Mode: new("enforce"), MatchModel: new(true)}}
			_, err := e.Execute(ctx, a, core.Request{Model: "different", Payload: []byte(`{"model":"different","input":"hi"}`)}, core.Options{SourceFormat: translator.FormatOpenAIResponse})
			if err == nil {
				t.Fatal("expected guard rejection")
			}
			store := codexcookie.Default.Acquire(a.ID, helps.StateCredential(a, "").Owner)
			if store.Header("https://chatgpt.com/backend-api/codex/responses") != "__oailb=server" {
				t.Fatal("guard rejection cleared cookies")
			}
		})
	}
}

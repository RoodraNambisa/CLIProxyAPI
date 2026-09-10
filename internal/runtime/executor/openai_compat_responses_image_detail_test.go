package executor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestOpenAICompatResponsesUserImageDetail(t *testing.T) {
	for _, tc := range []struct{ field, want string }{
		{``, ""}, {`,"detail":"low"`, "low"}, {`,"detail":"high"`, "high"}, {`,"detail":"auto"`, "auto"},
		{`,"detail":"original"`, "high"}, {`,"detail":" LOW "`, "low"}, {`,"detail":"future"`, ""}, {`,"detail":null`, ""}, {`,"detail":42`, ""},
	} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%t", tc.field, stream), func(t *testing.T) {
				calls := 0
				ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", streamTerminalRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					calls++
					body, errRead := io.ReadAll(r.Body)
					if errRead != nil {
						return nil, errRead
					}
					message := gjson.GetBytes(body, `messages.#(role=="user")`)
					detail := message.Get("content.1.image_url.detail")
					if detail.String() != tc.want || detail.Exists() != (tc.want != "") {
						t.Errorf("image detail=%s, want %q", detail.Raw, tc.want)
					}
					if message.Get("content.#").Int() != 3 || message.Get("content.0.text").String() != "before" || message.Get("content.1.image_url.url").String() != "https://fixture.invalid/image" || message.Get("content.2.text").String() != "after" {
						t.Error("image URL or content order changed")
					}
					response, contentType := `{"id":"fixture","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`, "application/json"
					if stream {
						contentType = "text/event-stream"
						response = "data: " + `{"id":"fixture","choices":[{"index":0,"delta":{"content":"ok"},"finish_reason":"stop"}]}` + "\n\ndata: [DONE]\n\n"
					}
					return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(response))}, nil
				}))
				e := NewOpenAICompatExecutor("image-detail", &config.Config{})
				auth := &coreauth.Auth{Provider: "image-detail", Attributes: map[string]string{"api_key": "fixture", "base_url": "https://fixture.invalid/v1"}}
				raw := []byte(`{"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"before"},{"type":"input_image","image_url":"https://fixture.invalid/image"` + tc.field + `},{"type":"input_text","text":"after"}]}]}`)
				before := bytes.Clone(raw)
				req, opts := core.Request{Model: "fixture", Payload: raw}, core.Options{SourceFormat: translator.FormatOpenAIResponse}
				if stream {
					result, err := e.ExecuteStream(ctx, auth, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
					}
				} else if _, err := e.Execute(ctx, auth, req, opts); err != nil {
					t.Fatal(err)
				}
				if calls != 1 || !bytes.Equal(raw, before) {
					t.Fatal("unexpected extra call or source mutation")
				}
			})
		}
	}
}

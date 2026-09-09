package executor

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

type alphaUsageCollector struct {
	mu      sync.Mutex
	authID  string
	records []usage.Record
	closed  bool
}

func (c *alphaUsageCollector) HandleUsage(_ context.Context, record usage.Record) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed && record.AuthID == c.authID {
		c.records = append(c.records, record)
	}
}

func TestCodexAlphaSearchUsageKeepsBaseModelAndReportedErrorTokens(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		withUsage bool
	}{
		{"success", 200, true}, {"reported failure", 503, true}, {"unknown usage", 200, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			collector := &alphaUsageCollector{authID: t.TempDir()}
			usage.RegisterPlugin(collector)
			t.Cleanup(func() { collector.mu.Lock(); collector.closed = true; collector.records = nil; collector.mu.Unlock() })
			body := `{"output":"result"}`
			if tc.withUsage {
				body = `{"output":"result","usage":{"input_tokens":5,"output_tokens":2,"input_tokens_details":{"cached_tokens":3},"output_tokens_details":{"reasoning_tokens":1},"total_tokens":7}}`
			}
			ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", alphaSearchRoundTripper(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: tc.status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, nil
			}))
			_, err := NewCodexExecutor(&config.Config{}).Execute(ctx, &auth.Auth{ID: collector.authID, Provider: "codex", Metadata: map[string]any{"access_token": "fixture"}}, core.Request{Model: "gpt-5.5(high)", Payload: []byte(`{"model":"gpt-5.5"}`)}, core.Options{SourceFormat: translator.FormatCodexAlphaSearch})
			if (err != nil) != (tc.status >= 400) {
				t.Fatal("incorrect search result")
			}
			if err := usage.DefaultManager().Barrier(t.Context()); err != nil {
				t.Fatal(err)
			}
			collector.mu.Lock()
			records := append([]usage.Record(nil), collector.records...)
			collector.mu.Unlock()
			if len(records) != 1 {
				t.Fatal("search did not publish exactly one final usage record")
			}
			record := records[0]
			if record.Model != "gpt-5.5" || record.Failed != (tc.status >= 400) || record.Stream {
				t.Fatal("search usage lost base model or outcome")
			}
			want := usage.Detail{}
			if tc.withUsage {
				want = usage.Detail{InputTokens: 5, OutputTokens: 2, CachedTokens: 3, ReasoningTokens: 1, TotalTokens: 7}
			}
			if record.Detail != want {
				t.Fatal("reported usage was lost or unknown tokens were estimated")
			}
		})
	}
}

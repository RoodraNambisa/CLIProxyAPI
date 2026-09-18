package auth

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

func responseModelFixture(t *testing.T) (*Manager, *Auth, core.Options) {
	t.Helper()
	m := NewManager(nil, nil, nil)
	m.SetConfig(&config.Config{SDKConfig: config.SDKConfig{ResponseModelRewrite: config.ResponseModelRewriteConfig{Enabled: true, Rules: []config.ResponseModelRewriteRule{{Providers: []string{"codex"}, AuthPriorities: []int{0}, RequestModels: []string{"team/gpt-6-*"}}}}}})
	auth, err := m.Register(WithSkipPersist(t.Context()), &Auth{ID: "response-model-fixture", Provider: "codex"})
	if err != nil {
		t.Fatal(err)
	}
	opts := ensureRequestedModelMetadata(core.Options{OriginalRequest: []byte(`{"model":"team/gpt-6-astra(high)","input":"hello"}`)}, "gpt-6-astra")
	return m, auth, opts
}

func TestResponseModelPolicyScopeAndSnapshot(t *testing.T) {
	m, auth, opts := responseModelFixture(t)
	ctx := m.WithRoutingPolicySnapshot(t.Context())
	if m.responseModelRewriteOptions(ctx, auth, opts, false) == nil {
		t.Fatal("default priority zero did not match")
	}
	for _, changed := range []*Auth{{ID: auth.ID, Provider: "xai"}, {ID: auth.ID, Provider: "codex", Attributes: map[string]string{"priority": "3"}}} {
		if m.responseModelRewriteOptions(ctx, changed, opts, false) != nil {
			t.Fatal("scope escaped credential rules")
		}
	}
	if m.responseModelRewriteOptions(core.WithSingleAttempt(ctx), auth, opts, false) != nil {
		t.Fatal("diagnostic request was rewritten")
	}
	m.SetConfig(&config.Config{})
	if m.responseModelRewriteOptions(t.Context(), auth, opts, false) != nil {
		t.Fatal("disabled policy was applied")
	}
	if m.responseModelRewriteOptions(ctx, auth, opts, false) == nil {
		t.Fatal("active request lost its snapshot")
	}
	for _, id := range []string{auth.Index, "other"} {
		m.SetConfig(&config.Config{SDKConfig: config.SDKConfig{ResponseModelRewrite: config.ResponseModelRewriteConfig{Enabled: true, Rules: []config.ResponseModelRewriteRule{{CredentialIDs: []string{id}}}}}})
		if matched := m.responseModelRewriteOptions(t.Context(), auth, opts, false) != nil; matched != (id == auth.Index) {
			t.Fatal("credential ID selector mismatch")
		}
	}
}

func TestResponseModelRewriteProtocolsAndOneCountPerStream(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			m, auth, opts := responseModelFixture(t)
			policy := m.responseModelRewriteOptions(t.Context(), auth, opts, stream)
			payload := []byte(`{"model":"gpt-5.6-luna","error":null,"response":{"model":"gpt-5.6-luna"},"message":{"model":"gpt-5.6-luna"},"output":[{"text":"gpt-5.6-luna","arguments":{"model":"gpt-5.6-luna"}}],"usage":{"total_tokens":123}}`)
			var actual []byte
			if stream {
				rewriter := NewStreamRewriter(*policy)
				frame := append(append([]byte("data: "), payload...), []byte("\n\n")...)
				for _, part := range [][]byte{frame[:42], frame[42:], frame} {
					actual = append(actual, rewriter.RewriteChunk(part)...)
				}
				actual = append(actual, rewriter.Finish()...)
				actual = bytes.TrimPrefix(bytes.SplitN(actual, []byte("\n\n"), 2)[0], []byte("data: "))
			} else {
				actual = rewriteModelWithOptions(payload, *policy)
			}
			for _, path := range []string{"model", "response.model", "message.model"} {
				if gjson.GetBytes(actual, path).String() != "team/gpt-6-astra(high)" {
					t.Fatalf("model field not rewritten: %s", actual)
				}
			}
			if gjson.GetBytes(actual, "output.0.arguments.model").String() != "gpt-5.6-luna" || gjson.GetBytes(actual, "usage.total_tokens").Int() != 123 {
				t.Fatal("business data changed")
			}
			stats := m.AuthResponseModelRewriteSummary(auth, true)
			if stats.Total != 1 || len(stats.Recent) != 1 || stats.Recent[0].OriginalModel != "gpt-5.6-luna" || !stats.Conditional {
				t.Fatalf("wrong counters: %+v", stats)
			}
		})
	}
}

func TestResponseModelRewriteIgnoresErrorsAndUnchangedFields(t *testing.T) {
	m, auth, opts := responseModelFixture(t)
	for _, raw := range []string{`{"error":{"model":"luna"},"model":"luna"}`, `{"type":"response.failed","response":{"model":"luna","error":{"code":"server_is_overloaded"}}}`, `{"model":null}`, `{"model":""}`, `{"output":[{"model":"luna"}]}`, `{"model":"team/gpt-6-astra(high)"}`} {
		result := rewriteModelWithOptions([]byte(raw), *m.responseModelRewriteOptions(t.Context(), auth, opts, false))
		if string(result) != raw {
			t.Fatalf("unexpected mutation: %s", result)
		}
	}
	if m.AuthResponseModelRewriteSummary(auth, true).Total != 0 {
		t.Fatal("unchanged/error responses were counted")
	}
	large := []byte(`data: {"model":"luna","output":"` + strings.Repeat("x", 2<<20) + "\"}\n\n")
	result := NewStreamRewriter(*m.responseModelRewriteOptions(t.Context(), auth, opts, true)).RewriteChunk(large)
	if !bytes.Contains(result, []byte(`"model":"team/gpt-6-astra(high)"`)) {
		t.Fatal("large complete event was not rewritten")
	}
}

func TestResponseModelRewriteMultilineSSEAndCounters(t *testing.T) {
	for _, newline := range []string{"\n", "\r\n"} {
		for _, split := range []bool{false, true} {
			t.Run(fmt.Sprintf("crlf=%v/split=%v", newline == "\r\n", split), func(t *testing.T) {
				m, auth, opts := responseModelFixture(t)
				frame := []byte(strings.Join([]string{"event: response.completed", "id: fixture", `data: {"type":"response.completed",`, `data: "response":{"model":"gpt-5.6-luna","status":"completed",`, `data: "output":[{"text":"keep gpt-5.6-luna"}],"usage":{"total_tokens":12}}}`, "", ""}, newline))
				rewriter := NewStreamRewriter(*m.responseModelRewriteOptions(t.Context(), auth, opts, true))
				var output []byte
				if split {
					for _, b := range frame {
						output = append(output, rewriter.RewriteChunk([]byte{b})...)
					}
				} else {
					output = rewriter.RewriteChunk(frame)
				}
				output = append(output, rewriter.Finish()...)
				if !bytes.Contains(output, []byte(`"model":"team/gpt-6-astra(high)"`)) || !bytes.Contains(output, []byte(`"text":"keep gpt-5.6-luna"`)) || !bytes.Contains(output, []byte("id: fixture")) {
					t.Fatalf("multiline event changed or not rewritten: %s", output)
				}
				if stats := m.AuthResponseModelRewriteSummary(auth, true); stats.Total != 1 || len(stats.Recent) != 1 {
					t.Fatalf("wrong multiline counter: %+v", stats)
				}
			})
		}
	}
}

func TestResponseModelRewriteCountersConcurrentAndBounded(t *testing.T) {
	m, auth, opts := responseModelFixture(t)
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			policy := m.responseModelRewriteOptions(t.Context(), auth, opts, false)
			rewriteModelWithOptions([]byte(`{"model":"luna"}`), *policy)
		})
	}
	wg.Wait()
	stats := m.AuthResponseModelRewriteSummary(auth, true)
	if stats.Total != 50 || len(stats.Recent) != responseModelRecentLimit {
		t.Fatalf("unexpected stats %+v", stats)
	}
	stats.Recent[0].OriginalModel = "changed"
	if m.AuthResponseModelRewriteSummary(auth, true).Recent[0].OriginalModel != "luna" {
		t.Fatal("mutable snapshot escaped")
	}
	if err := m.Delete(WithSkipPersist(context.Background()), auth.ID); err != nil {
		t.Fatal(err)
	}
	if m.AuthResponseModelRewriteSummary(auth, true).Total != 0 {
		t.Fatal("deleted credential retained counters")
	}
}

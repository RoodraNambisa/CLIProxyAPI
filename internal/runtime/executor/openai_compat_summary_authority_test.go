package executor

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestOpenAICompatSummaryRulesRetainFinalAuthority(t *testing.T) {
	for _, mode := range []string{"http", "sse", "compact"} {
		for _, scenario := range []string{"original-hide", "effort-only", "filter-absent", "override", "filter-with-alias"} {
			t.Run(mode+"/"+scenario, func(t *testing.T) {
				type observation struct{ effort, summary, alias string }
				var calls atomic.Int32
				observed := make(chan observation, 1)
				effortPath, summaryPath, aliasPath := "reasoning_effort", "reasoning.exclude", "include_reasoning"
				if mode == "compact" {
					effortPath, summaryPath, aliasPath = "reasoning.effort", "reasoning.summary", "reasoning.generate_summary"
				}
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					calls.Add(1)
					body, errRead := io.ReadAll(req.Body)
					if errRead != nil {
						t.Error(errRead)
						return
					}
					got := observation{gjson.GetBytes(body, effortPath).Raw, gjson.GetBytes(body, summaryPath).Raw, gjson.GetBytes(body, aliasPath).Raw}
					select {
					case observed <- got:
					default:
						t.Error("unexpected extra attempt")
					}
					if mode == "sse" {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, "data: {\"id\":\"chatcmpl_fixture\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"fixture\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
					} else {
						w.Header().Set("Content-Type", "application/json")
						if mode == "compact" {
							_, _ = io.WriteString(w, `{"id":"resp_fixture","object":"response.compaction","output":[]}`)
						} else {
							_, _ = io.WriteString(w, `{"id":"chatcmpl_fixture","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"fixture"},"finish_reason":"stop"}]}`)
						}
					}
				}))
				t.Cleanup(upstream.Close)
				models := []config.PayloadModelRule{{Name: "bound-compat"}}
				rules := config.PayloadConfig{}
				want := observation{effort: `"high"`, summary: "true"}
				if mode == "compact" {
					want.summary = ""
				}
				switch scenario {
				case "effort-only":
					rules.Override = []config.PayloadRule{{Models: models, Params: map[string]any{effortPath: "low"}}}
					want.effort = `"low"`
				case "filter-absent", "filter-with-alias":
					rules.Filter = []config.PayloadFilterRule{{Models: models, Params: []string{summaryPath}}}
					want.summary = ""
					if scenario == "filter-with-alias" {
						var value any = false
						want.alias = "false"
						if mode == "compact" {
							value = "concise"
							want.alias = `"concise"`
						}
						rules.Override = []config.PayloadRule{{Models: models, Params: map[string]any{aliasPath: value}}}
					}
				case "override":
					var value any = false
					want.summary = "false"
					if mode == "compact" {
						value = "detailed"
						want.summary = `"detailed"`
					}
					rules.Override = []config.PayloadRule{{Models: models, Params: map[string]any{summaryPath: value}}}
				}
				manager := compatCapabilityManager(t, upstream.URL+"/v1", rules, []string{"low", "high"}, "openrouter")
				current := compatCapabilityPayload(translator.FormatOpenAIResponse, "high")
				original, errSet := sjson.SetBytes(current, "reasoning.summary", nil)
				if errSet != nil {
					t.Fatal(errSet)
				}
				if scenario != "original-hide" && scenario != "effort-only" {
					current = original
				}
				req := core.Request{Model: "bound-compat", Payload: current}
				opts := core.Options{SourceFormat: translator.FormatOpenAIResponse, OriginalRequest: original}
				if mode == "compact" {
					opts.Alt = "responses/compact"
				}
				if mode == "sse" {
					result, err := manager.ExecuteStream(t.Context(), []string{"openrouter"}, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
					}
				} else if _, err := manager.Execute(t.Context(), []string{"openrouter"}, req, opts); err != nil {
					t.Fatal(err)
				}
				select {
				case got := <-observed:
					if got != want || calls.Load() != 1 {
						t.Fatalf("final fields=%+v want=%+v attempts=%d", got, want, calls.Load())
					}
				default:
					t.Fatal("missing upstream attempt")
				}
			})
		}
	}
}

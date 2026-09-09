package executor

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestAIStudioSummarySourceAcrossPreparedOperations(t *testing.T) {
	for _, operation := range []string{"execute", "stream", "count"} {
		for _, mode := range []string{"missing", "original-hide", "target-show"} {
			t.Run(operation+"/"+mode, func(t *testing.T) {
				current := []byte(`{"input":"fixture","reasoning":{"effort":"high"}}`)
				var original []byte
				want := ""
				if mode != "missing" {
					original, _ = sjson.SetBytes(current, "reasoning.summary", nil)
					want = "false"
				}
				if mode == "target-show" {
					current, _ = sjson.SetBytes(current, "reasoning.summary", "auto")
					want = "true"
				}
				req := core.Request{Model: "summary-fixture", Payload: current}
				if operation == "count" {
					req.Metadata = map[string]any{"action": "countTokens"}
				}
				_, prepared, err := NewAIStudioExecutor(&config.Config{}, "summary-fixture", nil).translateRequest(t.Context(), req, core.Options{SourceFormat: translator.FormatOpenAIResponse, OriginalRequest: original}, operation == "stream")
				if err != nil {
					t.Fatal(err)
				}
				if got := gjson.GetBytes(prepared.payload, "generationConfig.thinkingConfig.includeThoughts").Raw; got != want {
					t.Fatalf("summary=%s want=%q", got, want)
				}
				if operation == "count" && prepared.action != "countTokens" {
					t.Fatal("summary changed count operation")
				}
			})
		}
	}
}

func TestAntigravitySummaryVisibilityAcrossExecutionPaths(t *testing.T) {
	for _, model := range []string{"gemini-2.5-flash", "claude-sonnet-4-6"} {
		for _, operation := range []string{"execute", "stream", "count"} {
			for _, mode := range []string{"missing", "show", "hide"} {
				t.Run(fmt.Sprintf("%s/%s/%s", model, operation, mode), func(t *testing.T) {
					var calls atomic.Int32
					want := ""
					if mode == "show" {
						want = "true"
					} else if mode == "hide" {
						want = "false"
					}
					ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", streamTerminalRoundTripFunc(func(req *http.Request) (*http.Response, error) {
						calls.Add(1)
						body, err := io.ReadAll(req.Body)
						if err != nil {
							return nil, err
						}
						if got := gjson.GetBytes(body, "request.generationConfig.thinkingConfig.includeThoughts").Raw; got != want {
							t.Errorf("summary=%s want=%q", got, want)
						}
						result := `{"response":{"candidates":[{"content":{"role":"model","parts":[{"text":"fixture"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}}`
						contentType := "application/json"
						if operation == "stream" || operation != "count" && strings.Contains(model, "claude") {
							contentType = "text/event-stream"
							result = "data: " + result + "\n\n"
						}
						if operation == "count" {
							result = `{"totalTokens":12}`
						}
						return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(result))}, nil
					}))
					auth := antigravityStreamTestAuth()
					auth.ID = t.Name()
					body := []byte(`{"input":"fixture","reasoning":{"effort":"high"}}`)
					if mode == "show" {
						body, _ = sjson.SetBytes(body, "reasoning.summary", "auto")
					} else if mode == "hide" {
						body, _ = sjson.SetBytes(body, "reasoning.summary", nil)
					}
					_, err := executeMultiAgentTranslation(ctx, NewAntigravityExecutor(&config.Config{}), operation, auth, core.Request{Model: model, Payload: body}, core.Options{SourceFormat: translator.FormatOpenAIResponse, OriginalRequest: body})
					if err != nil {
						t.Fatal(err)
					}
					if calls.Load() != 1 {
						t.Fatalf("summary changed attempt count: %d", calls.Load())
					}
				})
			}
		}
	}
}

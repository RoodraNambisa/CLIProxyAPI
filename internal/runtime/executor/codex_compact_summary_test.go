package executor

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexCompactWithoutCapabilityBindingPreservesSummaryIntent(t *testing.T) {
	for _, source := range []translator.Format{translator.FormatCodex, translator.FormatOpenAIResponse} {
		for _, summary := range []string{"null", `"concise"`} {
			for _, oauth := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/oauth=%t", source, summary, oauth), func(t *testing.T) {
					observed := make(chan string, 1)
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
						body, err := io.ReadAll(req.Body)
						if err != nil {
							t.Error(err)
							return
						}
						if req.URL.Path != "/responses/compact" || gjson.GetBytes(body, "reasoning.effort").String() != "high" {
							t.Error("compact summary changed route or legacy effort")
						}
						observed <- gjson.GetBytes(body, "reasoning.summary").Raw
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, `{"id":"resp_fixture","object":"response.compaction","output":[]}`)
					}))
					t.Cleanup(server.Close)
					payload := []byte(`{"model":"gpt-5.4","input":"fixture","reasoning":{"effort":"high","summary":` + summary + `}}`)
					executor := NewCodexExecutor(&config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}})
					auth := &coreauth.Auth{ID: t.Name(), Provider: "codex", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}
					if oauth {
						delete(auth.Attributes, "api_key")
						auth.Metadata = map[string]any{"access_token": "fixture"}
					}
					if _, err := executor.Execute(t.Context(), auth, core.Request{Model: "gpt-5.4", Payload: payload}, core.Options{SourceFormat: source, OriginalRequest: payload, Alt: "responses/compact"}); err != nil {
						t.Fatal(err)
					}
					want := summary
					if summary == "null" {
						want = ""
					}
					select {
					case got := <-observed:
						if got != want {
							t.Fatalf("compact summary=%s want=%q", got, want)
						}
					default:
						t.Fatal("missing compact request")
					}
				})
			}
		}
	}
}

package openai

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
)

func TestCodexResponsesGeneratedStreamErrors(t *testing.T) {
	for _, client := range []struct {
		header, value string
		codex         bool
	}{
		{"User-Agent", "codex_cli_rs/0.153.4", true},
		{"User-Agent", "codex_cli_rs", true},
		{"User-Agent", "codex-tui/0.153.4", true},
		{"User-Agent", "Codex Desktop/0.153.4", true},
		{"Originator", "Codex Desktop", true},
		{"Originator", "CODEX-TUI/0.153.4", true},
		{"Originator", "codex_cli_rs", true},
		{"User-Agent", "codex_cli_rs_unrelated/1", false},
		{"Originator", "codex_cli_rs_unrelated", false},
		{"User-Agent", "curl/8", false},
		{"User-Agent", "", false},
	} {
		for _, rewrite := range []string{"none", "status", "body"} {
			t.Run(fmt.Sprintf("%s/%s/%s", client.header, client.value, rewrite), func(t *testing.T) {
				cfg := &sdkconfig.SDKConfig{}
				if rewrite != "none" {
					rule := sdkconfig.ErrorResponseRewriteRule{StatusCode: http.StatusTooManyRequests, ResponseStatusCode: http.StatusBadRequest}
					if rewrite == "body" {
						body := map[string]any{"custom": "saved rule"}
						rule.ResponseBody = &body
					}
					cfg.ErrorResponseRewrites = []sdkconfig.ErrorResponseRewriteRule{rule}
				}
				base := handlers.NewBaseAPIHandlers(cfg, nil)
				h := NewOpenAIResponsesAPIHandler(base)
				recorder := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(recorder)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
				c.Request.Header.Set(client.header, client.value)
				c.Writer.Header().Set("Content-Type", "text/event-stream")
				c.Writer.WriteHeaderNow()
				data := make(chan []byte)
				errs := make(chan *interfaces.ErrorMessage, 1)
				errs <- base.RewriteExecutionErrorResponse(&interfaces.ErrorMessage{StatusCode: http.StatusTooManyRequests, Error: errors.New(`{"error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"limited","param":"input"}}`)})
				close(errs)
				h.forwardResponsesStream(c, c.Writer.(http.Flusher), func(error) {}, data, errs, nil)
				body := recorder.Body.String()
				if rewrite == "body" {
					if !strings.Contains(body, "event: error\ndata: {\"custom\":\"saved rule\"}") {
						t.Fatal("client classification changed an explicit saved response body")
					}
					return
				}
				var event gjson.Result
				for _, line := range strings.Split(body, "\n") {
					if strings.HasPrefix(line, "data:") {
						event = gjson.Parse(strings.TrimPrefix(line, "data:"))
					}
				}
				if client.codex {
					if !strings.Contains(body, "event: response.failed\n") || event.Get("type").String() != "response.failed" || event.Get("response.status").String() != "failed" || event.Get("response.error.code").String() != "rate_limit_exceeded" || event.Get("response.error.param").String() != "input" {
						t.Fatal("Codex did not receive a failed terminal with its original error")
					}
				} else if event.Get("type").String() != "error" || event.Get("code").String() != "rate_limit_exceeded" {
					t.Fatal("ordinary client error format changed")
				}
			})
		}
	}
}

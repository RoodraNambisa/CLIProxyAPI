package management

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/tidwall/gjson"
)

func TestModelContextLengthManagementSaveReadAndRollback(t *testing.T) {
	for _, family := range []struct {
		key             string
		put, patch, get func(*Handler, *gin.Context)
	}{
		{"gemini-api-key", (*Handler).PutGeminiKeys, (*Handler).PatchGeminiKey, (*Handler).GetGeminiKeys},
		{"interactions-api-key", (*Handler).PutInteractionsKeys, (*Handler).PatchInteractionsKey, (*Handler).GetInteractionsKeys},
		{"claude-api-key", (*Handler).PutClaudeKeys, (*Handler).PatchClaudeKey, (*Handler).GetClaudeKeys},
		{"codex-api-key", (*Handler).PutCodexKeys, (*Handler).PatchCodexKey, (*Handler).GetCodexKeys},
		{"vertex-api-key", (*Handler).PutVertexCompatKeys, (*Handler).PatchVertexCompatKey, (*Handler).GetVertexCompatKeys},
		{"openai-compatibility", (*Handler).PutOpenAICompat, (*Handler).PatchOpenAICompat, (*Handler).GetOpenAICompat},
	} {
		for _, method := range []string{http.MethodPatch, http.MethodPut} {
			t.Run(family.key+"/"+method, func(t *testing.T) {
				entry := func(limit string) string {
					return `{"api-key":"fixture","name":"compat","base-url":"https://example.test","models":[{"name":"upstream","alias":"local","display-name":"Label","force-mapping":true,"max-context-length":` + limit + `}]}`
				}
				var cfg config.Config
				if err := json.Unmarshal([]byte(`{"`+family.key+`": [`+entry("131072")+`]}`), &cfg); err != nil {
					t.Fatal(err)
				}
				h := &Handler{cfg: &cfg, configFilePath: writeTestConfigFile(t)}
				if err := config.SaveConfigPreserveComments(h.configFilePath, h.cfg); err != nil {
					t.Fatal(err)
				}
				calls := 0
				h.runtimeConfigApplier = func(context.Context, *config.Config) (config.RuntimeApplyResult, error) {
					calls++
					return config.RuntimeApplyResult{Applied: true}, nil
				}
				for _, step := range []struct {
					value string
					valid bool
				}{
					{"1048576", true}, {"-1", false}, {"2147483648", false}, {"1.5", false}, {`"1048576"`, false},
					{"9223372036854775808", false}, {"0", true}, {"null", true},
				} {
					before, err := os.ReadFile(h.configFilePath)
					if err != nil {
						t.Fatal(err)
					}
					beforeConfig, err := json.Marshal(h.cfg)
					if err != nil {
						t.Fatal(err)
					}
					callsBefore := calls
					body := `[` + entry(step.value) + `]`
					if method == http.MethodPatch {
						body = `{"index":0,"value":` + entry(step.value) + `}`
					}
					recorder := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(recorder)
					c.Request = httptest.NewRequest(method, "/"+family.key, strings.NewReader(body))
					c.Request.Header.Set("Content-Type", "application/json")
					if method == http.MethodPatch {
						family.patch(h, c)
					} else {
						family.put(h, c)
					}
					want := http.StatusOK
					if !step.valid {
						want = http.StatusBadRequest
					}
					if recorder.Code != want {
						t.Fatalf("value %s: status = %d, want %d", step.value, recorder.Code, want)
					}
					if !step.valid {
						after, _ := os.ReadFile(h.configFilePath)
						afterConfig, _ := json.Marshal(h.cfg)
						if calls != callsBefore || !bytes.Equal(before, after) || !bytes.Equal(beforeConfig, afterConfig) {
							t.Fatal("rejected model override changed disk or runtime")
						}
						continue
					}
					if calls <= callsBefore {
						t.Fatal("valid model override did not apply")
					}
					loaded, err := config.LoadConfig(h.configFilePath)
					if err != nil {
						t.Fatal(err)
					}
					loadedJSON, _ := json.Marshal(loaded)
					path := family.key + ".0.models.0."
					wantLimit := int64(0)
					if step.value == "1048576" {
						wantLimit = 1048576
					}
					read := httptest.NewRecorder()
					readCtx, _ := gin.CreateTestContext(read)
					readCtx.Request = httptest.NewRequest(http.MethodGet, "/"+family.key, nil)
					family.get(h, readCtx)
					for source, data := range [][]byte{loadedJSON, read.Body.Bytes()} {
						if gjson.GetBytes(data, path+"max-context-length").Int() != wantLimit || gjson.GetBytes(data, path+"display-name").String() != "Label" || !gjson.GetBytes(data, path+"force-mapping").Bool() {
							t.Fatalf("source %d value %s: context=%d, label=%q, force=%v", source, step.value, gjson.GetBytes(data, path+"max-context-length").Int(), gjson.GetBytes(data, path+"display-name").String(), gjson.GetBytes(data, path+"force-mapping").Bool())
						}
					}
				}
			})
		}
	}
}

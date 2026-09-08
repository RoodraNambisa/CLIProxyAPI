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

func TestModelThinkingManagementSaveReadAndRollback(t *testing.T) {
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
		for _, method := range []string{http.MethodPut, http.MethodPatch} {
			t.Run(family.key+"/"+method, func(t *testing.T) {
				entry := func(value string) string {
					return `{"api-key":"fixture","name":"compat","base-url":"https://example.test","models":[{"name":"upstream","alias":"local","display-name":"Label","max-context-length":131072,"thinking":` + value + `}]}`
				}
				var cfg config.Config
				if err := json.Unmarshal([]byte(`{"`+family.key+`":[`+entry(`{"levels":["low"]}`)+`]}`), &cfg); err != nil {
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
					{`{"levels":["high","xhigh"],"zero_allowed":true}`, true},
					{`{"min":-1}`, false}, {`{"min":2,"max":1}`, false}, {`{"max":2147483648}`, false},
					{`{"max":1.5}`, false}, {`{"levels":["invalid"]}`, false}, {`{"levels":[1]}`, false},
					{`{"levels":[]}`, true}, {`null`, true},
				} {
					before, _ := os.ReadFile(h.configFilePath)
					beforeConfig, _ := json.Marshal(h.cfg)
					beforeCalls := calls
					body := `[` + entry(step.value) + `]`
					if method == http.MethodPatch {
						body = `{"index":0,"value":` + entry(step.value) + `}`
					}
					w := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(w)
					c.Request = httptest.NewRequest(method, "/"+family.key, strings.NewReader(body))
					c.Request.Header.Set("Content-Type", "application/json")
					if method == http.MethodPatch {
						family.patch(h, c)
					} else {
						family.put(h, c)
					}
					want := 200
					if !step.valid {
						want = 400
					}
					if w.Code != want {
						t.Fatalf("thinking save status=%d want=%d", w.Code, want)
					}
					if !step.valid {
						after, _ := os.ReadFile(h.configFilePath)
						afterConfig, _ := json.Marshal(h.cfg)
						if calls != beforeCalls || !bytes.Equal(before, after) || !bytes.Equal(beforeConfig, afterConfig) {
							t.Fatal("rejected thinking changed disk or runtime")
						}
						continue
					}
					if calls <= beforeCalls {
						t.Fatal("valid thinking did not apply")
					}
					loaded, err := config.LoadConfig(h.configFilePath)
					if err != nil {
						t.Fatal(err)
					}
					loadedJSON, _ := json.Marshal(loaded)
					read := httptest.NewRecorder()
					readCtx, _ := gin.CreateTestContext(read)
					readCtx.Request = httptest.NewRequest(http.MethodGet, "/"+family.key, nil)
					family.get(h, readCtx)
					path := family.key + ".0.models.0."
					for _, data := range [][]byte{loadedJSON, read.Body.Bytes()} {
						if gjson.GetBytes(data, path+"display-name").String() != "Label" || gjson.GetBytes(data, path+"max-context-length").Int() != 131072 {
							t.Fatal("thinking save changed existing model declarations")
						}
						thinking := gjson.GetBytes(data, path+"thinking")
						if step.value == "null" {
							if thinking.Exists() {
								t.Fatal("cleared thinking remained active")
							}
						} else if !thinking.IsObject() {
							t.Fatal("thinking object lost after save")
						} else if strings.Contains(step.value, "xhigh") && (thinking.Get("levels.1").String() != "xhigh" || !thinking.Get("zero_allowed").Bool()) {
							t.Fatal("thinking fields lost after save")
						}
					}
				}
			})
		}
	}
}

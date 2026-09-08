package management

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/tidwall/gjson"
)

func TestRequestScopedErrorCredentialPatchAndReadback(t *testing.T) {
	for _, family := range []struct {
		field, key string
		patch, get func(*Handler, *gin.Context)
	}{
		{"GeminiKey", "gemini-api-key", (*Handler).PatchGeminiKey, (*Handler).GetGeminiKeys},
		{"InteractionsKey", "interactions-api-key", (*Handler).PatchInteractionsKey, (*Handler).GetInteractionsKeys},
		{"ClaudeKey", "claude-api-key", (*Handler).PatchClaudeKey, (*Handler).GetClaudeKeys},
		{"CodexKey", "codex-api-key", (*Handler).PatchCodexKey, (*Handler).GetCodexKeys},
		{"VertexCompatAPIKey", "vertex-api-key", (*Handler).PatchVertexCompatKey, (*Handler).GetVertexCompatKeys},
		{"OpenAICompatibility", "openai-compatibility", (*Handler).PatchOpenAICompat, (*Handler).GetOpenAICompat},
	} {
		t.Run(family.key, func(t *testing.T) {
			cfg := &config.Config{}
			field := reflect.ValueOf(cfg).Elem().FieldByName(family.field)
			field.Set(reflect.MakeSlice(field.Type(), 1, 1))
			entry := field.Index(0)
			if key := entry.FieldByName("APIKey"); key.IsValid() {
				key.SetString("unchanged-fixture")
			}
			if name := entry.FieldByName("Name"); name.IsValid() {
				name.SetString("compat")
			}
			entry.FieldByName("BaseURL").SetString("https://example.test")
			h := &Handler{cfg: cfg, configFilePath: writeTestConfigFile(t)}
			if err := config.SaveConfigPreserveComments(h.configFilePath, h.cfg); err != nil {
				t.Fatal(err)
			}
			for _, step := range []struct {
				value string
				count int
				valid bool
			}{
				{`{}`, 0, true},
				{`{"request-scoped-errors":[{"status":400,"match":["fixture"],"action":"stop"}]}`, 1, true},
				{`{}`, 1, true},
				{`{"request-scoped-errors":[{"status":400,"match":["fixture"],"action":"bad"}]}`, 1, false},
				{`{"request-scoped-errors":[{"status":400,"match-regexr":["private-pattern["],"action":"stop"}]}`, 1, false},
				{`{"request-scoped-errors":[{"status":400.5,"match":["fixture"],"action":"stop"}]}`, 1, false},
				{`{"request-scoped-errors":"bad"}`, 1, false},
				{`{"request-scoped-errors":null}`, 0, true},
				{`{"request-scoped-errors":[]}`, 0, true},
			} {
				before, err := os.ReadFile(h.configFilePath)
				if err != nil {
					t.Fatal(err)
				}
				w := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(w)
				c.Request = httptest.NewRequest(http.MethodPatch, "/"+family.key, strings.NewReader(`{"index":0,"value":`+step.value+`}`))
				c.Request.Header.Set("Content-Type", "application/json")
				family.patch(h, c)
				want := http.StatusOK
				if !step.valid {
					want = http.StatusBadRequest
				}
				if w.Code != want || strings.Contains(w.Body.String(), "private-pattern") {
					t.Fatalf("patch status=%d want=%d", w.Code, want)
				}
				loaded, err := config.LoadConfig(h.configFilePath)
				if err != nil {
					t.Fatal(err)
				}
				for _, current := range []*config.Config{h.cfg, loaded} {
					item := reflect.ValueOf(current).Elem().FieldByName(family.field).Index(0)
					if item.FieldByName("RequestScopedErrors").Len() != step.count {
						t.Fatal("patch lost rules, missing-field inheritance or clearing")
					}
					if key := item.FieldByName("APIKey"); key.IsValid() && key.String() != "unchanged-fixture" {
						t.Fatal("rule-only patch changed credential")
					}
				}
				if !step.valid {
					after, _ := os.ReadFile(h.configFilePath)
					if !bytes.Equal(before, after) {
						t.Fatal("rejected rules changed disk")
					}
				}
				read := httptest.NewRecorder()
				getCtx, _ := gin.CreateTestContext(read)
				getCtx.Request = httptest.NewRequest(http.MethodGet, "/"+family.key, nil)
				family.get(h, getCtx)
				if read.Code != http.StatusOK || len(gjson.GetBytes(read.Body.Bytes(), family.key+".0.request-scoped-errors").Array()) != step.count {
					t.Fatal("GET rules differed from saved configuration")
				}
			}
		})
	}
}

func TestRequestScopedErrorInvalidReplacementDoesNotPublish(t *testing.T) {
	h := &Handler{cfg: &config.Config{CodexKey: []config.CodexKey{{APIKey: "original", BaseURL: "https://example.test"}}}, configFilePath: writeTestConfigFile(t)}
	if err := config.SaveConfigPreserveComments(h.configFilePath, h.cfg); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(h.configFilePath)
	calls := 0
	h.runtimeConfigApplier = func(context.Context, *config.Config) (config.RuntimeApplyResult, error) {
		calls++
		return config.RuntimeApplyResult{Applied: true}, nil
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPut, "/codex-api-key", strings.NewReader(`[{"api-key":"replacement","base-url":"https://example.test","request-scoped-errors":[{"status":400,"match-regexr":["["],"action":"stop"}]}]`))
	h.PutCodexKeys(c)
	after, _ := os.ReadFile(h.configFilePath)
	if w.Code != http.StatusBadRequest || calls != 0 || !bytes.Equal(before, after) || h.cfg.CodexKey[0].APIKey != "original" {
		t.Fatal("invalid replacement persisted or published")
	}
}

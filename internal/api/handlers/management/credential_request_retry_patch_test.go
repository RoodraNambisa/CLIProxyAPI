package management

import (
	"context"
	"encoding/json"
	"errors"
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

func TestCredentialRequestRetryConfigPatchAndReadback(t *testing.T) {
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
				key.SetString("test")
			}
			if name := entry.FieldByName("Name"); name.IsValid() {
				name.SetString("compat")
			}
			entry.FieldByName("BaseURL").SetString("https://example.test")
			one := 1
			entry.FieldByName("RequestRetry").Set(reflect.ValueOf(&one))
			h := &Handler{cfg: cfg, configFilePath: writeTestConfigFile(t)}
			if err := config.SaveConfigPreserveComments(h.configFilePath, h.cfg); err != nil {
				t.Fatal(err)
			}
			for _, row := range []struct {
				value string
				want  *int
				valid bool
			}{
				{`{}`, new(1), true}, {`{"request-retry":0}`, new(0), true}, {`{"request-retry":null}`, nil, true}, {`{"request-retry":-3}`, new(0), true},
				{`{"request-retry":2}`, new(2), true}, {`{"request-retry":1.5}`, new(2), false}, {`{"request-retry":"3"}`, new(2), false}, {`{"request-retry":2147483648}`, new(2), false},
			} {
				before, err := os.ReadFile(h.configFilePath)
				if err != nil {
					t.Fatal(err)
				}
				recorder := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(recorder)
				c.Request = httptest.NewRequest(http.MethodPatch, "/"+family.key, strings.NewReader(`{"index":0,"value":`+row.value+`}`))
				c.Request.Header.Set("Content-Type", "application/json")
				family.patch(h, c)
				wantStatus := http.StatusOK
				if !row.valid {
					wantStatus = http.StatusBadRequest
				}
				if recorder.Code != wantStatus {
					t.Fatalf("patch status=%d want=%d", recorder.Code, wantStatus)
				}
				if !row.valid {
					after, err := os.ReadFile(h.configFilePath)
					if err != nil || string(before) != string(after) {
						t.Fatal("invalid retry patch changed persisted config")
					}
				}
				loaded, err := config.LoadConfig(h.configFilePath)
				if err != nil {
					t.Fatal(err)
				}
				for _, current := range []*config.Config{h.cfg, loaded} {
					entry := reflect.ValueOf(current).Elem().FieldByName(family.field).Index(0)
					if key := entry.FieldByName("APIKey"); key.IsValid() && key.String() != "test" {
						t.Fatal("retry-only patch changed an untouched API key")
					}
					got := reflect.ValueOf(current).Elem().FieldByName(family.field).Index(0).FieldByName("RequestRetry")
					if (row.want == nil && !got.IsNil()) || (row.want != nil && (got.IsNil() || got.Elem().Int() != int64(*row.want))) {
						t.Fatal("patch lost zero, inheritance or rollback")
					}
				}
				reader := httptest.NewRecorder()
				getCtx, _ := gin.CreateTestContext(reader)
				getCtx.Request = httptest.NewRequest(http.MethodGet, "/"+family.key, nil)
				family.get(h, getCtx)
				got := gjson.GetBytes(reader.Body.Bytes(), family.key+".0.request-retry")
				if reader.Code != http.StatusOK || (row.want == nil && got.Exists()) || (row.want != nil && (!got.Exists() || got.Int() != int64(*row.want))) {
					t.Fatal("GET did not report the saved retry override")
				}
			}
		})
	}
}

func TestCredentialRequestRetryPatchStrictIntegerAndNull(t *testing.T) {
	for _, raw := range []string{`null`, `0`, `-3`, `2147483647`} {
		var value credentialRequestRetryPatch
		if err := json.Unmarshal([]byte(raw), &value); err != nil || !value.set {
			t.Fatal("valid retry patch rejected")
		}
	}
	for _, raw := range []string{`true`, `"1"`, `1.0`, `1.5`, `1e2`, `[]`, `{}`, `2147483648`, `-9223372036854775809`} {
		var value credentialRequestRetryPatch
		if err := json.Unmarshal([]byte(raw), &value); err == nil {
			t.Fatal("invalid retry patch accepted")
		}
	}
}

func TestCredentialRequestRetryPatchRuntimeFailureRollsBackClear(t *testing.T) {
	two := 2
	h := &Handler{cfg: &config.Config{CodexKey: []config.CodexKey{{APIKey: "test", BaseURL: "https://example.test", RequestRetry: &two}}}, configFilePath: writeTestConfigFile(t)}
	if err := config.SaveConfigPreserveComments(h.configFilePath, h.cfg); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(h.configFilePath)
	if err != nil {
		t.Fatal(err)
	}
	attempts, rollbacks := 0, 0
	h.runtimeConfigApplier = func(_ context.Context, candidate *config.Config) (config.RuntimeApplyResult, error) {
		if candidate.CodexKey[0].RequestRetry == nil {
			attempts++
			return config.RuntimeApplyResult{}, errors.New("synthetic runtime rejection")
		}
		rollbacks++
		return config.RuntimeApplyResult{Applied: true}, nil
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPatch, "/codex-api-key", strings.NewReader(`{"index":0,"value":{"request-retry":null}}`))
	c.Request.Header.Set("Content-Type", "application/json")
	h.PatchCodexKey(c)
	after, err := os.ReadFile(h.configFilePath)
	if err != nil || recorder.Code != 500 || gjson.GetBytes(recorder.Body.Bytes(), "error").String() != "runtime_update_failed" || attempts != 1 || rollbacks != 1 || string(before) != string(after) || h.cfg.CodexKey[0].RequestRetry == nil || *h.cfg.CodexKey[0].RequestRetry != 2 {
		t.Fatal("runtime rejection reported success or failed to restore the retry override")
	}
}

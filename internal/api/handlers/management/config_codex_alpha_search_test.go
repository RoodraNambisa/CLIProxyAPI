package management

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/tidwall/gjson"
)

func TestCodexAlphaSearchManagementSaveAndReload(t *testing.T) {
	for _, method := range []string{http.MethodPut, http.MethodPatch} {
		t.Run(method, func(t *testing.T) {
			h := &Handler{cfg: &config.Config{CodexKey: []config.CodexKey{{APIKey: "fixture", BaseURL: "https://example.invalid/v1", Websockets: true}}}, configFilePath: writeTestConfigFile(t)}
			if err := config.SaveConfigPreserveComments(h.configFilePath, h.cfg); err != nil {
				t.Fatal(err)
			}
			calls, applied := 0, false
			h.runtimeConfigApplier = func(_ context.Context, candidate *config.Config) (config.RuntimeApplyResult, error) {
				calls++
				applied = candidate.CodexKey[0].AlphaSearch
				return config.RuntimeApplyResult{Applied: true}, nil
			}
			for _, value := range []string{"true", "false", "true", "[]", `"true"`, "1", "null", "", "false"} {
				before, err := os.ReadFile(h.configFilePath)
				if err != nil {
					t.Fatal(err)
				}
				wasEnabled, callsBefore := h.cfg.CodexKey[0].AlphaSearch, calls
				field := ""
				if value != "" {
					field = `,"alpha-search":` + value
				}
				body := `[{"api-key":"fixture","base-url":"https://example.invalid/v1","websockets":true` + field + `}]`
				if method == http.MethodPatch {
					body = `{"index":0,"value":{` + strings.TrimPrefix(field, ",") + `}}`
				}
				w := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(w)
				c.Request = httptest.NewRequest(method, "/codex-api-key", strings.NewReader(body))
				if method == http.MethodPatch {
					h.PatchCodexKey(c)
				} else {
					h.PutCodexKeys(c)
				}
				invalid := value == "[]" || value == `"true"` || value == "1"
				if invalid {
					after, _ := os.ReadFile(h.configFilePath)
					if w.Code != http.StatusBadRequest || calls != callsBefore || h.cfg.CodexKey[0].AlphaSearch != wasEnabled || !bytes.Equal(before, after) {
						t.Fatalf("invalid %q changed configuration", value)
					}
					continue
				}
				want := value == "true"
				if method == http.MethodPatch && (value == "null" || value == "") {
					want = wasEnabled
				}
				if w.Code != http.StatusOK || calls == callsBefore || applied != want || h.cfg.CodexKey[0].AlphaSearch != want {
					t.Fatalf("value %q was not applied", value)
				}
				loaded, err := config.LoadConfig(h.configFilePath)
				if err != nil || len(loaded.CodexKey) != 1 || loaded.CodexKey[0].AlphaSearch != want || loaded.CodexKey[0].APIKey != "fixture" || !loaded.CodexKey[0].Websockets {
					t.Fatalf("saved configuration lost a field: %v", err)
				}
				read := httptest.NewRecorder()
				readCtx, _ := gin.CreateTestContext(read)
				readCtx.Request = httptest.NewRequest(http.MethodGet, "/codex-api-key", nil)
				h.GetCodexKeys(readCtx)
				if gjson.GetBytes(read.Body.Bytes(), "codex-api-key.0.alpha-search").Bool() != want {
					t.Fatal("GET did not reflect saved capability")
				}
			}
		})
	}
}

func TestCodexAlphaSearchManagementRollsBackRejectedRuntime(t *testing.T) {
	h := &Handler{cfg: &config.Config{CodexKey: []config.CodexKey{{APIKey: "fixture", BaseURL: "https://example.invalid/v1"}}}, configFilePath: writeTestConfigFile(t)}
	if err := config.SaveConfigPreserveComments(h.configFilePath, h.cfg); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(h.configFilePath)
	if err != nil {
		t.Fatal(err)
	}
	h.runtimeConfigApplier = func(_ context.Context, candidate *config.Config) (config.RuntimeApplyResult, error) {
		if candidate.CodexKey[0].AlphaSearch {
			return config.RuntimeApplyResult{}, errors.New("fixture runtime rejection")
		}
		return config.RuntimeApplyResult{Applied: true}, nil
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPatch, "/codex-api-key", strings.NewReader(`{"index":0,"value":{"alpha-search":true}}`))
	h.PatchCodexKey(c)
	after, err := os.ReadFile(h.configFilePath)
	if err != nil || w.Code != http.StatusInternalServerError || h.cfg.CodexKey[0].AlphaSearch || !bytes.Equal(before, after) {
		t.Fatal("runtime rejection left the capability enabled or reported success")
	}
}

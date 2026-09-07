package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestCredentialWeightConfigPatchRoundTripAndRejectedSave(t *testing.T) {
	for _, tc := range []struct {
		family string
		patch  func(*Handler, *gin.Context)
	}{{"GeminiKey", (*Handler).PatchGeminiKey}, {"InteractionsKey", (*Handler).PatchInteractionsKey}, {"ClaudeKey", (*Handler).PatchClaudeKey}, {"CodexKey", (*Handler).PatchCodexKey}, {"VertexCompatAPIKey", (*Handler).PatchVertexCompatKey}} {
		t.Run(tc.family, func(t *testing.T) {
			cfg := &config.Config{}
			field := reflect.ValueOf(cfg).Elem().FieldByName(tc.family)
			field.Set(reflect.MakeSlice(field.Type(), 1, 1))
			entry := field.Index(0)
			entry.FieldByName("APIKey").SetString("local-test-key")
			entry.FieldByName("BaseURL").SetString("https://example.invalid")
			h := &Handler{cfg: cfg, configFilePath: writeTestConfigFile(t)}
			for _, value := range []string{"0", "1000000", "omitted", "null"} {
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				body := `{"index":0,"value":{"weight":` + value + `}}`
				if value == "omitted" {
					body = `{"index":0,"value":{"prefix":"retained"}}`
				}
				c.Request = httptest.NewRequest(http.MethodPatch, "/weight", strings.NewReader(body))
				c.Request.Header.Set("Content-Type", "application/json")
				tc.patch(h, c)
				if rec.Code != http.StatusOK {
					t.Fatalf("valid weight rejected: status %d", rec.Code)
				}
				reloaded, err := config.LoadConfig(h.configFilePath)
				if err != nil {
					t.Fatal(err)
				}
				weight := reflect.ValueOf(reloaded).Elem().FieldByName(tc.family).Index(0).FieldByName("Weight")
				if reflect.ValueOf(reloaded).Elem().FieldByName(tc.family).Index(0).FieldByName("APIKey").String() != "local-test-key" {
					t.Fatal("weight-only patch replaced an untouched API key")
				}
				if value == "null" {
					if !weight.IsNil() {
						t.Fatal("cleared weight was restored on reload")
					}
				} else if weight.IsNil() || (value == "0" && weight.Elem().Int() != 0) || ((value == "1000000" || value == "omitted") && weight.Elem().Int() != 1000000) {
					t.Fatalf("saved weight changed on reload: input=%s, present=%v, value=%v", value, !weight.IsNil(), weight.Interface())
				}
			}
			before, err := os.ReadFile(h.configFilePath)
			if err != nil {
				t.Fatal(err)
			}
			for _, value := range []string{"1000001", "1.5", `"1"`, "-9223372036854775809", "true"} {
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPatch, "/weight", strings.NewReader(`{"index":0,"value":{"weight":`+value+`}}`))
				c.Request.Header.Set("Content-Type", "application/json")
				tc.patch(h, c)
				after, err := os.ReadFile(h.configFilePath)
				if err != nil || rec.Code != http.StatusBadRequest || string(before) != string(after) {
					t.Fatal("invalid weight changed the saved config")
				}
			}
		})
	}
}

func TestCredentialWeightAuthFilePatchAndClear(t *testing.T) {
	for _, batch := range []bool{false, true} {
		store := &memoryAuthStore{}
		manager := coreauth.NewManager(store, nil, nil)
		a := &coreauth.Auth{ID: "weight.json", FileName: "weight.json", Provider: "codex", Metadata: map[string]any{"type": "codex"}}
		if _, err := manager.Register(t.Context(), a); err != nil {
			t.Fatal(err)
		}
		h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
		for _, value := range []string{"0", "7", "omitted", "null"} {
			body := `{"name":"weight.json","weight":` + value + `}`
			if value == "omitted" {
				body = `{"name":"weight.json","note":"retained"}`
			}
			if batch {
				body = `{"names":["weight.json"],"fields":{"weight":` + value + `}}`
				if value == "omitted" {
					body = `{"names":["weight.json"],"fields":{"note":"retained"}}`
				}
			}
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPatch, "/weight", strings.NewReader(body))
			c.Request.Header.Set("Content-Type", "application/json")
			h.PatchAuthFileFields(c)
			if rec.Code != http.StatusOK {
				t.Fatalf("auth weight patch failed: status %d", rec.Code)
			}
			current, _ := manager.GetByID(a.ID)
			weight, present := configuredAuthFileWeight(current)
			if value == "null" {
				if present {
					t.Fatal("clearing weight left a shadowing source")
				}
			} else if !present || (value == "0" && weight != 0) || ((value == "7" || value == "omitted") && weight != 7) {
				t.Fatalf("auth patch did not update effective weight: input=%s, present=%v, value=%d", value, present, weight)
			}
		}
	}
	for _, raw := range []string{`{"weight":1.5}`, `{"weight":true}`, `{"weight":1000001}`} {
		if _, err := decodeAuthFileFieldValues(json.RawMessage(raw)); err == nil {
			t.Fatal("invalid batch weight accepted")
		}
	}
}

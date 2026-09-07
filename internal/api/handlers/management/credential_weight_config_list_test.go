package management

import (
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestCredentialWeightListReplacementRejectsBeforePersistence(t *testing.T) {
	for _, tc := range []struct {
		family string
		put    func(*Handler, *gin.Context)
	}{
		{"GeminiKey", (*Handler).PutGeminiKeys}, {"InteractionsKey", (*Handler).PutInteractionsKeys},
		{"ClaudeKey", (*Handler).PutClaudeKeys}, {"CodexKey", (*Handler).PutCodexKeys}, {"VertexCompatAPIKey", (*Handler).PutVertexCompatKeys},
	} {
		t.Run(tc.family, func(t *testing.T) {
			h := &Handler{cfg: &config.Config{}, configFilePath: writeTestConfigFile(t)}
			for _, value := range []string{"0", "1000001"} {
				before, err := os.ReadFile(h.configFilePath)
				if err != nil {
					t.Fatal(err)
				}
				recorder := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(recorder)
				c.Request = httptest.NewRequest(http.MethodPut, "/weight", strings.NewReader(`[{"api-key":"test","base-url":"https://example.test","weight":`+value+`}]`))
				tc.put(h, c)
				if value == "0" {
					if recorder.Code != http.StatusOK {
						t.Fatal("zero weight replacement failed")
					}
				} else {
					after, err := os.ReadFile(h.configFilePath)
					if err != nil || recorder.Code != http.StatusBadRequest || string(after) != string(before) {
						t.Fatal("invalid replacement changed saved config")
					}
				}
				field := reflect.ValueOf(h.cfg).Elem().FieldByName(tc.family)
				if field.Len() != 1 || field.Index(0).FieldByName("Weight").IsNil() || field.Index(0).FieldByName("Weight").Elem().Int() != 0 {
					t.Fatal("invalid replacement changed the in-memory credential")
				}
			}
		})
	}
}

func TestCredentialWeightOpenAICompatEntriesPatchAndRollback(t *testing.T) {
	h := &Handler{cfg: &config.Config{OpenAICompatibility: []config.OpenAICompatibility{{Name: "compat", BaseURL: "https://example.test"}}}, configFilePath: writeTestConfigFile(t)}
	for _, value := range []string{"0", "null", "1000001"} {
		before, err := os.ReadFile(h.configFilePath)
		if err != nil {
			t.Fatal(err)
		}
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodPatch, "/weight", strings.NewReader(`{"index":0,"value":{"api-key-entries":[{"api-key":"test","weight":`+value+`}]}}`))
		c.Request.Header.Set("Content-Type", "application/json")
		h.PatchOpenAICompat(c)
		if value == "1000001" {
			after, err := os.ReadFile(h.configFilePath)
			if err != nil || recorder.Code != http.StatusBadRequest || string(after) != string(before) || h.cfg.OpenAICompatibility[0].APIKeyEntries[0].Weight != nil {
				t.Fatal("invalid compatibility weight replaced current config")
			}
			continue
		}
		if recorder.Code != http.StatusOK {
			t.Fatal("valid compatibility weight patch failed")
		}
		reloaded, err := config.LoadConfig(h.configFilePath)
		if err != nil {
			t.Fatal(err)
		}
		weight := reloaded.OpenAICompatibility[0].APIKeyEntries[0].Weight
		if (value == "null" && weight != nil) || (value == "0" && (weight == nil || *weight != 0)) {
			t.Fatal("compatibility weight lost clear or zero semantics")
		}
	}
}

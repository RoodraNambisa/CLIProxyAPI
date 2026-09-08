package management

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestGeminiPatchUpdatesModelsAndPreservesOmittedFields(t *testing.T) {
	original := config.GeminiModel{Name: "upstream", Alias: "local", DisplayName: "Before", ForceMapping: true}
	h := &Handler{cfg: &config.Config{GeminiKey: []config.GeminiKey{{APIKey: "fixture", Models: []config.GeminiModel{original}}}}, configFilePath: writeTestConfigFile(t)}
	if err := config.SaveConfigPreserveComments(h.configFilePath, h.cfg); err != nil {
		t.Fatal(err)
	}
	for _, step := range []struct {
		body, label string
		count       int
	}{
		{`{"models":[{"name":"upstream","alias":"local","display-name":"After","force-mapping":true}]}`, "After", 1},
		{`{"prefix":"fixture-prefix"}`, "After", 1},
		{`{"models":null}`, "After", 1},
		{`{"models":[]}`, "", 0},
	} {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodPatch, "/gemini-api-key", strings.NewReader(`{"index":0,"value":`+step.body+`}`))
		c.Request.Header.Set("Content-Type", "application/json")
		h.PatchGeminiKey(c)
		if recorder.Code != http.StatusOK {
			t.Fatalf("patch status = %d", recorder.Code)
		}
		loaded, err := config.LoadConfig(h.configFilePath)
		if err != nil {
			t.Fatal(err)
		}
		for _, cfg := range []*config.Config{h.cfg, loaded} {
			if len(cfg.GeminiKey) != 1 || cfg.GeminiKey[0].APIKey != "fixture" || len(cfg.GeminiKey[0].Models) != step.count {
				t.Fatal("model patch changed credential or model count")
			}
			if step.count > 0 {
				model := cfg.GeminiKey[0].Models[0]
				if model.DisplayName != step.label || model.Name != original.Name || model.Alias != original.Alias || !model.ForceMapping {
					t.Fatal("model patch lost model fields")
				}
			}
		}
	}
}

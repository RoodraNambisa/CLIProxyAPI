package management

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestAuthImagePriorityConfigAPI(t *testing.T) {
	h := &Handler{cfg: &config.Config{}, configFilePath: writeTestConfigFile(t)}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPatch, "/v0/management/auth-model-exclusions",
		bytes.NewBufferString(`{"value":[{"models":["excluded"],"priorities":[3,5],"disable-image-generation":true,"disable-image-generation-priorities":[5,5]}]}`))
	c.Request.Header.Set("Content-Type", "application/json")
	h.PutAuthModelExclusions(c)
	if rec.Code != http.StatusOK {
		t.Fatal(rec.Code, rec.Body.String())
	}
	reloaded, err := config.LoadConfig(h.configFilePath)
	if err != nil {
		t.Fatal(err)
	}
	if got := reloaded.AuthModelExclusions[0].DisableImageGenerationPriorities; len(got) != 1 || got[0] != 5 {
		t.Fatalf("persisted scope = %v", got)
	}
	for _, get := range []func(*gin.Context){h.GetAuthModelExclusions, h.GetConfig} {
		rec = httptest.NewRecorder()
		c, _ = gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
		get(c)
		var got map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if rec.Code != http.StatusOK || got["auth-model-exclusions-image-priority-supported"] != true {
			t.Fatal("missing capability", rec.Body.String())
		}
		rule := got["auth-model-exclusions"].([]any)[0].(map[string]any)
		if scope := rule["disable-image-generation-priorities"].([]any); len(scope) != 1 || scope[0] != float64(5) {
			t.Fatal("missing scope", rule)
		}
	}
}

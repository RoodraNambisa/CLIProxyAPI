package management

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestXAIKeyManagementPersistenceAndValidation(t *testing.T) {
	h := &Handler{cfg: &config.Config{}, configFilePath: writeTestConfigFile(t)}
	call := func(method, path, body string, handler func(*gin.Context)) int {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(method, path, strings.NewReader(body))
		c.Request.Header.Set("Content-Type", "application/json")
		handler(c)
		return w.Code
	}
	if code := call("PUT", "/xai-api-key", `[{"api-key":"fixture-key","models":[{"name":"grok-4.6","alias":"fast"}],"headers":{"X-Test":"kept"}}]`, h.PutXAIKeys); code != 200 {
		t.Fatal(code)
	}
	reloaded, err := config.LoadConfig(h.configFilePath)
	if err != nil || len(reloaded.XAIKey) != 1 || reloaded.XAIKey[0].BaseURL != "https://api.x.ai/v1" || reloaded.XAIKey[0].Models[0].Alias != "fast" {
		t.Fatalf("native key not persisted: %v", err)
	}
	if code := call("PATCH", "/xai-api-key", `{"index":0,"value":{"weight":0,"prefix":"team"}}`, h.PatchXAIKey); code != 200 {
		t.Fatal(code)
	}
	if h.cfg.XAIKey[0].APIKey != "fixture-key" || h.cfg.XAIKey[0].Weight == nil || *h.cfg.XAIKey[0].Weight != 0 {
		t.Fatal("patch lost unchanged key or zero weight")
	}
	if code := call("PATCH", "/xai-api-key", `{"index":0,"value":{"weight":1.5}}`, h.PatchXAIKey); code != http.StatusBadRequest {
		t.Fatal("fractional weight accepted")
	}
	if code := call("PATCH", "/xai-api-key", `{"index":0,"value":{"base-url":" https://API.X.AI:443/v1/ "}}`, h.PatchXAIKey); code != 200 || h.cfg.XAIKey[0].BaseURL != "https://api.x.ai/v1" {
		t.Fatal("equivalent endpoint was not canonicalized")
	}
	if code := call("PATCH", "/xai-api-key", `{"index":0,"value":{"base-url":""}}`, h.PatchXAIKey); code != 200 || len(h.cfg.XAIKey) != 1 || h.cfg.XAIKey[0].BaseURL != "https://api.x.ai/v1" {
		t.Fatal("empty endpoint must reset the default without deleting the key")
	}
	if code := call("PATCH", "/xai-api-key", `{"index":0,"value":{"base-url":"https://user:secret@api.x.ai/v1"}}`, h.PatchXAIKey); code != http.StatusBadRequest || h.cfg.XAIKey[0].BaseURL != "https://api.x.ai/v1" {
		t.Fatal("invalid endpoint was not rejected and rolled back")
	}
	if code := call("DELETE", "/xai-api-key?index=0", "", h.DeleteXAIKey); code != 200 || len(h.cfg.XAIKey) != 0 {
		t.Fatal("native key deletion failed")
	}
}

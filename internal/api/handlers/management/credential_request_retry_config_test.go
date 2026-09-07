package management

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestCredentialRequestRetryInvalidReplacementDoesNotPersistOrPublish(t *testing.T) {
	one := 1
	h := &Handler{cfg: &config.Config{CodexKey: []config.CodexKey{{APIKey: "original", BaseURL: "https://example.test", RequestRetry: &one}}}, configFilePath: writeTestConfigFile(t)}
	if err := config.SaveConfigPreserveComments(h.configFilePath, h.cfg); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(h.configFilePath)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	h.runtimeConfigApplier = func(context.Context, *config.Config) (config.RuntimeApplyResult, error) {
		calls++
		return config.RuntimeApplyResult{Applied: true}, nil
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPut, "/codex-api-key", strings.NewReader(`[{"api-key":"replacement","base-url":"https://example.test","request-retry":2147483648}]`))
	h.PutCodexKeys(c)
	after, err := os.ReadFile(h.configFilePath)
	if err != nil || recorder.Code != http.StatusBadRequest || string(before) != string(after) || calls != 0 || h.cfg.CodexKey[0].APIKey != "original" || *h.cfg.CodexKey[0].RequestRetry != 1 {
		t.Fatalf("invalid replacement changed state or response: status=%d callbacks=%d", recorder.Code, calls)
	}
}

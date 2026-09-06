package openai

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestResponsesErrorLoggingDoesNotChangeOriginalError(t *testing.T) {
	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{RequestLog: true}, nil))
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	util.RegisterPromptCacheLogPolicy(c, []byte(`{"prompt_cache_key":"a"}`))
	original := &interfaces.ErrorMessage{StatusCode: 400, Error: errors.New("a"), Addon: http.Header{"Session-Id": {"a"}}}
	h.LoggingAPIResponseError(context.WithValue(t.Context(), "gin", c), original)
	value, exists := c.Get("API_RESPONSE_ERROR")
	if !exists {
		t.Fatal("missing diagnostic error")
	}
	logged := value.([]*interfaces.ErrorMessage)[0]
	if logged.Error.Error() != util.PromptCacheLogMarker || logged.Addon.Get("Session-Id") != util.PromptCacheLogMarker {
		t.Fatal("error diagnostics retained the cache key")
	}
	// Exercise the public error builder separately from diagnostic projection.
	wire, err := buildResponsesWebsocketErrorPayload(original)
	if err != nil || !strings.Contains(string(wire), `"message":"a"`) {
		t.Fatal("error builder changed upstream error text")
	}
	if original.Error.Error() != "a" {
		t.Fatal("logging altered original error")
	}
}

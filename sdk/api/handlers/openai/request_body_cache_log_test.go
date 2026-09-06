package openai

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
)

func TestReadOpenAIJSONRegistersDiagnosticKeyBeforeBodyRelease(t *testing.T) {
	for _, payload := range []string{`{"prompt_cache_key":"client-cache-key"}`, `{"input":"no key"}`} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewBufferString(payload))
		util.RegisterPromptCacheLogPolicy(c, []byte(`{"prompt_cache_key":"previous-cache-key"}`))
		body, err := readOpenAIJSONRequestBody(c)
		if err != nil || string(body) != payload || c.Request.Body != http.NoBody {
			t.Fatal("diagnostic registration changed parsing or body release")
		}
		clear(body)
		redactor := util.PromptCacheLogForGin(c)
		if payload == `{"input":"no key"}` {
			if redactor != nil {
				t.Fatal("missing key inherited an earlier diagnostic policy")
			}
		} else if redactor == nil || !redactor.ProtectsKey("client-cache-key") {
			t.Fatal("body release discarded the diagnostic key")
		}
	}
}

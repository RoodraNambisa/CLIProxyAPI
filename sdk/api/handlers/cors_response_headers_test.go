package handlers

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestUpstreamHeadersCannotSetLocalCORS(t *testing.T) {
	keys := []string{"Access-Control-Allow-Credentials", "Access-Control-Allow-Headers", "Access-Control-Allow-Methods", "Access-Control-Allow-Origin", "Access-Control-Expose-Headers", "Access-Control-Max-Age"}
	for _, key := range keys {
		for _, errorResponse := range []bool{false, true} {
			for _, localValue := range []string{"", "local-policy"} {
				t.Run(key+"/"+localValue+"/error="+map[bool]string{false: "false", true: "true"}[errorResponse], func(t *testing.T) {
					upstream := http.Header{strings.ToLower(key): {"upstream-policy"}, "X-Request-Id": {"fixture"}, "Retry-After": {"30"}}
					recorder := httptest.NewRecorder()
					if localValue != "" {
						recorder.Header().Set(key, localValue)
					}
					if errorResponse {
						c, _ := gin.CreateTestContext(recorder)
						c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
						handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{PassthroughHeaders: true}, nil)
						handler.WriteErrorResponse(c, &interfaces.ErrorMessage{StatusCode: 429, Error: errors.New("fixture limit"), Addon: upstream})
					} else {
						WriteUpstreamHeaders(recorder.Header(), ClientUpstreamHeaders(upstream, true))
					}
					if got := recorder.Header().Get(key); got != localValue {
						t.Errorf("%s = %q, want local policy %q", key, got, localValue)
					}
					if recorder.Header().Get("X-Request-Id") != "fixture" || recorder.Header().Get("Retry-After") != "30" {
						t.Error("ordinary response hints were lost")
					}
					if upstream[strings.ToLower(key)][0] != "upstream-policy" {
						t.Error("source headers were modified")
					}
				})
			}
		}
	}
}

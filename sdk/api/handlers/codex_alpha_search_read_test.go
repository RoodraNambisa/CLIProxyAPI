package handlers

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type alphaSearchFailingBody struct{ err error }

func (r alphaSearchFailingBody) Read([]byte) (int, error) { return 0, r.err }

func TestCodexAlphaSearchReadErrorsKeepContextStatusAndHidePrivateDetails(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status int
	}{
		{"cancel", context.Canceled, 499},
		{"deadline", context.DeadlineExceeded, http.StatusGatewayTimeout},
		{"too large", &http.MaxBytesError{Limit: 16 << 20}, http.StatusRequestEntityTooLarge},
		{"private error", errors.New("private fixture content"), http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			public := &codexSearchRequestReadError{cause: tc.err}
			if !errors.Is(public, tc.err) || strings.Contains(public.Error(), "private fixture content") {
				t.Fatal("sanitized read error lost its cause or exposed private details")
			}
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/backend-api/codex/alpha/search", nil)
			c.Request.Body = io.NopCloser(alphaSearchFailingBody{err: tc.err})
			h := NewCodexAlphaSearchAPIHandler(NewBaseAPIHandlers(&config.SDKConfig{}, coreauth.NewManager(nil, nil, nil)))
			h.Search(c)
			if recorder.Code != tc.status || strings.Contains(recorder.Body.String(), "private fixture content") {
				t.Fatalf("request read failure status = %d, want %d", recorder.Code, tc.status)
			}
		})
	}
}

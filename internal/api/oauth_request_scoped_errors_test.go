package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestOAuthRequestScopedErrorRoutesRequireManagementAuthentication(t *testing.T) {
	server := newTestServerWithConfig(t, func(cfg *config.Config) {
		cfg.RemoteManagement.SecretKey = "fixture-secret"
		cfg.RemoteManagement.AllowRemote = true
	})
	const path = "/v0/management/oauth-request-scoped-errors"
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		found := false
		for _, route := range server.engine.Routes() {
			if route.Method == method && route.Path == path {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing %s rule route", method)
		}
		w := httptest.NewRecorder()
		server.engine.ServeHTTP(w, httptest.NewRequest(method, path, nil))
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("%s without auth returned %d", method, w.Code)
		}
	}
}

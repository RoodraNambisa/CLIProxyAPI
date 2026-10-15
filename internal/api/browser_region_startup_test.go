package api

import (
	"net/http"
	"testing"
)

func TestBrowserRegionStartupContract(t *testing.T) {
	for _, path := range []string{"/v0/management/chatgpt-web/browser-region", "/v0/management/chatgpt-web/browser-region/preview"} {
		if !startupManagementPathAvailable(path) {
			t.Fatalf("safe read unavailable: %s", path)
		}
	}
	for _, method := range []string{http.MethodPatch, http.MethodPost, http.MethodPut, http.MethodDelete} {
		if startupManagementMethodAvailable(method) {
			t.Fatalf("startup allowed mutation: %s", method)
		}
	}
}

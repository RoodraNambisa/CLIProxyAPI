package management

import (
	"net/http"
	"reflect"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestAPIKeyAccessSaveFailureDoesNotLeavePendingMutations(t *testing.T) {
	for _, tc := range []struct {
		name, method, url, body string
		handler                 func(*Handler) gin.HandlerFunc
	}{
		{"patch-priority", http.MethodPatch, "/v0/management/api-key-groups", `{"api-key":"fixture","allowed-priorities":[2]}`, func(h *Handler) gin.HandlerFunc { return h.PatchAPIKeyGroups }},
		{"patch-name", http.MethodPatch, "/v0/management/api-key-groups", `{"api-key":"fixture","name":"Work"}`, func(h *Handler) gin.HandlerFunc { return h.PatchAPIKeyGroups }},
		{"put-groups", http.MethodPut, "/v0/management/api-key-groups", `[{"api-key":"fixture","excluded-priorities":[1]}]`, func(h *Handler) gin.HandlerFunc { return h.PutAPIKeyGroups }},
		{"delete-group", http.MethodDelete, "/v0/management/api-key-groups?api-key=fixture", "", func(h *Handler) gin.HandlerFunc { return h.DeleteAPIKeyGroups }},
		{"rename-key", http.MethodPatch, "/v0/management/api-keys", `{"old":"fixture","new":"renamed"}`, func(h *Handler) gin.HandlerFunc { return h.PatchAPIKeys }},
		{"replace-keys", http.MethodPut, "/v0/management/api-keys", `[]`, func(h *Handler) gin.HandlerFunc { return h.PutAPIKeys }},
		{"delete-key", http.MethodDelete, "/v0/management/api-keys?value=fixture", "", func(h *Handler) gin.HandlerFunc { return h.DeleteAPIKeys }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{SDKConfig: config.SDKConfig{APIKeys: []string{"fixture"}, APIKeyGroups: []config.APIKeyGroup{{APIKey: "fixture", Providers: []string{"codex"}, AllowedPriorities: []int{1}}}}}
			keys := append([]string(nil), cfg.APIKeys...)
			groups := cloneAPIKeyGroups(cfg.APIKeyGroups)
			// A directory cannot be read or persisted as a configuration file.
			h := NewHandler(cfg, t.TempDir(), nil)
			response := performAPIKeyConfigRequest(t, tc.handler(h), tc.method, tc.url, tc.body)
			if response.Code < 400 {
				t.Fatal("invalid storage target unexpectedly saved")
			}
			if !reflect.DeepEqual(cfg.APIKeys, keys) || !reflect.DeepEqual(cfg.APIKeyGroups, groups) {
				t.Fatal("failed API key save left a mutation for a later unrelated save")
			}
		})
	}
}

package management

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestCredentialWeightAuthBatchValidatesBeforeUpdatingAnyCredential(t *testing.T) {
	manager := coreauth.NewManager(&memoryAuthStore{}, nil, nil)
	for id, provider := range map[string]string{"first.json": "codex", "second.json": "claude"} {
		_, err := manager.Register(t.Context(), &coreauth.Auth{ID: id, FileName: id, Provider: provider, Attributes: map[string]string{"weight": "7"}, Metadata: map[string]any{"type": provider, "weight": 7}})
		if err != nil {
			t.Fatal(err)
		}
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	patch := func(weight string) int {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodPatch, "/weight", strings.NewReader(`{"names":["first.json","second.json"],"fields":{"weight":`+weight+`}}`))
		c.Request.Header.Set("Content-Type", "application/json")
		h.PatchAuthFileFields(c)
		return recorder.Code
	}
	for _, invalid := range []string{"1.5", "true", "1000001"} {
		if patch(invalid) != http.StatusBadRequest {
			t.Fatal("invalid batch weight was accepted")
		}
		for _, id := range []string{"first.json", "second.json"} {
			current, _ := manager.GetByID(id)
			if value, present := configuredAuthFileWeight(current); !present || value != 7 {
				t.Fatal("invalid batch partially updated credentials")
			}
		}
	}
	for _, value := range []string{"0", "null"} {
		if patch(value) != http.StatusOK {
			t.Fatal("valid cross-provider batch failed")
		}
		for _, id := range []string{"first.json", "second.json"} {
			current, _ := manager.GetByID(id)
			weight, present := configuredAuthFileWeight(current)
			if (value == "null" && present) || (value == "0" && (!present || weight != 0)) {
				t.Fatal("batch lost zero or left a shadowing weight")
			}
		}
	}
}

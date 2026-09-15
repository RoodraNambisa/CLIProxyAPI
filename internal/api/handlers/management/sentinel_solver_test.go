package management

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelconfig"
)

func TestPatchSentinelSolverIgnoresRetiredListenerSettings(t *testing.T) {
	for _, test := range []struct {
		name, body, path string
		status           int
	}{
		{"legacy with update", `{"listen":"invalid-address","tls":{"enable":true,"cert":"/missing"},"access-path":"/updated"}`, "/updated", http.StatusOK},
		{"legacy null", `{"listen":null,"tls":null}`, "/original", http.StatusOK},
		{"unknown field", `{"listen":null,"unknown":true}`, "/original", http.StatusBadRequest},
		{"active null", `{"tls":null,"api-keys":null}`, "/original", http.StatusBadRequest},
		{"missing key", `{"listen":null,"api-keys":[]}`, "/original", http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := writeTestConfigFile(t)
			cfg, err := config.LoadConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			cfg.SentinelSolver = sentinelconfig.Server{Enabled: true, AccessPath: "/original", APIKeys: []string{"fixture"}}
			handler := &Handler{cfg: cfg, configFilePath: path}
			out := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(out)
			ctx.Request = httptest.NewRequest(http.MethodPatch, "/v0/management/sentinel-solver", strings.NewReader(test.body))
			handler.PatchSentinelSolver(ctx)
			if out.Code != test.status {
				t.Fatalf("status %d: %s", out.Code, out.Body.String())
			}
			if cfg.SentinelSolver.Path() != test.path || !cfg.SentinelSolver.Enabled || len(cfg.SentinelSolver.APIKeys) != 1 {
				t.Fatal("retired or rejected settings changed active configuration")
			}
			if test.status == http.StatusOK {
				loaded, err := config.LoadConfig(path)
				if err != nil || loaded.SentinelSolver.Path() != test.path || loaded.TLS.Enable {
					t.Fatalf("saved configuration mismatch: %v", err)
				}
			}
		})
	}
}

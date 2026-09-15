package api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	management "github.com/router-for-me/CLIProxyAPI/v6/internal/api/handlers/management"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/managementasset"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelservice"
)

// NewSentinelOnlyServer constructs management and computation without any proxy runtime.
func NewSentinelOnlyServer(cfg *config.Config, path, password string) (*Server, error) {
	if cfg == nil || !cfg.SentinelSolver.Enabled {
		return nil, fmt.Errorf("--sentinel-solver-only requires sentinel-solver.enabled=true")
	}
	if err := cfg.ValidateSentinelSolver(); err != nil {
		return nil, err
	}
	snapshot, err := config.Clone(cfg)
	if err != nil {
		return nil, err
	}
	node, err := sentinelservice.New(snapshot.SentinelSolver)
	if err != nil {
		return nil, err
	}
	handler, err := management.NewSentinelOnlyHandler(snapshot, path)
	if err != nil {
		_ = node.Stop(context.Background())
		return nil, err
	}
	handler.SetLocalPassword(password)
	handler.SetSentinelSolver(node)
	engine := gin.New()
	engine.Use(logging.GinLogrusRecovery(), corsMiddleware())
	s := &Server{sentinelOnly: true, sentinelSolver: node, engine: engine, mgmt: handler, cfg: snapshot, configFilePath: path, localPassword: password, envManagementSecret: strings.TrimSpace(os.Getenv("MANAGEMENT_PASSWORD")) != ""}
	s.managementRoutesEnabled.Store(s.envManagementSecret || password != "" || cfg.RemoteManagement.SecretKey != "")
	prefix := config.ManagementAccessPathPrefix(snapshot.RemoteManagement.AccessPath)
	engine.GET(config.JoinManagementAccessPath(prefix, "/management.html"), s.serveManagementControlPanel)
	engine.GET(config.JoinManagementAccessPath(prefix, "/"), func(c *gin.Context) {
		c.Redirect(http.StatusTemporaryRedirect, config.JoinManagementAccessPath(prefix, "/management.html"))
	})
	mgmt := engine.Group(config.JoinManagementAccessPath(prefix, "/v0/management"), s.managementAvailabilityMiddleware(), handler.Middleware(), handler.ConfigMutationMiddleware())
	mgmt.GET("/runtime/capabilities", handler.GetRuntimeCapabilities)
	mgmt.GET("/sentinel-solver", handler.GetSentinelSolver)
	mgmt.PATCH("/sentinel-solver", handler.PatchSentinelSolver)
	mgmt.POST("/sentinel-solver/test-node", handler.TestSentinelNode)
	mgmt.GET("/config", handler.GetConfig)
	mgmt.GET("/config.yaml", handler.GetConfigYAML)
	mgmt.PUT("/config.yaml", handler.PutConfigYAML)
	mgmt.GET("/system/metrics", handler.GetSystemMetrics)
	mgmt.GET("/latest-version", handler.GetLatestVersion)
	mgmt.GET("/control-panel/update", handler.GetControlPanelUpdate)
	mgmt.POST("/control-panel/update", handler.PostControlPanelUpdate)
	handler.SetRuntimeConfigApplier(func(_ context.Context, candidate *config.Config) (config.RuntimeApplyResult, error) {
		err := s.updateSentinelOnlyClients(candidate)
		return config.RuntimeApplyResult{Applied: err == nil}, err
	})
	s.server = &http.Server{Addr: net.JoinHostPort(snapshot.Host, strconv.Itoa(snapshot.Port)), Handler: node.Handler(engine)}
	managementasset.SetCurrentConfig(snapshot)
	return s, nil
}

func (s *Server) updateSentinelOnlyClients(cfg *config.Config) error {
	s.configUpdateMu.Lock()
	defer s.configUpdateMu.Unlock()
	snapshot, err := config.Clone(cfg)
	if err != nil {
		return err
	}
	if err = snapshot.ValidateSentinelSolver(); err != nil {
		return err
	}
	old := s.currentConfig()
	// Gin routes cannot be replaced while serving. Surface changes require a restart.
	if old != nil && (snapshot.RemoteManagement.AccessPath != old.RemoteManagement.AccessPath || snapshot.Host != old.Host || snapshot.Port != old.Port || snapshot.TLS != old.TLS) {
		return fmt.Errorf("management listener/access-path changes require restarting the solver instance")
	}
	if err = s.sentinelSolver.UpdateConfig(snapshot.SentinelSolver); err != nil {
		return err
	}
	if err = s.mgmt.SetConfig(snapshot); err != nil {
		return err
	}
	s.setCurrentConfig(snapshot)
	managementasset.SetCurrentConfig(snapshot)
	s.managementRoutesEnabled.Store(s.envManagementSecret || s.localPassword != "" || snapshot.RemoteManagement.SecretKey != "")
	return nil
}

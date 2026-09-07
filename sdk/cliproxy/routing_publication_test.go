package cliproxy

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/api"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

// No HTTP requests are logged; the observer runs between runtime update stages.
type routingPublicationLogger struct {
	logging.RequestLogger
	onToggle func()
}

func (l *routingPublicationLogger) SetEnabled(bool) {
	if l.onToggle != nil {
		l.onToggle()
	}
}

func (*routingPublicationLogger) IsEnabled() bool { return false }

func TestRuntimeRoutingPublicationDoesNotMixSelectorAndPriorityRules(t *testing.T) {
	cfg := &config.Config{Routing: config.RoutingConfig{Strategy: "weighted-round-robin"}}
	cfg.RemoteManagement.DisableControlPanel = true
	manager := coreauth.NewManager(nil, &coreauth.WeightedRoundRobinSelector{}, nil)
	manager.RegisterExecutor(&weightedRoutingEntryExecutor{})
	for _, entry := range []struct{ id, weight, priority string }{{"zero", "0", "10"}, {"positive", "1", "0"}} {
		credential := &coreauth.Auth{ID: entry.id, Provider: "weighted-test", Status: coreauth.StatusActive, Attributes: map[string]string{"weight": entry.weight, "priority": entry.priority}}
		if _, err := manager.Register(coreauth.WithSkipPersist(t.Context()), credential); err != nil {
			t.Fatal(err)
		}
	}
	logger := &routingPublicationLogger{}
	server := api.NewServer(cfg, manager, nil, filepath.Join(t.TempDir(), "config.yaml"), api.WithRequestLoggerFactory(func(*config.Config, string) logging.RequestLogger { return logger }))
	t.Cleanup(func() {
		if err := server.Stop(context.WithoutCancel(t.Context())); err != nil {
			t.Error(err)
		}
	})
	service := &Service{cfg: cfg, coreManager: manager, server: server}
	observed := 0
	logger.onToggle = func() {
		observed++
		response, err := manager.Execute(coreauth.WithSkipPersist(t.Context()), []string{"weighted-test"}, coreexecutor.Request{}, coreexecutor.Options{})
		if err != nil || string(response.Payload) != "positive" {
			t.Errorf("request observed selector from new config with priority rules from old config: %q, %v", response.Payload, err)
		}
	}
	next, err := config.Clone(cfg)
	if err != nil {
		t.Fatal(err)
	}
	next.RequestLog = true
	next.Routing.Strategy = "round-robin"
	next.Routing.PriorityOverrides = []config.RoutingPriorityOverride{{Priority: 10, Strategy: "weighted-round-robin"}}
	result, err := service.ApplyRuntimeConfig(t.Context(), next)
	if err != nil || !result.Applied || observed != 1 {
		t.Fatalf("runtime reload did not reach the observation point: %v, count=%d", err, observed)
	}
}

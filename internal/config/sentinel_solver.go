package config

import (
	"fmt"
	"strings"
)

// ValidateSentinelSolver protects the configurable management surface as well
// as the fixed API namespaces checked by the portable solver configuration.
func (cfg *Config) ValidateSentinelSolver() error {
	if err := cfg.SentinelSolver.Validate(); err != nil {
		return err
	}
	mount := cfg.SentinelSolver.Path()
	prefix := ManagementAccessPathPrefix(cfg.RemoteManagement.AccessPath)
	if prefix != "" && (mount == prefix || strings.HasPrefix(prefix, mount+"/")) {
		return fmt.Errorf("sentinel-solver.access-path conflicts with the management access path")
	}
	for _, suffix := range []string{"/management.html", "/v0", "/anthropic/callback", "/codex/callback", "/antigravity/callback"} {
		route := prefix + suffix
		if mount == route || strings.HasPrefix(mount, route+"/") || strings.HasPrefix(route, mount+"/") {
			return fmt.Errorf("sentinel-solver.access-path conflicts with a management route")
		}
	}
	return nil
}

package diff

import (
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelcompat"
)

func TestSentinelCompatibilityDiffDoesNotExposeValues(t *testing.T) {
	old := &config.Config{}
	next := &config.Config{}
	next.ChatGPTWeb.Sentinel.GoVMCompatibility = sentinelcompat.Config{Enabled: true, EnvironmentProperties: []sentinelcompat.Property{{Path: "window.__fixture", Type: "string", Value: "private-value"}}}
	details := strings.Join(BuildConfigChangeDetails(old, next), "\n")
	if !strings.Contains(details, "go-vm-compatibility: updated") || strings.Contains(details, "private-value") || strings.Contains(details, "__fixture") {
		t.Fatalf("rule update not safely reported: %s", details)
	}
}

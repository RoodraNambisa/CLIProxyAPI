package diff

import (
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestCodexQuotaObservationChangeDetails(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		old := &config.Config{Codex: config.CodexConfig{ObserveQuota: !enabled}}
		current := &config.Config{Codex: config.CodexConfig{ObserveQuota: enabled}}
		changes := BuildConfigChangeDetails(old, current)
		if len(changes) != 1 || !strings.HasPrefix(changes[0], "codex.observe-quota:") || len(BuildConfigChangeDetails(current, current)) != 0 {
			t.Fatal("quota observation change reporting is inconsistent")
		}
	}
}

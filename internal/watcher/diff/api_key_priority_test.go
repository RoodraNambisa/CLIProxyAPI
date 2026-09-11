package diff

import (
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestAPIKeyPriorityChangeDetailsDoNotExposeKeys(t *testing.T) {
	before := &config.Config{}
	after := &config.Config{SDKConfig: config.SDKConfig{APIKeyGroups: []config.APIKeyGroup{{APIKey: "private-fixture-key", AllowedPriorities: []int{1}}}}}
	details := strings.Join(BuildConfigChangeDetails(before, after), "\n")
	if !strings.Contains(details, "api-key-groups") || strings.Contains(details, "private-fixture-key") {
		t.Fatal("priority update was invisible or exposed the key")
	}
}

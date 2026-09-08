package api

import (
	"bytes"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

func TestModelThinkingRejectsServerSideEffects(t *testing.T) {
	server := newTestServer(t)
	before := bytes.Clone(server.oldConfigYaml)
	requested, err := config.Clone(server.currentConfig())
	if err != nil {
		t.Fatal(err)
	}
	requested.CodexKey = []config.CodexKey{{APIKey: "fixture", BaseURL: "https://example.test", Models: []config.CodexModel{{Name: "upstream", Thinking: &registry.ThinkingSupport{Levels: []string{"invalid"}}}}}}
	requested.RequestLog = !requested.RequestLog
	toggles := 0
	server.loggerToggle = func(bool) { toggles++ }
	if err := server.UpdateClients(requested); err == nil {
		t.Fatal("server accepted invalid thinking")
	}
	if toggles != 0 || !bytes.Equal(before, server.oldConfigYaml) {
		t.Fatal("invalid thinking changed server state")
	}
}

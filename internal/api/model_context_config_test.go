package api

import (
	"bytes"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestModelContextLengthRejectsServerSideEffects(t *testing.T) {
	server := newTestServer(t)
	before := bytes.Clone(server.oldConfigYaml)
	requested, err := config.Clone(server.currentConfig())
	if err != nil {
		t.Fatal(err)
	}
	requested.CodexKey = []config.CodexKey{{APIKey: "fixture", BaseURL: "https://example.test", Models: []config.CodexModel{{Name: "upstream", MaxContextLength: -1}}}}
	requested.RequestLog = !requested.RequestLog
	toggles := 0
	server.loggerToggle = func(bool) { toggles++ }
	if err := server.UpdateClients(requested); err == nil {
		t.Fatal("server accepted an invalid model context length")
	}
	if toggles != 0 || !bytes.Equal(before, server.oldConfigYaml) {
		t.Fatal("invalid model declaration changed server state")
	}
}

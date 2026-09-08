package api

import (
	"bytes"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestRequestScopedErrorConfigRejectsServerSideEffects(t *testing.T) {
	server := newTestServer(t)
	before := bytes.Clone(server.oldConfigYaml)
	requested, err := config.Clone(server.currentConfig())
	if err != nil {
		t.Fatal(err)
	}
	requested.CodexKey = []config.CodexKey{{APIKey: "test", BaseURL: "https://example.test", RequestScopedErrors: []config.RequestScopedErrorRule{{Status: 400, Match: []string{"fixture"}, Action: "invalid"}}}}
	requested.RequestLog = !requested.RequestLog
	toggles := 0
	server.loggerToggle = func(bool) { toggles++ }
	if err := server.UpdateClients(requested); err == nil {
		t.Fatal("server accepted invalid rules")
	}
	if toggles != 0 || !bytes.Equal(before, server.oldConfigYaml) {
		t.Fatal("invalid rules changed server state")
	}
}

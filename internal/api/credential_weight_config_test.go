package api

import (
	"bytes"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestCredentialWeightInvalidConfigDoesNotApplyServerSideEffects(t *testing.T) {
	server := newTestServer(t)
	before := bytes.Clone(server.oldConfigYaml)
	requested, err := config.Clone(server.currentConfig())
	if err != nil {
		t.Fatal(err)
	}
	invalid := config.MaxCredentialWeight + 1
	requested.CodexKey = []config.CodexKey{{APIKey: "test", BaseURL: "https://example.test", Weight: &invalid}}
	requested.RequestLog = !requested.RequestLog
	toggles := 0
	server.loggerToggle = func(bool) { toggles++ }
	if err = server.UpdateClients(requested); err == nil {
		t.Fatal("API server accepted invalid weight")
	}
	if toggles != 0 || !bytes.Equal(server.oldConfigYaml, before) {
		t.Fatal("rejected config changed logger or installed snapshot")
	}
}

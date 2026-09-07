package api

import (
	"bytes"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestCredentialRequestRetryInvalidConfigDoesNotApplyServerSideEffects(t *testing.T) {
	large := int64(config.MaxCredentialRequestRetry) + 1
	if int64(int(large)) != large {
		t.Skip("Go int is 32 bits")
	}
	invalid := int(large)
	server := newTestServer(t)
	before := bytes.Clone(server.oldConfigYaml)
	requested, err := config.Clone(server.currentConfig())
	if err != nil {
		t.Fatal(err)
	}
	requested.CodexKey = []config.CodexKey{{APIKey: "test", BaseURL: "https://example.test", RequestRetry: &invalid}}
	requested.RequestLog = !requested.RequestLog
	toggles := 0
	server.loggerToggle = func(bool) { toggles++ }
	if err = server.UpdateClients(requested); err == nil {
		t.Fatal("API server accepted an invalid retry override")
	}
	if toggles != 0 || !bytes.Equal(server.oldConfigYaml, before) {
		t.Fatal("rejected retry override changed logger or installed snapshot")
	}
}

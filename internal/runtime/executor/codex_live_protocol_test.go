package executor

import (
	"errors"
	"net/http"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexLiveNeverEntersResponsesExecution(t *testing.T) {
	cfg := &config.Config{}
	for _, executor := range []auth.ProviderExecutor{NewCodexExecutor(cfg), NewCodexWebsocketsExecutor(cfg), NewCodexAutoExecutor(cfg)} {
		for _, alt := range []string{"", "responses/compact"} {
			req := core.Request{Model: "live-model", Payload: []byte(`{"audio":"opaque"}`)}
			opts := core.Options{SourceFormat: translator.FormatCodexLive, Alt: alt}
			_, errExecute := executor.Execute(t.Context(), nil, req, opts)
			_, errStream := executor.ExecuteStream(t.Context(), nil, req, opts)
			_, errCount := executor.CountTokens(t.Context(), nil, req, opts)
			for _, err := range []error{errExecute, errStream, errCount} {
				var native helps.CodexLiveNativeRouteError
				if !errors.As(err, &native) || native.StatusCode() != http.StatusNotImplemented || !native.SkipAuthResult() {
					t.Fatal("realtime entered Responses execution or affected credential health")
				}
			}
		}
	}
}

func TestCodexLiveResponsesPreflightRejectsBeforeCredentialSelection(t *testing.T) {
	m := auth.NewManager(nil, nil, nil)
	m.SetConfig(&config.Config{})
	m.RegisterExecutor(NewCodexAutoExecutor(&config.Config{}))
	req := core.Request{Model: "live-model", Payload: []byte(`{"audio":"opaque"}`)}
	opts := core.Options{SourceFormat: translator.FormatCodexLive}
	_, errExecute := m.Execute(t.Context(), []string{"codex"}, req, opts)
	_, errStream := m.ExecuteStream(t.Context(), []string{"codex"}, req, opts)
	_, errCount := m.ExecuteCount(t.Context(), []string{"codex"}, req, opts)
	for _, err := range []error{errExecute, errStream, errCount} {
		var native helps.CodexLiveNativeRouteError
		if !errors.As(err, &native) {
			t.Fatal("empty credential pool masked native protocol preflight error")
		}
	}
}

package helps

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
)

func TestCodexWebsocketDiagnosticRoles(t *testing.T) {
	if CodexWebsocketSessionLogID(" ") != "" || CodexWebsocketSessionLogID("key") != CodexWebsocketSessionLogID(" key ") {
		t.Fatal("session diagnostic identity is unstable")
	}
	if got := CodexWebsocketSessionLogID("client-cache-key"); !strings.HasPrefix(got, "sha256:") || strings.Contains(got, "client-cache-key") {
		t.Fatal("session diagnostic exposes the original identity")
	}
	for _, key := range []string{"a", "client-cache-key"} {
		closed := &websocket.CloseError{Code: websocket.CloseNormalClosure, Text: key}
		wrapped := fmt.Errorf("read upstream: %w", closed)
		for _, err := range []error{closed, wrapped, errors.New(key)} {
			masked := CodexWebsocketLogError(err, util.NewPromptCacheLogRedactor(key))
			if !strings.Contains(masked, util.PromptCacheLogMarker) || closed.Text != key {
				t.Fatal("close reason or scalar error was not independently protected")
			}
			if CodexWebsocketLogError(err, nil) != err.Error() {
				t.Fatal("missing policy changed the diagnostic")
			}
		}
		if !strings.HasPrefix(CodexWebsocketLogError(wrapped, util.NewPromptCacheLogRedactor(key)), "read upstream: websocket: close 1000") {
			t.Fatal("diagnostic projection lost the wrapper or close code")
		}
	}
	if CodexWebsocketLogError(nil, nil) != "" {
		t.Fatal("nil error produced a diagnostic")
	}
}

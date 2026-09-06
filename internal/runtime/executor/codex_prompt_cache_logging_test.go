package executor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	log "github.com/sirupsen/logrus"
)

func TestCodexSDKCacheKeyDiagnosticProtectionIndependentOfPassthrough(t *testing.T) {
	var output bytes.Buffer
	level, originalOutput := log.GetLevel(), log.StandardLogger().Out
	formatter := log.StandardLogger().Formatter
	log.SetLevel(log.DebugLevel)
	log.SetFormatter(&log.JSONFormatter{DisableTimestamp: true})
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetLevel(level); log.SetOutput(originalOutput); log.SetFormatter(formatter) })
	for _, tc := range []struct {
		passthrough bool
		key         string
	}{{false, "sdk-client-cache-key"}, {true, "sdk-client-cache-key"}, {false, "a"}, {true, "a"}} {
		t.Run(fmt.Sprintf("passthrough=%t/key-length=%d", tc.passthrough, len(tc.key)), func(t *testing.T) {
			key := tc.key
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(fmt.Sprintf(`{"error":{"message":%q,"type":"invalid_request_error"}}`, key)))
			}))
			defer server.Close()
			cfg := &config.Config{SDKConfig: config.SDKConfig{ProxyURL: "direct"}, Codex: config.CodexConfig{PassthroughPromptCacheKey: tc.passthrough}}
			executor := NewCodexExecutor(cfg)
			auth := &cliproxyauth.Auth{ID: "sdk-log-test", Provider: "codex", Attributes: map[string]string{"api_key": "test-key", "base_url": server.URL}}
			req := cliproxyexecutor.Request{Model: "gpt-5.4", Payload: []byte(fmt.Sprintf(`{"model":"gpt-5.4","input":"hello","prompt_cache_key":%q}`, key))}
			output.Reset()
			_, err := executor.Execute(t.Context(), auth, req, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse})
			if err == nil || !strings.Contains(err.Error(), fmt.Sprintf(`"message":%q`, key)) || strings.Contains(err.Error(), util.PromptCacheLogMarker) {
				t.Fatal("diagnostic redaction changed the public error")
			}
			for _, line := range bytes.Split(bytes.TrimSpace(output.Bytes()), []byte("\n")) {
				var entry struct {
					Message string `json:"msg"`
				}
				if err := json.Unmarshal(line, &entry); err != nil {
					t.Fatal("invalid diagnostic JSON")
				}
				if strings.HasSuffix(entry.Message, "error message: "+key) || strings.Contains(entry.Message, fmt.Sprintf(`"message":%q`, key)) {
					t.Fatal("SDK diagnostic leaked a scalar cache key")
				}
			}
			if !strings.Contains(output.String(), util.PromptCacheLogMarker) {
				t.Fatal("SDK diagnostic did not protect the cache key")
			}
		})
	}
}

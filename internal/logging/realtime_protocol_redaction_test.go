package logging

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRequestLogRedactsRealtimeSubprotocolCredentials(t *testing.T) {
	dir := t.TempDir()
	logger := NewFileRequestLogger(true, dir, dir, 10)
	const secret = "fixture-browser-credential-not-for-logs"
	protocols := "realtime, openai-insecure-api-key." + secret
	headers := map[string][]string{"Sec-Websocket-Protocol": {protocols}, "Upgrade": {"websocket"}}
	if errLog := logger.LogRequest("/v1/realtime", "GET", headers, nil, 503, nil, nil, nil, nil, nil, nil, nil, "fixture-realtime", time.Now(), time.Time{}); errLog != nil {
		t.Fatal(errLog)
	}
	entries, errRead := os.ReadDir(dir)
	if errRead != nil {
		t.Fatal(errRead)
	}
	if len(entries) == 0 {
		t.Fatal("request logger wrote no output")
	}
	for _, entry := range entries {
		body, errRead := os.ReadFile(filepath.Join(dir, entry.Name()))
		if errRead != nil {
			t.Fatal(errRead)
		}
		if strings.Contains(string(body), secret) || !strings.Contains(string(body), "openai-insecure-api-key.[REDACTED]") {
			t.Fatal("request log exposed a Realtime credential")
		}
	}
	if headers["Sec-Websocket-Protocol"][0] != protocols {
		t.Fatal("logging redaction modified the actual handshake header")
	}
}

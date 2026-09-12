package logging

import (
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

func TestLogFormatterPrintsVersionField(t *testing.T) {
	entry := log.NewEntry(log.New())
	entry.Time = time.Date(2026, 6, 9, 11, 10, 2, 0, time.Local)
	entry.Level = log.InfoLevel
	entry.Message = "fetched latest antigravity version"
	entry.Data["version"] = "2.2.1"

	formatted, errFormat := (&LogFormatter{}).Format(entry)
	if errFormat != nil {
		t.Fatalf("Format() error = %v", errFormat)
	}

	line := string(formatted)
	if !strings.Contains(line, "version=2.2.1") {
		t.Fatalf("formatted line %q missing version field", line)
	}
}

func TestLogFormatterKeepsFailureIdentityWithoutPrivateBody(t *testing.T) {
	entry := log.NewEntry(log.New())
	entry.Message = "provider request attempt failed"
	entry.Data = log.Fields{"request_id": "a5c2bae8", "auth_name": "my credential.json", "auth_index": "123abc", "provider": "codex", "status": 429, "code": "usage_limit_reached", "upstream_request_id": "upstream-fixture", "response_body": "private-body", "access_token": "private-token"}
	formatted, err := (&LogFormatter{}).Format(entry)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`request_id="a5c2bae8"`, `auth_name="my credential.json"`, "auth_index=123abc", "status=429", "code=usage_limit_reached", `upstream_request_id="upstream-fixture"`} {
		if !strings.Contains(string(formatted), want) {
			t.Fatalf("missing %q", want)
		}
	}
	if strings.Contains(string(formatted), "private-") {
		t.Fatal("private diagnostic data entered console log")
	}
}

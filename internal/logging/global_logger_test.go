package logging

import (
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/managementdiag"

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

func TestLogFormatterPrintsOnlyBoundedSafeUpstreamEvidence(t *testing.T) {
	entry := log.NewEntry(log.New())
	entry.Data = log.Fields{
		"upstream_status": 403, "upstream_stage": "upstream_request", "upstream_code": "cloudflare_challenge",
		"target_path":   managementdiag.NewManagementOnlyValueWithFallback("/?private=value", "/"),
		"response_type": "html", "content_type": "text/html", "cf_ray": "fixture-YUL",
		"response_text": managementdiag.NewManagementOnlyValueWithFallback("private management text", "Enable JavaScript"),
		"response_body": managementdiag.NewManagementOnlyValueWithFallback("private management body", "<html>token=secret-value\n"+strings.Repeat("detail ", 800)+"</html>"),
	}
	raw, err := (&LogFormatter{}).Format(entry)
	if err != nil {
		t.Fatal(err)
	}
	line := string(raw)
	for _, want := range []string{"upstream_status=403", "upstream_code=cloudflare_challenge", "response_type=html", `target_path="/"`, `response_text="Enable JavaScript"`, "response_body=", "log_preview_truncated=true"} {
		if !strings.Contains(line, want) {
			t.Fatalf("missing %q", want)
		}
	}
	if strings.Contains(line, "private management") || strings.Contains(line, "secret-value") || strings.Count(line, "\n") != 1 || len(line) > 3200 {
		t.Fatal("unsafe/unbounded diagnostic log")
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

func TestLogFormatterKeepsLocalPolicyOrigin(t *testing.T) {
	entry := log.NewEntry(log.New())
	entry.Data = log.Fields{"stage": "local_policy", "error_origin": "local", "policy": "disabled_image_generation_tool"}
	formatted, err := (&LogFormatter{}).Format(entry)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"stage=local_policy", "error_origin=local", "policy=disabled_image_generation_tool"} {
		if !strings.Contains(string(formatted), want) {
			t.Fatalf("formatted log missing %q", want)
		}
	}
}

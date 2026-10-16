package helps

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxyregion"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

func timeBody(messages ...map[string]any) []byte {
	v, _ := json.Marshal(map[string]any{"model": "test", "previous_response_id": "resp_1", "input": messages})
	return v
}
func timeMessage(role, text string) map[string]any {
	return map[string]any{"role": role, "content": text}
}

const cliTime = "<environment_context>\n<cwd>/safe</cwd>\n<current_date>2026-01-01</current_date>\n<timezone>UTC</timezone>\n</environment_context>"
const desktopTime = "<codex_apps_client_time_context>\n<current_date>2026-01-01</current_date>\n<timezone>UTC</timezone>\nUse this client time context for current dates.\n</codex_apps_client_time_context>"

func TestCodexTimeContextCLIAndDesktopAcrossDate(t *testing.T) {
	at := time.Date(2026, 3, 8, 6, 59, 0, 0, time.UTC)
	body := timeBody(timeMessage("user", cliTime), timeMessage("developer", desktopTime))
	for _, zone := range []string{"America/Los_Angeles", "Asia/Tokyo"} {
		got, changed, err := RewriteCodexTimeContext(body, zone, at, false)
		if err != nil || len(changed) != 2 {
			t.Fatalf("rewrite %v %v", changed, err)
		}
		loc, _ := time.LoadLocation(zone)
		text := gjson.GetBytes(got, "input.0.content").String()
		if !strings.Contains(text, "<timezone>"+zone+"</timezone>") || !strings.Contains(text, at.In(loc).Format("2006-01-02")) {
			t.Fatal(text)
		}
		if gjson.GetBytes(got, "previous_response_id").String() != "resp_1" {
			t.Fatal("continuation changed")
		}
	}
}
func TestCodexTimeContextPreservesHistoryToolsAndQuotes(t *testing.T) {
	body := timeBody(timeMessage("user", cliTime), timeMessage("assistant", "old answer"), timeMessage("user", cliTime), timeMessage("tool", cliTime), timeMessage("user", "```xml\n"+desktopTime+"\n```"))
	got, _, err := RewriteCodexTimeContext(body, "Asia/Tokyo", time.Now(), true)
	if err != nil {
		t.Fatal(err)
	}
	if gjson.GetBytes(got, "input.0.content").String() != cliTime || gjson.GetBytes(got, "input.3.content").String() != cliTime || gjson.GetBytes(got, "input.4.content").String() != gjson.GetBytes(body, "input.4.content").String() {
		t.Fatal("historical or quoted context changed")
	}
	if gjson.GetBytes(got, "input.2.content").String() == cliTime {
		t.Fatal("latest context unchanged")
	}
}
func TestCodexTimeContextIncrementalCompactAndManualIndependence(t *testing.T) {
	body := []byte(`{"type":"response.create","model":"x","previous_response_id":"resp","input":[{"type":"function_call_output","call_id":"call_1","output":"2026-01-01"}]}`)
	at := time.Now()
	got, changed, err := RewriteCodexTimeContext(body, "UTC", at, true)
	if err != nil || len(changed) != 0 || string(got) != string(body) {
		t.Fatal("compact injected context")
	}
	got, changed, err = RewriteCodexTimeContext(body, "UTC", at, false)
	if err != nil || len(changed) != 1 || gjson.GetBytes(got, "input.0.call_id").String() != "call_1" || gjson.GetBytes(got, "input.1.role").String() != "developer" {
		t.Fatal("incremental context missing")
	}
	cfg := config.CodexTimeContextConfig{Enabled: true, Mode: "manual", Timezone: "Asia/Tokyo"}
	ctx, trace := WithTimeContextDiagnostics(t.Context())
	got = ApplyCodexTimeContext(ctx, nil, nil, proxyregion.Route{}, body, CodexTimeContextSnapshot{Policy: cfg, At: at}, false)
	if len(trace.Items()) != 1 || trace.Items()[0].Source != "manual" || len(got) == len(body) {
		t.Fatal("manual mode required metadata")
	}
}
func TestCodexTimeContextOffInheritAndFallback(t *testing.T) {
	body := timeBody(timeMessage("user", cliTime))
	ctx, trace := WithTimeContextDiagnostics(t.Context())
	base := config.CodexTimeContextConfig{Enabled: true, Mode: "proxy", LookupFailure: "fallback", FallbackTimezone: "Asia/Tokyo"}
	auth := &coreauth.Auth{Metadata: map[string]any{config.CodexTimeContextMetadataKey: map[string]any{"mode": "off"}}}
	if got := ApplyCodexTimeContext(ctx, nil, auth, proxyregion.Route{}, body, CodexTimeContextSnapshot{Policy: base, At: time.Now()}, false); string(got) != string(body) {
		t.Fatal("off changed body")
	}
	auth.Metadata[config.CodexTimeContextMetadataKey] = map[string]any{"mode": "inherit"}
	got := ApplyCodexTimeContext(ctx, nil, auth, proxyregion.Route{}, body, CodexTimeContextSnapshot{Policy: base, At: time.Now()}, false)
	if !strings.Contains(string(got), "Asia/Tokyo") || trace.Items()[1].Source != "fallback" {
		t.Fatal("fallback not applied")
	}
}

func TestCodexManualProxyRegionPreservesLanguageAndSkipsLookup(t *testing.T) {
	s := proxyregion.New(t.Context(), config.ProxyIPConfig{})
	defer s.Close()
	s.SetManualSource(func(proxyregion.Route) (config.ProxyRegionConfig, string) {
		return config.ProxyRegionConfig{Timezone: "Asia/Tokyo", Language: "ja-JP"}, "global_proxy"
	})
	cfg := config.CodexTimeContextConfig{Enabled: true, Mode: "proxy"}
	body := timeBody(timeMessage("user", cliTime), timeMessage("developer", desktopTime))
	got := ApplyCodexTimeContext(t.Context(), s, nil, proxyregion.Route{ProxyURL: "http://fixture:80"}, body, CodexTimeContextSnapshot{Policy: cfg, At: time.Now()}, false)
	if !strings.Contains(string(got), "Asia/Tokyo") || strings.Contains(string(got), "ja-JP") || s.Counts()["pending"] != 0 {
		t.Fatalf("manual region not applied correctly %s", got)
	}
}

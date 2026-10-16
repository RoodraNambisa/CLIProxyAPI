package helps

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxyregion"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type CodexTimeContextSnapshot struct {
	Policy config.CodexTimeContextConfig
	At     time.Time
}
type CodexTimeContextDiagnostic struct {
	Mode     string               `json:"mode"`
	Timezone string               `json:"timezone,omitempty"`
	Date     string               `json:"current_date,omitempty"`
	Source   string               `json:"source"`
	Region   proxyregion.Snapshot `json:"region"`
	Changed  []string             `json:"changed"`
	Reason   string               `json:"reason,omitempty"`
}
type timeDiagnosticKey struct{}
type TimeContextDiagnostics struct {
	mu    sync.Mutex
	items []CodexTimeContextDiagnostic
}

func WithTimeContextDiagnostics(ctx context.Context) (context.Context, *TimeContextDiagnostics) {
	d := &TimeContextDiagnostics{}
	return context.WithValue(ctx, timeDiagnosticKey{}, d), d
}
func (d *TimeContextDiagnostics) Items() []CodexTimeContextDiagnostic {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]CodexTimeContextDiagnostic(nil), d.items...)
}
func RecordTimeContextDiagnostic(ctx context.Context, value CodexTimeContextDiagnostic) {
	if d, ok := ctx.Value(timeDiagnosticKey{}).(*TimeContextDiagnostics); ok {
		d.mu.Lock()
		if len(d.items) < 32 {
			d.items = append(d.items, value)
		}
		d.mu.Unlock()
	}
}

var timeContextBlock = regexp.MustCompile(`(?s)<(environment_context|codex_apps_client_time_context|codex_proxy_time_context)>.*?</(?:environment_context|codex_apps_client_time_context|codex_proxy_time_context)>`)
var timezoneTag = regexp.MustCompile(`<timezone>[^<>\r\n]*</timezone>`)
var currentDateTag = regexp.MustCompile(`<current_date>\d{4}-\d{2}-\d{2}</current_date>`)
var environmentMarker = regexp.MustCompile(`<(?:cwd|shell|environments|filesystem)(?:>|\s)`)

type timeText struct{ path, text string }
type timePatch struct {
	index, start, end int
	kind              string
}

func fencedBefore(text string, position int) bool {
	fence := ""
	for _, line := range strings.Split(text[:position], "\n") {
		trim := strings.TrimSpace(line)
		if strings.HasPrefix(trim, "```") || strings.HasPrefix(trim, "~~~") {
			mark := trim[:3]
			if fence == "" {
				fence = mark
			} else if mark == fence {
				fence = ""
			}
		}
	}
	return fence != ""
}
func nativeTimePatches(texts []timeText) []timePatch {
	latest := map[string]timePatch{}
	for idx, t := range texts {
		for _, loc := range timeContextBlock.FindAllStringSubmatchIndex(t.text, -1) {
			start, end := loc[0], loc[1]
			kind := t.text[loc[2]:loc[3]]
			block := t.text[start:end]
			if !strings.HasSuffix(block, "</"+kind+">") || fencedBefore(t.text, start) {
				continue
			}
			line := t.text[strings.LastIndex(t.text[:start], "\n")+1 : start]
			if strings.TrimSpace(line) != "" {
				continue
			}
			if !timezoneTag.MatchString(block) || !currentDateTag.MatchString(block) {
				continue
			}
			if kind == "environment_context" && !environmentMarker.MatchString(block) {
				continue
			}
			if kind == "codex_apps_client_time_context" && !strings.Contains(block, "Use this client time context") {
				continue
			}
			latest[kind] = timePatch{idx, start, end, kind}
		}
	}
	patches := make([]timePatch, 0, len(latest))
	for _, p := range latest {
		patches = append(patches, p)
	}
	sort.Slice(patches, func(i, j int) bool {
		if patches[i].index != patches[j].index {
			return patches[i].index < patches[j].index
		}
		return patches[i].start > patches[j].start
	})
	return patches
}

// RewriteCodexTimeContext edits only recognized context messages, not tools,
// code fences, search locations, response timestamps, or historical messages.
func RewriteCodexTimeContext(body []byte, timezone string, at time.Time, compact bool) ([]byte, []string, error) {
	location, err := time.LoadLocation(timezone)
	if err != nil || !config.ValidIANATimezone(timezone) {
		return body, nil, fmt.Errorf("invalid time-context timezone")
	}
	if at.IsZero() {
		at = time.Now()
	}
	date := at.In(location).Format("2006-01-02")
	texts := []timeText{}
	if v := gjson.GetBytes(body, "instructions"); v.Type == gjson.String {
		texts = append(texts, timeText{"instructions", v.String()})
	}
	for i, item := range gjson.GetBytes(body, "input").Array() {
		role := item.Get("role").String()
		if role != "user" && role != "developer" && role != "system" {
			continue
		}
		if typ := item.Get("type").String(); typ != "" && typ != "message" {
			continue
		}
		content := item.Get("content")
		if content.Type == gjson.String {
			texts = append(texts, timeText{fmt.Sprintf("input.%d.content", i), content.String()})
			continue
		}
		for j, part := range content.Array() {
			if typ := part.Get("type").String(); typ != "input_text" && typ != "text" {
				continue
			}
			if v := part.Get("text"); v.Type == gjson.String {
				texts = append(texts, timeText{fmt.Sprintf("input.%d.content.%d.text", i, j), v.String()})
			}
		}
	}
	patches := nativeTimePatches(texts)
	changed := []string{}
	dirty := map[int]bool{}
	for _, p := range patches {
		value := texts[p.index].text
		block := value[p.start:p.end]
		replacement := timezoneTag.ReplaceAllString(block, "<timezone>"+timezone+"</timezone>")
		replacement = currentDateTag.ReplaceAllString(replacement, "<current_date>"+date+"</current_date>")
		if replacement != block {
			texts[p.index].text = value[:p.start] + replacement + value[p.end:]
			dirty[p.index] = true
			changed = append(changed, p.kind)
		}
	}
	for idx := range dirty {
		body, err = sjson.SetBytes(body, texts[idx].path, texts[idx].text)
		if err != nil {
			return nil, nil, err
		}
	}
	if len(patches) == 0 && !compact {
		text := "<codex_proxy_time_context>\n<timezone>" + timezone + "</timezone>\n<current_date>" + date + "</current_date>\nUse this time context for current dates and schedules unless the user specifies another timezone.\n</codex_proxy_time_context>"
		message := map[string]any{"type": "message", "role": "developer", "content": []any{map[string]any{"type": "input_text", "text": text}}}
		input := gjson.GetBytes(body, "input")
		if input.Type == gjson.String {
			b, _ := json.Marshal([]any{map[string]any{"role": "user", "content": input.String()}, message})
			body, err = sjson.SetRawBytes(body, "input", b)
		} else if input.IsArray() {
			body, err = sjson.SetBytes(body, "input.-1", message)
		} else if !input.Exists() {
			body, err = sjson.SetBytes(body, "input", []any{message})
		} else {
			return body, changed, nil
		}
		if err != nil {
			return nil, nil, err
		}
		changed = append(changed, "codex_proxy_time_context:added")
	}
	return body, changed, nil
}

// SelectCodexTimeContext uses the same policy for execution and read-only previews.
// A preview never enqueues lookups or records a request as applied.
func SelectCodexTimeContext(ctx context.Context, service *proxyregion.Service, auth *coreauth.Auth, route proxyregion.Route, base config.CodexTimeContextConfig, lookup bool) CodexTimeContextDiagnostic {
	var override any
	if auth != nil {
		override = auth.Metadata[config.CodexTimeContextMetadataKey]
	}
	policy, err := config.EffectiveCodexTimeContext(base, override)
	diagnostic := CodexTimeContextDiagnostic{Mode: policy.Mode, Source: "disabled", Changed: []string{}}
	if err != nil {
		diagnostic.Reason = "invalid_policy"
		return diagnostic
	}
	if !policy.Enabled || policy.Mode == "off" {
		return diagnostic
	}
	zone := policy.Timezone
	diagnostic.Source = "manual"
	if policy.Mode == "proxy" {
		diagnostic.Source = "proxy"
		manual, _ := service.Manual(route)
		source, _ := service.ManualSources(route)
		if manual.Timezone != "" {
			diagnostic.Region = service.Preview(route)
			diagnostic.Region.Timezone = manual.Timezone
			diagnostic.Region.TimezoneSource = source
			diagnostic.Source = source
		} else {
			if lookup {
				diagnostic.Region = service.ResolveForRequest(ctx, route)
			} else {
				diagnostic.Region = service.Preview(route)
			}
		}
		zone = diagnostic.Region.Timezone
		diagnostic.Reason = diagnostic.Region.Reason
		if zone == "" {
			if policy.LookupFailure == "fallback" {
				zone = policy.FallbackTimezone
				diagnostic.Source = "fallback"
			} else {
				diagnostic.Source = "preserved"
				return diagnostic
			}
		}
	}
	diagnostic.Timezone = zone
	return diagnostic
}

func ApplyCodexTimeContext(ctx context.Context, service *proxyregion.Service, auth *coreauth.Auth, route proxyregion.Route, body []byte, snapshot CodexTimeContextSnapshot, compact bool) []byte {
	diagnostic := SelectCodexTimeContext(ctx, service, auth, route, snapshot.Policy, true)
	zone := diagnostic.Timezone
	if zone == "" {
		RecordTimeContextDiagnostic(ctx, diagnostic)
		return body
	}
	result, changed, err := RewriteCodexTimeContext(body, zone, snapshot.At, compact)
	if err != nil {
		diagnostic.Timezone = ""
		diagnostic.Reason = "context_rewrite_failed"
		RecordTimeContextDiagnostic(ctx, diagnostic)
		return body
	}
	diagnostic.Timezone = zone
	location, _ := time.LoadLocation(zone)
	at := snapshot.At
	if at.IsZero() {
		at = time.Now()
	}
	diagnostic.Date = at.In(location).Format("2006-01-02")
	diagnostic.Changed = changed
	RecordTimeContextDiagnostic(ctx, diagnostic)
	return result
}

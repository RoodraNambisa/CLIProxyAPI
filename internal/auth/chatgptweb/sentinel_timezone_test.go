package chatgptweb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

func TestConversationProofTimezoneMatchesRequirements(t *testing.T) {
	clock := time.Date(2026, time.October, 15, 9, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		offset int
		want   string
	}{
		{"Asia/Tokyo", -540, "Thu Oct 15 2026 18:00:00 GMT+0900"},
		{"America/New_York", 240, "Thu Oct 15 2026 05:00:00 GMT-0400"},
		{"UTC", 0, "Thu Oct 15 2026 09:00:00 GMT+0000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := ConversationTurnstileEnvironment{Persona: DefaultPersona(), Timezone: tc.name, TimezoneOffsetMinutes: tc.offset}
			p, err := BuildConversationRequirementsTokenWithEnvironment(env, nil, "", zeroReader{}, func() time.Time { return clock })
			if err != nil {
				t.Fatal(err)
			}
			proof, err := BuildConversationProofTokenWithEnvironment(t.Context(), "seed", "ff", env, nil, "", zeroReader{}, func() time.Time { return clock })
			if err != nil {
				t.Fatal(err)
			}
			for _, token := range []string{p, proof} {
				data, errDecode := base64.StdEncoding.DecodeString(strings.TrimSuffix(token[7:], "~S"))
				if errDecode != nil {
					t.Fatal(errDecode)
				}
				var fields []any
				if errDecode = json.Unmarshal(data, &fields); errDecode != nil {
					t.Fatal(errDecode)
				}
				if date, _ := fields[1].(string); !strings.HasPrefix(date, tc.want) {
					t.Fatalf("date = %q, want prefix %q", date, tc.want)
				}
			}
			wire, err := json.Marshal(ComputeEnvironment(env))
			if err != nil {
				t.Fatal(err)
			}
			var remote SentinelComputeEnvironment
			if err = json.Unmarshal(wire, &remote); err != nil {
				t.Fatal(err)
			}
			if got := remote.native(nil); got.Timezone != tc.name || got.TimezoneOffsetMinutes != tc.offset {
				t.Fatalf("RPC lost timezone: %+v", got)
			}
		})
	}
}

func TestSentinelSDKDateUsesRequestTimezone(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	request, sdk := sentinelRuntimeTestRequests(t, false)
	request.Environment.Timezone = "Asia/Tokyo"
	request.Environment.TimezoneOffsetMinutes = -540
	sdk.Environment = request.Environment
	source := strings.Replace(sentinelRuntimeTestSDK, `return "sdk-turnstile-token"`, `const d=new Date(1792054800000);return JSON.stringify({offset:d.getTimezoneOffset(),text:d.toString(),iso:d.toISOString(),utcHour:d.getUTCHours()})`, 1)
	sdk.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		return []byte(source), "application/javascript", target, nil
	}
	result, err := manager.SolveTurnstile(t.Context(), request, sdk, nil)
	if err != nil {
		t.Fatal(err)
	}
	var observed struct {
		Offset  int    `json:"offset"`
		Text    string `json:"text"`
		ISO     string `json:"iso"`
		UTCHour int    `json:"utcHour"`
	}
	if err = json.Unmarshal([]byte(result), &observed); err != nil {
		t.Fatal(err)
	}
	if observed.Offset != -540 || !strings.Contains(observed.Text, "GMT+0900") {
		t.Fatalf("SDK did not inherit request timezone: %+v", observed)
	}
	if observed.ISO != "2026-10-15T09:00:00.000Z" || observed.UTCHour != 9 {
		t.Fatalf("timezone changed absolute clock: %+v", observed)
	}
}

func TestClientTimezonePinningAndProxyClone(t *testing.T) {
	client, err := NewClient(DefaultPersona(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	zone := BrowserTimezone{Name: "Asia/Tokyo", OffsetMinutes: -540}
	if err = client.SetBrowserTimezone(zone); err != nil {
		t.Fatal(err)
	}
	if err = client.SetBrowserTimezone(zone); err != nil {
		t.Fatal("reapplying the same immutable timezone must succeed")
	}
	if err = client.SetBrowserTimezone(BrowserTimezone{Name: "UTC"}); err == nil {
		t.Fatal("in-flight client timezone was changed")
	}
	clone, err := client.CloneWithProxy("")
	if err != nil {
		t.Fatal(err)
	}
	defer clone.CloseIdleConnections()
	if got, ok := clone.BrowserTimezone(); !ok || got != zone {
		t.Fatal("proxy clone lost the pinned timezone")
	}
}

func TestConversationEnvironmentRejectsInvalidTimezone(t *testing.T) {
	for _, zone := range []BrowserTimezone{{Name: "Invalid/Zone"}, {Name: "UTC", OffsetMinutes: 841}, {Name: "UTC", OffsetMinutes: -841}} {
		client := &Client{}
		if err := client.SetBrowserTimezone(zone); err == nil {
			t.Fatal("invalid client timezone accepted")
		}
		input := computeTestInput()
		input.Environment.Timezone = zone.Name
		input.Environment.TimezoneOffsetMinutes = zone.OffsetMinutes
		if err := input.validate(); err == nil {
			t.Fatal("invalid remote timezone accepted")
		}
	}
}

func TestConversationProofUsesSharedPageClock(t *testing.T) {
	now := time.Date(2026, time.October, 15, 9, 0, 0, 0, time.UTC)
	started := now.Add(-1250 * time.Millisecond)
	env := ConversationTurnstileEnvironment{Persona: DefaultPersona(), PageStartedAt: started}
	fields, err := buildConversationPoWConfig(env, nil, "", zeroReader{}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if fields[13] != float64(1250) || math.Abs(fields[17].(float64)-float64(started.UnixMilli())) >= 0.001 {
		t.Fatalf("proof page clock mismatch: elapsed=%v origin=%v", fields[13], fields[17])
	}
}

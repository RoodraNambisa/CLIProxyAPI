package helps

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/tidwall/gjson"
)

func TestSanitizeCodexInputItemIDPrefixesAndPairing(t *testing.T) {
	for kind, prefix := range map[string]string{"message": "msg_", "reasoning": "rs_", "function_call": "fc_", "custom_tool_call": "ctc_", "custom_tool_call_output": "ctco_"} {
		body := []byte(fmt.Sprintf(`{"input":[{"type":%q,"id":"source","call_id":"paired","input":{"id":"business"}}]}`, kind))
		got := SanitizeCodexInputItemIDs(body)
		if gjson.GetBytes(got, "input.0.id").Str != prefix+"source" {
			t.Fatalf("wrong prefix for %s", kind)
		}
		if gjson.GetBytes(got, "input.0.call_id").Str != "paired" || gjson.GetBytes(got, "input.0.input.id").Str != "business" {
			t.Fatal("rewrote pairing or business IDs")
		}
		if twice := SanitizeCodexInputItemIDs(got); !bytes.Equal(got, twice) {
			t.Fatal("ID normalization is not idempotent")
		}
	}
}

func TestSanitizeCodexInputItemIDCharacterBoundaries(t *testing.T) {
	for _, suffix := range []string{strings.Repeat("a", 60), strings.Repeat("a", 61), strings.Repeat("a", 62), strings.Repeat("界", 61), strings.Repeat("界", 62)} {
		id := "fc_" + suffix
		body := []byte(fmt.Sprintf(`{"input":[{"type":"function_call","id":%q,"call_id":%q}]}`, id, id))
		got := SanitizeCodexInputItemIDs(body)
		result := gjson.GetBytes(got, "input.0.id").Str
		if utf8.RuneCountInString(result) > 64 || !strings.HasPrefix(result, "fc_") {
			t.Fatal("invalid ID length or prefix")
		}
		if utf8.RuneCountInString(id) <= 64 && result != id {
			t.Fatal("changed a valid ID")
		}
		if gjson.GetBytes(got, "input.0.call_id").Str != id {
			t.Fatal("changed pairing ID")
		}
		if !bytes.Equal(got, SanitizeCodexInputItemIDs(body)) {
			t.Fatal("shortened ID is not deterministic")
		}
	}
}

func TestSanitizeCodexInputItemIDCollisionReservations(t *testing.T) {
	long := "msg_" + strings.Repeat("long", 25)
	reserved := codexInputItemIDWithHashSuffix(long, 0)
	reservedNext := codexInputItemIDWithHashSuffix(long, 1)
	body := []byte(fmt.Sprintf(`{"input":[{"type":"message","id":%q},{"type":"message","id":%q},{"type":"message","id":%q},{"type":"message","id":"same"},{"type":"message","id":"msg_same"},{"type":"message","id":%q}]}`, long, reserved, reservedNext, long))
	got := SanitizeCodexInputItemIDs(body)
	items := gjson.GetBytes(got, "input").Array()
	if items[1].Get("id").Str != reserved || items[2].Get("id").Str != reservedNext || items[4].Get("id").Str != "msg_same" {
		t.Fatal("existing IDs were not preserved")
	}
	if items[0].Get("id").Str == reserved || items[0].Get("id").Str == reservedNext || items[3].Get("id").Str == "msg_same" {
		t.Fatal("allocated an occupied ID")
	}
	if items[0].Get("id").Str != items[5].Get("id").Str {
		t.Fatal("duplicate original ID lost its stable mapping")
	}
	if !bytes.Equal(got, SanitizeCodexInputItemIDs(got)) {
		t.Fatal("collision repair is not idempotent")
	}
}

func TestSanitizeCodexInputItemIDsEncryptedAndLegacyPayloads(t *testing.T) {
	id := "rs_" + strings.Repeat("x", 64)
	body := []byte(fmt.Sprintf(`{"input":[{"type":"reasoning","id":%q,"encrypted_content":"opaque"},{"type":"reasoning","id":%q},{"type":"reasoning","id":"rs_valid","encrypted_content":"unchanged"}]}`, id, id))
	got := SanitizeCodexInputItemIDs(body)
	items := gjson.GetBytes(got, "input").Array()
	if len(items) != 2 || items[1].Get("encrypted_content").Str != "unchanged" {
		t.Fatal("encrypted item handling changed opaque content")
	}
	if utf8.RuneCountInString(items[0].Get("id").Str) > 64 {
		t.Fatal("unencrypted reasoning was not shortened")
	}
	for _, raw := range []string{`{}`, `{"input":"text"}`, `{"input":null}`, `{"input":[{"type":"message"},{"id":42},{"type":"message","id":""}]}`} {
		if got := SanitizeCodexInputItemIDs([]byte(raw)); string(got) != raw {
			t.Fatal("changed a legacy payload without string IDs")
		}
	}
}

func TestSanitizeCodexOverlongPrefixCollisionKeepsDistinctOriginalIDs(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		withoutPrefix := strings.Repeat("x", 70)
		withPrefix := "msg_" + withoutPrefix
		first, second := withoutPrefix, withPrefix
		if reverse {
			first, second = second, first
		}
		body := []byte(fmt.Sprintf(`{"input":[{"type":"message","id":%q},{"type":"message","id":%q},{"type":"message","id":%q},{"type":"message","id":%q}]}`, first, second, first, second))
		got := SanitizeCodexInputItemIDs(body)
		items := gjson.GetBytes(got, "input").Array()
		if items[0].Get("id").Str == items[1].Get("id").Str ||
			items[0].Get("id").Str != items[2].Get("id").Str ||
			items[1].Get("id").Str != items[3].Get("id").Str {
			t.Fatal("prefix repair merged distinct overlong IDs or lost duplicate identity")
		}
		if !bytes.Equal(got, SanitizeCodexInputItemIDs(got)) {
			t.Fatal("overlong prefix collision repair is not idempotent")
		}
	}
}

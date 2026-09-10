package helps

import (
	"fmt"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestScalarPayloadUpdatesPreserveTypesAndOwnership(t *testing.T) {
	for _, payload := range []string{`{}`, `{"model":null,"stream":null}`, `{"model":123,"stream":"true"}`, `{"model":"other","stream":false}`, `{"model":"fixture","stream":true}`, `{"model":"fixtur\u0065","stream":true}`} {
		raw := []byte(payload)
		out, err := SetStringIfDifferent(raw, "model", "fixture")
		if err != nil {
			t.Fatal(err)
		}
		out, err = SetBoolIfDifferent(out, "stream", true)
		if err != nil || gjson.GetBytes(out, "model").Type != gjson.String || gjson.GetBytes(out, "model").String() != "fixture" || gjson.GetBytes(out, "stream").Type != gjson.True {
			t.Fatalf("normalization failed: %s, %v", out, err)
		}
		if string(raw) != payload {
			t.Fatal("caller payload changed")
		}
		if gjson.GetBytes(raw, "model").String() == "fixture" && &out[0] != &raw[0] {
			t.Fatal("unchanged fields copied the payload")
		}
	}
	for _, payload := range []string{`{}`, `{"stream":null}`, `{"stream":0}`, `{"stream":"false"}`, `{"stream":true}`, `{"stream":false}`} {
		out, err := SetBoolIfDifferent([]byte(payload), "stream", false)
		if err != nil || gjson.GetBytes(out, "stream").Type != gjson.False {
			t.Fatalf("false was not explicitly written: %s, %v", out, err)
		}
	}
	if out, err := SetStringIfDifferent([]byte(`{}`), "prompt_cache_key", "  key\t"); err != nil || gjson.GetBytes(out, "prompt_cache_key").String() != "  key\t" {
		t.Fatal("cache key whitespace changed")
	}
	if _, err := SetStringIfDifferent([]byte(`[]`), "model", "fixture"); err == nil {
		t.Fatal("array object-path error was swallowed")
	}
	if _, err := SetBoolIfDifferent([]byte(`[]`), "stream", true); err == nil {
		t.Fatal("array object-path error was swallowed")
	}
}

func BenchmarkCodexScalarPayloadUpdates(b *testing.B) {
	for _, size := range []int{128, 1 << 20, 10 << 20} {
		for _, position := range []string{"first", "last", "changed"} {
			fields := `"model":"fixture","stream":true`
			if position == "changed" {
				fields = `"model":"other","stream":false`
			}
			content := `"input":"` + strings.Repeat("x", size) + `"`
			payload := []byte(`{` + fields + `,` + content + `}`)
			if position == "last" {
				payload = []byte(`{` + content + `,` + fields + `}`)
			}
			for _, optimized := range []bool{false, true} {
				b.Run(fmt.Sprintf("bytes%d/%s/optimized%t", size, position, optimized), func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						var out []byte
						if optimized {
							out, _ = SetStringIfDifferent(payload, "model", "fixture")
							out, _ = SetBoolIfDifferent(out, "stream", true)
						} else {
							out, _ = sjson.SetBytes(payload, "model", "fixture")
							out, _ = sjson.SetBytes(out, "stream", true)
						}
						if len(out) == 0 {
							b.Fatal("missing updated field")
						}
					}
				})
			}
		}
	}
}

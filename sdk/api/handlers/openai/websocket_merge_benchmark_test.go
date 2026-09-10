package openai

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func websocketMergeBenchmarkHistory(items, textBytes int) []byte {
	var body strings.Builder
	body.WriteString(`{"model":"fixture","instructions":"inherited","input":[`)
	for index := range items {
		if index > 0 {
			body.WriteByte(',')
		}
		fmt.Fprintf(&body, `{"type":"message","role":"user","id":"msg_%d","content":%q}`, index, strings.Repeat("x", textBytes))
	}
	body.WriteString(`]}`)
	return []byte(body.String())
}

func TestWebsocketMergedHistoryKeepsEnvelopeAndBufferOwnership(t *testing.T) {
	previous := websocketMergeBenchmarkHistory(100, 128)
	previousCopy := bytes.Clone(previous)
	for _, explicit := range []string{"", `,"model":"chosen","instructions":null`} {
		next := []byte(`{"type":"response.append","previous_response_id":"discard","stream":false,"opaque":{"n":9007199254740993},"input":[{"role":"user","content":"next"}]` + explicit + `}`)
		nextCopy := bytes.Clone(next)
		normalized, history, err := normalizeResponseSubsequentRequest(next, previous, nil, false)
		if err != nil {
			t.Fatal(err.Error)
		}
		if !bytes.Equal(previous, previousCopy) || !bytes.Equal(next, nextCopy) || !bytes.Equal(normalized, history) {
			t.Fatal("history merge changed input buffers or its committed snapshot")
		}
		if gjson.GetBytes(normalized, "input.#").Int() != 101 || !gjson.GetBytes(normalized, "stream").Bool() || gjson.GetBytes(normalized, "type").Exists() || gjson.GetBytes(normalized, "previous_response_id").Exists() || gjson.GetBytes(normalized, "opaque.n").Raw != "9007199254740993" {
			t.Fatal("merged envelope or raw unknown fields changed")
		}
		if explicit == "" {
			if gjson.GetBytes(normalized, "model").String() != "fixture" || gjson.GetBytes(normalized, "instructions").String() != "inherited" {
				t.Fatal("missing fields no longer inherit previous values")
			}
		} else if gjson.GetBytes(normalized, "model").String() != "chosen" || gjson.GetBytes(normalized, "instructions").Raw != "null" {
			t.Fatal("explicit fields were overwritten by inheritance")
		}
		history[0] = '!'
		if normalized[0] != '{' {
			t.Fatal("committed history shares mutable request storage")
		}
	}
}

func BenchmarkWebsocketSubsequentMerge(b *testing.B) {
	for _, tc := range []struct {
		name  string
		items int
		bytes int
	}{{"short", 1, 128}, {"history100", 100, 128}, {"history1000", 1000, 128}, {"text1MiB", 1, 1 << 20}, {"text10MiB", 1, 10 << 20}} {
		b.Run(tc.name, func(b *testing.B) {
			previous := websocketMergeBenchmarkHistory(tc.items, tc.bytes)
			next := []byte(`{"type":"response.append","input":[{"role":"user","content":"next"}]}`)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, _, err := normalizeResponseSubsequentRequest(next, previous, nil, false); err != nil {
					b.Fatal(err.Error)
				}
			}
		})
	}
}

package helps

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexReplayPrefixIndexesPreserveExactHistory(t *testing.T) {
	for _, input := range []string{`[]`, `[{"role":"user","content":"你好"}]`, `[{"role":"user","content":"one"},{"role":"assistant","content":"same"},{"role":"user","content":"two"},{"role":"assistant","content":"same"}]`} {
		items := gjson.Parse(input).Array()
		indexes, end := codexReplayPrefixIndexes(items)
		prefix := ""
		for position := 0; position <= len(items); position++ {
			if position > 0 {
				prefix += "\x00item\x00" + items[position-1].Raw
			}
			want := sha256.Sum256([]byte(prefix))
			if got, exists := indexes[want]; !exists || got != position {
				t.Fatalf("prefix %d did not retain its history position", position)
			}
			if position == len(items) && end != want {
				t.Fatal("end fingerprint differs from the complete input")
			}
		}
		if len(indexes) != len(items)+1 {
			t.Fatal("repeated assistant text conflated different history prefixes")
		}
	}
	a, _ := codexReplayPrefixIndexes(gjson.Parse(`[{"content":"a"},{"content":"b"}]`).Array())
	_, changed := codexReplayPrefixIndexes(gjson.Parse(`[{"content":"b"},{"content":"a"}]`).Array())
	if _, exists := a[changed]; exists {
		t.Fatal("different history order matched")
	}
}

func TestCodexReplayAssistantFingerprintRequiresReliableText(t *testing.T) {
	want := sha256.Sum256([]byte("hello\n世界"))
	for _, input := range []string{
		`{"role":"assistant","content":"hello\n世界"}`,
		`{"type":"message","role":" ASSISTANT ","content":[{"type":"output_text","text":"hello\n"},{"type":"output_text","text":"世界"}]}`,
		`{"role":"assistant","content":[{"type":"input_text","text":"hello\n世界"}]}`,
	} {
		if got, ok := codexReplayAssistantFingerprint(gjson.Parse(input)); !ok || got != want {
			t.Fatal("equivalent assistant text lost its fingerprint")
		}
	}
	for _, input := range []string{
		`{"role":"user","content":"hello"}`, `{"type":"function_call","role":"assistant","content":"hello"}`,
		`{"role":"assistant","content":""}`, `{"role":"assistant","content":[]}`,
		`{"role":"assistant","content":[{"type":"output_text","text":7}]}`,
		`{"role":"assistant","content":[{"type":"output_text","text":"hello"},{"type":"input_image","file_id":"file"}]}`,
		`{"role":"assistant","content":[{"type":"refusal","refusal":"hello"}]}`,
		`{"role":"assistant","content":{"text":"hello"}}`,
	} {
		if _, ok := codexReplayAssistantFingerprint(gjson.Parse(input)); ok {
			t.Fatalf("unreliable assistant anchor accepted: %s", input)
		}
	}
}

func BenchmarkCodexReplayPrefixIndexes(b *testing.B) {
	for _, count := range []int{100, 1000} {
		parts := make([]json.RawMessage, count)
		for index := range parts {
			parts[index] = json.RawMessage(fmt.Sprintf(`{"role":"user","content":%q}`, strings.Repeat("x", 1024)))
		}
		body, _ := json.Marshal(parts)
		items := gjson.ParseBytes(body).Array()
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				codexReplayPrefixIndexes(items)
			}
		})
	}
}

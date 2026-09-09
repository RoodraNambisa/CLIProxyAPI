package common

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestResponsesUsageDetailsKeepExplicitCountsAndOtherFields(t *testing.T) {
	for _, path := range []string{"usage", "response.usage"} {
		for _, output := range []string{`null`, `[]`, `"legacy"`, `{}`, `{"reasoning_tokens":null}`, `{"reasoning_tokens":7,"audio_tokens":2}`} {
			for _, input := range []string{`null`, `[]`, `{}`, `{"cached_tokens":null}`, `{"cached_tokens":9,"cache_creation_tokens":3}`} {
				t.Run(fmt.Sprintf("%s/output=%s/input=%s", path, output, input), func(t *testing.T) {
					usage := fmt.Sprintf(`{"input_tokens":12,"output_tokens":10,"total_tokens":22,"output_tokens_details":%s,"input_tokens_details":%s}`, output, input)
					body, _ := sjson.SetRawBytes([]byte(`{"output":[{"arguments":"usage is business content"}],"future":true}`), path, []byte(usage))
					original := bytes.Clone(body)
					out := EnsureResponsesUsageDetails(body)
					if !gjson.ValidBytes(out) || !bytes.Equal(body, original) || !bytes.Equal(out, EnsureResponsesUsageDetails(out)) {
						t.Fatal("normalization changed its input or was not idempotent")
					}
					if gjson.GetBytes(out, path+".input_tokens").Int() != 12 || gjson.GetBytes(out, path+".output_tokens").Int() != 10 || gjson.GetBytes(out, path+".total_tokens").Int() != 22 || !gjson.GetBytes(out, "future").Bool() || gjson.GetBytes(out, "output.0.arguments").String() != "usage is business content" {
						t.Fatal("normalization changed totals or unrelated business fields")
					}
					for _, field := range []struct{ key, raw, count string }{{"output_tokens_details", output, "reasoning_tokens"}, {"input_tokens_details", input, "cached_tokens"}} {
						value := gjson.GetBytes(out, path+"."+field.key+"."+field.count)
						want := gjson.Get(field.raw, field.count).Int()
						if value.Type != gjson.Number || value.Int() != want {
							t.Fatal("missing counter was not defaulted or explicit counter was overwritten")
						}
					}
					if gjson.GetBytes(out, path+".output_tokens_details.audio_tokens").Int() != gjson.Get(output, "audio_tokens").Int() || gjson.GetBytes(out, path+".input_tokens_details.cache_creation_tokens").Int() != gjson.Get(input, "cache_creation_tokens").Int() {
						t.Fatal("additional usage counters were lost")
					}
				})
			}
		}
	}
}

func TestResponsesUsageDetailsPreserveAbsentUsageAndMalformedPayloads(t *testing.T) {
	for _, raw := range []string{"", " \n", `[]`, `{}`, `{"usage":null}`, `{"usage":[]}`, `{"usage":{}`, `{"object":"response.compaction","usage":{}}`, ` {"usage":{"output_tokens_details":{"reasoning_tokens":2},"input_tokens_details":{"cached_tokens":3}}} `} {
		body := []byte(raw)
		if !bytes.Equal(EnsureResponsesUsageDetails(body), body) {
			t.Fatalf("non-target or complete payload changed: %s", raw)
		}
	}
	out := EnsureResponsesUsageDetails([]byte(`{"usage":{},"response":{"usage":{}}}`))
	for _, path := range []string{"usage", "response.usage"} {
		if !gjson.GetBytes(out, path+".output_tokens_details.reasoning_tokens").Exists() || !gjson.GetBytes(out, path+".input_tokens_details.cached_tokens").Exists() {
			t.Fatal("missing detail objects were not supplied")
		}
	}
}

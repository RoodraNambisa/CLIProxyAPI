package common

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestResponsesSSEUsagePreservesFramingAndMetadata(t *testing.T) {
	for _, ending := range []string{"\n", "\r\n", "\r"} {
		for _, prefix := range []string{"data:", "data: ", " \tdata: "} {
			for _, multiline := range []bool{false, true} {
				t.Run(fmt.Sprintf("ending=%q/prefix=%q/multiline=%t", ending, prefix, multiline), func(t *testing.T) {
					metadata := ": keepalive" + ending + "id: fixture" + ending + "event: response.completed" + ending
					data := `{"type":"response.completed","response":{"usage":{},"output":[]}}`
					if multiline {
						data = strings.Replace(data, `,"response":`, ","+ending+": interleaved comment"+ending+prefix+`"response":`, 1)
					}
					footer := "retry: 123" + ending + ending + ": after" + ending + "data: [DONE]" + ending + ending
					body := []byte(metadata + prefix + data + ending + footer)
					original := bytes.Clone(body)
					out := EnsureResponsesUsageDetails(body)
					if !bytes.Equal(body, original) || !bytes.Equal(out, EnsureResponsesUsageDetails(out)) || !bytes.HasPrefix(out, []byte(metadata)) || !bytes.HasSuffix(out, []byte(footer)) {
						t.Fatal("usage normalization changed its input, metadata, framing or idempotence")
					}
					if multiline && !bytes.Contains(out, []byte(": interleaved comment"+ending)) {
						t.Fatal("multi-line normalization discarded an interleaved comment")
					}
					found := 0
					for _, line := range strings.Split(strings.ReplaceAll(string(out), ending, "\n"), "\n") {
						line = strings.TrimSpace(line)
						if !strings.HasPrefix(line, "data:") {
							continue
						}
						payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
						if payload == "[DONE]" {
							continue
						}
						if !gjson.Valid(payload) || !gjson.Get(payload, "response.usage.output_tokens_details.reasoning_tokens").Exists() || !gjson.Get(payload, "response.usage.input_tokens_details.cached_tokens").Exists() {
							t.Fatal("SSE payload lost its JSON shape or usage details")
						}
						found++
					}
					if found != 1 {
						t.Fatalf("data events=%d, want one", found)
					}
					clear(body)
					if !bytes.Contains(out, []byte("reasoning_tokens")) || !bytes.HasSuffix(out, []byte(footer)) {
						t.Fatal("modified SSE retained its source buffer")
					}
				})
			}
		}
	}
}

func TestResponsesSSEUsageLeavesNonTargetsAndBrokenJSONIntact(t *testing.T) {
	for _, body := range []string{
		"data: {\"usage\":{}\n\n",
		"data: {\"usage\":\ndata: broken}\n\n",
		"event: fixture\ndata: [DONE]\n\n",
		"data: {\"object\":\"response.compaction\",\"usage\":{}}\n\n",
		"data: {\"delta\":\"usage is content\"}\n\n",
	} {
		if out := EnsureResponsesUsageDetails([]byte(body)); string(out) != body {
			t.Fatalf("non-target SSE changed: %s", body)
		}
	}
	body := []byte("data: {\"usage\":{}}\n\ndata: {\"usage\":{}\n\ndata: {\"response\":{\"usage\":{}}}")
	out := EnsureResponsesUsageDetails(body)
	if !bytes.Contains(out, []byte("data: {\"usage\":{}\n\n")) || bytes.Count(out, []byte("reasoning_tokens")) != 2 || bytes.HasSuffix(out, []byte("\n")) {
		t.Fatal("mixed-frame normalization changed a broken frame or an unterminated final line")
	}
	escaped := EnsureResponsesUsageDetails([]byte(`{"\u0075sage":{}}`))
	if !gjson.GetBytes(escaped, "usage.output_tokens_details.reasoning_tokens").Exists() {
		t.Fatal("fast path skipped an escaped usage key")
	}
}

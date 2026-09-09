package helps

import (
	"bytes"
	"net/http"
	"reflect"
	"testing"
)

func TestClaudeSummaryBetaPreservesOtherHeadersAndEmptyDisplay(t *testing.T) {
	for _, display := range []string{`"summarized"`, `"omitted"`, `""`, `"  "`, `false`, `null`, `1`, `{}`} {
		headers := http.Header{"Anthropic-Beta": {"retained, redact-thinking-2026-02-12 ", "redact-thinking-2026-02-12-suffix,redact-thinking-2026-02-12"}, "User-Agent": {"saved-agent"}}
		original := headers.Clone()
		body := []byte(`{"thinking":{"display":` + display + `}}`)
		before := bytes.Clone(body)
		ApplyClaudeSummaryBeta(headers, body)
		if display == `"summarized"` || display == `"omitted"` {
			if !reflect.DeepEqual(headers.Values("Anthropic-Beta"), []string{"retained", "redact-thinking-2026-02-12-suffix"}) || headers.Get("User-Agent") != "saved-agent" {
				t.Fatal("display filtering changed another beta or software identity")
			}
		} else if !reflect.DeepEqual(headers, original) {
			t.Fatal("absent or invalid display changed the default beta headers")
		}
		if !bytes.Equal(body, before) {
			t.Fatal("beta filtering changed the request body")
		}
	}
	for _, headers := range []http.Header{nil, {}, {"Anthropic-Beta": {"redact-thinking-2026-02-12"}}} {
		ApplyClaudeSummaryBeta(headers, []byte(`{"thinking":{"display":"summarized"}}`))
		if headers.Get("Anthropic-Beta") != "" {
			t.Fatal("a redaction-only header was retained")
		}
	}
}

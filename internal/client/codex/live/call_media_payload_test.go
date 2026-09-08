package live

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestCallMediaPayloadPreservesSessionAndBusinessFields(t *testing.T) {
	for _, fixture := range []struct{ body, contentType string }{
		{`{"sdp":"original","session":{"model":"gpt-realtime","custom":{"sdp":"original"},"limit":90071992547409931234},"business":{"sdp":"original"}}`, "application/json"},
		{"v=0\r\noriginal\r\n", "application/sdp"},
	} {
		p, err := prepareCallPayload([]byte(fixture.body), fixture.contentType)
		if err != nil {
			t.Fatal(err)
		}
		offer, err := p.mediaOffer()
		if err != nil || !strings.Contains(offer, "original") {
			t.Fatal("client SDP was not recognized")
		}
		p, err = p.withModel("resolved-live-model")
		if err != nil {
			t.Fatal(err)
		}
		before := string(p.body)
		updated, err := p.withMediaOffer("v=0\r\nrelay\r\n")
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]json.RawMessage
		if err := json.Unmarshal(updated.body, &got); err != nil {
			t.Fatal(err)
		}
		if string(p.body) != before || updated.model != "resolved-live-model" || string(got["sdp"]) != `"v=0\r\nrelay\r\n"` {
			t.Fatal("media replacement changed its source, model or SDP")
		}
		if fixture.contentType == "application/json" && (!strings.Contains(string(got["session"]), "90071992547409931234") || !strings.Contains(string(got["session"]), `"sdp":"original"`) || string(got["business"]) != `{"sdp":"original"}`) {
			t.Fatal("media replacement rewrote unrelated JSON or rounded an integer")
		}
	}
	for _, raw := range []string{`{}`, `{"sdp":null}`, `{"sdp":123}`, `{"sdp":{}}`, `{"sdp":"  "}`} {
		p, err := prepareCallPayload([]byte(raw), "application/json")
		if err != nil {
			t.Fatal("default passthrough gained a media-only validation error")
		}
		if _, err := p.mediaOffer(); err == nil {
			t.Fatal("media relay accepted a missing or invalid offer")
		}
	}
	p := callPayload{body: []byte(`{"sdp":"original"}`), contentType: "application/json"}
	if _, err := p.withMediaOffer(strings.Repeat("x", maxCallBodySize)); !errors.Is(err, errCallBodyTooLarge) {
		t.Fatal("encoded media body exceeded its limit")
	}
}

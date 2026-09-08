package live

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestLiveClientSecretPayloadDefaultsAndLifetimeBounds(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, body := range []string{"", "null", "{}"} {
			request, errParse := parseClientSecretRequest([]byte(body), legacy)
			if errParse != nil || request.model != "gpt-realtime" || request.lifetime != 10*time.Minute {
				t.Fatal("legacy or current default session changed")
			}
		}
	}
	for _, seconds := range []string{"10", "7200"} {
		if _, errParse := parseClientSecretRequest([]byte(`{"expires_after":{"anchor":"created_at","seconds":`+seconds+`}}`), false); errParse != nil {
			t.Fatal("valid lifetime boundary rejected")
		}
	}
	for _, body := range []string{`[]`, `true`, `{"expires_after":{"seconds":9}}`, `{"expires_after":{"seconds":7201}}`, `{"expires_after":{"seconds":1e100}}`, `{"expires_after":{"seconds":10.5}}`, `{"expires_after":{"anchor":"last_used","seconds":30}}`, `{"expires_after":{"seconds":"30"}}`} {
		if _, errParse := parseClientSecretRequest([]byte(body), false); errParse == nil {
			t.Fatal("invalid lifetime or request type accepted")
		}
	}
}

func TestLiveClientSecretPayloadPreservesAliasAndOpaqueSessionFields(t *testing.T) {
	body := []byte(`{"session":{"id":"old-id","object":"old","expires_at":1,"client_secret":{"value":"old-secret"},"type":"realtime","model":"team/custom-realtime-preview","large":90071992547409931234,"tools":[{"parameters":{"id":"business","client_secret":"business-value"}}],"audio":{"output":{"voice":"marin"}}}}`)
	original := bytes.Clone(body)
	request, errParse := parseClientSecretRequest(body, false)
	if errParse != nil || request.model != "team/custom-realtime-preview" {
		t.Fatal("model alias was guessed or rewritten before selection")
	}
	if !bytes.Equal(body, original) || !bytes.Contains(request.session, []byte("90071992547409931234")) || !bytes.Contains(request.session, []byte("business-value")) || bytes.Contains(request.session, []byte("old-secret")) {
		t.Fatal("session normalization modified business fields, precision or input")
	}
	a := ClientSecretAuthorization{principal: "sess_fixture", session: request.session, expiresAt: time.Unix(1800000000, 0)}
	response, errResponse := clientSecretSessionResponse(a)
	if errResponse != nil {
		t.Fatal(errResponse)
	}
	var object map[string]json.RawMessage
	if errJSON := json.Unmarshal(response, &object); errJSON != nil {
		t.Fatal(errJSON)
	}
	if string(object["id"]) != `"sess_fixture"` || string(object["object"]) != `"realtime.session"` || string(object["expires_at"]) != "1800000000" || string(object["large"]) != "90071992547409931234" || object["client_secret"] != nil {
		t.Fatal("response identity or raw fields changed")
	}
}

func TestLiveClientSecretPayloadRejectsUnsupportedOrMalformedSession(t *testing.T) {
	for _, body := range []string{`{"type":"transcription"}`, `{"type":"translation"}`} {
		if _, errParse := parseClientSecretRequest([]byte(body), true); !errors.Is(errParse, errUnsupportedRealtimeSession) {
			t.Fatal("unsupported session was advertised as realtime")
		}
	}
	for _, body := range []string{`[]`, `{"type":3}`, `{"model":[]}`, `{"model":true}`} {
		if _, errParse := parseClientSecretRequest([]byte(body), true); errParse == nil {
			t.Fatal("malformed session was silently defaulted")
		}
	}
	if _, errRead := readClientSecretBody(strings.NewReader(strings.Repeat("x", clientSecretMaxBodySize+1))); errRead == nil {
		t.Fatal("oversized credential payload accepted")
	}
	if _, errParse := parseClientSecretRequest(bytes.Repeat([]byte("x"), clientSecretMaxBodySize+1), false); errParse == nil {
		t.Fatal("direct parser bypassed body bound")
	}
}

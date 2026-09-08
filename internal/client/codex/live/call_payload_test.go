package live

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

func TestCallPayloadPreservesNativeSessionAndModelMirrors(t *testing.T) {
	body := []byte(`{"sdp":"v=0\r\n","model":"public","large":90071992547409931234,"session":{"model":"prefix/public","tools":[{"name":"native","parameters":{"model":"do-not-replace","prompt_cache_key":"user-data"}}],"instructions":"plain","audio":{"output":{"voice":"marin"}}}}`)
	original := bytes.Clone(body)
	payload, errPayload := prepareCallPayload(body, "application/json; charset=utf-8")
	if errPayload != nil || payload.model != "prefix/public" {
		t.Fatalf("prepare model: %s, %v", payload.model, errPayload)
	}
	updated, errModel := payload.withModel("gpt-realtime")
	if errModel != nil || updated.model != registry.CodexLiveModelID {
		t.Fatalf("rewrite model: %s, %v", updated.model, errModel)
	}
	var decoded struct {
		Model   string          `json:"model"`
		Large   json.RawMessage `json:"large"`
		Session struct {
			Model string          `json:"model"`
			Tools json.RawMessage `json:"tools"`
			Audio json.RawMessage `json:"audio"`
		} `json:"session"`
	}
	if errJSON := json.Unmarshal(updated.body, &decoded); errJSON != nil {
		t.Fatal(errJSON)
	}
	if decoded.Model != registry.CodexLiveModelID || decoded.Session.Model != registry.CodexLiveModelID || string(decoded.Large) != "90071992547409931234" {
		t.Fatal("model mirrors or integer precision changed incorrectly")
	}
	if !bytes.Contains(decoded.Session.Tools, []byte(`"model":"do-not-replace"`)) || !bytes.Contains(decoded.Session.Audio, []byte(`"voice":"marin"`)) || !bytes.Equal(body, original) {
		t.Fatal("native fields or input buffer were modified")
	}
	if bytes.Contains(updated.body, []byte("additional_tools")) || bytes.Contains(updated.body, []byte("session_id")) {
		t.Fatal("Responses-only fields were synthesized")
	}
}

func TestCallPayloadDefaultsAndResolvedCustomModels(t *testing.T) {
	for _, body := range []string{`{}`, `{"model":null}`, `{"session":{}}`, `{"model":" ","session":{"model":""}}`} {
		payload, errPayload := prepareCallPayload([]byte(body), "")
		if errPayload != nil || payload.model != registry.CodexLiveModelID {
			t.Fatalf("default %s: %v", body, errPayload)
		}
		updated, errModel := payload.withModel("custom-realtime-preview")
		if errModel != nil || updated.model != "custom-realtime-preview" || !bytes.Contains(updated.body, []byte(`"model":"custom-realtime-preview"`)) {
			t.Fatal("resolved custom model was replaced by a guessed default")
		}
	}
}

func TestCallPayloadRejectsInvalidStructuredInputs(t *testing.T) {
	for _, body := range []string{"", "null", "[]", "true", `{`, `{"model":[]}`, `{"session":null}`, `{"session":[]}`, `{"session":{"model":4}}`} {
		if _, errPayload := prepareCallPayload([]byte(body), "application/json"); errPayload == nil {
			t.Fatalf("accepted malformed call: %s", body)
		}
	}
	for _, kind := range []string{"invalid;", "application/octet-stream", "multipart/form-data"} {
		if _, errPayload := prepareCallPayload([]byte("v=0"), kind); errPayload == nil {
			t.Fatalf("accepted invalid content type: %s", kind)
		}
	}
}

func TestCallPayloadMultipartAndRawSDP(t *testing.T) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if errField := writer.WriteField("sdp", "v=0\r\n"); errField != nil {
		t.Fatal(errField)
	}
	if errField := writer.WriteField("session", `{"model":"public-live","unknown":90071992547409931234}`); errField != nil {
		t.Fatal(errField)
	}
	if errClose := writer.Close(); errClose != nil {
		t.Fatal(errClose)
	}
	payload, errPayload := prepareCallPayload(body.Bytes(), writer.FormDataContentType())
	if errPayload != nil || payload.model != "public-live" || !bytes.Contains(payload.body, []byte("90071992547409931234")) {
		t.Fatalf("multipart lost session fields: %v", errPayload)
	}
	for _, kind := range []string{"application/sdp", "text/plain; charset=utf-8"} {
		payload, errPayload := prepareCallPayload([]byte("v=0\r\n"), kind)
		if errPayload != nil {
			t.Fatal(errPayload)
		}
		updated, errModel := payload.withModel("resolved-live")
		if errModel != nil || updated.contentType != "application/json" || !bytes.Contains(updated.body, []byte(`"sdp":"v=0\r\n"`)) || !bytes.Contains(updated.body, []byte(`"model":"resolved-live"`)) {
			t.Fatal("raw SDP could not carry the selected model")
		}
		if _, errEmpty := prepareCallPayload([]byte(" \n"), kind); errEmpty == nil {
			t.Fatal("accepted empty SDP")
		}
	}
}

func TestCallPayloadMultipartRejectsBrokenSessionOrMissingOffer(t *testing.T) {
	for _, fields := range []map[string]string{{"session": "{}"}, {"sdp": "v=0", "session": "null"}, {"sdp": "v=0", "session": "[]"}} {
		var body bytes.Buffer
		writer := multipart.NewWriter(&body)
		for key, value := range fields {
			if errField := writer.WriteField(key, value); errField != nil {
				t.Fatal(errField)
			}
		}
		if errClose := writer.Close(); errClose != nil {
			t.Fatal(errClose)
		}
		if _, errPayload := prepareCallPayload(body.Bytes(), writer.FormDataContentType()); errPayload == nil {
			t.Fatal("accepted broken multipart call")
		}
	}
	if _, errPayload := prepareCallPayload([]byte("--broken\r\n"), "multipart/form-data; boundary=broken"); errPayload == nil {
		t.Fatal("accepted unfinished multipart call")
	}
}

type callFailingReader struct{ err error }

func (r callFailingReader) Read([]byte) (int, error) { return 0, r.err }

func TestCallBodyLimitsAndReadErrors(t *testing.T) {
	body := strings.Repeat("x", maxCallBodySize)
	if data, errRead := readCallBody(strings.NewReader(body)); errRead != nil || len(data) != maxCallBodySize {
		t.Fatal("exact limit was rejected")
	}
	if data, errRead := readCallBody(io.MultiReader(strings.NewReader(body), strings.NewReader("extra"))); !errors.Is(errRead, errCallBodyTooLarge) || len(data) != maxCallBodySize {
		t.Fatal("oversized body was not bounded")
	}
	want := errors.New("fixture reader failure")
	if _, errRead := readCallBody(callFailingReader{want}); !errors.Is(errRead, want) {
		t.Fatal("read cause was lost")
	}
	if _, errPayload := prepareCallPayload([]byte(body+"x"), "text/plain"); !errors.Is(errPayload, errCallBodyTooLarge) {
		t.Fatal("direct payload caller bypassed limit")
	}
	if data, errRead := readCallBody(nil); data != nil || errRead != nil {
		t.Fatal("nil reader handling changed")
	}
}

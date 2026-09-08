package helps

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestCodexAlphaSearchEnvelopeAndOpaqueFields(t *testing.T) {
	body := []byte(`{"id":"search-id","model":"team/model","input":[{"type":"custom_tool_call_output","output":"opaque"}],"commands":[{"future":{"model":"keep","prompt_cache_key":"business","number":9007199254740993}}],"prompt_cache_key":"remove","prompt_cache_retention":"remove","future":true}`)
	original := bytes.Clone(body)
	routing, err := ParseCodexAlphaSearchRouting(body)
	if err != nil || routing.Model != "team/model" || routing.SessionID != "search-id" {
		t.Fatal("routing fields were not read")
	}
	got, err := RewriteCodexAlphaSearchBody(body, "upstream-model")
	if err != nil {
		t.Fatal(err)
	}
	var before, after map[string]json.RawMessage
	if err := json.Unmarshal(body, &before); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(got, &after); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"id", "input", "commands", "future"} {
		if !reflect.DeepEqual(before[key], after[key]) {
			t.Fatalf("changed opaque field %s", key)
		}
	}
	if string(after["model"]) != `"upstream-model"` || after["prompt_cache_key"] != nil || after["prompt_cache_retention"] != nil {
		t.Fatal("incorrect top-level rewrite")
	}
	if !bytes.Equal(body, original) {
		t.Fatal("mutated caller's request")
	}
	again, err := RewriteCodexAlphaSearchBody(got, "upstream-model")
	if err != nil || len(again) == 0 || &again[0] != &got[0] {
		t.Fatal("unchanged request was rebuilt")
	}
}

func TestCodexAlphaSearchEnvelopeValidation(t *testing.T) {
	for _, body := range []string{"", "null", "[]", `{"model":null}`, `{"model":3}`, `{"model":" "}`, `{"model":"ok","id":{}}`, `{"model":"ok"} trailing`} {
		if _, err := ParseCodexAlphaSearchRouting([]byte(body)); err == nil {
			t.Fatal("accepted invalid envelope")
		}
		if _, err := RewriteCodexAlphaSearchBody([]byte(body), "upstream"); err == nil {
			t.Fatal("rewrite accepted invalid envelope")
		}
	}
	for _, body := range []string{`{"model":"ok"}`, `{"model":"ok","id":null}`, `{"model":"ok","id":""}`} {
		if _, err := ParseCodexAlphaSearchRouting([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := RewriteCodexAlphaSearchBody([]byte(`{"model":"ok"}`), " "); err == nil {
		t.Fatal("accepted empty execution model")
	}
	base := `{"model":"ok","input":""}`
	body := []byte(strings.Replace(base, `"input":""`, `"input":"`+strings.Repeat("x", CodexAlphaSearchMaxRequestBytes-len(base))+`"`, 1))
	if len(body) != CodexAlphaSearchMaxRequestBytes {
		t.Fatal("incorrect boundary fixture")
	}
	if _, err := ParseCodexAlphaSearchRouting(body); err != nil {
		t.Fatal(err)
	}
	if _, err := ParseCodexAlphaSearchRouting(append(body, ' ')); err == nil {
		t.Fatal("accepted oversized request")
	}
}

func TestCodexAlphaSearchURLs(t *testing.T) {
	for _, tc := range []struct{ base, want string }{
		{"https://example.invalid/v1", "https://example.invalid/v1/alpha/search"},
		{"http://127.0.0.1:1234/v1/", "http://127.0.0.1:1234/v1/alpha/search"},
		{"https://example.invalid/root%2Ftenant/?mode=search%2Fv1&x=1", "https://example.invalid/root%2Ftenant/alpha/search?mode=search%2Fv1&x=1"},
		{"https://example.invalid", "https://example.invalid/alpha/search"},
	} {
		got, err := CodexAlphaSearchURL(tc.base, true)
		if err != nil || got != tc.want {
			t.Fatalf("endpoint mismatch: %s, %v", got, err)
		}
	}
	for _, base := range []string{"", "/v1", "ftp://example.invalid", "https:///v1", "https://user:fixture@example.invalid", "https://example.invalid/#section", "https://%xx"} {
		if _, err := CodexAlphaSearchURL(base, true); err == nil {
			t.Fatal("accepted invalid API key endpoint")
		}
	}
	got, err := CodexAlphaSearchURL("https://other.invalid", false)
	if err != nil || got != CodexAlphaSearchOAuthURL {
		t.Fatal("OAuth did not use its search endpoint")
	}
}

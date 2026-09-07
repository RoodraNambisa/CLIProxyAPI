package management

import (
	"encoding/json"
	"testing"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestCredentialWeightPatchDistinguishesOmissionNullAndZero(t *testing.T) {
	for _, tc := range []struct {
		body  string
		set   bool
		value *int
	}{
		{`{}`, false, nil},
		{`{"weight":null}`, true, nil},
		{`{"weight":0}`, true, new(0)},
		{`{"weight":-3}`, true, new(0)},
		{`{"weight":1000000}`, true, new(1000000)},
	} {
		var decoded struct {
			Weight credentialWeightPatch `json:"weight"`
		}
		if err := json.Unmarshal([]byte(tc.body), &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.Weight.set != tc.set || (decoded.Weight.value == nil) != (tc.value == nil) ||
			(tc.value != nil && *decoded.Weight.value != *tc.value) {
			t.Fatal("patch lost omission, clear or explicit zero")
		}
	}
	for _, value := range []string{"1.5", "true", "\"7\"", "1000001", "-9223372036854775809"} {
		var patch credentialWeightPatch
		if err := json.Unmarshal([]byte(value), &patch); err == nil {
			t.Fatal("invalid patch weight accepted")
		}
	}
}

func TestConfiguredAuthFileWeightUsesSelectorPrecedence(t *testing.T) {
	for _, tc := range []struct {
		attributes map[string]string
		metadata   map[string]any
		want       int64
		present    bool
	}{
		{nil, nil, 0, false},
		{map[string]string{"weight": " \t"}, map[string]any{"weight": 7}, 7, true},
		{map[string]string{"weight": ""}, map[string]any{"weight": 0}, 0, true},
		{map[string]string{"weight": "3"}, map[string]any{"weight": 7}, 3, true},
		{map[string]string{"weight": " \t"}, nil, 0, false},
	} {
		weight, present := configuredAuthFileWeight(&coreauth.Auth{Attributes: tc.attributes, Metadata: tc.metadata})
		if weight != tc.want || present != tc.present {
			t.Fatal("displayed configured weight differs from selection precedence")
		}
	}
}

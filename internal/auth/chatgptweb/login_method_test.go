package chatgptweb

import (
	"strings"
	"testing"
)

func TestNormalizeLoginMethod(t *testing.T) {
	for _, test := range []struct {
		input LoginMethod
		want  LoginMethod
	}{
		{input: "", want: LoginMethodAuto},
		{input: " AUTO ", want: LoginMethodAuto},
		{input: LoginMethodPasskey, want: LoginMethodPasskey},
		{input: LoginMethodPasswordTOTP, want: LoginMethodPasswordTOTP},
		{input: LoginMethodAPI798, want: LoginMethodAPI798},
	} {
		got, errNormalize := NormalizeLoginMethod(test.input)
		if errNormalize != nil || got != test.want {
			t.Fatalf("NormalizeLoginMethod(%q) = %q, %v; want %q", test.input, got, errNormalize, test.want)
		}
	}
	if _, errNormalize := NormalizeLoginMethod("password"); errNormalize == nil {
		t.Fatal("NormalizeLoginMethod() accepted an unsupported method")
	}
}

func TestNormalizeAPI798RequestURLPreservesOpaqueAuthCode(t *testing.T) {
	rawURL := "http://api798.com/get_code?email=user%2Blabel%40example.com&auth_code=a%252Bb+c%2Fd"
	got, errNormalize := normalizeAPI798RequestURL(rawURL, "user+label@example.com")
	if errNormalize != nil {
		t.Fatalf("normalizeAPI798RequestURL() error = %v", errNormalize)
	}
	want := strings.Replace(rawURL, "http://", "https://", 1)
	if got != want {
		t.Fatalf("normalizeAPI798RequestURL() = %q, want %q", got, want)
	}
}

func TestValidateAPI798URLRejectsUnsafeOrMismatchedURLs(t *testing.T) {
	for _, rawURL := range []string{
		"https://example.com/get_code?email=user@example.com&auth_code=secret",
		"https://api798.com:443/get_code?email=user@example.com&auth_code=secret",
		"https://user:pass@api798.com/get_code?email=user@example.com&auth_code=secret",
		"https://api798.com/other?email=user@example.com&auth_code=secret",
		"https://api798.com/get_code?email=other@example.com&auth_code=secret",
		"https://api798.com/get_code?email=user@example.com",
		"https://api798.com/get_code?email=user@example.com&auth_code=secret&next=https://example.com",
		" https://api798.com/get_code?email=user@example.com&auth_code=secret",
	} {
		t.Run(rawURL, func(t *testing.T) {
			if errValidate := ValidateAPI798URL(rawURL, "user@example.com"); errValidate == nil {
				t.Fatalf("ValidateAPI798URL(%q) accepted an unsafe URL", rawURL)
			}
		})
	}
}

func TestCredentialAPI798RoundTrip(t *testing.T) {
	rawURL := "https://api798.com/get_code?email=user%40example.com&auth_code=opaque%2520value"
	credential, errDecode := DecodeCredential([]byte(`{
		"type":"chatgpt-web",
		"email":"user@example.com",
		"login_method":"api798",
		"api798_url":"` + rawURL + `"
	}`))
	if errDecode != nil {
		t.Fatalf("DecodeCredential() error = %v", errDecode)
	}
	metadata := map[string]any{"unrelated": "preserved"}
	credential.ApplyToMetadata(metadata)
	reparsed, errParse := ParseCredential(metadata)
	if errParse != nil {
		t.Fatalf("ParseCredential() error = %v", errParse)
	}
	if reparsed.LoginMethod != LoginMethodAPI798 || reparsed.API798URL != rawURL {
		t.Fatalf("API798 settings = method %q URL %q", reparsed.LoginMethod, reparsed.API798URL)
	}
	if metadata["unrelated"] != "preserved" {
		t.Fatal("ApplyToMetadata() removed unrelated metadata")
	}
}

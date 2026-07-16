package proxyutil

import (
	"strings"
	"testing"
)

func TestParsePortSetNormalizesWithoutExpanding(t *testing.T) {
	set, err := ParsePortSet("6000,3336-5999,3334,3336-4000")
	if err != nil {
		t.Fatalf("ParsePortSet() error = %v", err)
	}
	if got, want := set.String(), "3334,3336-6000"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
	if got, want := set.Count(), 2666; got != want {
		t.Fatalf("Count() = %d, want %d", got, want)
	}
	for index, want := range []int{3334, 3336, 3337} {
		got, ok := set.PortAt(index)
		if !ok || got != want {
			t.Fatalf("PortAt(%d) = %d, %t; want %d, true", index, got, ok, want)
		}
	}
	last, ok := set.PortAt(set.Count() - 1)
	if !ok || last != 6000 {
		t.Fatalf("last port = %d, %t; want 6000, true", last, ok)
	}
	if !set.Contains(3334) || !set.Contains(6000) || set.Contains(3335) || set.Contains(6001) {
		t.Fatalf("Contains() returned an unexpected result for %q", set.String())
	}
}

func TestParsePortSetRejectsInvalidValues(t *testing.T) {
	for _, input := range []string{"0", "65536", "5-3", "1,,2", "one", "1-2-3"} {
		if _, err := ParsePortSet(input); err == nil {
			t.Fatalf("ParsePortSet(%q) error = nil", input)
		}
	}
}

func TestExpandURLTemplateUsesStableProvidedRandomness(t *testing.T) {
	expanded, values, err := ExpandURLTemplate(
		"http://user-session-{3}:pass@proxy.example:18080/path-{2}",
		"abc",
		strings.NewReader("\x00\x01\x02\x02\x01"),
	)
	if err != nil {
		t.Fatalf("ExpandURLTemplate() error = %v", err)
	}
	if got, want := expanded, "http://user-session-abc:pass@proxy.example:18080/path-cb"; got != want {
		t.Fatalf("expanded = %q, want %q", got, want)
	}
	if got, want := strings.Join(values, ","), "abc,cb"; got != want {
		t.Fatalf("values = %q, want %q", got, want)
	}
}

func TestExpandURLTemplateValuesRestoresPersistedBinding(t *testing.T) {
	got, err := ExpandURLTemplateValues(
		"http://user-session-{3}-{2}:pass@proxy.example:18080",
		[]string{"abc", "Z9"},
	)
	if err != nil {
		t.Fatalf("ExpandURLTemplateValues() error = %v", err)
	}
	if want := "http://user-session-abc-Z9:pass@proxy.example:18080"; got != want {
		t.Fatalf("ExpandURLTemplateValues() = %q, want %q", got, want)
	}
}

func TestExpandURLTemplateValuesRejectsInvalidPersistedValues(t *testing.T) {
	for _, values := range [][]string{{}, {"ab"}, {"a/b"}, {"abc", "extra"}} {
		if _, err := ExpandURLTemplateValues("http://user-{3}:pass@proxy.example:18080", values); err == nil {
			t.Fatalf("ExpandURLTemplateValues(%q) error = nil", values)
		}
	}
}

func TestValidateURLTemplateAndWithPort(t *testing.T) {
	template, ports, err := ValidateURLTemplate("socks5h://user:pass@[2001:db8::1]", "3334,3336-3338", "")
	if err != nil {
		t.Fatalf("ValidateURLTemplate() error = %v", err)
	}
	if template == "" || ports != "3334,3336-3338" {
		t.Fatalf("ValidateURLTemplate() = %q, %q", template, ports)
	}
	resolved, err := WithPort(template, 3336)
	if err != nil {
		t.Fatalf("WithPort() error = %v", err)
	}
	if got, want := resolved, "socks5h://user:pass@[2001:db8::1]:3336"; got != want {
		t.Fatalf("WithPort() = %q, want %q", got, want)
	}
}

func TestValidateURLTemplateRejectsUnsafeAuthorityForms(t *testing.T) {
	for _, template := range []string{
		"http://:8080",
		"http://proxy.example:8080/path",
		"http://proxy-{3}.example:8080",
		"http://proxy.example:{2}",
		"http://proxy.example:0",
		"http://proxy.example:65536",
		"socks5h://user:pass@2001:db8::1",
	} {
		if _, _, err := ValidateURLTemplate(template, "", "1a"); err == nil {
			t.Fatalf("ValidateURLTemplate(%q) error = nil", template)
		}
	}
}

func TestExpandURLTemplateRejectsModuloBiasedBytes(t *testing.T) {
	expanded, _, err := ExpandURLTemplate(
		"http://user-{1}:pass@proxy.example:8080",
		"abc",
		strings.NewReader("\xff\x01"),
	)
	if err != nil {
		t.Fatalf("ExpandURLTemplate() error = %v", err)
	}
	if got, want := expanded, "http://user-b:pass@proxy.example:8080"; got != want {
		t.Fatalf("expanded = %q, want %q", got, want)
	}
}

func TestNormalizePlaceholderCharsetDeduplicates(t *testing.T) {
	got, err := NormalizePlaceholderCharset("aabbcc")
	if err != nil {
		t.Fatalf("NormalizePlaceholderCharset() error = %v", err)
	}
	if got != "abc" {
		t.Fatalf("NormalizePlaceholderCharset() = %q, want abc", got)
	}
}

func TestMaskProxyURL(t *testing.T) {
	got := MaskProxyURL("http://user:p%40ss@proxy.example:18080")
	if got != "http://********:********@proxy.example:18080" {
		t.Fatalf("MaskProxyURL() = %q", got)
	}
	if strings.Contains(got, "user") || strings.Contains(got, "p%40ss") || strings.Contains(got, "p@ss") {
		t.Fatal("masked URL leaked credentials")
	}
}

func TestMaskProxyURLMasksUsernameOnlyCredential(t *testing.T) {
	got := MaskProxyURL("http://session-token@proxy.example:18080")
	if got != "http://%2A%2A%2A%2A%2A%2A%2A%2A@proxy.example:18080" && got != "http://********@proxy.example:18080" {
		t.Fatalf("MaskProxyURL() = %q", got)
	}
	if strings.Contains(got, "session-token") {
		t.Fatal("masked URL leaked username-only credential")
	}
}

func TestMaskedProxyURLMatchesCurrentAndLegacyMasks(t *testing.T) {
	raw := "http://user:secret@proxy.example:8080"
	for _, masked := range []string{
		"http://********:********@proxy.example:8080",
		"http://user:********@proxy.example:8080",
	} {
		if !MaskedProxyURLMatches(masked, raw) {
			t.Fatalf("MaskedProxyURLMatches(%q) = false", masked)
		}
	}
	if MaskedProxyURLMatches("http://other:********@proxy.example:8080", raw) {
		t.Fatal("MaskedProxyURLMatches() accepted a different legacy username")
	}
}

func TestMaskProxyURLMasksMalformedCredential(t *testing.T) {
	for _, raw := range []string{
		"http://user:sec%ret@proxy.example:8080",
		"http://user:secret",
		"user:secret@proxy.example:8080",
		"http:/user:secret@proxy.example:8080",
		"http://user:sec/ret@proxy.example:8080",
		"http://user:secret:8080",
		"http://[user:secret:stuff]",
	} {
		masked := MaskProxyURL(raw)
		if strings.Contains(masked, "secret") || strings.Contains(masked, "sec%ret") || !strings.Contains(masked, "********") {
			t.Fatalf("MaskProxyURL(%q) = %q", raw, masked)
		}
	}
	if got := MaskProxyURL("http://proxy.example:8080"); got != "http://proxy.example:8080" {
		t.Fatalf("valid proxy without credentials was masked: %q", got)
	}
}

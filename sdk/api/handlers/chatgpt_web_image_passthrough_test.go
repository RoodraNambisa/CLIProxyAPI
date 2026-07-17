package handlers

import "testing"

func TestProvidersSupportImageStreamPassthrough(t *testing.T) {
	for _, providers := range [][]string{
		{"codex"},
		{"chatgpt-web"},
		{"codex", "chatgpt-web"},
	} {
		if !providersSupportImageStreamPassthrough(providers) {
			t.Fatalf("providers %v should support image passthrough", providers)
		}
	}
	for _, providers := range [][]string{
		nil,
		{"xai"},
		{"codex", "xai"},
	} {
		if providersSupportImageStreamPassthrough(providers) {
			t.Fatalf("providers %v should not support image passthrough", providers)
		}
	}
}

package util

import "testing"

func TestIsRetiredGeminiCLIPath(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{path: "/v1internal:method", want: true},
		{path: "/v1internal:generateContent", want: true},
		{path: "/v1internal:streamGenerateContent", want: true},
		{path: "/v1internal:countTokens", want: true},
		{path: "/google/callback", want: true},
		{path: "/secret/google/callback", want: true},
		{path: "/v1internality", want: false},
		{path: "/v1internal", want: false},
		{path: "/v1internal/generateContent", want: false},
		{path: "/google/callback/extra", want: false},
		{path: "secret/google/callback", want: false},
		{path: "/v1/responses", want: false},
	}

	for _, test := range tests {
		if got := IsRetiredGeminiCLIPath(test.path); got != test.want {
			t.Fatalf("IsRetiredGeminiCLIPath(%q) = %t, want %t", test.path, got, test.want)
		}
	}
}

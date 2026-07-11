package util

import "testing"

func TestIsRetiredAmpPath(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{path: "/api/auth", want: true},
		{path: "/api/provider/openai/v1/messages", want: true},
		{path: "/auth", want: true},
		{path: "/auth/callback", want: true},
		{path: "/threads/example", want: true},
		{path: "/docs/example", want: true},
		{path: "/settings/example", want: true},
		{path: "/threads.rss", want: true},
		{path: "/news.rss", want: true},
		{path: "/api-v2/auth", want: false},
		{path: "/api/custom", want: false},
		{path: "/auth-v2/token", want: false},
		{path: "/v1/responses", want: false},
	}
	for _, test := range tests {
		if got := IsRetiredAmpPath(test.path); got != test.want {
			t.Fatalf("IsRetiredAmpPath(%q) = %t, want %t", test.path, got, test.want)
		}
	}
}

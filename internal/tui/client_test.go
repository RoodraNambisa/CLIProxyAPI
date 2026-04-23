package tui

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClientResolvePathWithAccessPath(t *testing.T) {
	client := NewClientWithAccessPath(8317, "secret", "secret-token")

	tests := []struct {
		name string
		path string
		want string
	}{
		{name: "management path", path: "/v0/management/config", want: "/secret-token/v0/management/config"},
		{name: "management query path", path: "/v0/management/api-keys?index=1", want: "/secret-token/v0/management/api-keys?index=1"},
		{name: "already prefixed", path: "/secret-token/v0/management/config", want: "/secret-token/v0/management/config"},
		{name: "non-management path", path: "/healthz", want: "/healthz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := client.resolvePath(tt.path); got != tt.want {
				t.Fatalf("resolvePath(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestClientResolvePathWithoutAccessPath(t *testing.T) {
	client := NewClient(8317, "secret")
	got := client.resolvePath("/v0/management/config")
	if got != "/v0/management/config" {
		t.Fatalf("resolvePath without access path = %q, want %q", got, "/v0/management/config")
	}
}

func TestClientDoRequestAppliesAccessPath(t *testing.T) {
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.String()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	client := NewClientWithAccessPath(8317, "secret", "secret-token")
	client.baseURL = server.URL

	_, code, err := client.doRequest(http.MethodGet, "/v0/management/config", nil)
	if err != nil {
		t.Fatalf("doRequest returned error: %v", err)
	}
	if code != http.StatusOK {
		t.Fatalf("doRequest status = %d, want %d", code, http.StatusOK)
	}
	if gotPath != "/secret-token/v0/management/config" {
		t.Fatalf("request path = %q, want %q", gotPath, "/secret-token/v0/management/config")
	}
}

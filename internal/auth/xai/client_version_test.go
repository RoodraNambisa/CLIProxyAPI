package xai

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStableClientVersionValidation(t *testing.T) {
	for _, value := range []string{"1.0.52", "1.0.13", "1.2.0", "2.0.0"} {
		if !ValidClientVersion(value) {
			t.Errorf("valid version rejected: %s", value)
		}
	}
	for _, value := range []string{"0.2.120", "1.0.12", "1.0.52-alpha", "1.0", "1.0.052", "+1.0.52", "1.0.52\r\n"} {
		if ValidClientVersion(value) {
			t.Errorf("invalid version accepted: %s", value)
		}
	}
}
func TestClientVersionFetchRejectsInvalidAndHTTPFailure(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
		valid  bool
	}{{200, `{"version":"1.0.52"}`, true}, {200, `{"version":"1.0.53-alpha"}`, false}, {426, `{"version":"1.0.54"}`, false}, {200, `{`, false}} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.status)
			_, _ = w.Write([]byte(tc.body))
		}))
		version, err := FetchClientVersion(t.Context(), server.Client(), server.URL)
		server.Close()
		if (err == nil) != tc.valid || (tc.valid && version != "1.0.52") {
			t.Fatalf("fetch: %s, %v", version, err)
		}
	}
}

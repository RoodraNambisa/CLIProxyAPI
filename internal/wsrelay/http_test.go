package wsrelay

import (
	"context"
	"net/http"
	"testing"
)

func TestNonStreamReleasesRequestBodyAfterSend(t *testing.T) {
	mgr := NewManager(Options{})
	req := &HTTPRequest{
		Method:  http.MethodPost,
		URL:     "https://example.test/v1/models/test:generateContent",
		Headers: http.Header{"Content-Type": []string{"application/json"}},
		Body:    []byte(`{"contents":[{"parts":[{"text":"large"}]}]}`),
	}

	_, err := mgr.NonStream(context.Background(), "missing-provider", req)
	if err == nil {
		t.Fatal("NonStream() error = nil, want missing provider error")
	}
	if req.Body != nil {
		t.Fatalf("request body retained after send failure: %q", req.Body)
	}
}

func TestReleaseHTTPRequestBodyClearsEncodedMessage(t *testing.T) {
	req := &HTTPRequest{Body: []byte("large")}
	msg := Message{Payload: map[string]any{"body": "large", "method": "POST"}}

	releaseHTTPRequestBody(req, &msg)

	if req.Body != nil {
		t.Fatalf("request body retained: %q", req.Body)
	}
	if msg.Payload != nil {
		t.Fatalf("message payload retained: %#v", msg.Payload)
	}
}

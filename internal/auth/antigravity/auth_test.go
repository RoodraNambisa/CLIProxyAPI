package antigravity

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type countingReader struct {
	reader io.Reader
	read   int
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, errRead := r.reader.Read(p)
	r.read += n
	return n, errRead
}

func TestFetchProjectIDFromLoadCodeAssist(t *testing.T) {
	auth := NewAntigravityAuth(nil, &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.String() != "https://cloudcode-pa.googleapis.com/v1internal:loadCodeAssist" {
			t.Fatalf("unexpected request URL: %s", req.URL.String())
		}
		assertLoadCodeAssistHeaders(t, req)
		assertJSONBody(t, req, map[string]any{"metadata": map[string]any{"ideType": "ANTIGRAVITY"}})
		return jsonResponse(`{"cloudaicompanionProject":"cogent-snow-4mnnp"}`), nil
	})})

	projectID, err := auth.FetchProjectID(context.Background(), "access-token")
	if err != nil {
		t.Fatalf("FetchProjectID error: %v", err)
	}
	if projectID != "cogent-snow-4mnnp" {
		t.Fatalf("projectID = %q", projectID)
	}
}

func TestFetchProjectIDFallsBackToDailyOnboardUser(t *testing.T) {
	var sawOnboard bool
	auth := NewAntigravityAuth(nil, &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.String() {
		case "https://cloudcode-pa.googleapis.com/v1internal:loadCodeAssist":
			assertLoadCodeAssistHeaders(t, req)
			return jsonResponse(`{"allowedTiers":[{"id":"free-tier","isDefault":true}]}`), nil
		case "https://daily-cloudcode-pa.googleapis.com/v1internal:onboardUser":
			sawOnboard = true
			assertOnboardUserHeaders(t, req)
			assertJSONBody(t, req, map[string]any{
				"tier_id": "free-tier",
				"metadata": map[string]any{
					"ide_type":    "ANTIGRAVITY",
					"ide_version": "2.2.1",
					"ide_name":    "antigravity",
				},
			})
			return jsonResponse(`{
				"done": true,
				"response": {
					"cloudaicompanionProject": {
						"id": "cogent-snow-4mnnp",
						"name": "cogent-snow-4mnnp",
						"projectNumber": "22597072101"
					}
				}
			}`), nil
		default:
			t.Fatalf("unexpected request URL: %s", req.URL.String())
			return nil, nil
		}
	})})

	projectID, err := auth.FetchProjectID(context.Background(), "access-token")
	if err != nil {
		t.Fatalf("FetchProjectID error: %v", err)
	}
	if !sawOnboard {
		t.Fatal("expected onboardUser fallback")
	}
	if projectID != "cogent-snow-4mnnp" {
		t.Fatalf("projectID = %q", projectID)
	}
}

func TestFetchProjectIDPreservesRetryDelayFromErrorBody(t *testing.T) {
	auth := NewAntigravityAuth(nil, &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Header:     make(http.Header),
			Body: io.NopCloser(strings.NewReader(
				`{"error":{"details":[{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"17s"}]}}`,
			)),
		}, nil
	})})

	_, errProject := auth.FetchProjectID(t.Context(), "access-token")
	var statusErr interface{ StatusCode() int }
	var retryErr interface{ RetryAfter() *time.Duration }
	if !errors.As(errProject, &statusErr) || statusErr.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("FetchProjectID() error = %#v, want 429", errProject)
	}
	if !errors.As(errProject, &retryErr) || retryErr.RetryAfter() == nil || *retryErr.RetryAfter() != 17*time.Second {
		t.Fatalf("FetchProjectID() error = %#v, want Retry-After 17s", errProject)
	}
}

func TestFetchProjectIDBoundsErrorBodyRead(t *testing.T) {
	reader := &countingReader{reader: strings.NewReader(strings.Repeat("x", int(controlPlaneErrorBodyLimit*8)))}
	auth := NewAntigravityAuth(nil, &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Header:     make(http.Header),
			Body:       io.NopCloser(reader),
		}, nil
	})})

	_, errProject := auth.FetchProjectID(t.Context(), "access-token")
	if errProject == nil {
		t.Fatal("FetchProjectID() error = nil, want control-plane error")
	}
	if int64(reader.read) > controlPlaneErrorBodyLimit {
		t.Fatalf("error body read = %d bytes, want at most %d", reader.read, controlPlaneErrorBodyLimit)
	}
}

func TestOnboardUserPreservesRetryDelayFromLongErrorBody(t *testing.T) {
	padding := strings.Repeat("x", 300)
	body := `{"padding":"` + padding + `","error":{"details":[{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"23s"}]}}`
	auth := NewAntigravityAuth(nil, &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(body)),
		}, nil
	})})

	_, errProject := auth.OnboardUser(t.Context(), "access-token", "free-tier")
	var retryErr interface{ RetryAfter() *time.Duration }
	if !errors.As(errProject, &retryErr) || retryErr.RetryAfter() == nil || *retryErr.RetryAfter() != 23*time.Second {
		t.Fatalf("OnboardUser() error = %#v, want Retry-After 23s", errProject)
	}
	if len(errProject.Error()) > 650 {
		t.Fatalf("OnboardUser() error is not bounded: %d bytes", len(errProject.Error()))
	}
}

func TestOnboardUserAcceptsSuccessBodyLargerThanErrorLimit(t *testing.T) {
	body := `{"padding":"` + strings.Repeat("x", int(controlPlaneErrorBodyLimit)) + `","done":true,"response":{"cloudaicompanionProject":"large-success-project"}}`
	auth := NewAntigravityAuth(nil, &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(body), nil
	})})

	projectID, errProject := auth.OnboardUser(t.Context(), "access-token", "free-tier")
	if errProject != nil {
		t.Fatalf("OnboardUser() error: %v", errProject)
	}
	if projectID != "large-success-project" {
		t.Fatalf("projectID = %q", projectID)
	}
}

func assertLoadCodeAssistHeaders(t *testing.T, req *http.Request) {
	t.Helper()
	if got := req.Header.Get("Authorization"); got != "Bearer access-token" {
		t.Fatalf("Authorization = %q", got)
	}
	if got := req.Header.Get("Accept"); got != "*/*" {
		t.Fatalf("Accept = %q", got)
	}
	if got := req.Header.Get("X-Goog-Api-Client"); got != "" {
		t.Fatalf("X-Goog-Api-Client = %q, want empty", got)
	}
	if got := req.Header.Get("Client-Metadata"); got != "" {
		t.Fatalf("Client-Metadata = %q, want empty", got)
	}
	if got := req.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q", got)
	}
	if got := req.Header.Get("User-Agent"); got != "antigravity/hub/2.2.1 darwin/arm64" {
		t.Fatalf("User-Agent = %q", got)
	}
}

func assertOnboardUserHeaders(t *testing.T, req *http.Request) {
	t.Helper()
	if got := req.Header.Get("Authorization"); got != "Bearer access-token" {
		t.Fatalf("Authorization = %q", got)
	}
	if got := req.Header.Get("Accept"); got != "*/*" {
		t.Fatalf("Accept = %q", got)
	}
	if got := req.Header.Get("X-Goog-Api-Client"); got != "gl-node/22.21.1" {
		t.Fatalf("X-Goog-Api-Client = %q", got)
	}
	if got := req.Header.Get("Client-Metadata"); got != "" {
		t.Fatalf("Client-Metadata = %q, want empty", got)
	}
	if got := req.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q", got)
	}
	if got := req.Header.Get("User-Agent"); got != "antigravity/hub/2.2.1 darwin/arm64 google-api-nodejs-client/10.3.0" {
		t.Fatalf("User-Agent = %q", got)
	}
}

func assertJSONBody(t *testing.T, req *http.Request, want map[string]any) {
	t.Helper()
	body, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	bodyText := string(body)
	req.Body = io.NopCloser(strings.NewReader(bodyText))
	var got map[string]any
	if errDecode := json.Unmarshal(body, &got); errDecode != nil {
		t.Fatalf("decode body: %v", errDecode)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("body = %#v, want %#v", got, want)
	}
}

func jsonResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

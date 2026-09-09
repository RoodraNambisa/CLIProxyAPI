package helps

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type usageTimingTestTransport struct {
	body       io.ReadCloser
	err        error
	closedIdle bool
	canceled   *http.Request
}

func (transport *usageTimingTestTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if err := request.Context().Err(); err != nil {
		return nil, err
	}
	if transport.err != nil {
		return nil, transport.err
	}
	return &http.Response{StatusCode: 200, Header: http.Header{}, Body: transport.body, Request: request}, nil
}

func (transport *usageTimingTestTransport) CloseIdleConnections() { transport.closedIdle = true }

func (transport *usageTimingTestTransport) CancelRequest(request *http.Request) {
	transport.canceled = request
}

type usageTimingTestBody struct {
	io.Reader
	closed bool
}

func (body *usageTimingTestBody) Close() error { body.closed = true; return nil }

func TestUsageHTTPTrackingPreservesClientAndBodyOwnership(t *testing.T) {
	reporter := NewUsageReporter(t.Context(), "codex", "fixture", nil)
	body := &usageTimingTestBody{Reader: strings.NewReader(": ping\n\n")}
	transport := &usageTimingTestTransport{body: body}
	client := &http.Client{Transport: transport, Timeout: time.Minute}
	tracked := reporter.TrackHTTPClient(client)
	if tracked == client || client.Transport != transport || tracked.Timeout != client.Timeout || reporter.TrackHTTPClient(tracked) != tracked {
		t.Fatal("tracking mutated client settings or stacked duplicate wrappers")
	}
	request, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://fixture.invalid", nil)
	response, err := tracked.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if token, packet := reporter.responseTimings(); token != 0 || packet != 0 {
		t.Fatal("response headers were counted as body content")
	}
	payload, err := io.ReadAll(response.Body)
	if err != nil || string(payload) != ": ping\n\n" {
		t.Fatal("tracking changed the response bytes")
	}
	if token, packet := reporter.responseTimings(); token != 0 || packet <= 0 {
		t.Fatal("keepalive bytes were counted as model content")
	}
	if err := response.Body.Close(); err != nil || !body.closed {
		t.Fatal("response body ownership was not forwarded")
	}
	tracked.CloseIdleConnections()
	if !transport.closedIdle {
		t.Fatal("idle transport cleanup was lost")
	}
	tracked.Transport.(interface{ CancelRequest(*http.Request) }).CancelRequest(request)
	if transport.canceled != request {
		t.Fatal("legacy transport cancellation was lost")
	}
}

func TestUsageHTTPTrackingPreservesCancellationAndErrors(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		reporter := NewUsageReporter(t.Context(), "codex", "fixture", nil)
		original := errors.New("fixture transport failure")
		ctx, cancel := context.WithCancel(t.Context())
		if canceled {
			cancel()
			original = context.Canceled
		}
		client := reporter.TrackHTTPClient(&http.Client{Transport: &usageTimingTestTransport{err: original}})
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://fixture.invalid", nil)
		_, err := client.Do(request)
		cancel()
		if !errors.Is(err, original) {
			t.Fatal("tracking changed the original failure")
		}
		if token, packet := reporter.responseTimings(); token != 0 || packet != 0 {
			t.Fatal("transport failure fabricated response data")
		}
	}
	var reporter *UsageReporter
	client := &http.Client{}
	if reporter.TrackHTTPClient(client) != client || (&UsageReporter{}).TrackHTTPClient(nil) != nil {
		t.Fatal("nil tracking changed legacy client handling")
	}
}

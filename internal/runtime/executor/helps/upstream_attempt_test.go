package helps

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestHTTPAttemptObservationPreservesTraceAndRejectsLocalValidation(t *testing.T) {
	var traces atomic.Int64
	ctx := executor.WithUpstreamAttempt(t.Context())
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { traces.Add(1) }})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(429) }))
	defer server.Close()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	response, err := DoUpstreamHTTPRequest(server.Client(), req)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if traces.Load() != 1 || !executor.IsUpstreamAttemptError(executor.ErrorFromUpstreamAttempt(ctx, errors.New("rate limited"))) {
		t.Fatal("transport observation discarded an existing trace or missed the upstream")
	}
	localCtx := executor.WithUpstreamAttempt(t.Context())
	invalid, _ := http.NewRequestWithContext(localCtx, http.MethodGet, "unsupported://invalid", nil)
	_, err = DoUpstreamHTTPRequest(server.Client(), invalid)
	if err == nil || executor.IsUpstreamAttemptError(executor.ErrorFromUpstreamAttempt(localCtx, err)) {
		t.Fatal("local URL rejection was marked as upstream traffic")
	}
}

func TestWebsocketAttemptObservationExcludesSuccessfulHandshakeAndLocalErrors(t *testing.T) {
	ctx := executor.WithUpstreamAttempt(t.Context())
	ObserveUpstreamWebsocketDial(ctx, &http.Response{StatusCode: 101}, nil)
	local := errors.New("missing session")
	ObserveUpstreamWebsocketWrite(ctx, local)
	if executor.IsUpstreamAttemptError(executor.ErrorFromUpstreamAttempt(ctx, local)) {
		t.Fatal("handshake or local validation counted as a request send")
	}
	ObserveUpstreamWebsocketWrite(ctx, nil)
	if !executor.IsUpstreamAttemptError(executor.ErrorFromUpstreamAttempt(ctx, local)) {
		t.Fatal("successful frame write did not mark the attempt")
	}
}

func TestHTTPRequestLimitCountsDisconnectButNotLocalValidation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = conn.Close()
	}))
	defer server.Close()
	for _, target := range []string{"unsupported://invalid", server.URL} {
		slot := &executor.AuthRequestSlot{}
		slot.Bind(&usageReporterTestReservation{consumed: true})
		ctx := executor.WithUpstreamAttemptSlot(t.Context(), slot)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, err = DoUpstreamHTTPRequest(server.Client(), req)
		if target == server.URL {
			if !errors.Is(err, io.EOF) || !slot.Committed() || slot.Release() {
				t.Fatalf("upstream disconnect refunded request capacity: %v", err)
			}
		} else if err == nil || slot.Committed() || !slot.Release() {
			t.Fatalf("local URL rejection consumed request capacity: %v", err)
		}
	}
}

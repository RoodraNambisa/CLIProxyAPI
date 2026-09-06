package helps

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptrace"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

// DoUpstreamHTTPRequest observes transport dispatch without replacing clients,
// transports, TLS profiles, proxy behavior, or timeout settings. An HTTP response
// also supplies evidence for custom transports that do not implement httptrace.
func DoUpstreamHTTPRequest(client *http.Client, req *http.Request) (*http.Response, error) {
	if req == nil || !executor.TracksUpstreamAttempt(req.Context()) {
		return client.Do(req)
	}
	ctx := req.Context()
	mark := func() { executor.MarkUpstreamAttempt(ctx) }
	trace := &httptrace.ClientTrace{
		DNSStart:     func(httptrace.DNSStartInfo) { mark() },
		ConnectStart: func(string, string) { mark() },
		GotConn:      func(httptrace.GotConnInfo) { mark() },
		WroteRequest: func(httptrace.WroteRequestInfo) { mark() },
	}
	response, err := client.Do(req.WithContext(httptrace.WithClientTrace(ctx, trace)))
	if response != nil {
		mark()
	}
	return response, err
}

func ObserveUpstreamWebsocketWrite(ctx context.Context, err error) error {
	var networkError net.Error
	if err == nil || errors.As(err, &networkError) {
		executor.MarkUpstreamAttempt(ctx)
	}
	return err
}

func ObserveUpstreamWebsocketDial(ctx context.Context, response *http.Response, err error) {
	if err == nil {
		return
	}
	var networkError net.Error
	if response != nil || errors.As(err, &networkError) {
		executor.MarkUpstreamAttempt(ctx)
	}
}

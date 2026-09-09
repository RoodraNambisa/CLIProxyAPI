package helps

import (
	"io"
	"net/http"
	"sync"
)

// TrackHTTPClient observes the first response-body bytes. It preserves the
// client's transport and settings; protocol observers measure actual content.
func (r *UsageReporter) TrackHTTPClient(client *http.Client) *http.Client {
	if r == nil || client == nil {
		return client
	}
	if tracked, ok := client.Transport.(*usageTimingTransport); ok && tracked.reporter == r {
		return client
	}
	tracked := *client
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	tracked.Transport = &usageTimingTransport{base: base, reporter: r}
	return &tracked
}

type usageTimingTransport struct {
	base     http.RoundTripper
	reporter *UsageReporter
}

func (transport *usageTimingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	transport.reporter.StartResponseTTFT()
	response, err := transport.base.RoundTrip(req)
	if err == nil && response != nil && response.Body != nil {
		response.Body = &usagePacketReadCloser{ReadCloser: response.Body, reporter: transport.reporter}
	}
	return response, err
}

func (transport *usageTimingTransport) CloseIdleConnections() {
	if closer, ok := transport.base.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

// CancelRequest preserves cancellation for transports using the legacy hook.
func (transport *usageTimingTransport) CancelRequest(req *http.Request) {
	if canceler, ok := transport.base.(interface{ CancelRequest(*http.Request) }); ok {
		canceler.CancelRequest(req)
	}
}

type usagePacketReadCloser struct {
	io.ReadCloser
	reporter *UsageReporter
	once     sync.Once
}

func (body *usagePacketReadCloser) Read(buffer []byte) (int, error) {
	if body == nil || body.ReadCloser == nil {
		return 0, io.ErrClosedPipe
	}
	n, err := body.ReadCloser.Read(buffer)
	if n > 0 {
		body.once.Do(body.reporter.RecordFirstPacket)
	}
	return n, err
}

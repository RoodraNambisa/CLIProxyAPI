package helps

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
)

type chatGPTWebRemoteImageStaticResolver map[string][]string

func (resolver chatGPTWebRemoteImageStaticResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	values, exists := resolver[host]
	if !exists {
		return nil, errors.New("host not found")
	}
	result := make([]net.IPAddr, 0, len(values))
	for _, value := range values {
		result = append(result, net.IPAddr{IP: net.ParseIP(value)})
	}
	return result, nil
}

func TestChatGPTWebRemoteImageFetchPinsValidatedPublicIPAndIgnoresProxy(t *testing.T) {
	pngBytes := chatGPTWebRemoteImageTestPNG()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if !strings.HasPrefix(request.Host, "public.example:") {
			t.Errorf("Host = %q", request.Host)
		}
		if request.Header.Get("Authorization") != "" || request.Header.Get("Cookie") != "" {
			t.Fatal("protected fetch forwarded credentials")
		}
		response.Header().Set("Content-Type", "image/png")
		_, _ = response.Write(pngBytes)
	}))
	defer server.Close()

	_, port, errSplit := net.SplitHostPort(server.Listener.Addr().String())
	if errSplit != nil {
		t.Fatal(errSplit)
	}
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	t.Setenv("NO_PROXY", "")

	var dialedAddress atomic.Value
	fetcher := newChatGPTWebRemoteImageFetcher(
		chatGPTWebRemoteImageStaticResolver{"public.example": {"93.184.216.34"}},
		func(ctx context.Context, network, address string) (net.Conn, error) {
			dialedAddress.Store(address)
			return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
		},
	)
	before := ChatGPTWebImageSpoolSnapshot()
	file, errFetch := fetcher.fetch(t.Context(), "http://public.example:"+port+"/image.png", int64(len(pngBytes)))
	if errFetch != nil {
		t.Fatalf("fetch() error = %v", errFetch)
	}
	if got := dialedAddress.Load(); got != net.JoinHostPort("93.184.216.34", port) {
		t.Fatalf("dialed address = %v", got)
	}
	if file.Size != int64(len(pngBytes)) || file.DeclaredContentType != "image/png" {
		t.Fatalf("download = size:%d type:%q", file.Size, file.DeclaredContentType)
	}
	during := ChatGPTWebImageSpoolSnapshot()
	if during.CurrentFiles != before.CurrentFiles+1 || during.CurrentBytes != before.CurrentBytes+int64(len(pngBytes)) {
		t.Fatalf("spool during = %#v, before %#v", during, before)
	}
	if errRemove := file.Remove(); errRemove != nil {
		t.Fatalf("Remove() error = %v", errRemove)
	}
	if errRemove := file.Remove(); errRemove != nil {
		t.Fatalf("second Remove() error = %v", errRemove)
	}
	after := ChatGPTWebImageSpoolSnapshot()
	if after.CurrentFiles != before.CurrentFiles || after.CurrentBytes != before.CurrentBytes {
		t.Fatalf("spool after = %#v, before %#v", after, before)
	}
}

func TestChatGPTWebRemoteImageFetchThroughProxyUsesPinnedIPAndOriginalHost(t *testing.T) {
	pngBytes := chatGPTWebRemoteImageTestPNG()
	targetAddress := net.JoinHostPort("93.184.216.34", "18080")
	var connectTarget atomic.Value
	var originHost atomic.Value
	proxyServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodConnect {
			t.Errorf("proxy method = %q", request.Method)
			response.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if request.Header.Get("Proxy-Authorization") == "" {
			t.Error("proxy CONNECT request omitted proxy authentication")
		}
		connectTarget.Store(request.Host)
		hijacker, ok := response.(http.Hijacker)
		if !ok {
			t.Error("proxy response does not support hijacking")
			return
		}
		connection, reader, errHijack := hijacker.Hijack()
		if errHijack != nil {
			t.Errorf("hijack proxy connection: %v", errHijack)
			return
		}
		defer connection.Close()
		if _, errWrite := io.WriteString(connection, "HTTP/1.1 200 Connection Established\r\n\r\n"); errWrite != nil {
			t.Errorf("write CONNECT response: %v", errWrite)
			return
		}
		originRequest, errRead := http.ReadRequest(reader.Reader)
		if errRead != nil {
			t.Errorf("read tunneled request: %v", errRead)
			return
		}
		if originRequest.Header.Get("Authorization") != "" || originRequest.Header.Get("Cookie") != "" ||
			originRequest.Header.Get("Proxy-Authorization") != "" {
			t.Errorf("tunneled request leaked credentials: %#v", originRequest.Header)
		}
		originHost.Store(originRequest.Host)
		responseText := "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: " + strconv.Itoa(len(pngBytes)) + "\r\nConnection: close\r\n\r\n"
		if _, errWrite := io.WriteString(connection, responseText); errWrite != nil {
			t.Errorf("write tunneled response headers: %v", errWrite)
			return
		}
		if _, errWrite := connection.Write(pngBytes); errWrite != nil {
			t.Errorf("write tunneled response body: %v", errWrite)
		}
	}))
	t.Cleanup(proxyServer.Close)

	proxyURL := strings.Replace(proxyServer.URL, "http://", "http://proxy-user:proxy-password@", 1)
	proxyDialer, mode, errDialer := proxyutil.BuildContextDialer(proxyURL)
	if errDialer != nil || mode != proxyutil.ModeProxy {
		t.Fatalf("BuildContextDialer() = mode:%v error:%v", mode, errDialer)
	}
	fetcher := newChatGPTWebRemoteImageFetcher(
		chatGPTWebRemoteImageStaticResolver{"public.example": {"93.184.216.34"}},
		proxyDialer.DialContext,
	)
	file, errFetch := fetcher.fetch(t.Context(), "http://public.example:18080/image.png", int64(len(pngBytes)))
	if errFetch != nil {
		t.Fatalf("fetch() error = %v", errFetch)
	}
	t.Cleanup(func() { _ = file.Remove() })
	if got := connectTarget.Load(); got != targetAddress {
		t.Fatalf("CONNECT target = %v, want %q", got, targetAddress)
	}
	if got := originHost.Load(); got != "public.example:18080" {
		t.Fatalf("origin Host = %v", got)
	}
}

func TestChatGPTWebRemoteImageFetchRejectsMixedDNSBeforeDial(t *testing.T) {
	var dialCalls atomic.Int64
	fetcher := newChatGPTWebRemoteImageFetcher(
		chatGPTWebRemoteImageStaticResolver{"mixed.example": {"93.184.216.34", "127.0.0.1"}},
		func(context.Context, string, string) (net.Conn, error) {
			dialCalls.Add(1)
			return nil, errors.New("must not dial")
		},
	)
	_, errFetch := fetcher.fetch(t.Context(), "http://mixed.example:8080/image", 1024)
	assertChatGPTWebRemoteImageErrorKind(t, errFetch, ChatGPTWebRemoteImageBlocked)
	if dialCalls.Load() != 0 {
		t.Fatalf("dial calls = %d", dialCalls.Load())
	}
}

func TestChatGPTWebRemoteImageProxyCannotBypassPrivateTargetValidation(t *testing.T) {
	var proxyDialCalls atomic.Int64
	fetcher := newChatGPTWebRemoteImageFetcher(
		chatGPTWebRemoteImageStaticResolver{"private.example": {"169.254.169.254"}},
		func(context.Context, string, string) (net.Conn, error) {
			proxyDialCalls.Add(1)
			return nil, errors.New("must not dial proxy")
		},
	)
	_, errFetch := fetcher.fetch(t.Context(), "http://private.example/latest/meta-data", 1024)
	assertChatGPTWebRemoteImageErrorKind(t, errFetch, ChatGPTWebRemoteImageBlocked)
	if proxyDialCalls.Load() != 0 {
		t.Fatalf("proxy dial calls = %d", proxyDialCalls.Load())
	}
	if strings.Contains(errFetch.Error(), "private.example") || strings.Contains(errFetch.Error(), "169.254.169.254") {
		t.Fatalf("safe error leaked target: %v", errFetch)
	}
}

func TestChatGPTWebRemoteImageFetchRevalidatesRedirectTargets(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Location", "http://private.example/image")
		response.WriteHeader(http.StatusFound)
	}))
	defer server.Close()
	_, port, errSplit := net.SplitHostPort(server.Listener.Addr().String())
	if errSplit != nil {
		t.Fatal(errSplit)
	}
	fetcher := newChatGPTWebRemoteImageFetcher(
		chatGPTWebRemoteImageStaticResolver{
			"public.example":  {"93.184.216.34"},
			"private.example": {"169.254.169.254"},
		},
		func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
		},
	)
	_, errFetch := fetcher.fetch(t.Context(), "http://public.example:"+port+"/redirect", 1024)
	assertChatGPTWebRemoteImageErrorKind(t, errFetch, ChatGPTWebRemoteImageBlocked)
}

func TestChatGPTWebRemoteImageFetchEnforcesStreamLimitAndEncoding(t *testing.T) {
	tests := []struct {
		name     string
		handler  http.HandlerFunc
		wantKind ChatGPTWebRemoteImageErrorKind
	}{
		{
			name: "chunked too large",
			handler: func(response http.ResponseWriter, _ *http.Request) {
				response.Header().Set("Content-Type", "image/png")
				response.(http.Flusher).Flush()
				_, _ = response.Write([]byte("12345"))
			},
			wantKind: ChatGPTWebRemoteImageTooLarge,
		},
		{
			name: "declared content length too large",
			handler: func(response http.ResponseWriter, _ *http.Request) {
				response.Header().Set("Content-Length", "5")
				response.Header().Set("Content-Type", "image/png")
				response.WriteHeader(http.StatusOK)
			},
			wantKind: ChatGPTWebRemoteImageTooLarge,
		},
		{
			name: "compressed response",
			handler: func(response http.ResponseWriter, _ *http.Request) {
				response.Header().Set("Content-Encoding", "gzip")
				_, _ = response.Write([]byte("123"))
			},
			wantKind: ChatGPTWebRemoteImageInvalid,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			server := httptest.NewServer(testCase.handler)
			defer server.Close()
			_, port, errSplit := net.SplitHostPort(server.Listener.Addr().String())
			if errSplit != nil {
				t.Fatal(errSplit)
			}
			fetcher := newChatGPTWebRemoteImageFetcher(
				chatGPTWebRemoteImageStaticResolver{"public.example": {"93.184.216.34"}},
				func(ctx context.Context, network, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
				},
			)
			_, errFetch := fetcher.fetch(t.Context(), "http://public.example:"+port+"/image", 4)
			assertChatGPTWebRemoteImageErrorKind(t, errFetch, testCase.wantKind)
		})
	}
}

func TestChatGPTWebRemoteImageFetchCancellationCleansPartialSpool(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "image/png")
		response.(http.Flusher).Flush()
		close(started)
		<-request.Context().Done()
	}))
	t.Cleanup(server.Close)
	_, port, errSplit := net.SplitHostPort(server.Listener.Addr().String())
	if errSplit != nil {
		t.Fatal(errSplit)
	}
	fetcher := newChatGPTWebRemoteImageFetcher(
		chatGPTWebRemoteImageStaticResolver{"public.example": {"93.184.216.34"}},
		func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
		},
	)
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	before := ChatGPTWebImageSpoolSnapshot()
	go func() {
		_, errFetch := fetcher.fetch(ctx, "http://public.example:"+port+"/image.png", 1024)
		result <- errFetch
	}()
	<-started
	cancel()
	assertChatGPTWebRemoteImageErrorKind(t, <-result, ChatGPTWebRemoteImageFetch)
	after := ChatGPTWebImageSpoolSnapshot()
	if after.CurrentFiles != before.CurrentFiles || after.CurrentBytes != before.CurrentBytes {
		t.Fatalf("spool after cancellation = %#v, before %#v", after, before)
	}
}

func TestChatGPTWebRemoteImageURLAndRedirectValidation(t *testing.T) {
	invalidURLs := []string{
		"", "ftp://example.com/image.png", "http://user:pass@example.com/image.png",
		"http://example.com:0/image.png", "http://example.com:65536/image.png", "http://[fe80::1%25lo0]/image.png",
	}
	for _, rawURL := range invalidURLs {
		if errValidate := ValidateChatGPTWebRemoteImageURL(rawURL); errValidate == nil {
			t.Fatalf("ValidateChatGPTWebRemoteImageURL(%q) error = nil", rawURL)
		}
	}
	if errValidate := ValidateChatGPTWebRemoteImageURL("https://example.com:8443/image.png"); errValidate != nil {
		t.Fatalf("public custom port error = %v", errValidate)
	}

	redirectURL, _ := url.Parse("http://example.com/image.png")
	redirect := &http.Request{URL: redirectURL, Header: http.Header{
		"Authorization": {"secret"}, "Cookie": {"secret"}, "Referer": {"https://source.example/private-token"},
	}}
	previousURL, _ := url.Parse("https://example.com/start")
	if errRedirect := validateChatGPTWebRemoteImageRedirect(redirect, []*http.Request{{URL: previousURL}}); errRedirect == nil {
		t.Fatal("HTTPS downgrade error = nil")
	}
	redirect.URL.Scheme = "https"
	if errRedirect := validateChatGPTWebRemoteImageRedirect(redirect, []*http.Request{{URL: previousURL}}); errRedirect != nil {
		t.Fatalf("safe redirect error = %v", errRedirect)
	}
	if redirect.Header.Get("Authorization") != "" || redirect.Header.Get("Cookie") != "" || redirect.Header.Get("Referer") != "" {
		t.Fatalf("redirect retained sensitive headers: %#v", redirect.Header)
	}
	redirect.URL.Scheme = "http"
	for index := 0; index <= chatGPTWebRemoteImageMaxRedirects; index++ {
		previous := make([]*http.Request, index)
		for item := range previous {
			previous[item] = &http.Request{URL: redirectURL}
		}
		errRedirect := validateChatGPTWebRemoteImageRedirect(redirect, previous)
		if index <= chatGPTWebRemoteImageMaxRedirects && errRedirect != nil {
			t.Fatalf("redirect %d error = %v", index, errRedirect)
		}
	}
	tooMany := make([]*http.Request, chatGPTWebRemoteImageMaxRedirects+1)
	for index := range tooMany {
		tooMany[index] = &http.Request{URL: redirectURL}
	}
	if errRedirect := validateChatGPTWebRemoteImageRedirect(redirect, tooMany); errRedirect == nil {
		t.Fatal("too many redirects error = nil")
	}
}

func TestChatGPTWebRemoteImagePublicIPRejectsSpecialRanges(t *testing.T) {
	blocked := []string{
		"127.0.0.1", "10.0.0.1", "100.64.0.1", "169.254.169.254", "192.0.2.1", "198.18.0.1",
		"::1", "::ffff:127.0.0.1", "fc00::1", "fe80::1", "2001:db8::1", "64:ff9b::7f00:1",
	}
	for _, rawIP := range blocked {
		if chatGPTWebRemoteImagePublicIP(netip.MustParseAddr(rawIP)) {
			t.Fatalf("special IP %s was allowed", rawIP)
		}
	}
	for _, rawIP := range []string{"1.1.1.1", "8.8.8.8", "2606:4700:4700::1111"} {
		if !chatGPTWebRemoteImagePublicIP(netip.MustParseAddr(rawIP)) {
			t.Fatalf("public IP %s was blocked", rawIP)
		}
	}
}

func assertChatGPTWebRemoteImageErrorKind(t *testing.T, err error, want ChatGPTWebRemoteImageErrorKind) {
	t.Helper()
	var remoteErr *ChatGPTWebRemoteImageError
	if !errors.As(err, &remoteErr) || remoteErr == nil || remoteErr.Kind != want {
		t.Fatalf("error = %v, kind = %#v, want %q", err, remoteErr, want)
	}
}

func chatGPTWebRemoteImageTestPNG() []byte {
	const encoded = "\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde\x00\x00\x00\x0cIDAT\x08\xd7c\xf8\xcf\xc0\x00\x00\x03\x01\x01\x00\x18\xdd\x8d\xb0\x00\x00\x00\x00IEND\xaeB`\x82"
	return []byte(encoded)
}

func TestChatGPTWebRemoteImageCustomPortSyntax(t *testing.T) {
	for _, port := range []int{1, 80, 443, 8443, 65535} {
		rawURL := "https://example.com:" + strconv.Itoa(port) + "/image"
		if errValidate := ValidateChatGPTWebRemoteImageURL(rawURL); errValidate != nil {
			t.Fatalf("port %d: %v", port, errValidate)
		}
	}
}

package helps

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
)

const (
	chatGPTWebRemoteImageTimeout      = 30 * time.Second
	chatGPTWebRemoteImageDialTimeout  = 10 * time.Second
	chatGPTWebRemoteImageMaxRedirects = 5
)

// ChatGPTWebRemoteImageErrorKind is a safe, low-cardinality download failure class.
type ChatGPTWebRemoteImageErrorKind string

const (
	ChatGPTWebRemoteImageInvalid  ChatGPTWebRemoteImageErrorKind = "invalid"
	ChatGPTWebRemoteImageBlocked  ChatGPTWebRemoteImageErrorKind = "blocked"
	ChatGPTWebRemoteImageTooLarge ChatGPTWebRemoteImageErrorKind = "too_large"
	ChatGPTWebRemoteImageFetch    ChatGPTWebRemoteImageErrorKind = "fetch_failed"
)

// ChatGPTWebRemoteImageError reports a protected fetch failure without retaining its URL.
type ChatGPTWebRemoteImageError struct {
	Kind  ChatGPTWebRemoteImageErrorKind
	cause error
}

func (err *ChatGPTWebRemoteImageError) Error() string {
	if err == nil {
		return "remote image download failed"
	}
	switch err.Kind {
	case ChatGPTWebRemoteImageInvalid:
		return "remote image URL is invalid"
	case ChatGPTWebRemoteImageBlocked:
		return "remote image URL is blocked"
	case ChatGPTWebRemoteImageTooLarge:
		return "remote image exceeds the size limit"
	default:
		return "remote image download failed"
	}
}

func (err *ChatGPTWebRemoteImageError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.cause
}

func newChatGPTWebRemoteImageError(kind ChatGPTWebRemoteImageErrorKind, cause error) error {
	return &ChatGPTWebRemoteImageError{Kind: kind, cause: cause}
}

type chatGPTWebRemoteImageResolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
}

type chatGPTWebRemoteImageFetcher struct {
	resolver    chatGPTWebRemoteImageResolver
	dialContext func(context.Context, string, string) (net.Conn, error)
}

// ChatGPTWebRemoteImageFile owns one protected temporary download.
type ChatGPTWebRemoteImageFile struct {
	mu                  sync.Mutex
	file                *os.File
	path                string
	tracker             *ChatGPTWebImageSpoolFile
	removed             bool
	Size                int64
	DeclaredContentType string
}

// SizeBytes returns the verified retained body size.
func (file *ChatGPTWebRemoteImageFile) SizeBytes() int64 {
	if file == nil {
		return 0
	}
	return file.Size
}

// ContentType returns the untrusted response declaration for comparison with
// the decoded image format.
func (file *ChatGPTWebRemoteImageFile) ContentType() string {
	if file == nil {
		return ""
	}
	return file.DeclaredContentType
}

// WithReader rewinds the temporary file and exposes it without revealing its path.
func (file *ChatGPTWebRemoteImageFile) WithReader(fn func(io.Reader) error) error {
	if file == nil || fn == nil {
		return errors.New("remote image file is unavailable")
	}
	file.mu.Lock()
	defer file.mu.Unlock()
	if file.removed || file.file == nil {
		return errors.New("remote image file is unavailable")
	}
	if _, errSeek := file.file.Seek(0, io.SeekStart); errSeek != nil {
		return errors.New("rewind remote image file")
	}
	return fn(file.file)
}

// Remove closes and removes the temporary file exactly once.
func (file *ChatGPTWebRemoteImageFile) Remove() error {
	if file == nil {
		return nil
	}
	file.mu.Lock()
	if file.removed {
		file.mu.Unlock()
		return nil
	}
	file.removed = true
	openFile := file.file
	file.file = nil
	path := file.path
	file.path = ""
	tracker := file.tracker
	file.tracker = nil
	file.mu.Unlock()

	var errCleanup error
	if openFile != nil {
		if errClose := openFile.Close(); errClose != nil {
			errCleanup = errors.New("close remote image file")
		}
	}
	if path != "" {
		if errRemove := os.Remove(path); errRemove != nil && !errors.Is(errRemove, os.ErrNotExist) {
			errCleanup = errors.New("remove remote image file")
		}
	}
	if tracker != nil {
		tracker.FinishCleanup(errCleanup)
	}
	return errCleanup
}

// FetchChatGPTWebRemoteImage downloads one public HTTP(S) image directly.
func FetchChatGPTWebRemoteImage(ctx context.Context, rawURL string) (*ChatGPTWebRemoteImageFile, error) {
	return FetchChatGPTWebRemoteImageWithProxy(ctx, rawURL, "")
}

// FetchChatGPTWebRemoteImageWithProxy downloads one public image through an
// optional connection-layer proxy. Target resolution and validation remain
// local, and the proxy receives only the validated pinned IP address.
func FetchChatGPTWebRemoteImageWithProxy(ctx context.Context, rawURL, proxyURL string) (*ChatGPTWebRemoteImageFile, error) {
	var dialContext func(context.Context, string, string) (net.Conn, error)
	if strings.TrimSpace(proxyURL) != "" {
		dialer, mode, errDialer := proxyutil.BuildContextDialer(proxyURL)
		if errDialer != nil || mode == proxyutil.ModeInvalid {
			return nil, newChatGPTWebRemoteImageError(ChatGPTWebRemoteImageFetch, errors.New("remote image proxy is invalid"))
		}
		if dialer != nil && mode == proxyutil.ModeProxy {
			dialContext = dialer.DialContext
		}
	}
	fetcher := newChatGPTWebRemoteImageFetcher(nil, nil)
	if dialContext != nil {
		fetcher = newChatGPTWebRemoteImageFetcher(nil, dialContext)
	}
	return fetcher.fetch(ctx, rawURL, ChatGPTWebMaxImageBytes)
}

// IsChatGPTWebRemoteImageURL reports whether a reference uses HTTP(S).
func IsChatGPTWebRemoteImageURL(value string) bool {
	parsed, errParse := url.Parse(strings.TrimSpace(value))
	if errParse != nil || parsed == nil {
		return false
	}
	scheme := strings.ToLower(strings.TrimSpace(parsed.Scheme))
	return scheme == "http" || scheme == "https"
}

// ValidateChatGPTWebRemoteImageURL validates URL syntax without resolving it.
func ValidateChatGPTWebRemoteImageURL(value string) error {
	_, errValidate := validateChatGPTWebRemoteImageURL(value)
	return errValidate
}

func newChatGPTWebRemoteImageFetcher(
	resolver chatGPTWebRemoteImageResolver,
	dialContext func(context.Context, string, string) (net.Conn, error),
) *chatGPTWebRemoteImageFetcher {
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	if dialContext == nil {
		dialer := &net.Dialer{Timeout: chatGPTWebRemoteImageDialTimeout, KeepAlive: 30 * time.Second}
		dialContext = dialer.DialContext
	}
	return &chatGPTWebRemoteImageFetcher{resolver: resolver, dialContext: dialContext}
}

func (fetcher *chatGPTWebRemoteImageFetcher) fetch(ctx context.Context, rawURL string, maxBytes int64) (*ChatGPTWebRemoteImageFile, error) {
	if fetcher == nil || fetcher.resolver == nil || fetcher.dialContext == nil || maxBytes < 1 {
		return nil, newChatGPTWebRemoteImageError(ChatGPTWebRemoteImageInvalid, errors.New("invalid fetch configuration"))
	}
	parsed, errValidate := validateChatGPTWebRemoteImageURL(rawURL)
	if errValidate != nil {
		return nil, errValidate
	}

	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           fetcher.protectedDialContext,
		ForceAttemptHTTP2:     true,
		DisableCompression:    true,
		TLSHandshakeTimeout:   chatGPTWebRemoteImageDialTimeout,
		IdleConnTimeout:       30 * time.Second,
		MaxIdleConns:          2,
		MaxIdleConnsPerHost:   1,
		ResponseHeaderTimeout: chatGPTWebRemoteImageTimeout,
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport:     transport,
		Timeout:       chatGPTWebRemoteImageTimeout,
		CheckRedirect: validateChatGPTWebRemoteImageRedirect,
	}
	downloadCtx, cancel := context.WithTimeout(ctx, chatGPTWebRemoteImageTimeout)
	defer cancel()
	request, errRequest := http.NewRequestWithContext(downloadCtx, http.MethodGet, parsed.String(), nil)
	if errRequest != nil {
		return nil, newChatGPTWebRemoteImageError(ChatGPTWebRemoteImageInvalid, errors.New("create remote image request"))
	}
	request.Header.Set("Accept", "image/png,image/jpeg,image/gif,image/webp,application/octet-stream;q=0.8")
	request.Header.Set("Accept-Encoding", "identity")
	request.Header.Set("User-Agent", "CLIProxyAPI-Remote-Image/1")
	response, errDo := client.Do(request)
	if errDo != nil {
		var protectedErr *ChatGPTWebRemoteImageError
		if errors.As(errDo, &protectedErr) {
			return nil, protectedErr
		}
		if errContext := downloadCtx.Err(); errContext != nil {
			return nil, newChatGPTWebRemoteImageError(ChatGPTWebRemoteImageFetch, errContext)
		}
		return nil, newChatGPTWebRemoteImageError(ChatGPTWebRemoteImageFetch, errors.New("remote image request failed"))
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, newChatGPTWebRemoteImageError(ChatGPTWebRemoteImageFetch, errors.New("remote status is not successful"))
	}
	if encoding := strings.TrimSpace(response.Header.Get("Content-Encoding")); encoding != "" && !strings.EqualFold(encoding, "identity") {
		return nil, newChatGPTWebRemoteImageError(ChatGPTWebRemoteImageInvalid, errors.New("encoded response is not allowed"))
	}
	if response.ContentLength > maxBytes {
		return nil, newChatGPTWebRemoteImageError(ChatGPTWebRemoteImageTooLarge, errors.New("content length exceeds limit"))
	}

	temporary, errCreate := os.CreateTemp("", "cliproxy-chatgpt-web-remote-image-*")
	if errCreate != nil {
		return nil, newChatGPTWebRemoteImageError(ChatGPTWebRemoteImageFetch, errors.New("create remote image spool"))
	}
	spooled := &ChatGPTWebRemoteImageFile{
		file:                temporary,
		path:                temporary.Name(),
		tracker:             BeginChatGPTWebImageSpool(),
		DeclaredContentType: response.Header.Get("Content-Type"),
	}
	keep := false
	defer func() {
		if !keep {
			_ = spooled.Remove()
		}
	}()
	copied, errCopy := io.Copy(temporary, io.LimitReader(response.Body, maxBytes+1))
	spooled.tracker.SetBytes(copied)
	if errCopy != nil {
		return nil, newChatGPTWebRemoteImageError(ChatGPTWebRemoteImageFetch, errors.New("copy remote image body"))
	}
	if copied > maxBytes {
		return nil, newChatGPTWebRemoteImageError(ChatGPTWebRemoteImageTooLarge, errors.New("remote body exceeds limit"))
	}
	if copied == 0 {
		return nil, newChatGPTWebRemoteImageError(ChatGPTWebRemoteImageInvalid, errors.New("remote image is empty"))
	}
	spooled.Size = copied
	keep = true
	return spooled, nil
}

func validateChatGPTWebRemoteImageRedirect(request *http.Request, via []*http.Request) error {
	if request == nil || request.URL == nil {
		return newChatGPTWebRemoteImageError(ChatGPTWebRemoteImageInvalid, errors.New("redirect URL is unavailable"))
	}
	if len(via) > chatGPTWebRemoteImageMaxRedirects {
		return newChatGPTWebRemoteImageError(ChatGPTWebRemoteImageBlocked, errors.New("too many redirects"))
	}
	if _, errURL := validateChatGPTWebRemoteImageURL(request.URL.String()); errURL != nil {
		return errURL
	}
	if len(via) > 0 && strings.EqualFold(via[len(via)-1].URL.Scheme, "https") && !strings.EqualFold(request.URL.Scheme, "https") {
		return newChatGPTWebRemoteImageError(ChatGPTWebRemoteImageBlocked, errors.New("HTTPS downgrade"))
	}
	request.Header.Del("Authorization")
	request.Header.Del("Cookie")
	request.Header.Del("Referer")
	return nil
}

func validateChatGPTWebRemoteImageURL(rawURL string) (*url.URL, error) {
	rawURL = strings.TrimSpace(rawURL)
	parsed, errParse := url.ParseRequestURI(rawURL)
	if errParse != nil || parsed == nil || !parsed.IsAbs() {
		return nil, newChatGPTWebRemoteImageError(ChatGPTWebRemoteImageInvalid, errors.New("URL syntax is invalid"))
	}
	scheme := strings.ToLower(strings.TrimSpace(parsed.Scheme))
	if scheme != "http" && scheme != "https" {
		return nil, newChatGPTWebRemoteImageError(ChatGPTWebRemoteImageInvalid, errors.New("unsupported scheme"))
	}
	if parsed.User != nil || parsed.Fragment != "" {
		return nil, newChatGPTWebRemoteImageError(ChatGPTWebRemoteImageInvalid, errors.New("URL credentials or fragments are not allowed"))
	}
	host := strings.TrimSpace(parsed.Hostname())
	if host == "" || strings.Contains(host, "%") {
		return nil, newChatGPTWebRemoteImageError(ChatGPTWebRemoteImageInvalid, errors.New("invalid host"))
	}
	portText := parsed.Port()
	if portText == "" {
		if scheme == "https" {
			portText = "443"
		} else {
			portText = "80"
		}
	}
	port, errPort := strconv.Atoi(portText)
	if errPort != nil || port < 1 || port > 65535 {
		return nil, newChatGPTWebRemoteImageError(ChatGPTWebRemoteImageInvalid, errors.New("invalid port"))
	}
	return parsed, nil
}

func (fetcher *chatGPTWebRemoteImageFetcher) protectedDialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, errSplit := net.SplitHostPort(address)
	if errSplit != nil {
		return nil, newChatGPTWebRemoteImageError(ChatGPTWebRemoteImageInvalid, errors.New("dial address is invalid"))
	}
	if parsedIP := net.ParseIP(host); parsedIP != nil {
		addressIP, ok := netip.AddrFromSlice(parsedIP)
		if !ok || !chatGPTWebRemoteImagePublicIP(addressIP) {
			return nil, newChatGPTWebRemoteImageError(ChatGPTWebRemoteImageBlocked, errors.New("non-public IP"))
		}
		return fetcher.dialContext(ctx, network, net.JoinHostPort(addressIP.Unmap().String(), port))
	}
	resolved, errResolve := fetcher.resolver.LookupIPAddr(ctx, host)
	if errResolve != nil || len(resolved) == 0 {
		return nil, newChatGPTWebRemoteImageError(ChatGPTWebRemoteImageFetch, errors.New("DNS resolution failed"))
	}
	candidates := make([]netip.Addr, 0, len(resolved))
	for _, candidate := range resolved {
		addressIP, ok := netip.AddrFromSlice(candidate.IP)
		if !ok || !chatGPTWebRemoteImagePublicIP(addressIP) {
			return nil, newChatGPTWebRemoteImageError(ChatGPTWebRemoteImageBlocked, errors.New("DNS returned a non-public IP"))
		}
		addressIP = addressIP.Unmap()
		if network == "tcp4" && !addressIP.Is4() || network == "tcp6" && !addressIP.Is6() {
			continue
		}
		candidates = append(candidates, addressIP)
	}
	var errLast error
	for _, candidate := range candidates {
		connection, errDial := fetcher.dialContext(ctx, network, net.JoinHostPort(candidate.String(), port))
		if errDial == nil {
			return connection, nil
		}
		errLast = errDial
	}
	if errLast == nil {
		errLast = errors.New("DNS returned no usable public IP")
	} else {
		errLast = errors.New("public target connection failed")
	}
	return nil, newChatGPTWebRemoteImageError(ChatGPTWebRemoteImageFetch, errLast)
}

func chatGPTWebRemoteImagePublicIP(address netip.Addr) bool {
	if !address.IsValid() {
		return false
	}
	address = address.Unmap()
	if !address.IsGlobalUnicast() || address.IsPrivate() || address.IsLoopback() || address.IsLinkLocalUnicast() ||
		address.IsLinkLocalMulticast() || address.IsMulticast() || address.IsUnspecified() {
		return false
	}
	for _, prefix := range chatGPTWebRemoteImageBlockedPrefixes {
		if prefix.Contains(address) {
			return false
		}
	}
	return true
}

var chatGPTWebRemoteImageBlockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.31.196.0/24"),
	netip.MustParsePrefix("192.52.193.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("192.175.48.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("255.255.255.255/32"),
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("2001::/32"),
	netip.MustParsePrefix("2001:2::/48"),
	netip.MustParsePrefix("2001:3::/32"),
	netip.MustParsePrefix("2001:4:112::/48"),
	netip.MustParsePrefix("2001:10::/28"),
	netip.MustParsePrefix("2001:20::/28"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("3fff::/20"),
	netip.MustParsePrefix("5f00::/16"),
	netip.MustParsePrefix("fec0::/10"),
}

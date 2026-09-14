package helps

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"syscall"
	"time"
)

func ChatGPTWebBootstrapRetryable(err error) bool {
	if err == nil || (errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)) {
		return false
	}
	var unknown x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalid x509.CertificateInvalidError
	if errors.As(err, &unknown) || errors.As(err, &hostname) || errors.As(err, &invalid) {
		return false
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return dns.IsTimeout || dns.IsTemporary
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, net.ErrClosed) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	var network net.Error
	return errors.As(err, &network) && (network.Timeout() || network.Temporary())
}

func ChatGPTWebBootstrapRetryDelay(retry int) time.Duration {
	return min(200*time.Millisecond<<min(max(retry-1, 0), 4), 2*time.Second)
}

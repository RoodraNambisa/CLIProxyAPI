package helps

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
	"time"
)

func TestChatGPTWebBootstrapRetryClassifier(t *testing.T) {
	if ChatGPTWebBootstrapRetryable(errors.Join(context.DeadlineExceeded, x509.UnknownAuthorityError{})) {
		t.Fatal("deadline must not make a certificate error retryable")
	}
	if !ChatGPTWebBootstrapRetryable(errors.Join(context.DeadlineExceeded, context.Canceled)) {
		t.Fatal("a canceled transport must retain the attempt deadline classification")
	}
	for _, err := range []error{io.EOF, io.ErrUnexpectedEOF, syscall.ECONNRESET, syscall.ECONNREFUSED, context.DeadlineExceeded, &net.DNSError{IsTemporary: true}} {
		if !ChatGPTWebBootstrapRetryable(fmt.Errorf("wrapped: %w", err)) {
			t.Errorf("not retryable: %v", err)
		}
	}
	for _, err := range []error{context.Canceled, x509.UnknownAuthorityError{}, x509.HostnameError{}, x509.CertificateInvalidError{}, &net.DNSError{IsNotFound: true}, errors.New("response too large"), errors.New("bad HTML")} {
		if ChatGPTWebBootstrapRetryable(fmt.Errorf("wrapped: %w", err)) {
			t.Errorf("retryable: %v", err)
		}
	}
	if ChatGPTWebBootstrapRetryDelay(1) != 200*time.Millisecond || ChatGPTWebBootstrapRetryDelay(5) != 2*time.Second {
		t.Fatal("backoff changed")
	}
}

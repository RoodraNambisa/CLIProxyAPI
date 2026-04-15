package managementasset

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDownloadAssetReturnsSHA256ForValidPayload(t *testing.T) {
	payload := []byte("<html><body>panel</body></html>")
	expectedSum := sha256.Sum256(payload)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	t.Cleanup(server.Close)

	data, digest, err := downloadAsset(context.Background(), server.Client(), server.URL)
	if err != nil {
		t.Fatalf("downloadAsset: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("payload = %q, want %q", string(data), string(payload))
	}
	if digest != hex.EncodeToString(expectedSum[:]) {
		t.Fatalf("digest = %q, want %q", digest, hex.EncodeToString(expectedSum[:]))
	}
}

func TestDownloadAssetRejectsOversizedPayload(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), maxAssetDownloadSize+1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	t.Cleanup(server.Close)

	_, _, err := downloadAsset(context.Background(), server.Client(), server.URL)
	if err == nil {
		t.Fatal("expected oversized payload error")
	}
	if !strings.Contains(err.Error(), "exceeds maximum allowed size") {
		t.Fatalf("error = %v, want size limit error", err)
	}
}

func TestDownloadAssetRejectsOversizedContentLengthBeforeRead(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "10485761")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("tiny"))
	}))
	t.Cleanup(server.Close)

	_, _, err := downloadAsset(context.Background(), server.Client(), server.URL)
	if err == nil {
		t.Fatal("expected content-length size limit error")
	}
	if !strings.Contains(err.Error(), "exceeds maximum allowed size") {
		t.Fatalf("error = %v, want size limit error", err)
	}
}

func TestEnsureFallbackManagementHTMLReturnsFalseWhenFallbackDisabled(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, ManagementFileName)
	ok := ensureFallbackManagementHTML(context.Background(), http.DefaultClient, localPath)
	if ok {
		t.Fatal("expected fallback sync to be disabled")
	}
	if _, err := os.Stat(localPath); !os.IsNotExist(err) {
		t.Fatalf("local fallback file should not be created, stat err = %v", err)
	}
}

package managementasset

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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

func TestCheckManagementHTMLStatusDetectsUpdate(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, ManagementFileName)
	if err := os.WriteFile(localPath, []byte("<html>old</html>"), 0o600); err != nil {
		t.Fatalf("write local asset: %v", err)
	}
	remotePayload := []byte("<html>new</html>")
	remoteSum := sha256.Sum256(remotePayload)
	remoteHash := hex.EncodeToString(remoteSum[:])

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/download":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(remotePayload)
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"assets":[{"name":"%s","browser_download_url":"%s/download","digest":"sha256:%s"}]}`, ManagementFileName, server.URL, remoteHash)
		}
	}))
	t.Cleanup(server.Close)

	status := CheckManagementHTMLStatus(context.Background(), dir, "", server.URL+"/repos/test/panel/releases/latest")
	if !status.LocalExists {
		t.Fatal("expected local asset to exist")
	}
	if status.RemoteHash != remoteHash {
		t.Fatalf("remote hash = %q, want %q", status.RemoteHash, remoteHash)
	}
	if !status.UpdateAvailable {
		t.Fatalf("expected update to be available, status=%+v", status)
	}
	if status.AssetURL != server.URL+"/download" {
		t.Fatalf("asset url = %q, want download url", status.AssetURL)
	}
}

func TestCheckManagementHTMLStatusRemoteErrorTakesPriorityOverLocalHashError(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, ManagementFileName)
	if err := os.Mkdir(localPath, 0o700); err != nil {
		t.Fatalf("create invalid local asset directory: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "release unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)

	status := CheckManagementHTMLStatus(context.Background(), dir, "", server.URL+"/repos/test/panel/releases/latest")
	if status.Error == "" {
		t.Fatal("expected status error")
	}
	if !strings.Contains(status.Error, "unexpected release status 503") {
		t.Fatalf("status error = %q, want remote release status", status.Error)
	}
	if !strings.Contains(status.Error, "local status error") {
		t.Fatalf("status error = %q, want local status detail", status.Error)
	}
}

func TestUpdateManagementHTMLForceDownloadsLatest(t *testing.T) {
	dir := t.TempDir()
	remotePayload := []byte("<html>updated</html>")
	remoteSum := sha256.Sum256(remotePayload)
	remoteHash := hex.EncodeToString(remoteSum[:])

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/download":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(remotePayload)
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"assets":[{"name":"%s","browser_download_url":"%s/download","digest":"sha256:%s"}]}`, ManagementFileName, server.URL, remoteHash)
		}
	}))
	t.Cleanup(server.Close)

	status := UpdateManagementHTML(context.Background(), dir, "", server.URL+"/repos/test/panel/releases/latest", true)
	if status.Error != "" {
		t.Fatalf("unexpected update error: %s", status.Error)
	}
	if !status.Updated {
		t.Fatalf("expected update flag, status=%+v", status)
	}
	if !status.LocalExists {
		t.Fatal("expected local asset to exist after update")
	}
	data, err := os.ReadFile(filepath.Join(dir, ManagementFileName))
	if err != nil {
		t.Fatalf("read local asset: %v", err)
	}
	if !bytes.Equal(data, remotePayload) {
		t.Fatalf("local asset = %q, want %q", string(data), string(remotePayload))
	}
}

func TestUpdateManagementHTMLContinuesWhenLocalHashFails(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, ManagementFileName)
	if err := os.WriteFile(localPath, []byte("<html>old</html>"), 0o600); err != nil {
		t.Fatalf("write local asset: %v", err)
	}
	if err := os.Chmod(localPath, 0o000); err != nil {
		t.Skipf("chmod local asset unreadable: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(localPath, 0o600)
	})
	if _, err := fileSHA256(localPath); err == nil {
		t.Skip("local permissions allow reading the unreadable test asset")
	}

	remotePayload := []byte("<html>recovered</html>")
	remoteSum := sha256.Sum256(remotePayload)
	remoteHash := hex.EncodeToString(remoteSum[:])

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/download":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(remotePayload)
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"assets":[{"name":"%s","browser_download_url":"%s/download","digest":"sha256:%s"}]}`, ManagementFileName, server.URL, remoteHash)
		}
	}))
	t.Cleanup(server.Close)

	status := UpdateManagementHTML(context.Background(), dir, "", server.URL+"/repos/test/panel/releases/latest", true)
	if status.Error != "" {
		t.Fatalf("unexpected update error: %s", status.Error)
	}
	if !status.Updated {
		t.Fatalf("expected update flag, status=%+v", status)
	}
	data, err := os.ReadFile(localPath)
	if err != nil {
		t.Fatalf("read local asset: %v", err)
	}
	if !bytes.Equal(data, remotePayload) {
		t.Fatalf("local asset = %q, want %q", string(data), string(remotePayload))
	}
}

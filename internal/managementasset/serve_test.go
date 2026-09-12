package managementasset

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/andybalholm/brotli"
)

func panelRequest(server *FileServer, path, method string, headers map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "/management.html", nil)
	for key, value := range headers {
		r.Header.Set(key, value)
	}
	w := httptest.NewRecorder()
	server.ServeFile(w, r, path)
	return w
}

func TestPanelCompressionAndValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), ManagementFileName)
	payload := bytes.Repeat([]byte("<p>management settings</p>"), 4096)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	server := &FileServer{}
	etags := map[string]bool{}
	for _, encoding := range []string{"identity", "gzip", "br"} {
		t.Run(encoding, func(t *testing.T) {
			headers := map[string]string{"Accept-Encoding": encoding}
			w := panelRequest(server, path, "GET", headers)
			if w.Code != 200 || w.Header().Get("Vary") != "Accept-Encoding" || w.Header().Get("Cache-Control") != "no-cache" {
				t.Fatalf("unexpected response: %d %v", w.Code, w.Header())
			}
			var reader io.Reader = w.Body
			switch encoding {
			case "gzip":
				gz, err := gzip.NewReader(w.Body)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := gz.Close(); err != nil {
						t.Error(err)
					}
				}()
				reader = gz
			case "br":
				reader = brotli.NewReader(w.Body)
			}
			body, err := io.ReadAll(reader)
			if err != nil || !bytes.Equal(body, payload) {
				t.Fatalf("invalid decoded body: %v", err)
			}
			etag := w.Header().Get("ETag")
			if etag == "" || etags[etag] {
				t.Fatal("missing or shared representation ETag")
			}
			etags[etag] = true
			head := panelRequest(server, path, "HEAD", headers)
			if head.Code != 200 || head.Body.Len() != 0 || head.Header().Get("Content-Length") != w.Header().Get("Content-Length") {
				t.Fatal("HEAD did not preserve representation metadata")
			}
			headers["If-None-Match"] = "W/" + etag
			cached := panelRequest(server, path, "GET", headers)
			if cached.Code != 304 || cached.Body.Len() != 0 || cached.Header().Get("ETag") != etag || cached.Header().Get("Vary") != "Accept-Encoding" {
				t.Fatalf("invalid conditional response: %d %v", cached.Code, cached.Header())
			}
		})
	}
}

func TestPanelEncodingNegotiationAndRanges(t *testing.T) {
	path := filepath.Join(t.TempDir(), ManagementFileName)
	if err := os.WriteFile(path, []byte("0123456789"), 0o600); err != nil {
		t.Fatal(err)
	}
	server := &FileServer{}
	for _, tt := range []struct {
		accept, rangeHeader, encoding string
		status                        int
	}{
		{"", "", "", 200}, {"deflate", "", "", 200},
		{"gzip, br", "", "br", 200}, {"GZIP;q=0.8, br;q=0.1", "", "gzip", 200},
		{"gzip;q=0, br;q=0", "", "", 200}, {"gzip;q=0, *;q=1", "", "br", 200},
		{"*;q=0", "", "", 406}, {"identity;q=0,gzip;q=0.1", "", "gzip", 200},
		{"identity;q=1,gzip;q=0.1", "", "", 200}, {"gzip;q=NaN,br;q=2", "", "", 200},
		{"gzip;q=invalid,br;q=-1", "", "", 200}, {"gzip", "bytes=2-5", "", 206},
		{"identity;q=0,gzip", "bytes=0-1,4-5", "gzip", 200},
	} {
		t.Run(tt.accept+tt.rangeHeader, func(t *testing.T) {
			w := panelRequest(server, path, "GET", map[string]string{"Accept-Encoding": tt.accept, "Range": tt.rangeHeader})
			if w.Code != tt.status || w.Header().Get("Content-Encoding") != tt.encoding {
				t.Fatalf("response = %d %v", w.Code, w.Header())
			}
			if tt.status == 206 && w.Body.String() != "2345" {
				t.Fatal("range changed")
			}
		})
	}
}

func TestPanelReplacementInvalidatesEvenWithSameSizeAndTimestamp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ManagementFileName)
	stamp := time.Now().Add(-time.Minute).Truncate(time.Second)
	replace := func(text string) {
		t.Helper()
		tmp := filepath.Join(dir, "replacement")
		if err := os.WriteFile(tmp, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(tmp, stamp, stamp); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, path); err != nil {
			t.Fatal(err)
		}
	}
	replace("old version")
	server := &FileServer{}
	headers := map[string]string{"Accept-Encoding": "br"}
	before := panelRequest(server, path, "GET", headers)
	headers["If-None-Match"] = before.Header().Get("ETag")
	replace("new version")
	after := panelRequest(server, path, "GET", headers)
	decoded, err := io.ReadAll(brotli.NewReader(after.Body))
	if err != nil || after.Code != 200 || string(decoded) != "new version" || after.Header().Get("ETag") == headers["If-None-Match"] {
		t.Fatalf("updated panel was not delivered: %d %v", after.Code, err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if gone := panelRequest(server, path, "GET", headers); gone.Code != 404 {
		t.Fatal("deleted asset was served from memory")
	}
}

func TestPanelCacheBoundAndPathChanges(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ManagementFileName)
	if err := os.WriteFile(path, []byte("one"), 0o600); err != nil {
		t.Fatal(err)
	}
	server := &FileServer{}
	panelRequest(server, path, "GET", nil)
	other := filepath.Join(dir, "other.html")
	if err := os.WriteFile(other, []byte("two"), 0o600); err != nil {
		t.Fatal(err)
	}
	if w := panelRequest(server, other, "GET", nil); w.Body.String() != "two" {
		t.Fatal("old path was retained")
	}
	if err := os.WriteFile(other, bytes.Repeat([]byte("x"), maxAssetDownloadSize+1), 0o600); err != nil {
		t.Fatal(err)
	}
	w := panelRequest(server, other, "GET", map[string]string{"Accept-Encoding": "gzip"})
	if w.Code != 200 || w.Body.Len() != maxAssetDownloadSize+1 || w.Header().Get("Content-Encoding") != "" || server.asset != nil {
		t.Fatal("oversized local asset was cached or truncated")
	}
	if w := panelRequest(server, dir, "GET", nil); w.Code != 404 {
		t.Fatal("directory was served")
	}
}

func TestPanelConcurrentReadsAndAtomicUpdates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ManagementFileName)
	if err := os.WriteFile(path, []byte(strings.Repeat("A", 8192)), 0o600); err != nil {
		t.Fatal(err)
	}
	server := &FileServer{}
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Go(func() {
			for range 20 {
				enc := []string{"gzip", "br"}[i%2]
				w := panelRequest(server, path, "GET", map[string]string{"Accept-Encoding": enc})
				var reader io.Reader = brotli.NewReader(w.Body)
				if enc == "gzip" {
					gz, err := gzip.NewReader(w.Body)
					if err != nil {
						t.Error(err)
						return
					}
					defer func() {
						if err := gz.Close(); err != nil {
							t.Error(err)
						}
					}()
					reader = gz
				}
				data, err := io.ReadAll(reader)
				if err != nil || w.Code != 200 || len(data) != 8192 || !bytes.Equal(data, bytes.Repeat(data[:1], 8192)) {
					t.Errorf("incomplete snapshot: %d %v", w.Code, err)
					return
				}
			}
		})
	}
	for i := range 10 {
		tmp := filepath.Join(dir, fmt.Sprintf("update-%d", i))
		if err := os.WriteFile(tmp, bytes.Repeat([]byte{byte('B' + i)}, 8192), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, path); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
}

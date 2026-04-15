package management

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func unzipAuthArchive(t *testing.T, data []byte) map[string]string {
	t.Helper()

	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("zip.NewReader: %v", err)
	}

	files := make(map[string]string, len(reader.File))
	for _, file := range reader.File {
		rc, errOpen := file.Open()
		if errOpen != nil {
			t.Fatalf("open zip entry %s: %v", file.Name, errOpen)
		}
		content, errRead := io.ReadAll(rc)
		if errRead != nil {
			_ = rc.Close()
			t.Fatalf("read zip entry %s: %v", file.Name, errRead)
		}
		if errClose := rc.Close(); errClose != nil {
			t.Fatalf("close zip entry %s: %v", file.Name, errClose)
		}
		files[file.Name] = string(content)
	}
	return files
}

func TestDownloadAuthFile_ReturnsFile(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	fileName := "download-user.json"
	expected := []byte(`{"type":"codex"}`)
	if err := os.WriteFile(filepath.Join(authDir, fileName), expected, 0o600); err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, nil)

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files/download?name="+url.QueryEscape(fileName), nil)
	h.DownloadAuthFile(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected download status %d, got %d with body %s", http.StatusOK, rec.Code, rec.Body.String())
	}
	if got := rec.Body.Bytes(); string(got) != string(expected) {
		t.Fatalf("unexpected download content: %q", string(got))
	}
}

func TestDownloadAuthFilesArchive_ReturnsSelectedFiles(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	files := map[string]string{
		"alpha.json": `{"type":"codex","email":"alpha@example.com"}`,
		"beta.json":  `{"type":"claude","email":"beta@example.com"}`,
		"gamma.json": `{"type":"gemini","email":"gamma@example.com"}`,
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(authDir, name), []byte(content), 0o600); err != nil {
			t.Fatalf("write auth file %s: %v", name, err)
		}
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, nil)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)

	body := bytes.NewBufferString(`{"names":["beta.json","alpha.json","beta.json"]}`)
	req := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files/archive", body)
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	h.DownloadAuthFilesArchive(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected archive status %d, got %d with body %s", http.StatusOK, rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/zip" {
		t.Fatalf("Content-Type = %q, want %q", got, "application/zip")
	}

	gotFiles := unzipAuthArchive(t, rec.Body.Bytes())
	if len(gotFiles) != 2 {
		t.Fatalf("expected 2 files in archive, got %d", len(gotFiles))
	}
	if gotFiles["alpha.json"] != files["alpha.json"] {
		t.Fatalf("alpha.json content mismatch: %q", gotFiles["alpha.json"])
	}
	if gotFiles["beta.json"] != files["beta.json"] {
		t.Fatalf("beta.json content mismatch: %q", gotFiles["beta.json"])
	}
	if _, exists := gotFiles["gamma.json"]; exists {
		t.Fatalf("did not expect gamma.json in selected archive")
	}
}

func TestDownloadAuthFilesArchive_ReturnsAllFiles(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	files := map[string]string{
		"alpha.json": `{"type":"codex"}`,
		"beta.json":  `{"type":"claude"}`,
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(authDir, name), []byte(content), 0o600); err != nil {
			t.Fatalf("write auth file %s: %v", name, err)
		}
	}
	if err := os.WriteFile(filepath.Join(authDir, "notes.txt"), []byte("ignore"), 0o600); err != nil {
		t.Fatalf("write notes.txt: %v", err)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, nil)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)

	req := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files/archive", bytes.NewBufferString(`{"all":true}`))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	h.DownloadAuthFilesArchive(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected archive status %d, got %d with body %s", http.StatusOK, rec.Code, rec.Body.String())
	}
	gotFiles := unzipAuthArchive(t, rec.Body.Bytes())
	if len(gotFiles) != len(files) {
		t.Fatalf("expected %d files in archive, got %d", len(files), len(gotFiles))
	}
	for name, content := range files {
		if gotFiles[name] != content {
			t.Fatalf("%s content mismatch: %q", name, gotFiles[name])
		}
	}
	if _, exists := gotFiles["notes.txt"]; exists {
		t.Fatalf("did not expect non-json file in archive")
	}
}

func TestDownloadAuthFilesArchive_RejectsInvalidPayload(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, nil)

	for _, body := range []string{
		`{}`,
		`{"all":true,"names":["alpha.json"]}`,
		`{"names":["../secret.json"]}`,
		`{"names":["alpha.txt"]}`,
	} {
		rec := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(rec)
		req := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files/archive", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		ctx.Request = req

		h.DownloadAuthFilesArchive(ctx)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("body %s: expected status %d, got %d with body %s", body, http.StatusBadRequest, rec.Code, rec.Body.String())
		}
	}
}

func TestDownloadAuthFilesArchive_ReturnsNotFoundWhenRequestedFileMissing(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(authDir, "alpha.json"), []byte(`{"type":"codex"}`), 0o600); err != nil {
		t.Fatalf("write alpha.json: %v", err)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, nil)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)

	reqBody, err := json.Marshal(map[string]any{"names": []string{"alpha.json", "missing.json"}})
	if err != nil {
		t.Fatalf("marshal request body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files/archive", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	h.DownloadAuthFilesArchive(ctx)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d with body %s", http.StatusNotFound, rec.Code, rec.Body.String())
	}
}

func TestDownloadAuthFile_RejectsPathSeparators(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, nil)

	for _, name := range []string{
		"../external/secret.json",
		`..\\external\\secret.json`,
		"nested/secret.json",
		`nested\\secret.json`,
	} {
		rec := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(rec)
		ctx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files/download?name="+url.QueryEscape(name), nil)
		h.DownloadAuthFile(ctx)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected %d for name %q, got %d with body %s", http.StatusBadRequest, name, rec.Code, rec.Body.String())
		}
	}
}

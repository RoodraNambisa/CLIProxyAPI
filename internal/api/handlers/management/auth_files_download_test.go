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
	"runtime"
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

func TestManagedAuthNameKeyForOS(t *testing.T) {
	if got := managedAuthNameKeyForOS("Foo.JSON", "windows"); got != "foo.json" {
		t.Fatalf("Windows key = %q, want foo.json", got)
	}
	if got := managedAuthNameKeyForOS("Foo.JSON", "linux"); got != "Foo.JSON" {
		t.Fatalf("Linux key = %q, want Foo.JSON", got)
	}
}

func TestNormalizeManagedAuthFileNameRejectsWindowsVolumeShapes(t *testing.T) {
	for _, name := range []string{`C:\auth.json`, `C:auth.json`, `\auth.json`, `\\server\share\auth.json`, `/auth.json`} {
		if normalized, errNormalize := normalizeManagedAuthFileName(name); errNormalize == nil {
			t.Fatalf("normalizeManagedAuthFileName(%q) = %q, want error", name, normalized)
		}
	}
	if normalized, errNormalize := normalizeManagedAuthFileName(`team\legacy.json`); runtime.GOOS != "windows" && (errNormalize != nil || normalized != `team\legacy.json`) {
		t.Fatalf("Unix literal backslash name = %q, error=%v", normalized, errNormalize)
	}
}

func TestDownloadAuthFile_ReturnsFile(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

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

func TestDownloadAuthFile_ReportsAuthDirectoryFailureAsServerError(t *testing.T) {
	authDirFile := filepath.Join(t.TempDir(), "not-a-directory")
	if errWrite := os.WriteFile(authDirFile, []byte("x"), 0o600); errWrite != nil {
		t.Fatalf("write auth-dir placeholder: %v", errWrite)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDirFile}, nil)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files/download?name=auth.json", nil)

	h.DownloadAuthFile(ctx)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusInternalServerError, recorder.Body.String())
	}
}

func TestDownloadAuthFile_MissingAuthDirectoryReturnsNotFound(t *testing.T) {
	authDir := filepath.Join(t.TempDir(), "missing")
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, nil)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files/download?name=auth.json", nil)
	h.DownloadAuthFile(ctx)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusNotFound, recorder.Body.String())
	}
}

func TestDownloadAuthFilesArchive_ReturnsSelectedFiles(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

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

	body := bytes.NewBufferString(`{"names":["beta.json","alpha.json","nested/../alpha.json"]}`)
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
	nestedDir := filepath.Join(authDir, "nested")
	if errMkdir := os.Mkdir(nestedDir, 0o700); errMkdir != nil {
		t.Fatalf("create nested directory: %v", errMkdir)
	}
	if errWrite := os.WriteFile(filepath.Join(nestedDir, "sidecar.json"), []byte(`{"not":"an auth file"}`), 0o600); errWrite != nil {
		t.Fatalf("write nested sidecar: %v", errWrite)
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
	if _, exists := gotFiles["nested/sidecar.json"]; exists {
		t.Fatal("did not expect nested JSON file in archive")
	}
}

func TestDownloadAuthFilesArchive_RejectsInvalidPayload(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

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

func TestDownloadAuthFilesArchive_ReturnsNotFoundWhenAuthDirectoryMissing(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	authDir := filepath.Join(t.TempDir(), "missing")
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, nil)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	for _, body := range []string{`{"names":["missing.json"]}`, `{"all":true}`} {
		recorder = httptest.NewRecorder()
		ctx, _ = gin.CreateTestContext(recorder)
		request := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files/archive", bytes.NewBufferString(body))
		request.Header.Set("Content-Type", "application/json")
		ctx.Request = request

		h.DownloadAuthFilesArchive(ctx)

		if recorder.Code != http.StatusNotFound {
			t.Fatalf("body %s: status = %d, want %d; response=%s", body, recorder.Code, http.StatusNotFound, recorder.Body.String())
		}
	}
}

func TestDownloadAuthFile_RejectsPathTraversal(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, nil)

	for _, name := range []string{
		"../external/secret.json",
		`..\\external\\secret.json`,
		"/external/secret.json",
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

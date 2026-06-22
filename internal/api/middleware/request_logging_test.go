package middleware

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestShouldSkipMethodForRequestLogging(t *testing.T) {
	tests := []struct {
		name string
		req  *http.Request
		skip bool
	}{
		{
			name: "nil request",
			req:  nil,
			skip: true,
		},
		{
			name: "post request should not skip",
			req: &http.Request{
				Method: http.MethodPost,
				URL:    &url.URL{Path: "/v1/responses"},
			},
			skip: false,
		},
		{
			name: "plain get should skip",
			req: &http.Request{
				Method: http.MethodGet,
				URL:    &url.URL{Path: "/v1/models"},
				Header: http.Header{},
			},
			skip: true,
		},
		{
			name: "responses websocket upgrade should not skip",
			req: &http.Request{
				Method: http.MethodGet,
				URL:    &url.URL{Path: "/v1/responses"},
				Header: http.Header{"Upgrade": []string{"websocket"}},
			},
			skip: false,
		},
		{
			name: "responses get without upgrade should skip",
			req: &http.Request{
				Method: http.MethodGet,
				URL:    &url.URL{Path: "/v1/responses"},
				Header: http.Header{},
			},
			skip: true,
		},
	}

	for i := range tests {
		got := shouldSkipMethodForRequestLogging(tests[i].req)
		if got != tests[i].skip {
			t.Fatalf("%s: got skip=%t, want %t", tests[i].name, got, tests[i].skip)
		}
	}
}

func TestShouldLogRequestSkipsManagementPaths(t *testing.T) {
	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "legacy management config", path: "/v0/management/config", want: false},
		{name: "prefixed management root", path: "/secret-token/v0/management", want: false},
		{name: "prefixed management config", path: "/secret-token/v0/management/config", want: false},
		{name: "prefixed management oauth callback", path: "/secret-token/v0/management/oauth-callback", want: false},
		{name: "management panel", path: "/management.html", want: false},
		{name: "similar path is not management api", path: "/secret-token/v0/managementevil/config", want: true},
		{name: "normal api path", path: "/v1/chat/completions", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldLogRequest(tt.path); got != tt.want {
				t.Fatalf("shouldLogRequest(%q) = %t, want %t", tt.path, got, tt.want)
			}
		})
	}
}

func TestShouldCaptureRequestBody(t *testing.T) {
	tests := []struct {
		name          string
		loggerEnabled bool
		req           *http.Request
		want          bool
	}{
		{
			name:          "logger enabled always captures",
			loggerEnabled: true,
			req: &http.Request{
				Body:          io.NopCloser(strings.NewReader("{}")),
				ContentLength: -1,
				Header:        http.Header{"Content-Type": []string{"application/json"}},
			},
			want: true,
		},
		{
			name:          "nil request",
			loggerEnabled: false,
			req:           nil,
			want:          false,
		},
		{
			name:          "small known size json in error-only mode",
			loggerEnabled: false,
			req: &http.Request{
				Body:          io.NopCloser(strings.NewReader("{}")),
				ContentLength: 2,
				Header:        http.Header{"Content-Type": []string{"application/json"}},
			},
			want: true,
		},
		{
			name:          "large known size skipped in error-only mode",
			loggerEnabled: false,
			req: &http.Request{
				Body:          io.NopCloser(strings.NewReader("x")),
				ContentLength: maxErrorOnlyCapturedRequestBodyBytes + 1,
				Header:        http.Header{"Content-Type": []string{"application/json"}},
			},
			want: false,
		},
		{
			name:          "unknown size skipped in error-only mode",
			loggerEnabled: false,
			req: &http.Request{
				Body:          io.NopCloser(strings.NewReader("x")),
				ContentLength: -1,
				Header:        http.Header{"Content-Type": []string{"application/json"}},
			},
			want: false,
		},
		{
			name:          "multipart skipped in error-only mode",
			loggerEnabled: false,
			req: &http.Request{
				Body:          io.NopCloser(strings.NewReader("x")),
				ContentLength: 1,
				Header:        http.Header{"Content-Type": []string{"multipart/form-data; boundary=abc"}},
			},
			want: false,
		},
	}

	for i := range tests {
		got := shouldCaptureRequestBody(tests[i].loggerEnabled, tests[i].req)
		if got != tests[i].want {
			t.Fatalf("%s: got %t, want %t", tests[i].name, got, tests[i].want)
		}
	}
}

func TestCaptureRequestInfoRegistersRequestBodyRelease(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"test","input":"large"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Request.ContentLength = int64(len(`{"model":"test","input":"large"}`))

	info, err := captureRequestInfo(c, true, config.RequestBodyReleaseConfig{
		Enable:       true,
		AfterSeconds: 30,
		MinBodyBytes: 1,
	})
	if err != nil {
		t.Fatalf("captureRequestInfo() error = %v", err)
	}
	if !strings.Contains(string(info.Body), `"large"`) {
		t.Fatalf("captured body = %s, want original body", info.Body)
	}
	ctrl := ensureRequestBodyReleaseController(c, config.RequestBodyReleaseConfig{
		Enable:       true,
		AfterSeconds: 30,
		MinBodyBytes: 1,
	}, int64(len(info.Body)))
	if ctrl == nil {
		t.Fatal("release controller = nil")
	}
	ctrl.Release()
	if got := string(info.Body); !strings.Contains(got, "request body released after 30s") {
		t.Fatalf("released body = %q, want placeholder", got)
	}
}

func TestCaptureRequestInfoLogOnlyReleaseUsesLogPlaceholder(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"test","input":"large"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Request.ContentLength = int64(len(`{"model":"test","input":"large"}`))

	info, err := captureRequestInfo(c, true, config.RequestBodyReleaseConfig{
		Enable:       true,
		LogOnly:      true,
		AfterSeconds: 30,
		MinBodyBytes: 1,
	})
	if err != nil {
		t.Fatalf("captureRequestInfo() error = %v", err)
	}
	ctrl := ensureRequestBodyReleaseController(c, config.RequestBodyReleaseConfig{
		Enable:       true,
		LogOnly:      true,
		AfterSeconds: 30,
		MinBodyBytes: 1,
	}, int64(len(info.Body)))
	if ctrl == nil {
		t.Fatal("release controller = nil")
	}
	ctrl.Release()
	if got := string(info.BodyBytes()); !strings.Contains(got, "request body log released after 30s") {
		t.Fatalf("released body = %q, want log placeholder", got)
	}
	if !ctrl.Replayable() {
		t.Fatal("Replayable() = false after log-only release")
	}
}

func TestEnsureRequestBodyReleaseControllerAllowsStreamOnlyRelease(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"test","input":"large"}`))
	c.Request.ContentLength = int64(len(`{"model":"test","input":"large"}`))

	ctrl := ensureRequestBodyReleaseController(c, config.RequestBodyReleaseConfig{
		Enable:       true,
		AfterSeconds: 0,
		MinBodyBytes: 1,
	}, c.Request.ContentLength)
	if ctrl == nil {
		t.Fatal("release controller = nil, want controller for stream-established release")
	}
	if ctrl.Released() {
		t.Fatal("Released() = true before explicit release")
	}
}

func TestCaptureRequestInfoRestoredBodyReleasesAfterHandlerRead(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := `{"model":"test","input":"large"}`
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Request.ContentLength = int64(len(body))

	_, err := captureRequestInfo(c, true, config.RequestBodyReleaseConfig{
		Enable:       true,
		AfterSeconds: 30,
		MinBodyBytes: 1,
	})
	if err != nil {
		t.Fatalf("captureRequestInfo() error = %v", err)
	}
	reader, ok := c.Request.Body.(*cliproxyexecutor.ReleasableReadCloser)
	if !ok {
		t.Fatalf("restored request body type = %T, want *ReleasableReadCloser", c.Request.Body)
	}
	got, err := io.ReadAll(c.Request.Body)
	if err != nil {
		t.Fatalf("ReadAll(restored body) error = %v", err)
	}
	if string(got) != body {
		t.Fatalf("restored body = %q, want %q", got, body)
	}
	if reader.Len() != 0 {
		t.Fatalf("restored body retained %d bytes after EOF, want 0", reader.Len())
	}
}

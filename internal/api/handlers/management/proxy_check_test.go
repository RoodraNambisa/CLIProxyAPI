package management

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestParseCloudflareTrace(t *testing.T) {
	trace := parseCloudflareTrace("ip=1.2.3.4\nloc=US\nhttp=http/2\ntls=TLSv1.3\ncolo=LAX\n")
	if trace["ip"] != "1.2.3.4" {
		t.Fatalf("ip = %q, want 1.2.3.4", trace["ip"])
	}
	if trace["loc"] != "US" {
		t.Fatalf("loc = %q, want US", trace["loc"])
	}
	if trace["http"] != "http/2" {
		t.Fatalf("http = %q, want http/2", trace["http"])
	}
	if trace["tls"] != "TLSv1.3" {
		t.Fatalf("tls = %q, want TLSv1.3", trace["tls"])
	}
	if trace["colo"] != "LAX" {
		t.Fatalf("colo = %q, want LAX", trace["colo"])
	}
}

func TestGetProxyURLCheckUsesConfiguredProxyURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("User-Agent"); got != proxyTraceUserAgent {
			t.Fatalf("user-agent = %q, want %q", got, proxyTraceUserAgent)
		}
		_, _ = w.Write([]byte("ip=1.2.3.4\nloc=US\nhttp=http/1.1\ntls=TLSv1.3\ncolo=LAX\n"))
	}))
	defer server.Close()

	originalURL := proxyTraceURL
	originalTimeout := proxyTraceTimeout
	proxyTraceURL = server.URL
	proxyTraceTimeout = time.Second
	t.Cleanup(func() {
		proxyTraceURL = originalURL
		proxyTraceTimeout = originalTimeout
	})

	h := &Handler{cfg: &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}}}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodGet, "/v0/management/proxy-url/check", nil)

	h.GetProxyURLCheck(c)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var resp proxyCheckResponse
	if errDecode := json.Unmarshal(rec.Body.Bytes(), &resp); errDecode != nil {
		t.Fatalf("decode response: %v", errDecode)
	}
	if !resp.OK {
		t.Fatalf("ok = false; response=%+v", resp)
	}
	if resp.Mode != "direct" {
		t.Fatalf("mode = %q, want direct", resp.Mode)
	}
	if resp.IP != "1.2.3.4" || resp.Location != "US" {
		t.Fatalf("response = %+v, want ip/loc", resp)
	}
}

func TestPostProxyURLCheckDoesNotPersistProxyURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ip=5.6.7.8\nloc=JP\n"))
	}))
	defer server.Close()

	originalURL := proxyTraceURL
	originalTimeout := proxyTraceTimeout
	proxyTraceURL = server.URL
	proxyTraceTimeout = time.Second
	t.Cleanup(func() {
		proxyTraceURL = originalURL
		proxyTraceTimeout = originalTimeout
	})

	h := &Handler{cfg: &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://saved-proxy.example.com:8080"}}}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v0/management/proxy-url/check", bytes.NewBufferString(`{"proxy-url":"direct"}`))
	c.Request.Header.Set("Content-Type", "application/json")

	h.PostProxyURLCheck(c)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if h.cfg.ProxyURL != "http://saved-proxy.example.com:8080" {
		t.Fatalf("proxy-url persisted = %q, want original", h.cfg.ProxyURL)
	}
	var resp proxyCheckResponse
	if errDecode := json.Unmarshal(rec.Body.Bytes(), &resp); errDecode != nil {
		t.Fatalf("decode response: %v", errDecode)
	}
	if resp.ProxyURL != "direct" || resp.IP != "5.6.7.8" || resp.Location != "JP" {
		t.Fatalf("response = %+v, want temporary direct proxy result", resp)
	}
}

func TestCheckProxyURLInvalidProxyReturnsDiagnostic(t *testing.T) {
	resp := checkProxyURL(context.Background(), "bad-value")
	if resp.OK {
		t.Fatal("ok = true, want false")
	}
	if resp.Mode != "invalid" {
		t.Fatalf("mode = %q, want invalid", resp.Mode)
	}
	if resp.Error != "invalid_proxy" {
		t.Fatalf("error = %q, want invalid_proxy", resp.Error)
	}
}

func TestCheckProxyURLMasksMalformedCredential(t *testing.T) {
	resp := checkProxyURL(context.Background(), "http://user:sec%ret@proxy.example:8080")
	encoded, errEncode := json.Marshal(resp)
	if errEncode != nil {
		t.Fatalf("marshal response: %v", errEncode)
	}
	if bytes.Contains(encoded, []byte("sec%ret")) || !bytes.Contains(encoded, []byte("********")) {
		t.Fatalf("response leaked proxy password: %s", encoded)
	}
}

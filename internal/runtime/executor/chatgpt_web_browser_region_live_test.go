package executor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/managementdiag"
	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/translator"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

// This opt-in test submits one real image request using an isolated, in-memory
// credential. It never writes credentials or changes a running service.
func TestChatGPTWebBrowserRegionLive(t *testing.T) {
	if os.Getenv("CPA_BROWSER_REGION_LIVE") != "1" {
		t.Skip("explicit live acceptance opt-in required")
	}
	inputPath := os.Getenv("CPA_VERIFY_INPUT")
	if inputPath == "" {
		t.Skip("explicit private input required")
	}
	if os.Getenv("CPA_VERIFY_RESULT") == "" || os.Getenv("CPA_VERIFY_IMAGE") == "" || os.Getenv("CPA_VERIFY_EXIT_HASH") == "" {
		t.Fatal("explicit result, image and expected exit hash are required")
	}
	previousOutput := log.StandardLogger().Out
	log.SetOutput(io.Discard)
	defer log.SetOutput(previousOutput)
	var input struct {
		Metadata map[string]any      `json:"metadata"`
		Proxy    string              `json:"probe_proxy"`
		Images   config.ImagesConfig `json:"images"`
	}
	data, err := os.ReadFile(inputPath)
	if err != nil || json.Unmarshal(data, &input) != nil {
		t.Fatal("private input unavailable")
	}
	cfg := &config.Config{}
	cfg.Images = input.Images
	cfg.Images.ChatGPTWeb.RequestTimeoutSeconds = 0
	cfg.Images.ChatGPTWeb.BootstrapTimeoutSeconds = 0
	cfg.Images.ChatGPTWeb.BootstrapRetries = 0
	cfg.ChatGPTWeb.Timezone = "UTC"
	cfg.ChatGPTWeb.BrowserRegion.AutoTimezone = true
	cfg.ChatGPTWeb.BrowserRegion.AutoLanguage = true
	if zone := os.Getenv("CPA_VERIFY_TIMEZONE"); zone != "" {
		cfg.ChatGPTWeb.Timezone = zone
	}
	ctx, cancel := context.WithTimeout(t.Context(), 180*time.Second)
	defer cancel()
	var tunnelSnapshot func() map[string]any
	if network := os.Getenv("CPA_VERIFY_NETWORK"); network != "" {
		input.Proxy, tunnelSnapshot = browserRegionLiveFamilyTunnel(t, ctx, network)
	}
	e := NewChatGPTWebExecutor(cfg, nil)
	defer func() { _ = e.Close() }()
	auth := &coreauth.Auth{ID: "isolated-main-verification", Provider: "chatgpt-web", Status: coreauth.StatusActive, Metadata: input.Metadata, ProxyURL: input.Proxy}
	client, _, err := e.newRuntimeClientForAcquisition(auth, false)
	if err != nil {
		t.Fatal("client initialization failed")
	}
	defer client.CloseIdleConnections()
	result := map[string]any{"source": "browser-region-zero-image-budget", "started_at": time.Now().UTC().Format(time.RFC3339Nano)}
	zone := e.chatGPTWebTimezoneForClient(client)
	result["cold_timezone"], result["cold_timezone_offset_minutes"] = zone.Timezone, zone.OffsetMinutes
	defer func() {
		if tunnelSnapshot != nil {
			result["direct_egress"] = tunnelSnapshot()
		}
		encoded, _ := json.Marshal(result)
		if err := os.WriteFile(os.Getenv("CPA_VERIFY_RESULT"), encoded, 0600); err != nil {
			t.Error("safe result save failed")
		}
		fmt.Println(string(encoded))
	}()
	response, trace, err := client.DoNoRedirectOnce(ctx, "GET", e.chatGPTWebBaseURL()+"/cdn-cgi/trace", nil, nil)
	if err != nil || response == nil || response.StatusCode != 200 {
		t.Fatal("trace unavailable; no generation sent")
	}
	for _, line := range strings.Split(string(trace), "\n") {
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		switch key {
		case "ip":
			result["exit_hash"] = fmt.Sprintf("%x", sha256.Sum256([]byte(value)))[:16]
		case "loc", "colo":
			result[key] = value
		}
	}
	if result["exit_hash"] != os.Getenv("CPA_VERIFY_EXIT_HASH") {
		t.Fatal("exit mismatch; no generation sent")
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		region, _ := e.BrowserRegionPreview(auth, false)
		if region.RefreshState == "idle" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	region, _ := e.BrowserRegionPreview(auth, false)
	result["region"] = region
	result["logical_image_budget_seconds"] = cfg.Images.ChatGPTWeb.RequestTimeoutSeconds
	if region.TimezoneSource != "fresh_cache" {
		t.Fatal("region lookup did not succeed; no generation sent")
	}
	if os.Getenv("CPA_VERIFY_METADATA_ONLY") == "1" {
		result["metadata_success"] = true
		return
	}
	started := time.Now()
	output, err := e.Execute(ctx, auth, core.Request{
		Model:   "gpt-image-2",
		Payload: []byte(`{"model":"gpt-image-2","input":"画一只小狗","tools":[{"type":"image_generation","output_format":"png"}]}`),
	}, core.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex})
	result["elapsed_ms"], result["response_bytes"], result["success"] = time.Since(started).Milliseconds(), len(output.Payload), err == nil
	snapshot := e.SentinelSnapshot()
	result["sdk_initialized"], result["sdk_fallbacks"] = snapshot.Initialized, snapshot.CompatibilityFallbacks
	if err != nil {
		result["error_type"] = fmt.Sprintf("%T", err)
		result["safe_transport_error"], _ = managementdiag.ProcessText(err.Error(), managementdiag.DetailLevelSafe, 512)
		var diagnostic interface {
			AuthErrorDiagnostic() *coreauth.ErrorDiagnostic
		}
		if errors.As(err, &diagnostic) {
			if detail := diagnostic.AuthErrorDiagnostic(); detail != nil {
				result["diagnostic"] = map[string]any{"code": detail.Code, "stage": detail.Stage, "target_host": detail.TargetHost, "target_path": detail.TargetPath}
			}
		}
		var chain []string
		for current := err; current != nil && len(chain) < 10; current = errors.Unwrap(current) {
			chain = append(chain, fmt.Sprintf("%T", current))
		}
		result["error_chain"] = chain
		var stage interface{ ChatGPTWebFailureStage() string }
		if errors.As(err, &stage) {
			result["failure_stage"] = stage.ChatGPTWebFailureStage()
		}
		var code interface{ ExecutionResultErrorCode() string }
		if errors.As(err, &code) {
			result["error_code"] = code.ExecutionResultErrorCode()
		}
		flags := map[string]bool{}
		for _, word := range []string{"certificate", "cloudflare", "bootstrap", "deadline", "unexpected", "404", "403", "429", "missing", "sentinel", "503", "eof", "empty", "resource", "parse", "closed", "proxy"} {
			flags[word] = strings.Contains(strings.ToLower(err.Error()), word)
		}
		result["safe_error_flags"] = flags
		var status interface{ StatusCode() int }
		if errors.As(err, &status) {
			result["status"] = status.StatusCode()
		}
		result["model_not_found"] = strings.Contains(strings.ToLower(err.Error()), "model not found")
		t.Fatal("generation failed; see safe result")
	}
	encoded := gjson.GetBytes(output.Payload, "response.output.0.result").String()
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(decoded) == 0 {
		t.Fatal("missing image")
	}
	dimensions, format, err := image.DecodeConfig(bytes.NewReader(decoded))
	if err != nil {
		t.Fatal("invalid image")
	}
	result["image_width"], result["image_height"], result["image_format"] = dimensions.Width, dimensions.Height, format
	if err := os.WriteFile(os.Getenv("CPA_VERIFY_IMAGE"), decoded, 0600); err != nil {
		t.Fatal("image save failed")
	}
}

// This loopback CONNECT tunnel only fixes the address family. It forwards the
// original end-to-end TLS bytes without terminating TLS or modifying HTTP.
func browserRegionLiveFamilyTunnel(t *testing.T, ctx context.Context, network string) (string, func() map[string]any) {
	t.Helper()
	if network != "tcp4" && network != "tcp6" {
		t.Fatal("invalid address family")
	}
	var connected, wrongFamily, dialErrors atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		_, port, err := net.SplitHostPort(request.Host)
		if request.Method != http.MethodConnect || err != nil || port != "443" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		upstream, err := (&net.Dialer{}).DialContext(ctx, network, request.Host)
		if err != nil {
			dialErrors.Add(1)
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer func() { _ = upstream.Close() }()
		if address, ok := upstream.LocalAddr().(*net.TCPAddr); !ok || (address.IP.To4() == nil) != (network == "tcp6") {
			wrongFamily.Add(1)
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		connected.Add(1)
		downstream, buffered, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer func() { _ = downstream.Close() }()
		stop := context.AfterFunc(ctx, func() { _ = downstream.Close(); _ = upstream.Close() })
		defer stop()
		if _, err = buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
			return
		}
		if buffered.Flush() != nil {
			return
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = io.Copy(upstream, buffered)
			_ = upstream.Close()
		}()
		_, _ = io.Copy(downstream, upstream)
		_ = downstream.Close()
		<-done
	}))
	t.Cleanup(server.Close)
	return server.URL, func() map[string]any {
		return map[string]any{"network": network, "connections": connected.Load(), "wrong_family": wrongFamily.Load(), "dial_errors": dialErrors.Load()}
	}
}

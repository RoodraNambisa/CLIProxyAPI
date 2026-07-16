package management

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxypool"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestGetProxyPoolStatus(t *testing.T) {
	manager := newProxyRuntimeTestManager(t, proxyRuntimeTestConfig("http://user:status-secret@127.0.0.1:1"))
	h := &Handler{proxyPoolManager: manager}
	router := proxyRuntimeTestRouter(h)

	recorder := performProxyRuntimeRequest(t, router, http.MethodGet, "/proxy-pools/RUNTIME/status", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var status proxypool.PoolStatus
	decodeProxyRuntimeResponse(t, recorder, &status)
	if status.Name != "runtime" || status.BindingCount != 0 {
		t.Fatalf("pool status = %+v", status)
	}

	recorder = performProxyRuntimeRequest(t, router, http.MethodGet, "/proxy-pools/missing/status", "")
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("missing status = %d, want %d; body=%s", recorder.Code, http.StatusNotFound, recorder.Body.String())
	}
}

func TestCheckProxyPoolValidatesSampleAndMasksResults(t *testing.T) {
	proxyServerURL := newProxyRuntimeTraceProxy(t)
	secretOne := "check-secret-one"
	secretTwo := "check-secret-two"
	manager := newProxyRuntimeTestManager(t, proxyRuntimeTestConfig(
		proxyURLWithCredential(t, proxyServerURL, secretOne),
		proxyURLWithCredential(t, proxyServerURL, secretTwo),
	))
	h := &Handler{proxyPoolManager: manager}
	router := proxyRuntimeTestRouter(h)

	if defaultProxyPoolCheckSample != 10 {
		t.Fatalf("default sample = %d, want 10", defaultProxyPoolCheckSample)
	}
	recorder := performProxyRuntimeRequest(t, router, http.MethodPost, "/proxy-pools/runtime/check", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("empty body status = %d, want %d; body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	recorder = performProxyRuntimeRequest(t, router, http.MethodPost, "/proxy-pools/runtime/check", `{}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var response struct {
		Results []proxypool.CheckResult `json:"results"`
	}
	decodeProxyRuntimeResponse(t, recorder, &response)
	if len(response.Results) != 2 {
		t.Fatalf("result count = %d, want 2", len(response.Results))
	}
	for _, result := range response.Results {
		if !result.OK || !strings.Contains(result.ProxyURL, "********") {
			t.Fatalf("check result = %+v", result)
		}
	}
	assertProxyRuntimeSecretsMasked(t, recorder.Body.String(), secretOne, secretTwo)

	for _, body := range []string{`{"sample":0}`, `{"sample":101}`, `{"sample":"bad"}`} {
		recorder = performProxyRuntimeRequest(t, router, http.MethodPost, "/proxy-pools/runtime/check", body)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("body %s status = %d, want %d; response=%s", body, recorder.Code, http.StatusBadRequest, recorder.Body.String())
		}
	}

	recorder = performProxyRuntimeRequest(t, router, http.MethodPost, "/proxy-pools/missing/check", `{}`)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("missing status = %d, want %d; body=%s", recorder.Code, http.StatusNotFound, recorder.Body.String())
	}
}

func TestProxyBindingHandlersResolveIndexesDeduplicateAndReportPerItemStatus(t *testing.T) {
	proxyServerURL := newProxyRuntimeTraceProxy(t)
	secretOne := "binding-secret-one"
	secretTwo := "binding-secret-two"
	manager := newProxyRuntimeTestManager(t, proxyRuntimeTestConfig(
		proxyURLWithCredential(t, proxyServerURL, secretOne),
		proxyURLWithCredential(t, proxyServerURL, secretTwo),
	))
	authManager := coreauth.NewManager(nil, nil, nil)
	authA := registerProxyRuntimeAuth(t, authManager, "auth-a")
	registerProxyRuntimeAuth(t, authManager, "auth-b")
	manager.SetAuthSource(authManager)
	h := &Handler{authManager: authManager, proxyPoolManager: manager}
	router := proxyRuntimeTestRouter(h)

	body, errMarshal := json.Marshal(rebindProxyBindingsRequest{
		AuthIDs:     []string{"auth-a", " auth-a "},
		AuthIndexes: []string{authA.EnsureIndex(), authA.EnsureIndex()},
	})
	if errMarshal != nil {
		t.Fatalf("marshal request: %v", errMarshal)
	}
	recorder := performProxyRuntimeRequest(t, router, http.MethodPost, "/proxy-bindings/rebind", string(body))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var response struct {
		Results []proxypool.RebindResult `json:"results"`
	}
	decodeProxyRuntimeResponse(t, recorder, &response)
	if len(response.Results) != 1 || response.Results[0].AuthID != "auth-a" || !response.Results[0].Updated {
		t.Fatalf("rebind results = %+v", response.Results)
	}
	assertProxyRuntimeSecretsMasked(t, recorder.Body.String(), secretOne, secretTwo)

	recorder = performProxyRuntimeRequest(t, router, http.MethodGet, "/proxy-bindings", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("bindings status = %d, want %d; body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var bindingsResponse struct {
		Bindings []proxypool.BindingStatus `json:"bindings"`
	}
	decodeProxyRuntimeResponse(t, recorder, &bindingsResponse)
	if len(bindingsResponse.Bindings) != 1 || bindingsResponse.Bindings[0].AuthIndex != authA.EnsureIndex() {
		t.Fatalf("bindings = %+v", bindingsResponse.Bindings)
	}
	if !strings.Contains(bindingsResponse.Bindings[0].ProxyURL, "********") {
		t.Fatalf("binding proxy URL = %q", bindingsResponse.Bindings[0].ProxyURL)
	}
	assertProxyRuntimeSecretsMasked(t, recorder.Body.String(), secretOne, secretTwo)

	recorder = performProxyRuntimeRequest(t, router, http.MethodPost, "/proxy-bindings/rebind", `{"auth_ids":["auth-b","missing"]}`)
	if recorder.Code != http.StatusMultiStatus {
		t.Fatalf("partial status = %d, want %d; body=%s", recorder.Code, http.StatusMultiStatus, recorder.Body.String())
	}
	response.Results = nil
	decodeProxyRuntimeResponse(t, recorder, &response)
	if len(response.Results) != 2 || !response.Results[0].Updated || response.Results[1].HTTPStatus != http.StatusNotFound {
		t.Fatalf("partial results = %+v", response.Results)
	}

	recorder = performProxyRuntimeRequest(t, router, http.MethodPost, "/proxy-bindings/rebind", `{"auth_ids":["missing","missing"]}`)
	if recorder.Code != http.StatusMultiStatus {
		t.Fatalf("failed status = %d, want %d; body=%s", recorder.Code, http.StatusMultiStatus, recorder.Body.String())
	}
	response.Results = nil
	decodeProxyRuntimeResponse(t, recorder, &response)
	if len(response.Results) != 1 || response.Results[0].Updated {
		t.Fatalf("failed results = %+v", response.Results)
	}

	recorder = performProxyRuntimeRequest(t, router, http.MethodPost, "/proxy-bindings/rebind", `{"auth_indexes":["unknown-index","unknown-index"]}`)
	if recorder.Code != http.StatusMultiStatus {
		t.Fatalf("unknown index status = %d, want %d; body=%s", recorder.Code, http.StatusMultiStatus, recorder.Body.String())
	}
	response.Results = nil
	decodeProxyRuntimeResponse(t, recorder, &response)
	if len(response.Results) != 1 || response.Results[0].HTTPStatus != http.StatusNotFound {
		t.Fatalf("unknown index results = %+v", response.Results)
	}
	if response.Results[0].AuthIndex != "unknown-index" || response.Results[0].AuthID != "" {
		t.Fatalf("unknown index identity = %+v", response.Results[0])
	}
}

func TestRebindProxyBindingsRejectsInvalidInput(t *testing.T) {
	manager := newProxyRuntimeTestManager(t, proxyRuntimeTestConfig("http://user:input-secret@127.0.0.1:1"))
	authManager := coreauth.NewManager(nil, nil, nil)
	h := &Handler{authManager: authManager, proxyPoolManager: manager}
	router := proxyRuntimeTestRouter(h)

	for _, body := range []string{`{`, `{}`, `{"auth_ids":[]}`, `{"auth_ids":[" "]}`, `{"auth_indexes":"bad"}`} {
		recorder := performProxyRuntimeRequest(t, router, http.MethodPost, "/proxy-bindings/rebind", body)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("body %s status = %d, want %d; response=%s", body, recorder.Code, http.StatusBadRequest, recorder.Body.String())
		}
	}
}

func proxyRuntimeTestRouter(h *Handler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/proxy-pools/:name/status", h.GetProxyPoolStatus)
	router.POST("/proxy-pools/:name/check", h.CheckProxyPool)
	router.GET("/proxy-bindings", h.GetProxyBindings)
	router.POST("/proxy-bindings/rebind", h.RebindProxyBindings)
	return router
}

func performProxyRuntimeRequest(t *testing.T, router http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	router.ServeHTTP(recorder, request)
	return recorder
}

func decodeProxyRuntimeResponse(t *testing.T, recorder *httptest.ResponseRecorder, target any) {
	t.Helper()
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), target); errDecode != nil {
		t.Fatalf("decode response: %v; body=%s", errDecode, recorder.Body.String())
	}
}

func newProxyRuntimeTestManager(t *testing.T, cfg *internalconfig.Config) *proxypool.Manager {
	t.Helper()
	manager, errNew := proxypool.NewManager("", cfg)
	if errNew != nil {
		t.Fatalf("NewManager() error = %v", errNew)
	}
	return manager
}

func proxyRuntimeTestConfig(proxyURLs ...string) *internalconfig.Config {
	entries := make([]internalconfig.ProxyPoolEntryConfig, 0, len(proxyURLs))
	for index, proxyURL := range proxyURLs {
		entries = append(entries, internalconfig.ProxyPoolEntryConfig{
			ID:          fmt.Sprintf("node-%d", index+1),
			URLTemplate: proxyURL,
		})
	}
	cfg := &internalconfig.Config{}
	cfg.ProxyPools = []internalconfig.ProxyPoolConfig{{
		Name:         "runtime",
		BindAttempts: 20,
		Entries:      entries,
	}}
	cfg.ProxyRules = []internalconfig.ProxyRuleConfig{{
		Name:      "codex",
		Pool:      "runtime",
		Providers: []string{"codex"},
	}}
	return cfg
}

func registerProxyRuntimeAuth(t *testing.T, manager *coreauth.Manager, id string) *coreauth.Auth {
	t.Helper()
	auth, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{ID: id, Provider: "codex"})
	if errRegister != nil {
		t.Fatalf("Register(%s) error = %v", id, errRegister)
	}
	return auth
}

func proxyURLWithCredential(t *testing.T, rawURL, password string) string {
	t.Helper()
	parsedURL, errParse := url.Parse(rawURL)
	if errParse != nil {
		t.Fatalf("parse proxy URL: %v", errParse)
	}
	parsedURL.User = url.UserPassword("runtime-user", password)
	return parsedURL.String()
}

func assertProxyRuntimeSecretsMasked(t *testing.T, body string, secrets ...string) {
	t.Helper()
	for _, secret := range secrets {
		if strings.Contains(body, secret) {
			t.Fatalf("response leaked proxy password %q: %s", secret, body)
		}
	}
}

func newProxyRuntimeTraceProxy(t *testing.T) string {
	t.Helper()
	certificateServer := httptest.NewTLSServer(http.NotFoundHandler())
	certificates := append([]tls.Certificate(nil), certificateServer.TLS.Certificates...)
	certificateServer.Close()

	tlsConfig := &tls.Config{Certificates: certificates}
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodConnect {
			http.Error(w, "CONNECT required", http.StatusMethodNotAllowed)
			return
		}
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "hijacking unavailable", http.StatusInternalServerError)
			return
		}
		connection, buffered, errHijack := hijacker.Hijack()
		if errHijack != nil {
			return
		}
		defer func() { _ = connection.Close() }()
		_, _ = buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n")
		if errFlush := buffered.Flush(); errFlush != nil {
			return
		}

		tlsConnection := tls.Server(connection, tlsConfig)
		if errHandshake := tlsConnection.Handshake(); errHandshake != nil {
			return
		}
		traceRequest, errRead := http.ReadRequest(bufio.NewReader(tlsConnection))
		if errRead != nil {
			return
		}
		if traceRequest.Body != nil {
			_, _ = io.Copy(io.Discard, traceRequest.Body)
			_ = traceRequest.Body.Close()
		}
		payload := "ip=203.0.113.10\nloc=US\nhttp=http/1.1\ntls=TLSv1.3\ncolo=LAX\n"
		_, _ = fmt.Fprintf(tlsConnection, "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", len(payload), payload)
		_ = tlsConnection.Close()
	}))

	originalTransport := http.DefaultTransport
	testTransport := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	http.DefaultTransport = testTransport
	t.Cleanup(func() {
		testTransport.CloseIdleConnections()
		http.DefaultTransport = originalTransport
		proxyServer.Close()
	})
	return proxyServer.URL
}

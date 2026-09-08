package live

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/api/middleware"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
)

func secretHandlerContext(t *testing.T, body string) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	c, w := liveHandlerRequest(t.Context(), "fixture-issuer", "")
	c.Set("accessProvider", "fixture-access")
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/realtime/client_secrets", strings.NewReader(body))
	return c, w
}

func TestLiveClientSecretHandlerCurrentAndLegacyIssueLocalCapabilities(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		cfg := &config.Config{}
		cfg.Codex.LiveEnabled = true
		h := NewHandler(cfg, nil)
		defer h.Close()
		body := `{"session":{"model":"team/voice","large":90071992547409931234},"expires_after":{"seconds":10}}`
		if legacy {
			body = `{"model":"team/voice","large":90071992547409931234}`
		}
		c, w := secretHandlerContext(t, body)
		if legacy {
			h.CreateLegacySession(c)
		} else {
			h.CreateClientSecret(c)
		}
		if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("credential response was not successful and non-cacheable")
		}
		var response map[string]json.RawMessage
		if errJSON := json.Unmarshal(w.Body.Bytes(), &response); errJSON != nil {
			t.Fatal(errJSON)
		}
		secret, session := response, response["session"]
		if legacy {
			session = w.Body.Bytes()
			if errJSON := json.Unmarshal(response["client_secret"], &secret); errJSON != nil {
				t.Fatal(errJSON)
			}
		}
		var token string
		if errJSON := json.Unmarshal(secret["value"], &token); errJSON != nil || !validClientSecretToken(token) {
			t.Fatal("invalid local capability response")
		}
		a, errAuth := h.clientSecrets.authenticate(token)
		owner, _ := requestCallOwner(c)
		if errAuth != nil || a.owner != owner || a.model != "team/voice" || !bytes.Contains(session, []byte("90071992547409931234")) {
			t.Fatal("credential session lost owner, alias or integer precision")
		}
	}
}

func TestLiveClientSecretHandlerRejectsDisabledInvalidAndRecursiveIssuance(t *testing.T) {
	for _, tc := range []struct {
		enabled, temporary bool
		body               string
		status             int
	}{{false, false, `{}`, 503}, {true, true, `{}`, 403}, {true, false, `{"session":{"type":"transcription"}}`, 501}, {true, false, `{"expires_after":{"seconds":9}}`, 400}, {true, false, strings.Repeat("x", clientSecretMaxBodySize+1), 413}} {
		cfg := &config.Config{}
		cfg.Codex.LiveEnabled = tc.enabled
		h := NewHandler(cfg, nil)
		c, w := secretHandlerContext(t, tc.body)
		if tc.temporary {
			c.Set(clientSecretContextKey, ClientSecretAuthorization{})
		}
		h.CreateClientSecret(c)
		if w.Code != tc.status {
			t.Fatalf("credential rejection status=%d, want %d", w.Code, tc.status)
		}
		if !tc.enabled && !strings.Contains(w.Body.String(), liveDisabledCode) {
			t.Fatal("disabled issuance lost its explicit code")
		}
		h.clientSecrets.mu.Lock()
		count := len(h.clientSecrets.entries)
		h.clientSecrets.mu.Unlock()
		if count != 0 {
			t.Fatal("rejected request issued a credential")
		}
		h.Close()
	}
}

type delayedSecretEntropy struct{ started, release chan struct{} }

func (r delayedSecretEntropy) Read(buffer []byte) (int, error) {
	close(r.started)
	<-r.release
	for i := range buffer {
		buffer[i] = byte(i + 1)
	}
	return len(buffer), nil
}

func TestLiveClientSecretHandlerRechecksGenerationAfterEntropyAcquisition(t *testing.T) {
	for _, reenable := range []bool{false, true} {
		cfg := &config.Config{}
		cfg.Codex.LiveEnabled = true
		h := NewHandler(cfg, nil)
		defer h.Close()
		reader := delayedSecretEntropy{make(chan struct{}), make(chan struct{})}
		h.secretRandom = reader
		c, w := secretHandlerContext(t, `{}`)
		done := make(chan struct{})
		go func() { h.CreateClientSecret(c); close(done) }()
		<-reader.started
		h.UpdateConfig(&config.Config{})
		if reenable {
			h.UpdateConfig(cfg)
		}
		close(reader.release)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("issuance did not release pending generation")
		}
		if w.Code != 503 || !strings.Contains(w.Body.String(), liveDisabledCode) {
			t.Fatal("old generation issued a credential")
		}
		h.clientSecrets.mu.Lock()
		count := len(h.clientSecrets.entries)
		h.clientSecrets.mu.Unlock()
		if count != 0 {
			t.Fatal("old issuance remained registered")
		}
	}
}

func TestLiveClientSecretHandlerDeliveryFailureRevokesCapability(t *testing.T) {
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	h := NewHandler(cfg, nil)
	defer h.Close()
	c, _ := secretHandlerContext(t, `{}`)
	c.Writer = failedCallWriter{c.Writer}
	h.CreateClientSecret(c)
	if len(h.clientSecrets.entries) != 0 {
		t.Fatal("failed delivery left an active credential")
	}
}

func TestLiveClientSecretHandlerProtectsActualRequestAndResponseLogs(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		cfg := &config.Config{}
		cfg.Codex.LiveEnabled = true
		cfg.RequestLog = true
		h := NewHandler(cfg, nil)
		defer h.Close()
		dir := t.TempDir()
		logger := logging.NewFileRequestLogger(true, dir, dir, 10)
		engine := gin.New()
		engine.Use(middleware.RequestLoggingMiddleware(logger))
		engine.Use(func(c *gin.Context) { c.Set("apiKey", "fixture-issuer"); c.Set("accessProvider", "fixture-access") })
		path := "/v1/realtime/client_secrets"
		body := `{"stream":true,"session":{"client_secret":{"value":"old-request-secret"}}}`
		if legacy {
			path = "/v1/realtime/sessions"
			body = `{"stream":true,"client_secret":{"value":"old-request-secret"}}`
			engine.POST(path, h.CreateLegacySession)
		} else {
			engine.POST(path, h.CreateClientSecret)
		}
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		engine.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatal("logging changed successful issuance")
		}
		var parsed struct {
			Value        string `json:"value"`
			ClientSecret struct {
				Value string `json:"value"`
			} `json:"client_secret"`
		}
		if errJSON := json.Unmarshal(w.Body.Bytes(), &parsed); errJSON != nil {
			t.Fatal(errJSON)
		}
		token := parsed.Value
		if legacy {
			token = parsed.ClientSecret.Value
		}
		if !validClientSecretToken(token) {
			t.Fatal("wire credential was redacted")
		}
		entries, errRead := os.ReadDir(dir)
		if errRead != nil || len(entries) == 0 {
			t.Fatal("logger did not write an issuance record")
		}
		for _, entry := range entries {
			data, errRead := os.ReadFile(filepath.Join(dir, entry.Name()))
			if errRead != nil {
				t.Fatal(errRead)
			}
			if bytes.Contains(data, []byte(token)) || bytes.Contains(data, []byte("old-request-secret")) {
				t.Fatal("issuance log exposed a credential")
			}
		}
	}
}

func TestLiveClientSecretHandlerEntropyFailureIsLocal(t *testing.T) {
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	h := NewHandler(cfg, nil)
	defer h.Close()
	h.secretRandom = io.LimitReader(strings.NewReader("x"), 1)
	c, w := secretHandlerContext(t, `{}`)
	h.CreateClientSecret(c)
	if w.Code != 500 || len(h.clientSecrets.entries) != 0 {
		t.Fatal("entropy failure was reported as a usable credential")
	}
}

func TestLiveClientSecretHandlerCapacityAndCollisionsKeepExistingCredentials(t *testing.T) {
	for _, collision := range []bool{false, true} {
		cfg := &config.Config{}
		cfg.Codex.LiveEnabled = true
		h := NewHandler(cfg, nil)
		defer h.Close()
		c, w := secretHandlerContext(t, `{}`)
		owner, _ := requestCallOwner(c)
		var existingToken string
		if collision {
			token, a, errPrepare := prepareClientSecret(bytes.NewReader(make([]byte, 50)), time.Now(), []byte(`{}`), "gpt-realtime", owner, clientSecretDefaultLifetime)
			if errPrepare != nil {
				t.Fatal(errPrepare)
			}
			if errPut := h.clientSecrets.put(token, a); errPut != nil {
				t.Fatal(errPut)
			}
			existingToken = token
			h.secretRandom = bytes.NewReader(make([]byte, 4*50))
		} else {
			for i := 0; i < clientSecretMaxEntriesPerIssuer; i++ {
				token, a := preparedSecret(t, owner, time.Now())
				if errPut := h.clientSecrets.put(token, a); errPut != nil {
					t.Fatal(errPut)
				}
				existingToken = token
			}
		}
		h.CreateClientSecret(c)
		want := 429
		if collision {
			want = 500
		}
		if w.Code != want {
			t.Fatalf("local issuance failure status=%d, want %d", w.Code, want)
		}
		if !collision && (w.Header().Get("Retry-After") != "1" || !strings.Contains(w.Body.String(), "rate_limit_error")) {
			t.Fatal("local capacity error lost its retry hint")
		}
		if _, errAuth := h.clientSecrets.authenticate(existingToken); errAuth != nil {
			t.Fatal("failed issuance changed an existing credential")
		}
	}
}

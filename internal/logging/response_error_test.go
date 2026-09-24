package logging

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/managementdiag"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

func TestGinAccessLogIncludesSanitizedFinalErrorWithoutRequestLogger(t *testing.T) {
	gin.SetMode(gin.TestMode)
	hook := logtest.NewGlobal()
	defer hook.Reset()
	const clientKey = "opaque-client-key-fixture"
	const body = `{"error":{"code":"auth_not_found","type":"server_error","message":"no auth available for person@example.com opaque-client-key-fixture"},"input":"request-body-sentinel","access_token":"upstream-token-sentinel"}`
	router := gin.New()
	router.Use(GinLogrusLogger())
	router.POST("/v1/chat/completions", func(c *gin.Context) {
		c.Set("apiKey", clientKey)
		c.Data(503, "application/json", []byte(body))
	})
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	request.Header.Set("Authorization", "Bearer "+clientKey)
	writer := httptest.NewRecorder()
	router.ServeHTTP(writer, request)
	if writer.Code != 503 || writer.Body.String() != body {
		t.Fatal("logging altered the client response")
	}
	entry := hook.LastEntry()
	if entry == nil || entry.Level != log.ErrorLevel || !strings.Contains(entry.Message, "no auth available") || entry.Data["code"] != "auth_not_found" {
		t.Fatal("access log omitted the final failure")
	}
	preview := entry.Data["response_body"].(managementdiag.ManagementOnlyValue).Value()
	for _, secret := range []string{clientKey, "person@example.com", "request-body-sentinel", "upstream-token-sentinel"} {
		if strings.Contains(entry.Message+preview, secret) {
			t.Fatal("access diagnostics retained sensitive content")
		}
	}
	if entry.Data["status"] != 503 || entry.Data["method"] != "POST" {
		t.Fatal("structured HTTP metadata is missing")
	}
}

func TestGinAccessLogRetainsHTTPStatusWhenStreamReportsFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	hook := logtest.NewGlobal()
	defer hook.Reset()
	router := gin.New()
	router.Use(GinLogrusLogger())
	router.GET("/stream", func(c *gin.Context) {
		c.Header("Content-Type", "text/event-stream")
		_, _ = c.Writer.WriteString("data: answer-not-to-log\n\n")
		c.Writer.Flush()
		RecordResponseError(c, 500, []byte(`{"error":{"code":"resource_exhausted","message":"capacity exhausted"}}`))
	})
	writer := httptest.NewRecorder()
	router.ServeHTTP(writer, httptest.NewRequest(http.MethodGet, "/stream", nil))
	entry := hook.LastEntry()
	if writer.Code != 200 || entry.Data["status"] != 200 || entry.Level != log.ErrorLevel || !strings.Contains(entry.Message, "error_status=500") || !strings.Contains(entry.Message, "capacity exhausted") {
		t.Fatal("stream error replaced HTTP status or disappeared")
	}
	if strings.Contains(entry.Message, "answer-not-to-log") {
		t.Fatal("successful stream data was captured")
	}
}

func TestGinAccessLogKeepsUpstreamEvidencePrivateAndClearsRetries(t *testing.T) {
	gin.SetMode(gin.TestMode)
	hook := logtest.NewGlobal()
	defer hook.Reset()
	for _, mode := range []string{"failure", "retry", "success", "late"} {
		router := gin.New()
		router.Use(GinLogrusLogger())
		router.POST("/v1/images/generations", func(c *gin.Context) {
			ctx := c.Request.Context()
			SetRequestCredential(ctx, CredentialIdentity{Provider: "chatgpt-web", Index: "first"})
			fields := log.Fields{"stage": "upstream_request", "code": "cloudflare_challenge", "status": 403, "response_type": "html", "response_body": managementdiag.NewManagementOnlyValueWithFallback("<html>upstream detail</html>", "<html>upstream detail</html>")}
			SetRequestUpstreamDiagnostic(ctx, "first", fields)
			if mode == "retry" || mode == "late" {
				SetRequestCredential(ctx, CredentialIdentity{Provider: "chatgpt-web", Index: "second"})
			}
			if mode == "late" {
				SetRequestUpstreamDiagnostic(ctx, "first", fields)
			}
			if mode == "success" {
				c.String(200, "image-result")
				return
			}
			c.JSON(403, gin.H{"error": gin.H{"code": "permission_denied", "message": "<redacted-non-json-response-body>"}})
		})
		writer := httptest.NewRecorder()
		router.ServeHTTP(writer, httptest.NewRequest("POST", "/v1/images/generations", nil))
		entry := hook.LastEntry()
		if entry.Data["request_id"] == "--------" {
			t.Fatal("images request missing correlation ID")
		}
		_, has := entry.Data["upstream_code"]
		if has != (mode == "failure") {
			t.Fatalf("mode %s leaked/lost attempt diagnostic", mode)
		}
		if strings.Contains(writer.Body.String(), "upstream detail") {
			t.Fatal("internal evidence entered API response")
		}
		if mode == "failure" {
			raw, _ := (&LogFormatter{}).Format(entry)
			if !strings.Contains(string(raw), "upstream detail") {
				t.Fatal("file log lost upstream evidence")
			}
		}
	}
}

func TestErrorResponseWriterBoundsCaptureAndPreservesFlushing(t *testing.T) {
	for _, status := range []int{200, 502} {
		base := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(base)
		writer := &errorResponseWriter{ResponseWriter: c.Writer}
		writer.WriteHeader(status)
		data := strings.Repeat("x", responseErrorCaptureLimit)
		_, _ = writer.Write([]byte(data))
		_, _ = writer.WriteString(data)
		if err := http.NewResponseController(writer).Flush(); err != nil {
			t.Fatal(err)
		}
		if !base.Flushed || base.Body.Len() != 2*len(data) {
			t.Fatal("response forwarding changed")
		}
		if status == 200 && (len(writer.body) != 0 || writer.truncated) {
			t.Fatal("successful body was retained")
		}
		if status == 502 && (len(writer.body) != responseErrorCaptureLimit || !writer.truncated) {
			t.Fatal("error capture is not bounded")
		}
	}
}

func TestAccessErrorWriterPreservesWebsocketUpgrade(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(GinLogrusLogger())
	router.GET("/ws", func(c *gin.Context) {
		conn, err := (&websocket.Upgrader{}).Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = conn.Close() }()
		if err := conn.WriteMessage(websocket.TextMessage, []byte("fixture")); err != nil {
			t.Error(err)
		}
	})
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { router.ServeHTTP(w, r); close(done) }))
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_, data, err := conn.ReadMessage()
	if err != nil || string(data) != "fixture" {
		t.Fatal("websocket forwarding changed")
	}
	<-done
}

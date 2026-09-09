package openai

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestResponsesSSEFramerValidatesBeforeOutputAndPreservesTrust(t *testing.T) {
	for _, incomplete := range []bool{false, true} {
		for _, trusted := range []bool{false, true} {
			for _, projection := range []bool{false, true} {
				t.Run(fmt.Sprintf("incomplete=%t/trusted=%t/projection=%t", incomplete, trusted, projection), func(t *testing.T) {
					framer := &responsesSSEFramer{passthrough: trusted}
					if projection {
						framer.rewriteTerminalError = func(frame []byte) ([]byte, *interfaces.ErrorMessage, bool) { return frame, nil, false }
					}
					wire := `data: {"type":` + "}\n\n"
					if incomplete {
						wire = `data: {"type":"unfinished"`
					}
					var out strings.Builder
					framer.WriteChunk(&out, []byte(wire))
					framer.Flush(&out)
					if trusted {
						if framer.Err() != nil || out.String() != wire {
							t.Fatal("trusted bytes were validated or discarded")
						}
						return
					}
					if framer.Err() == nil || out.Len() != 0 || len(framer.pending) != 0 {
						t.Fatal("malformed frame was delivered or silently discarded")
					}
				})
			}
		}
	}
}

func TestResponsesSSEValidationPreservesPendingUpstreamError(t *testing.T) {
	h, recorder, c, flusher := newResponsesStreamTestHandler(t)
	c.Writer.WriteHeaderNow()
	framer := &responsesSSEFramer{}
	var first strings.Builder
	framer.WriteChunk(&first, []byte(`data: {"type":"unfinished"`))
	data := make(chan []byte)
	close(data)
	errs := make(chan *interfaces.ErrorMessage, 1)
	original := errors.New("original upstream failure")
	errs <- &interfaces.ErrorMessage{StatusCode: 429, Error: original}
	close(errs)
	var canceled error
	h.forwardResponsesStream(c, flusher, func(err error) { canceled = err }, data, errs, framer)
	if !errors.Is(canceled, original) || !strings.Contains(recorder.Body.String(), "original upstream failure") || strings.Contains(recorder.Body.String(), "invalid upstream Responses SSE data JSON") {
		t.Fatal("trailing validation hid the existing upstream error")
	}
}

func TestResponsesSSEMalformedFirstDataFailsBeforeHeaders(t *testing.T) {
	for _, incomplete := range []bool{false, true} {
		t.Run(fmt.Sprint(incomplete), func(t *testing.T) {
			wire := `data: {"type":` + "}\n\n"
			if incomplete {
				wire = `data: {"type":"unfinished"`
			}
			executor := &responsesMetadataCaptureExecutor{chunks: [][]byte{[]byte(wire)}}
			manager := coreauth.NewManager(nil, nil, nil)
			manager.RegisterExecutor(executor)
			model := fmt.Sprintf("sse-validation-%t", incomplete)
			auth := &coreauth.Auth{ID: model, Provider: "codex", Status: coreauth.StatusActive}
			if _, err := manager.Register(coreauth.WithSkipPersist(t.Context()), auth); err != nil {
				t.Fatal(err)
			}
			registry.GetGlobalRegistry().RegisterClient(auth.ID, "codex", []*registry.ModelInfo{{ID: model}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
			h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager))
			router := gin.New()
			router.POST("/v1/responses", h.Responses)
			request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(fmt.Sprintf(`{"model":%q,"stream":true,"input":"fixture"}`, model)))
			request.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusBadGateway || !strings.Contains(recorder.Body.String(), "invalid upstream Responses SSE data JSON") || strings.Contains(recorder.Header().Get("Content-Type"), "text/event-stream") {
				t.Fatal("malformed first data committed a successful SSE response")
			}
		})
	}
}

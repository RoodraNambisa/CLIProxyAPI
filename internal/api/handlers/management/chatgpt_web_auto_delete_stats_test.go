package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestGetChatGPTWebAutoDeleteDeadStats(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := &Handler{}
	handler.SetChatGPTWebDeadAuthDeleteCountProvider(func() uint64 { return 7 })
	router := gin.New()
	router.GET("/chatgpt-web/auto-delete-dead/stats", handler.GetChatGPTWebAutoDeleteDeadStats)

	request := httptest.NewRequest(http.MethodGet, "/chatgpt-web/auto-delete-dead/stats", nil)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if cacheControl := recorder.Header().Get("Cache-Control"); cacheControl != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", cacheControl)
	}
	var response chatGPTWebAutoDeleteDeadStatsResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.DeletedCount != 7 {
		t.Fatalf("deleted_count = %d, want 7", response.DeletedCount)
	}
}

package management

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestGetChatGPTWebImageTasksReturnsBoundedSnapshot(t *testing.T) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/chatgpt-web/image-tasks", nil)
	(&Handler{}).GetChatGPTWebImageTasks(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
}

func TestCancelChatGPTWebImageTaskRejectsUnknownID(t *testing.T) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "id", Value: "missing"}}
	ctx.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/chatgpt-web/image-tasks/missing", nil)
	(&Handler{}).CancelChatGPTWebImageTask(ctx)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: %s", recorder.Code, recorder.Body.String())
	}
}

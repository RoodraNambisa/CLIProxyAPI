package management

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
)

// GetChatGPTWebImageTasks returns active task diagnostics without upstream identifiers.
func (h *Handler) GetChatGPTWebImageTasks(c *gin.Context) {
	c.JSON(http.StatusOK, runtimeexecutor.ChatGPTWebImageTasksSnapshot())
}

// CancelChatGPTWebImageTask cancels one active task without affecting other image work.
func (h *Handler) CancelChatGPTWebImageTask(c *gin.Context) {
	id := strings.TrimSpace(c.Param("id"))
	result, found := runtimeexecutor.CancelChatGPTWebImageTask(id)
	if !found {
		c.JSON(http.StatusNotFound, gin.H{"error": gin.H{
			"message": "chatgpt web image task not found",
			"type":    "invalid_request_error",
			"code":    "chatgpt_web_image_task_not_found",
		}})
		return
	}
	c.JSON(http.StatusAccepted, result)
}

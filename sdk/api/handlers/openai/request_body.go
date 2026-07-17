package openai

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	executorhelps "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
)

func readOpenAIJSONRequestBody(c *gin.Context) ([]byte, error) {
	return readOpenAIJSONRequestBodyWithLimit(c, int64(executorhelps.ChatGPTWebMaxRequestBytes))
}

func readOpenAIImageJSONRequestBody(c *gin.Context) ([]byte, error) {
	return readOpenAIJSONRequestBodyWithLimit(c, int64(maxImageMultipartBytes))
}

func readOpenAIJSONRequestBodyWithLimit(c *gin.Context, limit int64) ([]byte, error) {
	if c == nil || c.Request == nil {
		return nil, errors.New("request is nil")
	}
	if limit < 1 {
		return nil, errors.New("request body limit is invalid")
	}
	if c.Request.ContentLength > limit {
		return nil, &http.MaxBytesError{Limit: limit}
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, limit)
	return c.GetRawData()
}

func openAIJSONRequestTooLarge(err error) bool {
	var maxBytesErr *http.MaxBytesError
	return errors.As(err, &maxBytesErr)
}

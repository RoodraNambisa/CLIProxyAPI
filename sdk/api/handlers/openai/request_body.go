package openai

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	executorhelps "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	log "github.com/sirupsen/logrus"
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
		releaseOpenAIRequestBody(c)
		return nil, errors.New("request body limit is invalid")
	}
	if c.Request.ContentLength > limit {
		releaseOpenAIRequestBody(c)
		return nil, &http.MaxBytesError{Limit: limit}
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, limit)
	body, err := c.GetRawData()
	releaseOpenAIRequestBody(c)
	return body, err
}

func releaseOpenAIRequestBody(c *gin.Context) {
	if c == nil || c.Request == nil {
		return
	}
	if body := c.Request.Body; body != nil && body != http.NoBody {
		if err := body.Close(); err != nil {
			log.WithError(err).Debug("failed to close parsed OpenAI request body")
		}
	}
	c.Request.Body = http.NoBody
	c.Request.ContentLength = 0
}

func releaseOpenAIMultipartRequest(c *gin.Context) {
	if c == nil || c.Request == nil {
		return
	}
	if form := c.Request.MultipartForm; form != nil {
		if err := form.RemoveAll(); err != nil {
			log.WithError(err).Warn("failed to remove parsed OpenAI multipart files")
		}
		c.Request.MultipartForm = nil
	}
	releaseOpenAIRequestBody(c)
}

func openAIJSONRequestTooLarge(err error) bool {
	var maxBytesErr *http.MaxBytesError
	return errors.As(err, &maxBytesErr)
}

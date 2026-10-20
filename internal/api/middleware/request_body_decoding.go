package middleware

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	log "github.com/sirupsen/logrus"
)

// RequestBodyDecodingMiddleware runs after authentication and before body audit.
// Plain requests retain their existing streaming and body-limit behavior.
func RequestBodyDecodingMiddleware(limit int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		encoding := strings.TrimSpace(strings.Join(c.Request.Header.Values("Content-Encoding"), ","))
		if encoding == "" || strings.EqualFold(encoding, "identity") {
			c.Next()
			return
		}
		body, err := util.ReadDecodedRequestBody(c.Request, limit)
		if c.Request.Body != nil {
			if errClose := c.Request.Body.Close(); errClose != nil {
				log.WithError(errClose).Debug("close encoded request body")
			}
		}
		c.Request.Body = http.NoBody
		c.Request.GetBody = nil
		if err != nil {
			status := http.StatusBadRequest
			var overflow *http.MaxBytesError
			if errors.As(err, &overflow) {
				status = http.StatusRequestEntityTooLarge
			}
			c.AbortWithStatusJSON(status, gin.H{"error": gin.H{
				"message": err.Error(), "type": "invalid_request_error",
			}})
			return
		}
		c.Request.Body = io.NopCloser(bytes.NewReader(body))
		c.Next()
	}
}

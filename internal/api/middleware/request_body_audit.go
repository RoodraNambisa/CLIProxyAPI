package middleware

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

// RequestBodyAuditConfigProvider returns the current request body audit configuration.
type RequestBodyAuditConfigProvider func() config.RequestBodyAuditConfig

// RequestBodyAuditMiddleware blocks model API requests whose raw body contains configured byte keywords.
func RequestBodyAuditMiddleware(provider RequestBodyAuditConfigProvider) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c == nil || c.Request == nil || !shouldAuditRequestBody(c.Request) {
			c.Next()
			return
		}
		if provider == nil {
			c.Next()
			return
		}

		cfg := config.NormalizeRequestBodyAudit(provider())
		if !cfg.Enable {
			c.Next()
			return
		}
		keywords := config.CompiledRequestBodyAuditKeywords(cfg)
		if len(keywords) == 0 {
			c.Next()
			return
		}
		if c.Request.Body == nil || c.Request.Body == http.NoBody {
			c.Next()
			return
		}

		body, tooLarge, errRead := readAuditRequestBody(c.Request, cfg.MaxBodyBytes)
		if errRead != nil {
			writeRequestBodyAuditError(c, cfg.Error)
			return
		}
		if tooLarge && cfg.RejectOversize {
			writeRequestBodyAuditError(c, cfg.Error)
			return
		}

		restoreAuditRequestBody(c.Request, body, tooLarge)
		if requestBodyAuditMatched(body, keywords, cfg.CaseSensitive) {
			writeRequestBodyAuditError(c, cfg.Error)
			return
		}

		c.Next()
	}
}

func shouldAuditRequestBody(req *http.Request) bool {
	if req == nil || req.URL == nil {
		return false
	}
	switch req.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	}
	path := req.URL.Path
	return strings.HasPrefix(path, "/v1/") ||
		strings.HasPrefix(path, "/v1beta/")
}

func readAuditRequestBody(req *http.Request, maxBodyBytes int64) ([]byte, bool, error) {
	if req == nil || req.Body == nil {
		return nil, false, nil
	}
	if maxBodyBytes <= 0 {
		body, err := io.ReadAll(req.Body)
		return body, false, err
	}
	body, err := io.ReadAll(io.LimitReader(req.Body, maxBodyBytes+1))
	if err != nil {
		return nil, false, err
	}
	return body, int64(len(body)) > maxBodyBytes, nil
}

func restoreAuditRequestBody(req *http.Request, body []byte, hasRemainder bool) {
	if req == nil {
		return
	}
	if hasRemainder && req.Body != nil {
		req.Body = io.NopCloser(io.MultiReader(bytes.NewReader(body), req.Body))
		return
	}
	req.Body = io.NopCloser(bytes.NewReader(body))
}

func requestBodyAuditMatched(body []byte, keywords [][]byte, caseSensitive bool) bool {
	if len(body) == 0 || len(keywords) == 0 {
		return false
	}
	if caseSensitive {
		for _, keyword := range keywords {
			if len(keyword) > 0 && bytes.Contains(body, keyword) {
				return true
			}
		}
		return false
	}

	var foldedBody []byte
	for _, keyword := range keywords {
		if len(keyword) == 0 {
			continue
		}
		if asciiBytes(keyword) {
			if containsASCIIFold(body, keyword) {
				return true
			}
			continue
		}
		if foldedBody == nil {
			foldedBody = bytes.ToLower(body)
		}
		if bytes.Contains(foldedBody, keyword) {
			return true
		}
	}
	return false
}

func asciiBytes(value []byte) bool {
	for _, b := range value {
		if b >= 0x80 {
			return false
		}
	}
	return true
}

func containsASCIIFold(haystack, needle []byte) bool {
	if len(needle) == 0 {
		return true
	}
	if len(needle) > len(haystack) {
		return false
	}

	var stackPrefix [256]int
	var prefix []int
	if len(needle) <= len(stackPrefix) {
		prefix = stackPrefix[:len(needle)]
	} else {
		prefix = make([]int, len(needle))
	}

	for index, matched := 1, 0; index < len(needle); index++ {
		value := asciiLower(needle[index])
		for matched > 0 && value != asciiLower(needle[matched]) {
			matched = prefix[matched-1]
		}
		if value == asciiLower(needle[matched]) {
			matched++
		}
		prefix[index] = matched
	}

	matched := 0
	for index := 0; index < len(haystack); {
		var value byte
		if haystack[index] < utf8.RuneSelf {
			value = asciiLower(haystack[index])
			index++
		} else {
			r, size := utf8.DecodeRune(haystack[index:])
			index += size
			r = unicode.ToLower(r)
			if r >= utf8.RuneSelf {
				matched = 0
				continue
			}
			value = byte(r)
		}

		for matched > 0 && value != asciiLower(needle[matched]) {
			matched = prefix[matched-1]
		}
		if value == asciiLower(needle[matched]) {
			matched++
		}
		if matched == len(needle) {
			return true
		}
	}
	return false
}

func asciiLower(value byte) byte {
	if value >= 'A' && value <= 'Z' {
		return value + ('a' - 'A')
	}
	return value
}

func writeRequestBodyAuditError(c *gin.Context, errCfg config.RequestBodyAuditErrorConfig) {
	errCfg = config.NormalizeRequestBodyAuditError(errCfg)
	body, err := json.Marshal(struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code,omitempty"`
		} `json:"error"`
	}{Error: struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    string `json:"code,omitempty"`
	}{
		Message: errCfg.Message,
		Type:    errCfg.Type,
		Code:    errCfg.Code,
	}})
	if err != nil {
		body = []byte(`{"error":{"message":"Request body was rejected by policy.","type":"invalid_request_error","code":"request_body_blocked"}}`)
	}
	c.Data(errCfg.StatusCode, "application/json; charset=utf-8", body)
	c.Abort()
}

package helps

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var chatGPTWebExplicitWait = regexp.MustCompile(`\bplease (?:wait for|try again in) (an?|[0-9]{1,6}) (seconds?|minutes?|hours?|days?)\b`)

// ChatGPTWebImageRateLimit recognizes the tool's explicit frequency-limit
// failure, not arbitrary assistant text or an empty image result.
func ChatGPTWebImageRateLimit(message string) (bool, *time.Duration) {
	body := strings.ToLower(strings.TrimSpace(message))
	matched := false
	for _, prefix := range []string{"you're generating images too quickly.", "you\u2019re generating images too quickly.", "you are generating images too quickly."} {
		if strings.HasPrefix(body, prefix) {
			matched = true
			break
		}
	}
	if !matched {
		return false, nil
	}
	return true, chatGPTWebRelativeWait(body)
}

// ChatGPTWebUploadLimitRetryAfter extracts a wait only from the structured
// upload-count rejection. Storage-full errors must not enter this path.
func ChatGPTWebUploadLimitRetryAfter(status int, path string, body []byte) *time.Duration {
	if status != http.StatusTooManyRequests || len(body) > 1<<20 {
		return nil
	}
	if path != "/backend-api/files" && path != "/backend-api/files/process_upload_stream" &&
		!(strings.HasPrefix(path, "/backend-api/files/") && strings.HasSuffix(path, "/uploaded")) {
		return nil
	}
	type detail struct {
		Code      string `json:"code"`
		ErrorCode string `json:"error_code"`
		Type      string `json:"type"`
		Message   string `json:"message"`
	}
	var response struct {
		Detail detail `json:"detail"`
		Error  detail `json:"error"`
	}
	if json.Unmarshal(body, &response) != nil {
		return nil
	}
	for _, entry := range []detail{response.Detail, response.Error} {
		if entry.Code != "throttled" && entry.ErrorCode != "throttled" && entry.Type != "throttled" {
			continue
		}
		message := strings.ToLower(strings.TrimSpace(entry.Message))
		if !strings.HasPrefix(message, "you've reached our limit of file uploads.") &&
			!strings.HasPrefix(message, "you\u2019ve reached our limit of file uploads.") {
			continue
		}
		return chatGPTWebRelativeWait(message)
	}
	return nil
}

func chatGPTWebRelativeWait(message string) *time.Duration {
	match := chatGPTWebExplicitWait.FindStringSubmatch(message)
	if len(match) != 3 {
		return nil
	}
	count := int64(1)
	if match[1] != "a" && match[1] != "an" {
		var err error
		count, err = strconv.ParseInt(match[1], 10, 64)
		if err != nil || count <= 0 {
			return nil
		}
	}
	unit := time.Second
	switch strings.TrimSuffix(match[2], "s") {
	case "minute":
		unit = time.Minute
	case "hour":
		unit = time.Hour
	case "day":
		unit = 24 * time.Hour
	}
	// Bound untrusted hints before multiplication; unknown formats keep the
	// existing scheduler backoff rather than inventing a recovery time.
	if count > int64(7*24*time.Hour/unit) {
		return nil
	}
	delay := time.Duration(count) * unit
	return &delay
}

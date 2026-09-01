package handlers

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	executorhelps "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

const chatGPTWebImageErrorSanitizationContextKey = "chatgpt_web_image_error_sanitization"

var (
	safeImageSizePattern     = regexp.MustCompile(`(?i)invalid value for 'size':\s*["']?([0-9]{1,6}x[0-9]{1,6}|auto)["']?`)
	internalImageTaskPattern = regexp.MustCompile(`(?i)\b(?:task|conversation)(?:\s+id)?\b`)
	internalImagePollPattern = regexp.MustCompile(`(?i)\bpoll(?:ed|ing|s)?\b`)
)

type chatGPTWebImageErrorSanitizationSnapshot struct {
	enabled bool
	image   bool
	maxEdge int
	maxN    int
}

type sanitizedImageErrorDetail struct {
	Message string  `json:"message"`
	Type    string  `json:"type"`
	Param   *string `json:"param,omitempty"`
	Code    string  `json:"code,omitempty"`
}

type sanitizedImageErrorResponse struct {
	Error sanitizedImageErrorDetail `json:"error"`
}

type sanitizedImageExecutionError struct {
	cause                   error
	originalStatusCode      int
	originalErrorText       string
	responseStatusRewritten bool
	responseBody            []byte
}

// BeginChatGPTWebImageErrorSanitization captures a fresh policy snapshot for a logical request.
func (h *BaseAPIHandler) BeginChatGPTWebImageErrorSanitization(c *gin.Context, imageRequest bool) {
	if c == nil {
		return
	}
	resolved := config.ChatGPTWebImageConfig{}.Resolved()
	if h != nil && h.Cfg != nil {
		resolved = h.Cfg.Images.ChatGPTWeb.Resolved()
	}
	c.Set(chatGPTWebImageErrorSanitizationContextKey, chatGPTWebImageErrorSanitizationSnapshot{
		enabled: resolved.SanitizeErrorResponses,
		image:   imageRequest,
		maxEdge: resolved.MaxResizeEdgePixels,
		maxN:    resolved.MaxN,
	})
}

func (e *sanitizedImageExecutionError) Error() string {
	if e == nil {
		return ""
	}
	if len(e.responseBody) > 0 {
		return string(e.responseBody)
	}
	if e.cause != nil {
		return e.cause.Error()
	}
	return http.StatusText(e.originalStatusCode)
}

func (e *sanitizedImageExecutionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *sanitizedImageExecutionError) OriginalStatusCode() int {
	if e == nil {
		return 0
	}
	return e.originalStatusCode
}

func (e *sanitizedImageExecutionError) OriginalErrorText() string {
	if e == nil {
		return ""
	}
	return e.originalErrorText
}

func (e *sanitizedImageExecutionError) ResponseStatusRewritten() bool {
	return e != nil && e.responseStatusRewritten
}

func (e *sanitizedImageExecutionError) ErrorResponseRewritten() bool { return e != nil }

func (e *sanitizedImageExecutionError) RewrittenResponseBody() []byte {
	if e == nil || e.responseBody == nil {
		return nil
	}
	return bytes.Clone(e.responseBody)
}

func (e *sanitizedImageExecutionError) ChatGPTWebImageErrorSanitized() bool { return e != nil }

// PinChatGPTWebImageErrorSanitization captures the error policy for one logical request.
// Repeated calls only update whether the current request has entered a Web image path.
func (h *BaseAPIHandler) PinChatGPTWebImageErrorSanitization(c *gin.Context, imageRequest bool) {
	if c == nil {
		return
	}
	if existing, ok := c.Get(chatGPTWebImageErrorSanitizationContextKey); ok {
		if snapshot, valid := existing.(chatGPTWebImageErrorSanitizationSnapshot); valid {
			snapshot.image = imageRequest
			c.Set(chatGPTWebImageErrorSanitizationContextKey, snapshot)
			return
		}
	}
	h.BeginChatGPTWebImageErrorSanitization(c, imageRequest)
}

func chatGPTWebImageErrorSanitizationSnapshotFromGin(c *gin.Context) (chatGPTWebImageErrorSanitizationSnapshot, bool) {
	if c == nil {
		return chatGPTWebImageErrorSanitizationSnapshot{}, false
	}
	value, ok := c.Get(chatGPTWebImageErrorSanitizationContextKey)
	if !ok {
		return chatGPTWebImageErrorSanitizationSnapshot{}, false
	}
	snapshot, ok := value.(chatGPTWebImageErrorSanitizationSnapshot)
	return snapshot, ok
}

// ProjectChatGPTWebImageErrorResponse returns an output-only sanitized error.
// The input remains unchanged for retry, cooldown, logging, and usage classification.
func (h *BaseAPIHandler) ProjectChatGPTWebImageErrorResponse(c *gin.Context, msg *interfaces.ErrorMessage, sourceBody ...[]byte) *interfaces.ErrorMessage {
	snapshot, ok := chatGPTWebImageErrorSanitizationSnapshotFromGin(c)
	if !ok || !snapshot.enabled || !snapshot.image || msg == nil || msg.Error == nil {
		return msg
	}
	originalText := OriginalErrorText(msg)
	if !isChatGPTWebImageError(msg.Error, originalText, sourceBody...) {
		return msg
	}

	status := msg.StatusCode
	if status <= 0 {
		status = http.StatusInternalServerError
	}
	if body, rewritten := RewrittenErrorResponseBody(msg); rewritten && safeSanitizedImageResponseBody(body) {
		return newSanitizedImageErrorMessage(msg, status, body)
	}

	detail := classifySanitizedImageError(msg, snapshot)
	body, errMarshal := json.Marshal(sanitizedImageErrorResponse{Error: detail})
	if errMarshal != nil {
		body = []byte(`{"error":{"message":"An error occurred while processing the image.","type":"server_error","code":"internal_server_error"}}`)
	}
	return newSanitizedImageErrorMessage(msg, status, body)
}

// BuildPublicErrorResponseBody applies the final image sanitizer before encoding an error body.
func (h *BaseAPIHandler) BuildPublicErrorResponseBody(c *gin.Context, msg *interfaces.ErrorMessage, sourceBody ...[]byte) ([]byte, *interfaces.ErrorMessage) {
	projected := h.ProjectChatGPTWebImageErrorResponse(c, msg, sourceBody...)
	status := http.StatusInternalServerError
	errText := http.StatusText(status)
	if projected != nil {
		if projected.StatusCode > 0 {
			status = projected.StatusCode
			errText = http.StatusText(status)
		}
		if projected.Error != nil && strings.TrimSpace(projected.Error.Error()) != "" {
			errText = projected.Error.Error()
		}
	}
	return BuildErrorResponseBodyForMessage(status, errText, projected), projected
}

// IsChatGPTWebImageErrorResponseSanitized reports whether an error is a final public projection.
func IsChatGPTWebImageErrorResponseSanitized(msg *interfaces.ErrorMessage) bool {
	if msg == nil || msg.Error == nil {
		return false
	}
	var sanitized interface{ ChatGPTWebImageErrorSanitized() bool }
	return errors.As(msg.Error, &sanitized) && sanitized.ChatGPTWebImageErrorSanitized()
}

// ChatGPTWebImageErrorSanitizationEnabled reports the pinned public-response policy.
func ChatGPTWebImageErrorSanitizationEnabled(c *gin.Context) bool {
	snapshot, ok := chatGPTWebImageErrorSanitizationSnapshotFromGin(c)
	return ok && snapshot.enabled && snapshot.image
}

// ShouldForwardSanitizedImageErrorHeader applies the public error header allowlist.
func ShouldForwardSanitizedImageErrorHeader(msg *interfaces.ErrorMessage, key, value string) bool {
	if !IsChatGPTWebImageErrorResponseSanitized(msg) {
		return true
	}
	if !strings.EqualFold(strings.TrimSpace(key), "Retry-After") || msg == nil ||
		(msg.StatusCode != http.StatusTooManyRequests && msg.StatusCode != http.StatusServiceUnavailable) {
		return false
	}
	return validRetryAfter(strings.TrimSpace(value))
}

func newSanitizedImageErrorMessage(msg *interfaces.ErrorMessage, status int, body []byte) *interfaces.ErrorMessage {
	originalStatus := OriginalErrorStatusCode(msg)
	if originalStatus <= 0 {
		originalStatus = status
	}
	_, statusRewritten := RewrittenErrorResponseStatus(msg)
	return &interfaces.ErrorMessage{
		StatusCode: status,
		Error: &sanitizedImageExecutionError{
			cause:                   msg.Error,
			originalStatusCode:      originalStatus,
			originalErrorText:       OriginalErrorText(msg),
			responseStatusRewritten: statusRewritten,
			responseBody:            bytes.Clone(body),
		},
		Addon: sanitizedImageErrorAddon(msg.Addon, status),
	}
}

func sanitizedImageErrorAddon(addon http.Header, status int) http.Header {
	filtered := make(http.Header)
	if addon == nil || (status != http.StatusTooManyRequests && status != http.StatusServiceUnavailable) {
		return filtered
	}
	for _, value := range addon.Values("Retry-After") {
		value = strings.TrimSpace(value)
		if validRetryAfter(value) {
			filtered.Add("Retry-After", value)
		}
	}
	return filtered
}

func validRetryAfter(value string) bool {
	if _, errParse := strconv.ParseUint(value, 10, 63); errParse == nil {
		return true
	}
	_, errParse := http.ParseTime(value)
	return errParse == nil
}

func isChatGPTWebImageError(err error, originalText string, sourceBody ...[]byte) bool {
	if err == nil {
		return false
	}
	var classified interface{ ChatGPTWebImageErrorClass() string }
	if errors.As(err, &classified) && strings.TrimSpace(classified.ChatGPTWebImageErrorClass()) != "" {
		return true
	}
	var failureStage interface{ ChatGPTWebFailureStage() string }
	if errors.As(err, &failureStage) && strings.TrimSpace(failureStage.ChatGPTWebFailureStage()) != "" {
		return true
	}
	var coded interface{ ExecutionResultErrorCode() string }
	if errors.As(err, &coded) {
		code := strings.ToLower(strings.TrimSpace(coded.ExecutionResultErrorCode()))
		if strings.HasPrefix(code, "chatgpt_web_image_") || strings.HasPrefix(code, "remote_image_") {
			return true
		}
	}
	var modelled interface{ ExecutionResultModel() string }
	if errors.As(err, &modelled) && strings.EqualFold(strings.TrimSpace(modelled.ExecutionResultModel()), "gpt-image-2") {
		return true
	}
	var mismatch *executorhelps.ChatGPTWebImageMIMEMismatchError
	if errors.As(err, &mismatch) {
		return true
	}
	var remote *executorhelps.ChatGPTWebRemoteImageError
	if errors.As(err, &remote) {
		return true
	}
	if containsChatGPTWebImageProvenance(originalText) {
		return true
	}
	for _, body := range sourceBody {
		if containsChatGPTWebImageProvenance(string(body)) {
			return true
		}
	}
	lower := strings.ToLower(originalText)
	return strings.Contains(lower, "image mime type mismatch") ||
		strings.Contains(lower, "invalid base64 image data") ||
		strings.Contains(lower, "image data url") ||
		strings.Contains(lower, "decode image:")
}

func classifySanitizedImageError(msg *interfaces.ErrorMessage, snapshot chatGPTWebImageErrorSanitizationSnapshot) sanitizedImageErrorDetail {
	status := msg.StatusCode
	if status <= 0 {
		status = http.StatusInternalServerError
	}
	originalText := OriginalErrorText(msg)
	parsed := parseImageErrorDetail(originalText)
	parameter := safeImageErrorParameter(parsed.Param)
	code := strings.ToLower(strings.TrimSpace(parsed.Code))
	var coded interface{ ExecutionResultErrorCode() string }
	if errors.As(msg.Error, &coded) && coded != nil {
		if structuredCode := strings.ToLower(strings.TrimSpace(coded.ExecutionResultErrorCode())); structuredCode != "" {
			code = structuredCode
		}
	}
	lower := strings.ToLower(strings.TrimSpace(parsed.Message))
	if lower == "" {
		lower = strings.ToLower(originalText)
	}

	if code == "moderation_blocked" || strings.Contains(lower, "moderation_blocked") {
		return sanitizedImageErrorDetail{
			Message: "Your request was rejected by the safety system.",
			Type:    "image_generation_user_error",
			Code:    "moderation_blocked",
		}
	}

	var mismatch *executorhelps.ChatGPTWebImageMIMEMismatchError
	if errors.As(msg.Error, &mismatch) {
		declared := executorhelps.CanonicalChatGPTWebImageMIME(mismatch.Declared)
		detected := executorhelps.CanonicalChatGPTWebImageMIME(mismatch.Detected)
		if declared != "" && detected != "" {
			return invalidImageParameter("image", "Invalid value for 'image': declared MIME type "+strconv.Quote(declared)+" does not match detected format "+strconv.Quote(detected)+".")
		}
	}

	var remote *executorhelps.ChatGPTWebRemoteImageError
	if errors.As(msg.Error, &remote) {
		switch remote.Kind {
		case executorhelps.ChatGPTWebRemoteImageInvalid:
			return invalidImageParameter("image_url", "Invalid value for 'image_url': a valid public HTTP(S) URL is required.")
		case executorhelps.ChatGPTWebRemoteImageBlocked:
			return invalidImageParameter("image_url", "Invalid value for 'image_url': the URL is not permitted.")
		case executorhelps.ChatGPTWebRemoteImageTooLarge:
			return invalidImageParameter("image_url", "Invalid value for 'image_url': the downloaded image exceeds the allowed size.")
		}
	}

	class := ""
	var classified interface{ ChatGPTWebImageErrorClass() string }
	if errors.As(msg.Error, &classified) {
		class = strings.ToLower(strings.TrimSpace(classified.ChatGPTWebImageErrorClass()))
	}
	var classParameter interface{ ChatGPTWebImageErrorParameter() string }
	if errors.As(msg.Error, &classParameter) {
		if candidate := safeImageErrorParameter(classParameter.ChatGPTWebImageErrorParameter()); candidate != "" {
			parameter = candidate
		}
	}
	if parameter == "" {
		parameter = safeImageParameterFromMessage(parsed.Message)
	}

	if parameter == "size" || class == "size" {
		maxEdge := snapshot.maxEdge
		if maxEdge <= 0 {
			maxEdge = executorhelps.ChatGPTWebImageHardMaxEdge
		}
		prefix := "Invalid value for 'size'"
		if value := safeImageSizeValue(parsed.Message); value != "" {
			prefix += ": " + strconv.Quote(value)
		}
		message := prefix + ". Width and height must be positive multiples of 16, the aspect ratio must be between 1:3 and 3:1, each edge must not exceed " + strconv.Itoa(maxEdge) + " pixels, and the total pixel count must be between " + strconv.Itoa(executorhelps.ChatGPTWebImageMinPixels) + " and " + strconv.Itoa(executorhelps.ChatGPTWebImageMaxPixels) + "."
		return invalidImageParameter("size", message)
	}
	if parameter == "n" || class == "image_count" {
		maxN := snapshot.maxN
		if maxN <= 0 {
			maxN = config.DefaultChatGPTWebMaxN
		}
		return invalidImageParameter("n", "Invalid value for 'n': it must be an integer between 1 and "+strconv.Itoa(maxN)+".")
	}
	if class == "image_reference" || strings.Contains(lower, "does not support this image reference") || strings.Contains(lower, "only supports base64 image inputs") {
		return unsupportedImageDetail("Unsupported image reference.", "image")
	}
	if class == "mask_reference" || strings.Contains(lower, "image mask reference") {
		return unsupportedImageDetail("Unsupported image mask.", "mask")
	}
	if class == "mask" || strings.Contains(lower, "webp mask") || strings.Contains(lower, "webp image inputs with a mask") {
		return invalidImageParameter("mask", "Invalid value for 'mask': WebP masks are not supported.")
	}
	if class == "unsupported_parameter" || code == "unsupported_parameter" {
		if parameter != "" {
			return unsupportedImageDetail("Unsupported parameter: '"+parameter+"'.", parameter)
		}
		return sanitizedImageErrorDetail{Message: "The image request contains an unsupported parameter.", Type: "invalid_request_error", Code: "unsupported_parameter"}
	}
	if strings.Contains(lower, "does not support image_generation") || strings.Contains(lower, "cannot guarantee image_generation") {
		if parameter != "" {
			return unsupportedImageDetail("Unsupported parameter: '"+parameter+"'.", parameter)
		}
		return sanitizedImageErrorDetail{Message: "The image request contains an unsupported parameter.", Type: "invalid_request_error", Code: "unsupported_parameter"}
	}
	if class == "mime_mismatch" || strings.Contains(lower, "mime type mismatch") || strings.Contains(lower, "declared mime type") {
		param := "image"
		if strings.HasPrefix(code, "remote_image_") || strings.Contains(lower, "remote_image_") {
			param = "image_url"
		}
		declared, detected := safeMIMEPair(parsed.Message)
		if declared != "" && detected != "" {
			return invalidImageParameter(param, "Invalid value for '"+param+"': declared MIME type "+strconv.Quote(declared)+" does not match detected format "+strconv.Quote(detected)+".")
		}
		return invalidImageParameter(param, "Invalid value for '"+param+"': the declared MIME type does not match the image content.")
	}
	if class == "base64" || strings.Contains(lower, "base64 payload") || strings.Contains(lower, "invalid base64") || strings.Contains(lower, "must use base64") {
		return invalidImageParameter(imageParameterOrDefault(parameter, "image"), "Invalid value for '"+imageParameterOrDefault(parameter, "image")+"': the image data is not valid base64.")
	}
	if class == "unsupported_format" || strings.Contains(lower, "unsupported mime type") || strings.Contains(lower, "unsupported image format") || strings.Contains(lower, "invalid mime type") {
		return invalidImageParameter(imageParameterOrDefault(parameter, "image"), "Invalid value for '"+imageParameterOrDefault(parameter, "image")+"': supported formats are JPEG, PNG, GIF, and WebP.")
	}
	if class == "image_too_large" || strings.Contains(lower, "image exceeds") || strings.Contains(lower, "image inputs exceed") {
		param := imageParameterOrDefault(parameter, "image")
		if strings.HasPrefix(code, "remote_image_") || strings.Contains(lower, "remote_image_") {
			param = "image_url"
		}
		if strings.Contains(lower, "items") {
			return invalidImageParameter("image", "Invalid value for 'image': at most "+strconv.Itoa(executorhelps.ChatGPTWebMaxImageInputs)+" image inputs are allowed.")
		}
		return invalidImageParameter(param, "Invalid value for '"+param+"': each image must not exceed "+strconv.Itoa(executorhelps.ChatGPTWebMaxImageBytes)+" bytes and all image inputs together must not exceed "+strconv.Itoa(executorhelps.ChatGPTWebMaxImageRequestBytes)+" bytes.")
	}
	if class == "invalid_value" || code == "invalid_value" {
		if parameter != "" {
			return invalidImageParameter(parameter, "Invalid value for '"+parameter+"'.")
		}
		return sanitizedImageErrorDetail{Message: "Invalid image input.", Type: "invalid_request_error", Code: "invalid_value"}
	}
	if code == "remote_image_url_invalid" || strings.Contains(lower, "remote_image_url_invalid") {
		return invalidImageParameter("image_url", "Invalid value for 'image_url': a valid public HTTP(S) URL is required.")
	}
	if code == "remote_image_url_blocked" || strings.Contains(lower, "remote_image_url_blocked") {
		return invalidImageParameter("image_url", "Invalid value for 'image_url': the URL is not permitted.")
	}
	if code == "remote_image_invalid_content" || strings.Contains(lower, "remote_image_invalid_content") {
		return invalidImageParameter("image_url", "Invalid value for 'image_url': the downloaded content is not a supported image.")
	}
	if strings.Contains(lower, "invalid image data url") {
		return invalidImageParameter(imageParameterOrDefault(parameter, "image"), "Invalid value for '"+imageParameterOrDefault(parameter, "image")+"': a valid base64 image data URL is required.")
	}
	if class == "image_input" || strings.Contains(lower, "decode image:") || strings.Contains(lower, "could not be decoded") || strings.Contains(lower, "unknown format") {
		param := imageParameterOrDefault(parameter, "image")
		return invalidImageParameter(param, "Invalid value for '"+param+"': the image content could not be decoded as JPEG, PNG, GIF, or WebP.")
	}

	if status == http.StatusUnauthorized {
		return sanitizedImageErrorDetail{Message: "Authentication failed for image generation.", Type: "authentication_error", Code: "invalid_api_key"}
	}
	if status == http.StatusForbidden {
		return sanitizedImageErrorDetail{Message: "Permission denied for image generation.", Type: "permission_error", Code: "permission_denied"}
	}
	if status == http.StatusTooManyRequests {
		return sanitizedImageErrorDetail{Message: "Rate limit reached for image generation. Please try again later.", Type: "rate_limit_error", Code: "rate_limit_exceeded"}
	}
	if status == http.StatusServiceUnavailable {
		return sanitizedImageErrorDetail{Message: "The service is temporarily unavailable. Please try again later.", Type: "server_error", Code: "internal_server_error"}
	}
	if status >= http.StatusInternalServerError {
		return sanitizedImageErrorDetail{Message: "An error occurred while processing the image.", Type: "server_error", Code: "internal_server_error"}
	}
	return sanitizedImageErrorDetail{Message: "The image request could not be processed.", Type: "invalid_request_error", Code: "invalid_value"}
}

func invalidImageParameter(parameter, message string) sanitizedImageErrorDetail {
	parameter = safeImageErrorParameter(parameter)
	detail := sanitizedImageErrorDetail{Message: message, Type: "invalid_request_error", Code: "invalid_value"}
	if parameter != "" {
		detail.Param = &parameter
	}
	return detail
}

func unsupportedImageDetail(message, parameter string) sanitizedImageErrorDetail {
	parameter = safeImageErrorParameter(parameter)
	detail := sanitizedImageErrorDetail{Message: message, Type: "invalid_request_error", Code: "unsupported_parameter"}
	if parameter != "" {
		detail.Param = &parameter
	}
	return detail
}

func imageParameterOrDefault(parameter, fallback string) string {
	if parameter = safeImageErrorParameter(parameter); parameter != "" {
		return parameter
	}
	return fallback
}

func safeImageErrorParameter(parameter string) string {
	parameter = strings.ToLower(strings.TrimSpace(parameter))
	switch parameter {
	case "size", "n", "quality", "background", "output_format", "input_fidelity", "moderation",
		"output_compression", "partial_images", "image", "images", "image_url", "mask", "action":
		return parameter
	default:
		return ""
	}
}

func safeImageParameterFromMessage(message string) string {
	lower := strings.ToLower(message)
	for _, parameter := range []string{
		"output_compression", "partial_images", "input_fidelity", "output_format",
		"background", "moderation", "quality", "action", "size", "mask", "n",
	} {
		if strings.Contains(lower, `"`+parameter+`"`) || strings.Contains(lower, "'"+parameter+"'") {
			return parameter
		}
	}
	return ""
}

func safeImageSizeValue(message string) string {
	match := safeImageSizePattern.FindStringSubmatch(message)
	if len(match) != 2 {
		return ""
	}
	return strings.ToLower(match[1])
}

func safeMIMEPair(message string) (string, string) {
	words := strings.FieldsFunc(message, func(r rune) bool {
		return !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') && r != '/'
	})
	values := make([]string, 0, 2)
	for _, word := range words {
		if mimeType := executorhelps.CanonicalChatGPTWebImageMIME(word); mimeType != "" {
			values = append(values, mimeType)
			if len(values) == 2 {
				return values[0], values[1]
			}
		}
	}
	return "", ""
}

func parseImageErrorDetail(text string) struct {
	Message string
	Param   string
	Code    string
} {
	parsed := struct {
		Message string
		Param   string
		Code    string
	}{}
	var envelope struct {
		Error struct {
			Message string  `json:"message"`
			Param   *string `json:"param"`
			Code    string  `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal([]byte(strings.TrimSpace(text)), &envelope) == nil {
		parsed.Message = envelope.Error.Message
		parsed.Code = envelope.Error.Code
		if envelope.Error.Param != nil {
			parsed.Param = *envelope.Error.Param
		}
	}
	if strings.TrimSpace(parsed.Message) == "" {
		parsed.Message = text
	}
	return parsed
}

func safeSanitizedImageResponseBody(body []byte) bool {
	if len(bytes.TrimSpace(body)) == 0 || !json.Valid(body) {
		return false
	}
	var value any
	if json.Unmarshal(body, &value) != nil || containsInternalImageResponseValue(value) {
		return false
	}
	return !containsChatGPTWebImageInternalDetail(string(body))
}

// SanitizeChatGPTWebImageProtocolPayload removes internal fields outside the
// projected error object in Responses SSE and WebSocket terminal envelopes.
func SanitizeChatGPTWebImageProtocolPayload(payload []byte) ([]byte, error) {
	var value any
	if errUnmarshal := json.Unmarshal(payload, &value); errUnmarshal != nil {
		return nil, errUnmarshal
	}
	cleaned, keep := sanitizeInternalImageProtocolValue(value)
	if !keep {
		return nil, errors.New("sanitized image protocol payload has no safe root value")
	}
	encoded, errMarshal := json.Marshal(cleaned)
	if errMarshal != nil {
		return nil, errMarshal
	}
	if containsInternalImageResponseValue(cleaned) || containsChatGPTWebImageInternalDetail(string(encoded)) {
		return nil, errors.New("sanitized image protocol payload still contains internal details")
	}
	return encoded, nil
}

func sanitizeInternalImageProtocolValue(value any) (any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		for key, item := range typed {
			if internalImageResponseKey(key) {
				delete(typed, key)
				continue
			}
			cleaned, keep := sanitizeInternalImageProtocolValue(item)
			if !keep {
				delete(typed, key)
				continue
			}
			typed[key] = cleaned
		}
		return typed, true
	case []any:
		cleaned := make([]any, 0, len(typed))
		for _, item := range typed {
			value, keep := sanitizeInternalImageProtocolValue(item)
			if keep {
				cleaned = append(cleaned, value)
			}
		}
		return cleaned, true
	case string:
		if containsUnsafeImageResponseString(typed) {
			return nil, false
		}
		return typed, true
	default:
		return value, true
	}
}

func internalImageResponseKey(key string) bool {
	normalized := strings.NewReplacer("_", "", "-", "", ".", "").Replace(strings.ToLower(strings.TrimSpace(key)))
	if containsUnsafeImageResponseString(key) {
		return true
	}
	for _, prefix := range []string{"poll", "task", "conversation", "failure", "upstream", "accesstoken", "authorization", "credential"} {
		if strings.HasPrefix(normalized, prefix) {
			return true
		}
	}
	return false
}

func containsInternalImageResponseValue(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, item := range typed {
			if internalImageResponseKey(key) {
				return true
			}
			if containsInternalImageResponseValue(item) {
				return true
			}
		}
	case []any:
		for _, item := range typed {
			if containsInternalImageResponseValue(item) {
				return true
			}
		}
	case string:
		return containsUnsafeImageResponseString(typed)
	}
	return false
}

func containsUnsafeImageResponseString(value string) bool {
	lower := strings.ToLower(value)
	if containsChatGPTWebImageInternalDetail(value) || internalImageTaskPattern.MatchString(value) || internalImagePollPattern.MatchString(value) {
		return true
	}
	for _, marker := range []string{"http://", "https://", "file://", "/tmp/", "/var/", "/users/"} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func containsChatGPTWebImageInternalDetail(value string) bool {
	if containsChatGPTWebImageProvenance(value) {
		return true
	}
	lower := strings.ToLower(value)
	for _, marker := range []string{"image_generation_capacity", "image_memory_capacity"} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func containsChatGPTWebImageProvenance(value string) bool {
	lower := strings.ToLower(value)
	for _, marker := range []string{
		"chatgpt web", "chatgpt-web", "chatgpt_web", "picture_v2",
		"failure_stage", "task_id", "task id", "conversation_id", "conversation id",
		"poll_stalled", "polling", "poll failed", "poll state", "poll status", "remote_image_",
		"upstream_response", "upstream_body", "upstream_url", "access_token", "authorization",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

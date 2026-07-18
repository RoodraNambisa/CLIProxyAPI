package executor

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/color"
	_ "image/gif"
	_ "image/jpeg"
	"image/png"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	"github.com/google/uuid"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	log "github.com/sirupsen/logrus"
	_ "golang.org/x/image/webp"
)

const (
	chatGPTWebMaxImageBytes             = helps.ChatGPTWebMaxImageBytes
	chatGPTWebMaxImageRequestBytes      = helps.ChatGPTWebMaxImageRequestBytes
	chatGPTWebMaxImageResponseBytes     = 32 << 20
	chatGPTWebMaxImageEditDecodedBytes  = 96 << 20
	chatGPTWebDecodedImageBytesPerPixel = 8
	chatGPTWebImageEditBytesPerPixel    = 16
	chatGPTWebMaxImagePixels            = chatGPTWebMaxImageEditDecodedBytes / chatGPTWebDecodedImageBytesPerPixel
	chatGPTWebImageDownloadAccept       = "image/png,image/jpeg,image/gif,image/webp,application/octet-stream;q=0.8"
	chatGPTWebMaxAssetRedirects         = 5
	chatGPTWebAssetSettleAttempts       = 4
	chatGPTWebImageMaxPollAttempts      = 180
	chatGPTWebImageStreamMaxBytes       = 128 << 20
	chatGPTWebImageStreamMaxEvents      = 65_536
	chatGPTWebPollResponseMaxBytes      = 128 << 20
)

var chatGPTWebAssetHostSuffixes = []string{
	"blob.core.windows.net",
	"chatgpt.com",
	"oaiusercontent.com",
	"oaistatic.com",
	"openai.com",
}

type chatGPTWebUploadedImage struct {
	FileID   string
	FileName string
	MIMEType string
	Size     int
	Width    int
	Height   int
}

type chatGPTWebAssetTransportError struct {
	statusErr
	cause error
}

func (e chatGPTWebAssetTransportError) Unwrap() error { return e.cause }

type chatGPTWebImageExecution struct {
	response *fhttp.Response
	headers  http.Header
	inputIDs map[string]struct{}
}

type chatGPTWebPollResponseBudget struct {
	maxResponseBytes int
	remainingBytes   int
}

func newChatGPTWebPollResponseBudget(maxBytes int) *chatGPTWebPollResponseBudget {
	return &chatGPTWebPollResponseBudget{
		maxResponseBytes: maxBytes,
		remainingBytes:   maxBytes,
	}
}

func newChatGPTWebPollResponseLimit(maxBytes int) *chatGPTWebPollResponseBudget {
	return &chatGPTWebPollResponseBudget{
		maxResponseBytes: maxBytes,
		remainingBytes:   -1,
	}
}

func (budget *chatGPTWebPollResponseBudget) consume(size int) error {
	if budget == nil || size <= 0 {
		return nil
	}
	if budget.maxResponseBytes < 1 || size > budget.maxResponseBytes {
		return &helps.ChatGPTWebResponseLimitError{
			Message: fmt.Sprintf("chatgpt web polling response exceeds %d bytes", budget.maxResponseBytes),
		}
	}
	if budget.remainingBytes >= 0 {
		if size > budget.remainingBytes {
			return &helps.ChatGPTWebResponseLimitError{
				Message: fmt.Sprintf("chatgpt web polling responses exceed %d total bytes", budget.maxResponseBytes),
			}
		}
		budget.remainingBytes -= size
	}
	return nil
}

func (e *ChatGPTWebExecutor) buildChatGPTWebConversationMessages(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, messages []helps.ChatGPTWebMessage) ([]map[string]any, error) {
	output := make([]map[string]any, 0, len(messages))
	for index := range messages {
		message := &messages[index]
		if strings.TrimSpace(message.ID) == "" {
			message.ID = uuid.NewString()
		}
		parts := make([]any, 0, len(message.Parts))
		uploads := make([]chatGPTWebUploadedImage, 0, len(message.Parts))
		for index, part := range message.Parts {
			if part.Text != "" {
				parts = append(parts, part.Text)
			}
			if strings.TrimSpace(part.ImageURL) == "" {
				continue
			}
			uploaded, err := e.uploadChatGPTWebImage(ctx, client, credential, part.ImageURL, fmt.Sprintf("image_%d.png", index+1))
			if err != nil {
				return nil, err
			}
			uploads = append(uploads, uploaded)
			parts = append(parts, map[string]any{
				"content_type":  "image_asset_pointer",
				"asset_pointer": "file-service://" + uploaded.FileID,
				"width":         uploaded.Width,
				"height":        uploaded.Height,
				"size_bytes":    uploaded.Size,
			})
		}
		item := map[string]any{
			"id":     message.ID,
			"author": map[string]any{"role": message.Role},
		}
		if len(uploads) == 0 {
			item["content"] = map[string]any{
				"content_type": "text",
				"parts":        []string{chatGPTWebTextParts(parts)},
			}
		} else {
			attachments := make([]any, 0, len(uploads))
			for _, uploaded := range uploads {
				attachments = append(attachments, chatGPTWebAttachment(uploaded))
			}
			item["content"] = map[string]any{"content_type": "multimodal_text", "parts": parts}
			item["metadata"] = map[string]any{"attachments": attachments}
		}
		output = append(output, item)
	}
	return output, nil
}

func chatGPTWebTextParts(parts []any) string {
	var text strings.Builder
	for _, part := range parts {
		if value, ok := part.(string); ok {
			text.WriteString(value)
		}
	}
	return text.String()
}

func (e *ChatGPTWebExecutor) executeChatGPTWebImage(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, prepared *chatGPTWebPreparedRequest) ([]byte, http.Header, error) {
	execution, err := e.beginChatGPTWebImage(ctx, client, credential, prepared)
	if err != nil {
		return nil, nil, err
	}
	completed, err := e.finishChatGPTWebImage(ctx, client, credential, prepared, execution)
	return completed, execution.headers, chatGPTWebCommittedRequestError(ctx, err)
}

func (e *ChatGPTWebExecutor) beginChatGPTWebImage(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, prepared *chatGPTWebPreparedRequest) (*chatGPTWebImageExecution, error) {
	imageRequest := prepared.request.Image
	if imageRequest == nil {
		return nil, errors.New("chatgpt web image request is nil")
	}
	upstreamPrompt := strings.TrimSpace(imageRequest.Prompt)
	imageInputs := append([]string(nil), imageRequest.Images...)
	if strings.TrimSpace(imageRequest.MaskURL) != "" {
		if len(imageInputs) == 0 {
			return nil, statusErr{code: http.StatusBadRequest, msg: "image mask requires an input image", skipAuthResult: true}
		}
		maskImageIndex := imageRequest.MaskImageIndex
		if maskImageIndex < 0 || maskImageIndex >= len(imageInputs) {
			return nil, statusErr{code: http.StatusBadRequest, msg: "image mask target is invalid", skipAuthResult: true}
		}
		composited, err := compositeChatGPTWebMask(imageInputs[maskImageIndex], imageRequest.MaskURL)
		if err != nil {
			var unsupportedTool *helps.ChatGPTWebUnsupportedToolError
			return nil, statusErr{
				code:           http.StatusBadRequest,
				msg:            err.Error(),
				skipAuthResult: true,
				retryOtherAuth: errors.As(err, &unsupportedTool),
			}
		}
		imageInputs[maskImageIndex] = composited
	}
	uploads := make([]chatGPTWebUploadedImage, 0, len(imageInputs))
	inputIDs := make(map[string]struct{}, len(imageInputs))
	for index, imageURL := range imageInputs {
		uploaded, err := e.uploadChatGPTWebImage(ctx, client, credential, imageURL, fmt.Sprintf("image_%d.png", index+1))
		if err != nil {
			return nil, err
		}
		uploads = append(uploads, uploaded)
		inputIDs[uploaded.FileID] = struct{}{}
	}

	requirements, err := e.chatGPTWebRequirements(ctx, client, credential)
	if err != nil {
		return nil, err
	}
	conduit, err := e.prepareChatGPTWebImageConversation(ctx, client, credential, requirements, upstreamPrompt)
	if err != nil {
		return nil, err
	}
	response, err := e.openChatGPTWebImageConversation(ctx, client, credential, requirements, conduit, upstreamPrompt, uploads)
	if err != nil {
		return nil, err
	}
	return &chatGPTWebImageExecution{
		response: response,
		headers:  cloneChatGPTWebHeaders(response.Header),
		inputIDs: inputIDs,
	}, nil
}

func (e *ChatGPTWebExecutor) finishChatGPTWebImage(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, prepared *chatGPTWebPreparedRequest, execution *chatGPTWebImageExecution) ([]byte, error) {
	if execution == nil || execution.response == nil {
		return nil, errors.New("chatgpt web image execution is nil")
	}
	imageRequest := prepared.request.Image
	response := execution.response
	accumulator := &helps.ChatGPTWebImageAccumulator{}
	errStream := consumeChatGPTWebImageStream(ctx, response.Body, accumulator)
	if errClose := response.Body.Close(); errClose != nil {
		log.Errorf("chatgpt web executor: close image response body: %v", errClose)
	}
	if errStream != nil {
		return nil, errStream
	}
	if accumulator.FailureStatus != "" {
		return nil, chatGPTWebImageFailureError(accumulator.FailureStatus)
	}
	filterChatGPTWebInputImageIDs(accumulator, execution.inputIDs)
	hasStreamOutput := len(accumulator.FileIDs) > 0 || len(accumulator.SedimentIDs) > 0
	if strings.TrimSpace(accumulator.ConversationID) == "" {
		if !accumulator.Terminal {
			return nil, statusErr{
				code:           http.StatusBadGateway,
				msg:            "chatgpt web image stream ended without an explicit terminal state or conversation ID",
				skipAuthResult: true,
				retryOtherAuth: true,
			}
		}
		if !hasStreamOutput {
			return nil, statusErr{
				code:           http.StatusBadGateway,
				msg:            "chatgpt web image generation completed without an image",
				skipAuthResult: true,
				retryOtherAuth: true,
			}
		}
	} else if !accumulator.Terminal || !hasStreamOutput {
		if err := e.pollChatGPTWebImageConversation(ctx, client, credential, accumulator, execution.inputIDs, hasStreamOutput); err != nil {
			if hasStreamOutput {
				return nil, statusErr{
					code:           http.StatusBadGateway,
					msg:            "chatgpt web image conversation did not settle: " + err.Error(),
					skipAuthResult: true,
					retryOtherAuth: true,
				}
			}
			return nil, err
		}
	}
	if accumulator.FailureStatus != "" {
		return nil, chatGPTWebImageFailureError(accumulator.FailureStatus)
	}
	if !accumulator.Terminal {
		return nil, statusErr{
			code:           http.StatusBadGateway,
			msg:            "chatgpt web image conversation ended without an explicit terminal state",
			skipAuthResult: true,
			retryOtherAuth: true,
		}
	}
	images, err := e.downloadChatGPTWebImagesLimited(ctx, client, credential, accumulator, prepared.maxImageResults)
	if err != nil {
		return nil, err
	}
	if len(images) == 0 {
		return nil, statusErr{code: http.StatusBadGateway, msg: "chatgpt web did not return image output"}
	}
	completed, err := buildChatGPTWebImageCompletedEvent(prepared.routeModel, imageRequest.OutputFormat, images)
	if err != nil {
		return nil, err
	}
	return completed, nil
}

func chatGPTWebImageFailureError(status string) error {
	return statusErr{
		code:           http.StatusBadGateway,
		msg:            "chatgpt web image generation failed: " + strings.TrimSpace(status),
		skipAuthResult: true,
		retryOtherAuth: true,
	}
}

func buildChatGPTWebImageCompletedEvent(routeModel, requestedFormat string, images [][]byte) ([]byte, error) {
	requestedFormat = normalizeChatGPTWebImageOutputFormat(requestedFormat)
	totalBytes := 0
	for index, imageData := range images {
		outputFormat := chatGPTWebImageOutputFormat(imageData)
		if outputFormat == "" {
			return nil, statusErr{
				code:           http.StatusBadGateway,
				msg:            "chatgpt web returned an unrecognized image format",
				skipAuthResult: true,
				retryOtherAuth: true,
			}
		}
		if requestedFormat == "png" && outputFormat != requestedFormat {
			converted, err := convertChatGPTWebImageToPNG(imageData, outputFormat)
			if err != nil {
				return nil, fmt.Errorf("convert chatgpt web %s image to png: %w", outputFormat, err)
			}
			imageData = converted
			images[index] = converted
			outputFormat = "png"
		}
		if requestedFormat != "" && requestedFormat != outputFormat {
			return nil, statusErr{
				code:           http.StatusBadGateway,
				msg:            fmt.Sprintf("chatgpt web returned %s image data instead of requested %s", outputFormat, requestedFormat),
				skipAuthResult: true,
				retryOtherAuth: true,
			}
		}
		if len(imageData) > chatGPTWebMaxImageResponseBytes-totalBytes {
			return nil, statusErr{
				code:           http.StatusBadGateway,
				msg:            fmt.Sprintf("chatgpt web image response exceeds %d bytes", chatGPTWebMaxImageResponseBytes),
				skipAuthResult: true,
			}
		}
		totalBytes += len(imageData)
	}
	var output bytes.Buffer
	output.Grow(base64.StdEncoding.EncodedLen(totalBytes) + 1024*len(images) + 256)
	output.WriteString(`{"type":"response.completed","response":{"id":`)
	writeChatGPTWebJSONString(&output, "resp_"+strings.ReplaceAll(uuid.NewString(), "-", ""))
	output.WriteString(`,"object":"response","created_at":`)
	_, _ = fmt.Fprintf(&output, "%d", time.Now().Unix())
	output.WriteString(`,"status":"completed","model":`)
	writeChatGPTWebJSONString(&output, routeModel)
	output.WriteString(`,"output":[`)

	for index, imageData := range images {
		outputFormat := chatGPTWebImageOutputFormat(imageData)
		if index > 0 {
			output.WriteByte(',')
		}
		output.WriteString(`{"type":"image_generation_call","id":`)
		writeChatGPTWebJSONString(&output, "ig_"+strings.ReplaceAll(uuid.NewString(), "-", ""))
		output.WriteString(`,"status":"completed","result":"`)
		encoder := base64.NewEncoder(base64.StdEncoding, &output)
		if _, err := encoder.Write(imageData); err != nil {
			return nil, fmt.Errorf("encode chatgpt web image result: %w", err)
		}
		if err := encoder.Close(); err != nil {
			return nil, fmt.Errorf("finish chatgpt web image result: %w", err)
		}
		images[index] = nil
		output.WriteString(`","output_format":`)
		writeChatGPTWebJSONString(&output, outputFormat)
		output.WriteByte('}')
	}
	output.WriteString(`]}}`)
	return output.Bytes(), nil
}

func convertChatGPTWebImageToPNG(data []byte, outputFormat string) ([]byte, error) {
	mimeType := "image/" + strings.TrimSpace(outputFormat)
	if outputFormat == "jpeg" {
		mimeType = "image/jpeg"
	}
	decoded, _, err := decodeAndValidateChatGPTWebImage(data, mimeType)
	if err != nil {
		return nil, err
	}
	var output bytes.Buffer
	if err = png.Encode(&output, decoded); err != nil {
		return nil, err
	}
	if output.Len() > chatGPTWebMaxImageResponseBytes {
		return nil, fmt.Errorf("converted image exceeds %d bytes", chatGPTWebMaxImageResponseBytes)
	}
	return output.Bytes(), nil
}

func writeChatGPTWebJSONString(output *bytes.Buffer, value string) {
	encoded, _ := json.Marshal(value)
	_, _ = output.Write(encoded)
}

func (e *ChatGPTWebExecutor) prepareChatGPTWebImageConversation(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, requirements chatGPTWebRequirements, prompt string) (string, error) {
	path := "/backend-api/f/conversation/prepare"
	headers := chatGPTWebRequirementsHeaders(e.chatGPTWebHeaders(credential, path, nil), requirements)
	headers["accept"] = "*/*"
	headers["content-type"] = "application/json"
	body := map[string]any{
		"action":                 "next",
		"fork_from_shared_post":  false,
		"parent_message_id":      uuid.NewString(),
		"model":                  "gpt-5-3",
		"client_prepare_state":   "success",
		"timezone_offset_min":    -480,
		"timezone":               "Asia/Shanghai",
		"conversation_mode":      map[string]any{"kind": "primary_assistant"},
		"system_hints":           []string{"picture_v2"},
		"partial_query":          chatGPTWebUserTextMessage(prompt),
		"supports_buffering":     true,
		"supported_encodings":    []string{"v1"},
		"client_contextual_info": map[string]any{"app_name": "chatgpt.com"},
	}
	_, data, err := e.doChatGPTWebJSONWithHeaders(ctx, client, credential, path, headers, body)
	if err != nil {
		return "", err
	}
	token := strings.TrimSpace(gjsonString(data, "conduit_token"))
	if token == "" {
		return "", chatGPTWebLocalProtocolError(
			http.StatusBadGateway,
			"chatgpt web image prepare response is missing conduit token",
		)
	}
	return token, nil
}

func (e *ChatGPTWebExecutor) openChatGPTWebImageConversation(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, requirements chatGPTWebRequirements, conduit, prompt string, uploads []chatGPTWebUploadedImage) (*fhttp.Response, error) {
	path := "/backend-api/f/conversation"
	headers := chatGPTWebRequirementsHeaders(e.chatGPTWebHeaders(credential, path, nil), requirements)
	headers["accept"] = "text/event-stream"
	headers["content-type"] = "application/json"
	headers["x-conduit-token"] = conduit
	headers["x-oai-turn-trace-id"] = uuid.NewString()
	parts := make([]any, 0, len(uploads)+1)
	attachments := make([]any, 0, len(uploads))
	for _, uploaded := range uploads {
		parts = append(parts, map[string]any{
			"content_type":  "image_asset_pointer",
			"asset_pointer": "file-service://" + uploaded.FileID,
			"width":         uploaded.Width,
			"height":        uploaded.Height,
			"size_bytes":    uploaded.Size,
		})
		attachments = append(attachments, chatGPTWebAttachment(uploaded))
	}
	parts = append(parts, prompt)
	content := map[string]any{"content_type": "text", "parts": []string{prompt}}
	if len(uploads) > 0 {
		content = map[string]any{"content_type": "multimodal_text", "parts": parts}
	}
	metadata := map[string]any{
		"developer_mode_connector_ids": []any{},
		"selected_github_repos":        []any{},
		"selected_all_github_repos":    false,
		"system_hints":                 []string{"picture_v2"},
		"serialization_metadata":       map[string]any{"custom_symbol_offsets": []any{}},
	}
	if len(attachments) > 0 {
		metadata["attachments"] = attachments
	}
	body := map[string]any{
		"action": "next",
		"messages": []any{map[string]any{
			"id": uuid.NewString(), "author": map[string]any{"role": "user"}, "create_time": float64(time.Now().UnixNano()) / 1e9,
			"content": content, "metadata": metadata,
		}},
		"parent_message_id":                    uuid.NewString(),
		"model":                                "gpt-5-3",
		"client_prepare_state":                 "sent",
		"timezone_offset_min":                  -480,
		"timezone":                             "Asia/Shanghai",
		"conversation_mode":                    map[string]any{"kind": "primary_assistant"},
		"enable_message_followups":             true,
		"system_hints":                         []string{"picture_v2"},
		"supports_buffering":                   true,
		"supported_encodings":                  []string{"v1"},
		"client_contextual_info":               chatGPTWebClientContext(),
		"paragen_cot_summary_display_override": "allow",
		"force_parallel_switch":                "auto",
	}
	return e.doChatGPTWebJSONStream(ctx, client, credential, path, headers, body)
}

func (e *ChatGPTWebExecutor) uploadChatGPTWebImage(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, imageURL, fileName string) (chatGPTWebUploadedImage, error) {
	data, mimeType, err := decodeChatGPTWebImageReference(imageURL)
	if err != nil {
		return chatGPTWebUploadedImage{}, statusErr{code: http.StatusBadRequest, msg: err.Error(), skipAuthResult: true}
	}
	_, config, err := decodeAndValidateChatGPTWebImage(data, mimeType)
	if err != nil {
		return chatGPTWebUploadedImage{}, statusErr{code: http.StatusBadRequest, msg: "decode image: " + err.Error(), skipAuthResult: true}
	}
	if extension := extensionForChatGPTWebMIME(mimeType); extension != "" {
		fileName = strings.TrimSuffix(fileName, filepath.Ext(fileName)) + extension
	}
	path := "/backend-api/files"
	_, metadataData, err := e.doChatGPTWebJSON(ctx, client, credential, path, map[string]any{
		"file_name": fileName,
		"file_size": len(data),
		"use_case":  "multimodal",
		"width":     config.Width,
		"height":    config.Height,
	})
	if err != nil {
		return chatGPTWebUploadedImage{}, chatGPTWebAssetNetworkError(ctx, "signing", err)
	}
	var metadata map[string]any
	if err := json.Unmarshal(metadataData, &metadata); err != nil {
		return chatGPTWebUploadedImage{}, chatGPTWebLocalProtocolError(
			http.StatusBadGateway,
			"decode chatgpt web upload metadata: "+err.Error(),
		)
	}
	fileID := strings.TrimSpace(fmt.Sprint(metadata["file_id"]))
	uploadURL := strings.TrimSpace(fmt.Sprint(metadata["upload_url"]))
	if fileID == "" || fileID == "<nil>" || uploadURL == "" || uploadURL == "<nil>" {
		return chatGPTWebUploadedImage{}, chatGPTWebLocalProtocolError(
			http.StatusBadGateway,
			"chatgpt web upload response is incomplete",
		)
	}
	uploadHeaders := map[string]string{
		"content-type":    mimeType,
		"x-ms-blob-type":  "BlockBlob",
		"x-ms-version":    "2020-04-08",
		"origin":          e.chatGPTWebBaseURL(),
		"referer":         e.chatGPTWebBaseURL() + "/",
		"accept":          "application/json, text/plain, */*",
		"accept-language": credential.Persona.AcceptLanguage,
	}
	response, finalUploadURL, err := e.doChatGPTWebAssetRequest(ctx, client, credential, http.MethodPut, uploadURL, uploadHeaders, data, true)
	if err != nil {
		sanitizedErr := newChatGPTWebAssetTransportError(ctx, "upload", err)
		helps.RecordAPIResponseError(ctx, e.configSnapshot(), sanitizedErr)
		return chatGPTWebUploadedImage{}, sanitizedErr
	}
	payload, errRead := readChatGPTWebErrorBody(response.Body)
	errClose := response.Body.Close()
	if errRead != nil || errClose != nil {
		cause := errRead
		if cause == nil {
			cause = errClose
		}
		sanitizedErr := newChatGPTWebAssetTransportError(ctx, "upload response", cause)
		helps.RecordAPIResponseError(ctx, e.configSnapshot(), sanitizedErr)
		return chatGPTWebUploadedImage{}, sanitizedErr
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return chatGPTWebUploadedImage{}, newChatGPTWebAssetStatusError(response.StatusCode, finalUploadURL, payload, response.Header)
	}
	confirmPath := "/backend-api/files/" + fileID + "/uploaded"
	if _, _, err := e.doChatGPTWebJSON(ctx, client, credential, confirmPath, map[string]any{}); err != nil {
		return chatGPTWebUploadedImage{}, chatGPTWebAssetNetworkError(ctx, "confirmation", err)
	}
	return chatGPTWebUploadedImage{
		FileID: fileID, FileName: fileName, MIMEType: mimeType, Size: len(data), Width: config.Width, Height: config.Height,
	}, nil
}

func (e *ChatGPTWebExecutor) doChatGPTWebAssetRequest(
	ctx context.Context,
	client *chatgptwebauth.Client,
	credential *chatgptwebauth.Credential,
	method string,
	targetURL string,
	headers map[string]string,
	data []byte,
	signedUpload bool,
) (*fhttp.Response, string, error) {
	if client == nil {
		return nil, "", errors.New("chatgpt web asset client is nil")
	}
	currentURL, err := e.validateChatGPTWebAssetURL(targetURL)
	if err != nil {
		return nil, "", err
	}
	for redirects := 0; ; redirects++ {
		var body io.Reader
		if len(data) > 0 {
			body = bytes.NewReader(data)
		}
		e.recordChatGPTWebRequest(ctx, credential, method, currentURL.String(), headers, nil)
		response, errRequest := client.DoNoRedirectStream(ctx, method, currentURL.String(), headers, body)
		if errRequest != nil {
			return nil, currentURL.String(), errRequest
		}
		helps.RecordAPIResponseMetadata(ctx, e.configSnapshot(), response.StatusCode, chatGPTWebResponseLogHeaders(response.Header))
		if !chatGPTWebAssetRedirectStatus(method, response.StatusCode, signedUpload) {
			return response, currentURL.String(), nil
		}
		if redirects >= chatGPTWebMaxAssetRedirects {
			_ = response.Body.Close()
			return nil, currentURL.String(), errors.New("chatgpt web asset redirect limit exceeded")
		}
		location := strings.TrimSpace(response.Header.Get("Location"))
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, chatGPTWebMaxErrorBodyBytes))
		if errClose := response.Body.Close(); errClose != nil {
			return nil, currentURL.String(), fmt.Errorf("close chatgpt web asset redirect response: %w", errClose)
		}
		if location == "" {
			return nil, currentURL.String(), errors.New("chatgpt web asset redirect is missing location")
		}
		locationURL, errLocation := url.Parse(location)
		if errLocation != nil {
			return nil, currentURL.String(), errors.New("chatgpt web asset redirect is invalid")
		}
		nextURL, errValidate := e.validateChatGPTWebAssetURL(currentURL.ResolveReference(locationURL).String())
		if errValidate != nil {
			return nil, currentURL.String(), errValidate
		}
		if signedUpload {
			if errRedirect := validateChatGPTWebSignedUploadRedirect(currentURL, nextURL); errRedirect != nil {
				return nil, currentURL.String(), errRedirect
			}
		}
		currentURL = nextURL
	}
}

func chatGPTWebAssetRedirectStatus(method string, status int, signedUpload bool) bool {
	if signedUpload || !strings.EqualFold(strings.TrimSpace(method), http.MethodGet) {
		return status == http.StatusTemporaryRedirect || status == http.StatusPermanentRedirect
	}
	switch status {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther,
		http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return true
	default:
		return false
	}
}

func (e *ChatGPTWebExecutor) validateChatGPTWebAssetURL(rawURL string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed == nil || parsed.Host == "" || parsed.Hostname() == "" {
		return nil, errors.New("chatgpt web asset URL is invalid")
	}
	if parsed.User != nil {
		return nil, errors.New("chatgpt web asset URL credentials are not allowed")
	}
	scheme := strings.ToLower(strings.TrimSpace(parsed.Scheme))
	if scheme != "http" && scheme != "https" {
		return nil, errors.New("chatgpt web asset URL scheme is invalid")
	}
	baseURL, _ := url.Parse(e.chatGPTWebBaseURL())
	if sameChatGPTWebAssetOrigin(baseURL, parsed) {
		return parsed, nil
	}
	if scheme != "https" {
		return nil, errors.New("chatgpt web asset URL must use HTTPS")
	}
	if port := parsed.Port(); port != "" && port != "443" {
		return nil, errors.New("chatgpt web asset URL port is not allowed")
	}
	host := strings.ToLower(strings.TrimSuffix(strings.TrimSpace(parsed.Hostname()), "."))
	if net.ParseIP(host) != nil || !chatGPTWebAssetHostAllowed(host) {
		return nil, errors.New("chatgpt web asset URL host is not allowed")
	}
	return parsed, nil
}

func sameChatGPTWebAssetOrigin(left, right *url.URL) bool {
	if left == nil || right == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(left.Scheme), strings.TrimSpace(right.Scheme)) &&
		strings.EqualFold(strings.TrimSpace(left.Host), strings.TrimSpace(right.Host))
}

func chatGPTWebAssetHostAllowed(host string) bool {
	host = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(host), "."))
	for _, suffix := range chatGPTWebAssetHostSuffixes {
		if host == suffix || strings.HasSuffix(host, "."+suffix) {
			return true
		}
	}
	return false
}

func validateChatGPTWebSignedUploadRedirect(currentURL, nextURL *url.URL) error {
	if currentURL == nil || nextURL == nil {
		return errors.New("chatgpt web upload redirect is invalid")
	}
	nextScheme := strings.ToLower(strings.TrimSpace(nextURL.Scheme))
	if nextScheme != "http" && nextScheme != "https" {
		return errors.New("chatgpt web upload redirect scheme is invalid")
	}
	currentScheme := strings.ToLower(strings.TrimSpace(currentURL.Scheme))
	if currentScheme == "https" && nextScheme != "https" {
		return errors.New("chatgpt web upload redirect HTTPS downgrade is not allowed")
	}
	if !strings.EqualFold(strings.TrimSpace(currentURL.Host), strings.TrimSpace(nextURL.Host)) {
		return errors.New("chatgpt web upload redirect host is not allowed")
	}
	return nil
}

func chatGPTWebImageConfig(data []byte, mimeType string) (image.Config, error) {
	config, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err == nil {
		return config, validateChatGPTWebImageConfig(config)
	}
	if !strings.EqualFold(strings.TrimSpace(mimeType), "image/webp") {
		return image.Config{}, err
	}
	width, height, ok := chatGPTWebWebPDimensions(data)
	if !ok {
		return image.Config{}, err
	}
	config = image.Config{Width: width, Height: height}
	return config, validateChatGPTWebImageConfig(config)
}

func decodeAndValidateChatGPTWebImage(data []byte, mimeType string) (image.Image, image.Config, error) {
	config, err := chatGPTWebImageConfig(data, mimeType)
	if err != nil {
		return nil, image.Config{}, err
	}
	decoded, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, image.Config{}, err
	}
	bounds := decoded.Bounds()
	if bounds.Dx() != config.Width || bounds.Dy() != config.Height {
		return nil, image.Config{}, errors.New("decoded image dimensions do not match its header")
	}
	if err := validateChatGPTWebImageConfig(image.Config{Width: bounds.Dx(), Height: bounds.Dy()}); err != nil {
		return nil, image.Config{}, err
	}
	return decoded, config, nil
}

func validateChatGPTWebImageConfig(config image.Config) error {
	if config.Width <= 0 || config.Height <= 0 {
		return errors.New("image dimensions must be positive")
	}
	pixels := int64(config.Width) * int64(config.Height)
	if pixels > chatGPTWebMaxImagePixels {
		return fmt.Errorf("image dimensions exceed %d pixels", chatGPTWebMaxImagePixels)
	}
	return nil
}

func chatGPTWebWebPDimensions(data []byte) (int, int, bool) {
	if len(data) < 30 || string(data[:4]) != "RIFF" || string(data[8:12]) != "WEBP" {
		return 0, 0, false
	}
	switch string(data[12:16]) {
	case "VP8X":
		width := 1 + int(data[24]) + int(data[25])<<8 + int(data[26])<<16
		height := 1 + int(data[27]) + int(data[28])<<8 + int(data[29])<<16
		return width, height, width > 0 && height > 0
	case "VP8L":
		if len(data) < 25 || data[20] != 0x2f {
			return 0, 0, false
		}
		width := 1 + int(data[21]) + int(data[22]&0x3f)<<8
		height := 1 + int(data[22]>>6) + int(data[23])<<2 + int(data[24]&0x0f)<<10
		return width, height, width > 0 && height > 0
	case "VP8 ":
		for index := 20; index+9 < len(data); index++ {
			if data[index] == 0x9d && data[index+1] == 0x01 && data[index+2] == 0x2a {
				width := int(binary.LittleEndian.Uint16(data[index+3:index+5]) & 0x3fff)
				height := int(binary.LittleEndian.Uint16(data[index+5:index+7]) & 0x3fff)
				return width, height, width > 0 && height > 0
			}
		}
	}
	return 0, 0, false
}

func consumeChatGPTWebImageStream(ctx context.Context, body io.Reader, accumulator *helps.ChatGPTWebImageAccumulator) error {
	return consumeChatGPTWebImageStreamWithLimits(
		ctx,
		body,
		accumulator,
		chatGPTWebImageStreamMaxBytes,
		chatGPTWebImageStreamMaxEvents,
	)
}

func consumeChatGPTWebImageStreamWithLimits(ctx context.Context, body io.Reader, accumulator *helps.ChatGPTWebImageAccumulator, maxBytes, maxEvents int) error {
	decoder := helps.NewChatGPTWebSSEDecoder(chatGPTWebSSEMaxFrameBytes)
	buffer := make([]byte, 32<<10)
	totalBytes := 0
	eventCount := 0
	applyPayloads := func(payloads [][]byte) (bool, error) {
		for _, payload := range payloads {
			eventCount++
			if maxEvents > 0 && eventCount > maxEvents {
				return false, &helps.ChatGPTWebResponseLimitError{
					Message: "chatgpt web image stream exceeds the event limit",
				}
			}
			done, err := accumulator.Apply(payload)
			if err != nil {
				return false, err
			}
			if done {
				return true, nil
			}
		}
		return false, nil
	}
	for {
		count, errRead := body.Read(buffer)
		if count > 0 {
			if maxBytes > 0 && count > maxBytes-totalBytes {
				return &helps.ChatGPTWebResponseLimitError{
					Message: "chatgpt web image stream exceeds the response limit",
				}
			}
			totalBytes += count
			payloads, err := decoder.Feed(buffer[:count], false)
			if err != nil {
				return err
			}
			done, errConsume := applyPayloads(payloads)
			if errConsume != nil {
				return errConsume
			}
			if done {
				return nil
			}
		}
		if errRead != nil {
			if !errors.Is(errRead, io.EOF) {
				return errRead
			}
			payloads, err := decoder.Feed(nil, true)
			if err != nil {
				return err
			}
			done, errConsume := applyPayloads(payloads)
			if errConsume != nil {
				return errConsume
			}
			if done {
				return nil
			}
			if ctx != nil && ctx.Err() != nil {
				return ctx.Err()
			}
			return helps.IncompleteStreamError("chatgpt web image")
		}
	}
}

func (e *ChatGPTWebExecutor) pollChatGPTWebImageConversation(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, accumulator *helps.ChatGPTWebImageAccumulator, inputIDs map[string]struct{}, hasInitialOutput bool) error {
	if hasInitialOutput {
		if err := waitForChatGPTWebPoll(ctx, e.imageSettleWait); err != nil {
			return err
		}
	} else if err := waitForChatGPTWebPoll(ctx, e.imageInitialWait); err != nil {
		return err
	}
	maxPolls := e.imageMaxPolls
	if maxPolls <= 0 {
		maxPolls = chatGPTWebImageMaxPollAttempts
	}
	responseBudget := newChatGPTWebPollResponseBudget(chatGPTWebPollResponseMaxBytes)
	for pollAttempt := 1; pollAttempt <= maxPolls; pollAttempt++ {
		path := "/backend-api/conversation/" + accumulator.ConversationID
		_, payload, err := e.doChatGPTWebPollGET(ctx, client, credential, path, nil, responseBudget)
		if err == nil {
			if errCapture := helps.CaptureChatGPTWebImageConversation(payload, accumulator); errCapture != nil {
				return errCapture
			}
			filterChatGPTWebInputImageIDs(accumulator, inputIDs)
			if accumulator.FailureStatus != "" {
				return chatGPTWebImageFailureError(accumulator.FailureStatus)
			}
			if len(accumulator.FileIDs) > 0 || len(accumulator.SedimentIDs) > 0 {
				if accumulator.Terminal {
					return nil
				}
				if errWait := waitForChatGPTWebPoll(ctx, e.imageSettleWait); errWait != nil {
					return errWait
				}
				continue
			}
			if accumulator.Terminal {
				return statusErr{
					code:           http.StatusBadGateway,
					msg:            "chatgpt web image generation completed without an image",
					skipAuthResult: true,
					retryOtherAuth: true,
				}
			}
		} else {
			if ctx != nil && ctx.Err() != nil {
				return ctx.Err()
			}
			delay, retryable := chatGPTWebPollRetryDelay(err, e.imagePollInterval)
			if !retryable {
				return err
			}
			if pollAttempt >= maxPolls {
				return err
			}
			if errWait := waitForChatGPTWebPoll(ctx, delay); errWait != nil {
				return errWait
			}
			continue
		}
		if pollAttempt >= maxPolls {
			break
		}
		if errWait := waitForChatGPTWebPoll(ctx, e.imagePollInterval); errWait != nil {
			return errWait
		}
	}
	if len(accumulator.FileIDs) > 0 || len(accumulator.SedimentIDs) > 0 {
		return nil
	}
	return statusErr{
		code:           http.StatusBadGateway,
		msg:            fmt.Sprintf("chatgpt web image generation remained incomplete after %d polls", maxPolls),
		skipAuthResult: true,
		retryOtherAuth: true,
	}
}

func chatGPTWebPollRetryDelay(err error, fallback time.Duration) (time.Duration, bool) {
	var limitErr *helps.ChatGPTWebResponseLimitError
	if errors.As(err, &limitErr) {
		return 0, false
	}
	statusCode := statusCodeFromError(err)
	retryable := statusCode == 0 || statusCode == http.StatusNotFound || statusCode == http.StatusConflict ||
		statusCode == http.StatusLocked || statusCode == http.StatusTooManyRequests ||
		(statusCode >= http.StatusInternalServerError && statusCode <= 599)
	if !retryable {
		return 0, false
	}
	var retryAfter interface{ RetryAfter() *time.Duration }
	if errors.As(err, &retryAfter) {
		if delay := retryAfter.RetryAfter(); delay != nil && *delay >= 0 {
			return *delay, true
		}
	}
	if fallback < 0 {
		fallback = 0
	}
	return fallback, true
}

func waitForChatGPTWebPoll(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		if ctx != nil {
			return ctx.Err()
		}
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	if ctx == nil {
		<-timer.C
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (e *ChatGPTWebExecutor) downloadChatGPTWebImages(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, accumulator *helps.ChatGPTWebImageAccumulator) ([][]byte, error) {
	return e.downloadChatGPTWebImagesLimited(ctx, client, credential, accumulator, 0)
}

func (e *ChatGPTWebExecutor) downloadChatGPTWebImagesLimited(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, accumulator *helps.ChatGPTWebImageAccumulator, maxResults int) ([][]byte, error) {
	references := accumulator.References
	if len(references) == 0 {
		references = make([]helps.ChatGPTWebImageReference, 0, len(accumulator.FileIDs)+len(accumulator.SedimentIDs))
		for _, fileID := range accumulator.FileIDs {
			references = append(references, helps.ChatGPTWebImageReference{Kind: "file", ID: fileID})
		}
		for _, sedimentID := range accumulator.SedimentIDs {
			references = append(references, helps.ChatGPTWebImageReference{Kind: "sediment", ID: sedimentID})
		}
	}
	urls := make([]string, 0, len(references))
	for _, reference := range references {
		if maxResults > 0 && len(urls) >= maxResults {
			break
		}
		var path string
		switch reference.Kind {
		case "file":
			if reference.ID == "file_upload" {
				continue
			}
			path = "/backend-api/files/" + reference.ID + "/download"
		case "sediment":
			if strings.TrimSpace(accumulator.ConversationID) == "" {
				return nil, chatGPTWebImageOutputProtocolError("chatgpt web sediment image output is missing a conversation ID")
			}
			path = "/backend-api/conversation/" + accumulator.ConversationID + "/attachment/" + reference.ID + "/download"
		default:
			continue
		}
		downloadURL, err := e.resolveChatGPTWebImageDownloadURL(ctx, client, credential, path)
		if err != nil {
			return nil, err
		}
		urls = append(urls, downloadURL)
	}
	images := make([][]byte, 0, len(urls))
	totalBytes := 0
	for _, downloadURL := range urls {
		remainingBytes := chatGPTWebMaxImageResponseBytes - totalBytes
		if remainingBytes <= 0 {
			return nil, statusErr{
				code:           http.StatusBadGateway,
				msg:            fmt.Sprintf("chatgpt web image response exceeds %d bytes", chatGPTWebMaxImageResponseBytes),
				skipAuthResult: true,
			}
		}
		downloadLimit := min(chatGPTWebMaxImageBytes, remainingBytes)
		payload, err := e.downloadChatGPTWebImageAsset(ctx, client, credential, downloadURL, downloadLimit)
		if err != nil {
			return nil, err
		}
		if totalBytes > chatGPTWebMaxImageResponseBytes-len(payload) {
			return nil, statusErr{
				code:           http.StatusBadGateway,
				msg:            fmt.Sprintf("chatgpt web image response exceeds %d bytes", chatGPTWebMaxImageResponseBytes),
				skipAuthResult: true,
			}
		}
		totalBytes += len(payload)
		images = append(images, payload)
	}
	return images, nil
}

func (e *ChatGPTWebExecutor) resolveChatGPTWebImageDownloadURL(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, path string) (string, error) {
	var lastErr error
	for attempt := 0; attempt < chatGPTWebAssetSettleAttempts; attempt++ {
		_, payload, err := e.doChatGPTWebGET(ctx, client, credential, path, nil)
		if err == nil {
			if downloadURL := firstChatGPTWebURL(payload); downloadURL != "" {
				return downloadURL, nil
			}
			err = chatGPTWebImageOutputProtocolError("chatgpt web image output metadata is missing a download URL")
		} else {
			err = chatGPTWebAssetNetworkError(ctx, "download URL", err)
		}
		lastErr = err
		delay, retryable := chatGPTWebAssetRetryDelay(err, e.imageSettleWait)
		if !retryable {
			return "", err
		}
		if attempt+1 >= chatGPTWebAssetSettleAttempts {
			return "", chatGPTWebFinalAssetError(err)
		}
		if errWait := waitForChatGPTWebPoll(ctx, delay); errWait != nil {
			return "", errWait
		}
	}
	return "", chatGPTWebFinalAssetError(lastErr)
}

func (e *ChatGPTWebExecutor) downloadChatGPTWebImageAsset(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, downloadURL string, maxBytes int) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt < chatGPTWebAssetSettleAttempts; attempt++ {
		payload, err, retryable := e.downloadChatGPTWebImageAssetOnce(ctx, client, credential, downloadURL, maxBytes)
		if err == nil {
			return payload, nil
		}
		lastErr = err
		if !retryable {
			return nil, err
		}
		if attempt+1 >= chatGPTWebAssetSettleAttempts {
			return nil, chatGPTWebFinalAssetError(err)
		}
		delay, _ := chatGPTWebAssetRetryDelay(err, e.imageSettleWait)
		if errWait := waitForChatGPTWebPoll(ctx, delay); errWait != nil {
			return nil, errWait
		}
	}
	return nil, chatGPTWebFinalAssetError(lastErr)
}

func (e *ChatGPTWebExecutor) downloadChatGPTWebImageAssetOnce(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, downloadURL string, maxBytes int) ([]byte, error, bool) {
	downloadHeaders := map[string]string{"accept": chatGPTWebImageDownloadAccept}
	response, finalDownloadURL, err := e.doChatGPTWebAssetRequest(ctx, client, credential, http.MethodGet, downloadURL, downloadHeaders, nil, false)
	if err != nil {
		sanitizedErr := newChatGPTWebAssetTransportError(ctx, "download", err)
		helps.RecordAPIResponseError(ctx, e.configSnapshot(), sanitizedErr)
		return nil, sanitizedErr, true
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		payload := readAndCloseChatGPTWebErrorBody(response.Body)
		statusErr := newChatGPTWebAssetStatusError(response.StatusCode, finalDownloadURL, payload, response.Header)
		return nil, statusErr, chatGPTWebAssetSettleStatusRetryable(response.StatusCode)
	}
	contentType := response.Header.Get("Content-Type")
	payload, errRead := readChatGPTWebBoundedBody(response.Body, maxBytes)
	errClose := response.Body.Close()
	if errRead != nil {
		return nil, newChatGPTWebAssetTransportError(ctx, "download response", errRead), false
	}
	if errClose != nil {
		return nil, newChatGPTWebAssetTransportError(ctx, "download response", errClose), false
	}
	if len(payload) == 0 {
		return nil, chatGPTWebImageOutputProtocolError("chatgpt web image download is empty"), true
	}
	if errValidate := validateChatGPTWebDownloadedImage(payload, contentType); errValidate != nil {
		return nil, chatGPTWebImageOutputProtocolError("chatgpt web image download is invalid: " + errValidate.Error()), true
	}
	return payload, nil, false
}

func chatGPTWebAssetRetryDelay(err error, fallback time.Duration) (time.Duration, bool) {
	statusCode := statusCodeFromError(err)
	if statusCode != 0 && !chatGPTWebAssetSettleStatusRetryable(statusCode) {
		return 0, false
	}
	var retryAfter interface{ RetryAfter() *time.Duration }
	if errors.As(err, &retryAfter) {
		if delay := retryAfter.RetryAfter(); delay != nil && *delay >= 0 {
			return *delay, true
		}
	}
	if fallback < 0 {
		fallback = 0
	}
	return fallback, true
}

func chatGPTWebAssetSettleStatusRetryable(code int) bool {
	switch code {
	case http.StatusNotFound, http.StatusRequestTimeout, http.StatusConflict,
		http.StatusLocked, http.StatusTooEarly, http.StatusTooManyRequests:
		return true
	default:
		return code >= http.StatusInternalServerError
	}
}

func chatGPTWebFinalAssetError(err error) error {
	if err == nil {
		return statusErr{
			code:           http.StatusBadGateway,
			msg:            "chatgpt web image asset did not become available",
			skipAuthResult: true,
		}
	}
	var transportErr chatGPTWebAssetTransportError
	if errors.As(err, &transportErr) {
		transportErr.skipAuthResult = true
		transportErr.retryOtherAuth = false
		return transportErr
	}
	var httpErr chatGPTWebHTTPError
	if errors.As(err, &httpErr) {
		httpErr.statusErr.skipAuthResult = true
		httpErr.statusErr.retryOtherAuth = false
		return httpErr
	}
	var localErr statusErr
	if errors.As(err, &localErr) {
		localErr.skipAuthResult = true
		localErr.retryOtherAuth = false
		return localErr
	}
	return chatGPTWebAssetTransportError{
		statusErr: statusErr{
			code:           http.StatusBadGateway,
			msg:            "chatgpt web image asset request failed",
			skipAuthResult: true,
		},
		cause: sanitizeChatGPTWebAssetTransportCause(err),
	}
}

func chatGPTWebCommittedRequestError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	var transportErr chatGPTWebAssetTransportError
	if errors.As(err, &transportErr) {
		transportErr.skipAuthResult = true
		transportErr.retryOtherAuth = false
		return transportErr
	}
	var httpErr chatGPTWebHTTPError
	if errors.As(err, &httpErr) {
		httpErr.statusErr.skipAuthResult = true
		httpErr.statusErr.retryOtherAuth = false
		return httpErr
	}
	var localErr statusErr
	if errors.As(err, &localErr) {
		localErr.skipAuthResult = true
		localErr.retryOtherAuth = false
		return localErr
	}
	var limitErr *helps.ChatGPTWebResponseLimitError
	if errors.As(err, &limitErr) {
		return err
	}
	code := statusCodeFromError(err)
	if code == 0 {
		code = http.StatusBadGateway
	}
	return statusErr{
		code:           code,
		msg:            err.Error(),
		skipAuthResult: true,
	}
}

func validateChatGPTWebDownloadedImage(data []byte, contentType string) error {
	mimeType := strings.TrimSpace(contentType)
	if parsed, _, err := mime.ParseMediaType(mimeType); err == nil {
		mimeType = parsed
	}
	_, _, err := decodeAndValidateChatGPTWebImage(data, mimeType)
	return err
}

func chatGPTWebImageOutputProtocolError(message string) error {
	return statusErr{
		code:           http.StatusBadGateway,
		msg:            message,
		skipAuthResult: true,
		retryOtherAuth: true,
	}
}

func readChatGPTWebBoundedBody(body io.Reader, maxBytes int) ([]byte, error) {
	if body == nil {
		return nil, errors.New("chatgpt web response body is nil")
	}
	if maxBytes < 1 {
		return nil, errors.New("chatgpt web response body limit is invalid")
	}
	payload, err := io.ReadAll(io.LimitReader(body, int64(maxBytes)+1))
	if err != nil {
		return nil, fmt.Errorf("read chatgpt web response body: %w", err)
	}
	if len(payload) > maxBytes {
		return nil, fmt.Errorf("chatgpt web image exceeds %d bytes", maxBytes)
	}
	return payload, nil
}

func (e *ChatGPTWebExecutor) doChatGPTWebGET(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, path string, extra map[string]string) (*fhttp.Response, []byte, error) {
	return e.doChatGPTWebGETWithBudget(ctx, client, credential, path, extra, nil, true)
}

func (e *ChatGPTWebExecutor) doChatGPTWebPollGET(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, path string, extra map[string]string, budget *chatGPTWebPollResponseBudget) (*fhttp.Response, []byte, error) {
	return e.doChatGPTWebGETWithBudget(ctx, client, credential, path, extra, budget, false)
}

func (e *ChatGPTWebExecutor) doChatGPTWebGETWithBudget(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, path string, extra map[string]string, budget *chatGPTWebPollResponseBudget, logBody bool) (*fhttp.Response, []byte, error) {
	headers := e.chatGPTWebHeaders(credential, path, extra)
	e.recordChatGPTWebRequest(ctx, credential, http.MethodGet, path, headers, nil)
	response, err := client.DoNoRedirectStream(ctx, http.MethodGet, e.chatGPTWebBaseURL()+path, headers, nil)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.configSnapshot(), err)
		return nil, nil, err
	}
	helps.RecordAPIResponseMetadata(ctx, e.configSnapshot(), response.StatusCode, chatGPTWebResponseLogHeaders(response.Header))
	data, err := readChatGPTWebResponseBody(response, chatGPTWebMaxJSONBodyBytes)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.configSnapshot(), err)
		return response, nil, err
	}
	if err = budget.consume(len(data)); err != nil {
		helps.RecordAPIResponseError(ctx, e.configSnapshot(), err)
		return response, nil, err
	}
	sanitizedData := chatGPTWebResponseLogBody(path, data)
	if logBody {
		helps.AppendAPIResponseChunk(ctx, e.configSnapshot(), sanitizedData)
	} else {
		helps.AppendAPIResponseChunk(ctx, e.configSnapshot(), []byte("<chatgpt web polling response body omitted>"))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return response, nil, newChatGPTWebStatusError(response.StatusCode, path, sanitizedData, response.Header)
	}
	return response, data, nil
}

func chatGPTWebAssetNetworkError(ctx context.Context, action string, err error) error {
	if err == nil || statusCodeFromError(err) != 0 {
		return err
	}
	return newChatGPTWebAssetTransportError(ctx, action, err)
}

func newChatGPTWebAssetTransportError(ctx context.Context, action string, cause error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return chatGPTWebAssetTransportError{
		statusErr: statusErr{
			code:           http.StatusBadGateway,
			msg:            "chatgpt web image asset " + strings.TrimSpace(action) + " failed",
			skipAuthResult: true,
			retryOtherAuth: true,
		},
		cause: sanitizeChatGPTWebAssetTransportCause(cause),
	}
}

func sanitizeChatGPTWebAssetTransportCause(cause error) error {
	var urlError *url.Error
	if !errors.As(cause, &urlError) {
		return cause
	}
	return &url.Error{
		Op:  urlError.Op,
		URL: "<redacted>",
		Err: sanitizeChatGPTWebAssetTransportCause(urlError.Err),
	}
}

func newChatGPTWebAssetStatusError(code int, path string, body []byte, headers fhttp.Header) chatGPTWebHTTPError {
	err := newChatGPTWebStatusError(code, path, body, headers)
	err.statusErr.skipAuthResult = true
	err.statusErr.retryOtherAuth = chatGPTWebAssetStatusRetryable(code)
	return err
}

func chatGPTWebAssetStatusRetryable(code int) bool {
	switch code {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound,
		http.StatusRequestTimeout, http.StatusConflict, http.StatusLocked,
		http.StatusTooEarly, http.StatusTooManyRequests:
		return true
	default:
		return code >= http.StatusInternalServerError
	}
}

func chatGPTWebRequirementsHeaders(headers map[string]string, requirements chatGPTWebRequirements) map[string]string {
	headers["openai-sentinel-chat-requirements-token"] = requirements.Token
	if requirements.ProofToken != "" {
		headers["openai-sentinel-proof-token"] = requirements.ProofToken
	}
	if requirements.TurnstileToken != "" {
		headers["openai-sentinel-turnstile-token"] = requirements.TurnstileToken
	}
	if requirements.SOToken != "" {
		headers["openai-sentinel-so-token"] = requirements.SOToken
	}
	return headers
}

func chatGPTWebUserTextMessage(prompt string) map[string]any {
	return map[string]any{
		"id": uuid.NewString(), "author": map[string]any{"role": "user"},
		"content": map[string]any{"content_type": "text", "parts": []string{prompt}},
	}
}

func chatGPTWebAttachment(uploaded chatGPTWebUploadedImage) map[string]any {
	return map[string]any{
		"id": uploaded.FileID, "mimeType": uploaded.MIMEType, "name": uploaded.FileName,
		"size": uploaded.Size, "width": uploaded.Width, "height": uploaded.Height,
	}
}

func decodeChatGPTWebImageReference(value string) ([]byte, string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, "", errors.New("image reference is empty")
	}
	mimeType := "image/png"
	payload := value
	if strings.HasPrefix(strings.ToLower(value), "data:") {
		comma := strings.IndexByte(value, ',')
		if comma < 0 {
			return nil, "", errors.New("invalid image data URL")
		}
		metadata := value[5:comma]
		payload = value[comma+1:]
		if semicolon := strings.IndexByte(metadata, ';'); semicolon >= 0 {
			mimeType = strings.TrimSpace(metadata[:semicolon])
		} else if strings.TrimSpace(metadata) != "" {
			mimeType = strings.TrimSpace(metadata)
		}
		if !strings.Contains(strings.ToLower(metadata), ";base64") {
			return nil, "", errors.New("image data URL must use base64 encoding")
		}
	}
	if _, err := chatGPTWebEncodedImageSize(payload, chatGPTWebMaxImageBytes); err != nil {
		return nil, "", err
	}
	data, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return nil, "", fmt.Errorf("decode image base64: %w", err)
	}
	if !strings.HasPrefix(strings.ToLower(mimeType), "image/") {
		return nil, "", fmt.Errorf("unsupported image MIME type %q", mimeType)
	}
	return data, strings.ToLower(mimeType), nil
}

func validateChatGPTWebImageRequest(request *helps.ChatGPTWebImageRequest) error {
	if request == nil {
		return nil
	}
	if strings.TrimSpace(request.Size) != "" {
		return statusErr{
			code:           http.StatusBadRequest,
			msg:            "chatgpt web does not support an exact image size",
			skipAuthResult: true,
			retryOtherAuth: true,
		}
	}
	if quality := strings.TrimSpace(request.Quality); quality != "" && !strings.EqualFold(quality, "auto") {
		return statusErr{
			code:           http.StatusBadRequest,
			msg:            "chatgpt web does not support an exact image quality",
			skipAuthResult: true,
			retryOtherAuth: true,
		}
	}
	references := append([]string(nil), request.Images...)
	if mask := strings.TrimSpace(request.MaskURL); mask != "" {
		references = append(references, mask)
	}
	for _, reference := range references {
		reference = strings.TrimSpace(reference)
		if strings.Contains(reference, "://") && !strings.HasPrefix(strings.ToLower(reference), "data:") {
			return statusErr{
				code:           http.StatusBadRequest,
				msg:            "chatgpt web only supports base64 image inputs",
				skipAuthResult: true,
				retryOtherAuth: true,
			}
		}
	}
	if err := helps.ValidateChatGPTWebImageReferences(references, chatGPTWebMaxImageBytes, chatGPTWebMaxImageRequestBytes); err != nil {
		return statusErr{
			code:           http.StatusRequestEntityTooLarge,
			msg:            err.Error(),
			skipAuthResult: true,
		}
	}
	return nil
}

func chatGPTWebEncodedImageSize(value string, maxBytes int) (int, error) {
	return helps.ChatGPTWebEncodedImageSize(value, maxBytes)
}

func compositeChatGPTWebMask(imageURL, maskURL string) (string, error) {
	imageData, imageMIME, err := decodeChatGPTWebImageReference(imageURL)
	if err != nil {
		return "", err
	}
	maskData, maskMIME, err := decodeChatGPTWebImageReference(maskURL)
	if err != nil {
		return "", err
	}
	if imageMIME == "image/webp" || maskMIME == "image/webp" {
		return "", &helps.ChatGPTWebUnsupportedToolError{
			Message: "WebP mask compositing is not supported for ChatGPT Web; use PNG, JPEG, or GIF",
		}
	}
	sourceConfig, err := chatGPTWebImageConfig(imageData, imageMIME)
	if err != nil {
		return "", fmt.Errorf("decode input image dimensions: %w", err)
	}
	maskConfig, err := chatGPTWebImageConfig(maskData, maskMIME)
	if err != nil {
		return "", fmt.Errorf("decode image mask dimensions: %w", err)
	}
	if sourceConfig.Width != maskConfig.Width || sourceConfig.Height != maskConfig.Height {
		return "", errors.New("image mask dimensions must match the input image")
	}
	if err := validateChatGPTWebImageEditMemory(sourceConfig); err != nil {
		return "", err
	}
	source, _, err := image.Decode(bytes.NewReader(imageData))
	if err != nil {
		return "", fmt.Errorf("decode input image: %w", err)
	}
	mask, _, err := image.Decode(bytes.NewReader(maskData))
	if err != nil {
		return "", fmt.Errorf("decode image mask: %w", err)
	}
	bounds := source.Bounds()
	if mask.Bounds().Dx() != bounds.Dx() || mask.Bounds().Dy() != bounds.Dy() {
		return "", errors.New("image mask dimensions must match the input image")
	}
	result := &chatGPTWebMaskedImage{
		source: source,
		mask:   mask,
		bounds: image.Rect(0, 0, bounds.Dx(), bounds.Dy()),
	}
	var output strings.Builder
	output.WriteString("data:image/png;base64,")
	encoder := base64.NewEncoder(base64.StdEncoding, &output)
	if err := png.Encode(encoder, result); err != nil {
		return "", fmt.Errorf("encode masked image: %w", err)
	}
	if err := encoder.Close(); err != nil {
		return "", fmt.Errorf("finish masked image encoding: %w", err)
	}
	return output.String(), nil
}

func validateChatGPTWebImageEditMemory(config image.Config) error {
	pixels := int64(config.Width) * int64(config.Height)
	if pixels > int64(chatGPTWebMaxImageEditDecodedBytes/chatGPTWebImageEditBytesPerPixel) {
		return fmt.Errorf("image edit decoded data exceeds %d bytes", chatGPTWebMaxImageEditDecodedBytes)
	}
	return nil
}

type chatGPTWebMaskedImage struct {
	source image.Image
	mask   image.Image
	bounds image.Rectangle
}

func (*chatGPTWebMaskedImage) ColorModel() color.Model {
	return color.NRGBAModel
}

func (masked *chatGPTWebMaskedImage) Bounds() image.Rectangle {
	return masked.bounds
}

func (masked *chatGPTWebMaskedImage) At(x, y int) color.Color {
	sourceBounds := masked.source.Bounds()
	maskBounds := masked.mask.Bounds()
	sourceColor := color.NRGBAModel.Convert(masked.source.At(
		sourceBounds.Min.X+x-masked.bounds.Min.X,
		sourceBounds.Min.Y+y-masked.bounds.Min.Y,
	)).(color.NRGBA)
	_, _, _, maskAlpha := masked.mask.At(
		maskBounds.Min.X+x-masked.bounds.Min.X,
		maskBounds.Min.Y+y-masked.bounds.Min.Y,
	).RGBA()
	sourceColor.A = uint8((uint32(sourceColor.A)*maskAlpha + 0x7fff) / 0xffff)
	return sourceColor
}

func filterChatGPTWebInputImageIDs(accumulator *helps.ChatGPTWebImageAccumulator, inputIDs map[string]struct{}) {
	filtered := accumulator.FileIDs[:0]
	for _, fileID := range accumulator.FileIDs {
		if _, input := inputIDs[fileID]; input {
			continue
		}
		filtered = append(filtered, fileID)
	}
	accumulator.FileIDs = filtered
	filteredReferences := accumulator.References[:0]
	for _, reference := range accumulator.References {
		if reference.Kind == "file" {
			if _, input := inputIDs[reference.ID]; input {
				continue
			}
		}
		filteredReferences = append(filteredReferences, reference)
	}
	accumulator.References = filteredReferences
}

func normalizeChatGPTWebImageOutputFormat(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return "png"
	case "jpg", "jpeg":
		return "jpeg"
	case "png", "gif", "webp":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return ""
	}
}

func chatGPTWebImageOutputFormat(data []byte) string {
	if len(data) >= 12 && string(data[:4]) == "RIFF" && string(data[8:12]) == "WEBP" {
		return "webp"
	}
	switch http.DetectContentType(data) {
	case "image/png":
		return "png"
	case "image/jpeg":
		return "jpeg"
	case "image/gif":
		return "gif"
	case "image/webp":
		return "webp"
	default:
		return ""
	}
}

func extensionForChatGPTWebMIME(mimeType string) string {
	switch strings.ToLower(strings.TrimSpace(mimeType)) {
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	default:
		extensions, _ := mime.ExtensionsByType(mimeType)
		if len(extensions) > 0 {
			return extensions[0]
		}
		return ""
	}
}

func firstChatGPTWebURL(payload []byte) string {
	var root map[string]any
	if err := json.Unmarshal(payload, &root); err != nil {
		return ""
	}
	for _, key := range []string{"download_url", "url"} {
		if value := strings.TrimSpace(fmt.Sprint(root[key])); value != "" && value != "<nil>" {
			return value
		}
	}
	return ""
}

func gjsonString(payload []byte, path string) string {
	var root map[string]any
	if err := json.Unmarshal(payload, &root); err != nil {
		return ""
	}
	value := root[path]
	if value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func statusCodeFromError(err error) int {
	if status, ok := err.(interface{ StatusCode() int }); ok {
		return status.StatusCode()
	}
	return 0
}

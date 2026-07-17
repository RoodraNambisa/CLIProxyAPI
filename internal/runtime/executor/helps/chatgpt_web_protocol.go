package helps

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
)

var (
	chatGPTWebImageReferenceIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	chatGPTWebImageFileIDPattern      = regexp.MustCompile(`^file_00000000[a-f0-9]{24}$`)
)

const (
	// ChatGPTWebMaxImageInputs bounds the number of upstream upload operations
	// created by one request.
	ChatGPTWebMaxImageInputs = 16
	// ChatGPTWebMaxImageBytes is the per-image decoded size accepted by the
	// ChatGPT Web runtime.
	ChatGPTWebMaxImageBytes = 50 << 20
	// ChatGPTWebMaxImageRequestBytes is the total decoded image input size.
	ChatGPTWebMaxImageRequestBytes = 100 << 20
	// ChatGPTWebMaxEncodedImageRequestBytes bounds a canonical image request
	// before translation and JSON decoding multiply its memory footprint.
	ChatGPTWebMaxEncodedImageRequestBytes = ((ChatGPTWebMaxImageRequestBytes + 2) / 3 * 4) + (8 << 20)
	// ChatGPTWebMaxRequestBytes bounds every raw request before translation,
	// cloning, and JSON decoding multiply its memory footprint.
	ChatGPTWebMaxRequestBytes = ChatGPTWebMaxEncodedImageRequestBytes
	// ChatGPTWebMaxTextRequestBytes bounds requests that do not contain image
	// inputs.
	ChatGPTWebMaxTextRequestBytes       = 16 << 20
	chatGPTWebMaxConversationTextBytes  = 16 << 20
	chatGPTWebMaxConversationEventBytes = 32 << 20
	chatGPTWebMaxConversationEvents     = 32_768
	chatGPTWebMaxImageOutputReferences  = 32
)

// ChatGPTWebUnsupportedToolError reports a tool declaration that this provider
// cannot execute but another provider may support.
type ChatGPTWebUnsupportedToolError struct {
	Message string
}

func (err *ChatGPTWebUnsupportedToolError) Error() string {
	if err == nil {
		return "chatgpt web does not support the selected tool"
	}
	return err.Message
}

// ChatGPTWebUnsupportedRequestError reports a valid request feature that this
// provider cannot execute but another provider may support.
type ChatGPTWebUnsupportedRequestError struct {
	Message string
}

func (err *ChatGPTWebUnsupportedRequestError) Error() string {
	if err == nil {
		return "chatgpt web does not support the request"
	}
	return err.Message
}

// IsChatGPTWebProviderUnsupported reports whether another provider may support
// the rejected request feature.
func IsChatGPTWebProviderUnsupported(err error) bool {
	var unsupportedTool *ChatGPTWebUnsupportedToolError
	if errors.As(err, &unsupportedTool) {
		return true
	}
	var unsupportedRequest *ChatGPTWebUnsupportedRequestError
	return errors.As(err, &unsupportedRequest)
}

// ChatGPTWebResponseLimitError reports an upstream response that exceeds a
// bounded runtime representation.
type ChatGPTWebResponseLimitError struct {
	Message string
}

func (err *ChatGPTWebResponseLimitError) Error() string {
	if err == nil {
		return "chatgpt web response exceeds the configured limit"
	}
	return err.Message
}

func (*ChatGPTWebResponseLimitError) StatusCode() int      { return 502 }
func (*ChatGPTWebResponseLimitError) SkipAuthResult() bool { return true }
func (*ChatGPTWebResponseLimitError) RetryOtherAuth() bool { return false }

// ChatGPTWebContentPart is a normalized text or image input part.
type ChatGPTWebContentPart struct {
	Text     string
	ImageURL string
}

// ChatGPTWebMessage is a normalized Responses message used by the web client.
type ChatGPTWebMessage struct {
	ID    string
	Role  string
	Parts []ChatGPTWebContentPart
}

// ChatGPTWebImageRequest describes an image_generation request embedded in a
// canonical Responses payload.
type ChatGPTWebImageRequest struct {
	Prompt       string
	Images       []string
	MaskURL      string
	Size         string
	Quality      string
	Action       string
	OutputFormat string
}

// ChatGPTWebRequest is the subset of canonical Responses understood by the
// ChatGPT Web upstream.
type ChatGPTWebRequest struct {
	Model           string
	Messages        []ChatGPTWebMessage
	ReasoningEffort string
	WebSearch       bool
	Image           *ChatGPTWebImageRequest
}

// ChatGPTWebSSEDecoder reconstructs SSE data payloads across arbitrary network
// chunk boundaries. Event, id, retry and comment fields are ignored.
type ChatGPTWebSSEDecoder struct {
	pendingLine []byte
	frame       []byte
	hasData     bool
	maxBytes    int
	feedErr     error
}

// NewChatGPTWebSSEDecoder creates a bounded decoder. The default limit is 50 MiB
// because generated image and conversation patch events can be large.
func NewChatGPTWebSSEDecoder(maxBytes int) *ChatGPTWebSSEDecoder {
	if maxBytes <= 0 {
		maxBytes = 50 << 20
	}
	return &ChatGPTWebSSEDecoder{maxBytes: maxBytes}
}

// Feed consumes bytes and emits every complete SSE data payload.
func (decoder *ChatGPTWebSSEDecoder) Feed(chunk []byte, flush bool) ([][]byte, error) {
	if decoder == nil {
		return nil, errors.New("chatgpt web SSE decoder is nil")
	}
	decoder.feedErr = nil
	var payloads [][]byte
	err := ObserveSSELines(&decoder.pendingLine, chunk, flush, decoder.maxBytes, func(line []byte) {
		line = bytes.TrimSuffix(line, []byte{'\r'})
		if len(line) == 0 {
			if decoder.hasData {
				payloads = append(payloads, decoder.frame)
			}
			decoder.frame = nil
			decoder.hasData = false
			return
		}
		if line[0] == ':' {
			return
		}
		field, value, found := bytes.Cut(line, []byte{':'})
		if !found || !bytes.Equal(field, []byte("data")) {
			return
		}
		value = bytes.TrimPrefix(value, []byte{' '})
		additionalBytes := len(value)
		if decoder.hasData {
			additionalBytes++
		}
		if additionalBytes > decoder.maxBytes-len(decoder.frame) {
			decoder.feedErr = &ChatGPTWebResponseLimitError{
				Message: fmt.Sprintf("chatgpt web SSE frame exceeds %d bytes", decoder.maxBytes),
			}
			return
		}
		if decoder.hasData {
			decoder.frame = append(decoder.frame, '\n')
		}
		decoder.frame = append(decoder.frame, value...)
		decoder.hasData = true
	})
	if err != nil {
		return nil, &ChatGPTWebResponseLimitError{Message: err.Error()}
	}
	if decoder.feedErr != nil {
		err = decoder.feedErr
		decoder.frame = nil
		decoder.hasData = false
		return nil, err
	}
	if flush && decoder.hasData {
		payloads = append(payloads, decoder.frame)
		decoder.frame = nil
		decoder.hasData = false
	}
	return payloads, nil
}

// ParseChatGPTWebRequest parses a canonical OpenAI Responses request.
func ParseChatGPTWebRequest(payload []byte) (ChatGPTWebRequest, error) {
	return ParseChatGPTWebRequestWithForcedTool(payload, "")
}

// ParseChatGPTWebRequestWithForcedTool parses a canonical request while
// selecting a provider-specific tool required by the route.
func ParseChatGPTWebRequestWithForcedTool(payload []byte, forcedTool string) (ChatGPTWebRequest, error) {
	var root map[string]any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&root); err != nil {
		return ChatGPTWebRequest{}, fmt.Errorf("decode canonical Responses request: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return ChatGPTWebRequest{}, errors.New("decode canonical Responses request: multiple JSON values")
		}
		return ChatGPTWebRequest{}, fmt.Errorf("decode canonical Responses request trailing data: %w", err)
	}
	if err := validateChatGPTWebTextFormat(root); err != nil {
		return ChatGPTWebRequest{}, err
	}
	if err := validateChatGPTWebRequestControls(root); err != nil {
		return ChatGPTWebRequest{}, err
	}
	request := ChatGPTWebRequest{
		Model:           strings.TrimSpace(stringFromAny(root["model"])),
		ReasoningEffort: strings.TrimSpace(nestedString(root, "reasoning", "effort")),
	}
	if instructions := strings.TrimSpace(textFromAny(root["instructions"])); instructions != "" {
		request.Messages = append(request.Messages, ChatGPTWebMessage{
			Role:  "developer",
			Parts: []ChatGPTWebContentPart{{Text: instructions}},
		})
	}
	inputMessages, err := messagesFromResponsesInput(root["input"])
	if err != nil {
		return ChatGPTWebRequest{}, err
	}
	request.Messages = append(request.Messages, inputMessages...)

	var imageTool map[string]any
	webSearchTool := false
	var unsupportedTool *ChatGPTWebUnsupportedToolError
	if tools, ok := root["tools"].([]any); ok {
		for _, rawTool := range tools {
			tool, okTool := rawTool.(map[string]any)
			if !okTool {
				continue
			}
			typeName := strings.ToLower(strings.TrimSpace(stringFromAny(tool["type"])))
			switch typeName {
			case "web_search", "web_search_preview", "web_search_preview_2025_03_11":
				webSearchTool = true
			case "image_generation":
				if imageTool == nil {
					imageTool = tool
				}
			case "function":
				name := chatGPTWebFunctionName(tool)
				if name == "image_gen.imagegen" {
					if imageTool == nil {
						imageTool = tool
					}
					continue
				}
				if unsupportedTool == nil {
					unsupportedTool = &ChatGPTWebUnsupportedToolError{
						Message: fmt.Sprintf("chatgpt web does not support function tool %q", name),
					}
				}
			case "namespace":
				name := strings.ToLower(strings.TrimSpace(stringFromAny(tool["name"])))
				if member := chatGPTWebImageNamespaceMember(tool); name == "image_gen" && member != nil {
					if imageTool == nil {
						imageTool = member
					}
					continue
				}
				if unsupportedTool == nil {
					unsupportedTool = &ChatGPTWebUnsupportedToolError{
						Message: fmt.Sprintf("chatgpt web does not support namespace tool %q", name),
					}
				}
			default:
				if unsupportedTool == nil {
					unsupportedTool = &ChatGPTWebUnsupportedToolError{
						Message: fmt.Sprintf("chatgpt web does not support tool type %q", typeName),
					}
				}
			}
		}
	}
	choiceMode := "auto"
	selectedTool := ""
	explicitImageRequiresDeclaration := false
	if rawChoice, exists := root["tool_choice"]; exists {
		switch choice := rawChoice.(type) {
		case string:
			switch strings.ToLower(strings.TrimSpace(choice)) {
			case "":
			case "auto":
			case "none":
				choiceMode = "none"
			case "required":
				choiceMode = "required"
			default:
				return ChatGPTWebRequest{}, fmt.Errorf("chatgpt web does not support tool_choice %q", choice)
			}
		case map[string]any:
			choiceMode = "explicit"
			switch selected := chatGPTWebSpecialToolChoice(choice); selected {
			case "image":
				selectedTool = selected
				explicitImageRequiresDeclaration = strings.EqualFold(strings.TrimSpace(stringFromAny(choice["type"])), "namespace")
			case "search":
				selectedTool = selected
			default:
				return ChatGPTWebRequest{}, &ChatGPTWebUnsupportedToolError{
					Message: "chatgpt web does not support the selected tool",
				}
			}
		default:
			return ChatGPTWebRequest{}, fmt.Errorf("chatgpt web does not support tool_choice type %T", rawChoice)
		}
	}
	if forced := normalizeChatGPTWebForcedTool(forcedTool); forced != "" {
		choiceMode = "explicit"
		selectedTool = forced
	}
	if selectedTool == "" {
		selectedTool = chatGPTWebModelForcedTool(request.Model)
		if selectedTool != "" {
			choiceMode = "explicit"
		}
	}
	switch choiceMode {
	case "none":
		selectedTool = ""
	case "explicit":
		if selectedTool == "image" && imageTool == nil {
			if explicitImageRequiresDeclaration {
				return ChatGPTWebRequest{}, &ChatGPTWebUnsupportedToolError{
					Message: "chatgpt web image_gen namespace does not declare imagegen",
				}
			}
			imageTool = map[string]any{"type": "image_generation"}
		}
	case "required":
		count := 0
		if webSearchTool {
			count++
			selectedTool = "search"
		}
		if imageTool != nil {
			count++
			selectedTool = "image"
		}
		if count != 1 || unsupportedTool != nil {
			return ChatGPTWebRequest{}, &ChatGPTWebUnsupportedToolError{
				Message: "chatgpt web cannot preserve required tool selection",
			}
		}
	case "auto":
		if unsupportedTool != nil || imageTool != nil || webSearchTool {
			return ChatGPTWebRequest{}, &ChatGPTWebUnsupportedToolError{
				Message: "chatgpt web cannot preserve automatic tool selection",
			}
		}
	}
	request.WebSearch = selectedTool == "search"
	if request.WebSearch && chatGPTWebReasoningControlsRequested(root) {
		return ChatGPTWebRequest{}, &ChatGPTWebUnsupportedRequestError{
			Message: "chatgpt web search does not support reasoning controls",
		}
	}
	if selectedTool != "image" {
		imageTool = nil
	}
	if imageTool != nil {
		if err := validateChatGPTWebImageTool(imageTool); err != nil {
			return ChatGPTWebRequest{}, err
		}
		request.Image, err = imageRequestFromMessages(request.Messages, imageTool)
		if err != nil {
			return ChatGPTWebRequest{}, err
		}
	}
	if len(request.Messages) == 0 {
		return ChatGPTWebRequest{}, errors.New("chatgpt web request has no input messages")
	}
	return request, nil
}

func chatGPTWebReasoningControlsRequested(root map[string]any) bool {
	reasoning, ok := root["reasoning"].(map[string]any)
	if !ok {
		return false
	}
	for _, value := range reasoning {
		if value == nil {
			continue
		}
		if text, okText := value.(string); okText && strings.TrimSpace(text) == "" {
			continue
		}
		return true
	}
	return false
}

func normalizeChatGPTWebForcedTool(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "image", "search":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return ""
	}
}

func chatGPTWebModelForcedTool(model string) string {
	if ChatGPTWebModelUsesImageGeneration(model) {
		return "image"
	}
	model = strings.ToLower(strings.TrimSpace(model))
	switch {
	case strings.HasPrefix(model, "gpt-4o-search-preview"),
		strings.HasPrefix(model, "gpt-4o-mini-search-preview"),
		strings.HasPrefix(model, "gpt-5-search-api"):
		return "search"
	default:
		return ""
	}
}

// ChatGPTWebModelUsesImageGeneration reports whether a model route implicitly
// selects the ChatGPT Web image generation tool.
func ChatGPTWebModelUsesImageGeneration(model string) bool {
	model = strings.ToLower(strings.TrimSpace(model))
	return strings.HasPrefix(model, "gpt-image-") || model == "chatgpt-image-latest"
}

// ValidateChatGPTWebImageReferences checks encoded data URLs without decoding
// their full payloads.
func ValidateChatGPTWebImageReferences(references []string, maxImageBytes, maxTotalBytes int) error {
	if maxImageBytes < 1 || maxTotalBytes < 1 {
		return errors.New("chatgpt web image size limit is invalid")
	}
	if len(references) > ChatGPTWebMaxImageInputs {
		return fmt.Errorf("chatgpt web image inputs exceed %d items", ChatGPTWebMaxImageInputs)
	}
	totalBytes := 0
	for _, reference := range references {
		size, err := ChatGPTWebEncodedImageSize(reference, maxImageBytes)
		if err != nil {
			return err
		}
		if totalBytes > maxTotalBytes-size {
			return fmt.Errorf("chatgpt web image inputs exceed %d bytes", maxTotalBytes)
		}
		totalBytes += size
	}
	return nil
}

// ChatGPTWebEncodedImageSize returns the decoded base64 size without allocating
// the decoded image.
func ChatGPTWebEncodedImageSize(value string, maxBytes int) (int, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, errors.New("image reference is empty")
	}
	payload := value
	if strings.HasPrefix(strings.ToLower(value), "data:") {
		comma := strings.IndexByte(value, ',')
		if comma < 0 {
			return 0, errors.New("invalid image data URL")
		}
		metadata := strings.Split(strings.ToLower(value[len("data:"):comma]), ";")
		if len(metadata) == 0 || !strings.HasPrefix(strings.TrimSpace(metadata[0]), "image/") {
			return 0, errors.New("image data URL has an invalid MIME type")
		}
		base64Encoded := false
		for _, parameter := range metadata[1:] {
			if strings.TrimSpace(parameter) == "base64" {
				base64Encoded = true
				break
			}
		}
		if !base64Encoded {
			return 0, errors.New("image data URL must use base64 encoding")
		}
		payload = value[comma+1:]
	}
	if strings.TrimSpace(payload) == "" {
		return 0, errors.New("image base64 payload is empty")
	}
	decoder := base64.NewDecoder(base64.StdEncoding.Strict(), strings.NewReader(payload))
	var buffer [32 * 1024]byte
	decodedBytes := 0
	for {
		n, err := decoder.Read(buffer[:])
		if n > 0 {
			if maxBytes > 0 && decodedBytes > maxBytes-n {
				return 0, fmt.Errorf("chatgpt web image exceeds %d bytes", maxBytes)
			}
			decodedBytes += n
		}
		if err == io.EOF {
			return decodedBytes, nil
		}
		if err != nil {
			return 0, fmt.Errorf("invalid base64 image data: %w", err)
		}
	}
}

func validateChatGPTWebImageTool(tool map[string]any) error {
	for _, field := range []string{"background", "input_fidelity", "moderation", "output_compression"} {
		if value, exists := tool[field]; exists && strings.TrimSpace(stringFromAny(value)) != "" {
			return &ChatGPTWebUnsupportedToolError{
				Message: fmt.Sprintf("chatgpt web does not support image_generation field %q", field),
			}
		}
	}
	if value := strings.ToLower(strings.TrimSpace(stringFromAny(tool["output_format"]))); value != "" && value != "png" {
		return &ChatGPTWebUnsupportedToolError{
			Message: fmt.Sprintf("chatgpt web cannot guarantee image_generation output_format %q", value),
		}
	}
	if value, exists := tool["partial_images"]; exists && strings.TrimSpace(stringFromAny(value)) != "" &&
		strings.TrimSpace(stringFromAny(value)) != "0" {
		return &ChatGPTWebUnsupportedToolError{
			Message: "chatgpt web does not support image_generation partial_images",
		}
	}
	switch value := strings.ToLower(strings.TrimSpace(stringFromAny(tool["action"]))); value {
	case "", "auto", "generate", "edit":
	default:
		return &ChatGPTWebUnsupportedToolError{
			Message: fmt.Sprintf("chatgpt web does not support image_generation action %q", value),
		}
	}
	return nil
}

func validateChatGPTWebTextFormat(root map[string]any) error {
	text, ok := root["text"].(map[string]any)
	if !ok || text == nil {
		return nil
	}
	format, exists := text["format"]
	if !exists || format == nil {
		return nil
	}
	formatObject, ok := format.(map[string]any)
	if !ok {
		return &ChatGPTWebUnsupportedRequestError{
			Message: "chatgpt web does not support the requested text format",
		}
	}
	formatType := strings.ToLower(strings.TrimSpace(stringFromAny(formatObject["type"])))
	if formatType == "" || formatType == "text" {
		return nil
	}
	return &ChatGPTWebUnsupportedRequestError{
		Message: fmt.Sprintf("chatgpt web does not support text format %q", formatType),
	}
}

func validateChatGPTWebRequestControls(root map[string]any) error {
	for _, field := range []string{
		"previous_response_id",
		"conversation",
		"max_output_tokens",
		"max_tool_calls",
		"temperature",
		"top_p",
		"truncation",
	} {
		value, exists := root[field]
		if !exists || value == nil {
			continue
		}
		if text, ok := value.(string); ok && strings.TrimSpace(text) == "" {
			continue
		}
		return &ChatGPTWebUnsupportedRequestError{
			Message: fmt.Sprintf("chatgpt web does not support Responses field %q", field),
		}
	}
	if background, exists := root["background"].(bool); exists && background {
		return &ChatGPTWebUnsupportedRequestError{
			Message: `chatgpt web does not support Responses field "background"`,
		}
	}
	if serviceTier := strings.ToLower(strings.TrimSpace(stringFromAny(root["service_tier"]))); serviceTier != "" &&
		serviceTier != "auto" && serviceTier != "default" {
		return &ChatGPTWebUnsupportedRequestError{
			Message: fmt.Sprintf("chatgpt web does not support service_tier %q", serviceTier),
		}
	}
	if reasoning, ok := root["reasoning"].(map[string]any); ok {
		if summary := strings.TrimSpace(stringFromAny(reasoning["summary"])); summary != "" && !strings.EqualFold(summary, "auto") {
			return &ChatGPTWebUnsupportedRequestError{
				Message: fmt.Sprintf("chatgpt web does not support reasoning summary %q", summary),
			}
		}
	}
	if text, ok := root["text"].(map[string]any); ok {
		if verbosity := strings.TrimSpace(stringFromAny(text["verbosity"])); verbosity != "" {
			return &ChatGPTWebUnsupportedRequestError{
				Message: fmt.Sprintf("chatgpt web does not support text verbosity %q", verbosity),
			}
		}
	}
	return nil
}

func chatGPTWebSpecialToolChoice(choice map[string]any) string {
	typeName := strings.ToLower(strings.TrimSpace(stringFromAny(choice["type"])))
	switch typeName {
	case "image_generation":
		return "image"
	case "web_search", "web_search_preview", "web_search_preview_2025_03_11":
		return "search"
	case "function":
		if chatGPTWebFunctionName(choice) == "image_gen.imagegen" {
			return "image"
		}
		return ""
	case "namespace":
		if strings.EqualFold(strings.TrimSpace(stringFromAny(choice["name"])), "image_gen") {
			return "image"
		}
		return ""
	default:
		return ""
	}
}

func chatGPTWebFunctionName(tool map[string]any) string {
	name := strings.ToLower(strings.TrimSpace(stringFromAny(tool["name"])))
	if function, ok := tool["function"].(map[string]any); ok {
		name = strings.ToLower(strings.TrimSpace(stringFromAny(function["name"])))
	}
	return name
}

func chatGPTWebImageNamespaceMember(tool map[string]any) map[string]any {
	members, _ := tool["tools"].([]any)
	for _, rawMember := range members {
		member, ok := rawMember.(map[string]any)
		if !ok || !strings.EqualFold(strings.TrimSpace(stringFromAny(member["type"])), "function") {
			continue
		}
		if chatGPTWebFunctionName(member) == "imagegen" {
			return member
		}
	}
	return nil
}

func messagesFromResponsesInput(input any) ([]ChatGPTWebMessage, error) {
	switch value := input.(type) {
	case string:
		if strings.TrimSpace(value) == "" {
			return nil, nil
		}
		return []ChatGPTWebMessage{{Role: "user", Parts: []ChatGPTWebContentPart{{Text: value}}}}, nil
	case map[string]any:
		if message, ok := messageFromResponsesItem(value); ok {
			return []ChatGPTWebMessage{message}, nil
		}
		return nil, unsupportedChatGPTWebInputItem(value)
	case []any:
		if allContentParts(value) {
			parts := contentPartsFromAny(value)
			if len(parts) > 0 {
				return []ChatGPTWebMessage{{Role: "user", Parts: parts}}, nil
			}
			return nil, nil
		}
		messages := make([]ChatGPTWebMessage, 0, len(value))
		for _, rawItem := range value {
			item, ok := rawItem.(map[string]any)
			if !ok {
				return nil, &ChatGPTWebUnsupportedRequestError{
					Message: fmt.Sprintf("chatgpt web does not support Responses input item %T", rawItem),
				}
			}
			if message, okMessage := messageFromResponsesItem(item); okMessage {
				messages = append(messages, message)
				continue
			}
			return nil, unsupportedChatGPTWebInputItem(item)
		}
		return messages, nil
	case nil:
		return nil, nil
	default:
		return nil, &ChatGPTWebUnsupportedRequestError{
			Message: fmt.Sprintf("chatgpt web does not support Responses input type %T", input),
		}
	}
}

func unsupportedChatGPTWebInputItem(item map[string]any) error {
	typeName := strings.TrimSpace(stringFromAny(item["type"]))
	if typeName == "" {
		typeName = "unknown"
	}
	return &ChatGPTWebUnsupportedRequestError{
		Message: fmt.Sprintf("chatgpt web does not support Responses input item type %q", typeName),
	}
}

func messageFromResponsesItem(item map[string]any) (ChatGPTWebMessage, bool) {
	typeName := strings.ToLower(strings.TrimSpace(stringFromAny(item["type"])))
	if typeName != "" && typeName != "message" && typeName != "input_text" && typeName != "text" && typeName != "input_image" && typeName != "image_url" {
		return ChatGPTWebMessage{}, false
	}
	if content, exists := item["content"]; exists && content != nil && !chatGPTWebContentPartsSupported(content) {
		return ChatGPTWebMessage{}, false
	}
	role := strings.ToLower(strings.TrimSpace(stringFromAny(item["role"])))
	if role == "" {
		role = "user"
	}
	parts := contentPartsFromAny(item["content"])
	if len(parts) == 0 {
		parts = contentPartsFromAny(item)
	}
	if len(parts) == 0 {
		return ChatGPTWebMessage{}, false
	}
	return ChatGPTWebMessage{ID: strings.TrimSpace(stringFromAny(item["id"])), Role: role, Parts: parts}, true
}

func chatGPTWebContentPartsSupported(value any) bool {
	switch typed := value.(type) {
	case string:
		return true
	case []any:
		for _, item := range typed {
			if !chatGPTWebContentPartsSupported(item) {
				return false
			}
		}
		return true
	case map[string]any:
		switch strings.ToLower(strings.TrimSpace(stringFromAny(typed["type"]))) {
		case "", "input_text", "text", "output_text":
			return true
		case "input_image", "image", "image_url":
			return imageURLFromAny(typed["image_url"]) != "" || imageURLFromAny(typed["url"]) != ""
		default:
			return false
		}
	case nil:
		return true
	default:
		return false
	}
}

func allContentParts(values []any) bool {
	if len(values) == 0 {
		return false
	}
	for _, rawValue := range values {
		value, ok := rawValue.(map[string]any)
		if !ok {
			return false
		}
		switch strings.ToLower(strings.TrimSpace(stringFromAny(value["type"]))) {
		case "input_text", "text", "output_text", "input_image", "image", "image_url":
		default:
			return false
		}
	}
	return true
}

func contentPartsFromAny(value any) []ChatGPTWebContentPart {
	switch typed := value.(type) {
	case string:
		if typed == "" {
			return nil
		}
		return []ChatGPTWebContentPart{{Text: typed}}
	case []any:
		parts := make([]ChatGPTWebContentPart, 0, len(typed))
		for _, item := range typed {
			parts = append(parts, contentPartsFromAny(item)...)
		}
		return parts
	case map[string]any:
		typeName := strings.ToLower(strings.TrimSpace(stringFromAny(typed["type"])))
		switch typeName {
		case "input_text", "text", "output_text":
			if text := stringFromAny(typed["text"]); text != "" {
				return []ChatGPTWebContentPart{{Text: text}}
			}
		case "input_image", "image", "image_url":
			imageURL := imageURLFromAny(typed["image_url"])
			if imageURL == "" {
				imageURL = imageURLFromAny(typed["url"])
			}
			if imageURL != "" {
				return []ChatGPTWebContentPart{{ImageURL: imageURL}}
			}
		default:
			if text := stringFromAny(typed["text"]); text != "" {
				return []ChatGPTWebContentPart{{Text: text}}
			}
		}
	}
	return nil
}

func imageURLFromAny(value any) string {
	if text, ok := value.(string); ok {
		return strings.TrimSpace(text)
	}
	if object, ok := value.(map[string]any); ok {
		return strings.TrimSpace(stringFromAny(object["url"]))
	}
	return ""
}

func imageRequestFromMessages(messages []ChatGPTWebMessage, tool map[string]any) (*ChatGPTWebImageRequest, error) {
	request := &ChatGPTWebImageRequest{
		Size:         strings.TrimSpace(stringFromAny(tool["size"])),
		Quality:      strings.TrimSpace(stringFromAny(tool["quality"])),
		Action:       strings.ToLower(strings.TrimSpace(stringFromAny(tool["action"]))),
		OutputFormat: strings.ToLower(strings.TrimSpace(stringFromAny(tool["output_format"]))),
	}
	if mask, ok := tool["input_image_mask"].(map[string]any); ok {
		request.MaskURL = imageURLFromAny(mask["image_url"])
	}
	var instructions []string
	var text []string
	lastUserIndex := -1
	for index := range messages {
		if strings.EqualFold(strings.TrimSpace(messages[index].Role), "user") {
			lastUserIndex = index
		}
	}
	for index, message := range messages {
		role := strings.ToLower(strings.TrimSpace(message.Role))
		if role != "developer" && role != "system" && index != lastUserIndex {
			continue
		}
		for _, part := range message.Parts {
			if strings.TrimSpace(part.Text) != "" {
				if role == "developer" || role == "system" {
					instructions = append(instructions, strings.TrimSpace(part.Text))
				} else {
					text = append(text, strings.TrimSpace(part.Text))
				}
			}
			if index == lastUserIndex && strings.TrimSpace(part.ImageURL) != "" {
				request.Images = append(request.Images, strings.TrimSpace(part.ImageURL))
			}
		}
	}
	promptParts := make([]string, 0, len(instructions)+len(text))
	promptParts = append(promptParts, instructions...)
	promptParts = append(promptParts, text...)
	request.Prompt = strings.Join(promptParts, "\n\n")
	if request.Action == "" || request.Action == "auto" {
		if len(request.Images) > 0 {
			request.Action = "edit"
		} else {
			request.Action = "generate"
		}
	}
	switch request.Action {
	case "generate":
		if len(request.Images) > 0 || request.MaskURL != "" {
			return nil, &ChatGPTWebUnsupportedToolError{
				Message: "chatgpt web cannot preserve image_generation action \"generate\" with input images",
			}
		}
	case "edit":
		if len(request.Images) == 0 {
			return nil, &ChatGPTWebUnsupportedToolError{
				Message: "chatgpt web image_generation action \"edit\" requires an input image",
			}
		}
	}
	if request.Quality == "" {
		request.Quality = "auto"
	}
	return request, nil
}

// ChatGPTWebConversationAccumulator turns web conversation full-message and
// patch events into append-only assistant deltas.
type ChatGPTWebConversationAccumulator struct {
	rawText         string
	text            string
	emittedText     string
	historyMessages []string
	historyIDs      map[string]struct{}
	historyIndex    int
	ignoringHistory bool
	terminalError   string
	eventBytes      int
	eventCount      int
}

// NewChatGPTWebConversationAccumulator creates a text accumulator that strips
// assistant history echoed by the web conversation endpoint.
func NewChatGPTWebConversationAccumulator(messages []ChatGPTWebMessage) *ChatGPTWebConversationAccumulator {
	var history []string
	historyIDs := make(map[string]struct{})
	for _, message := range messages {
		if message.Role != "assistant" {
			continue
		}
		if id := strings.TrimSpace(message.ID); id != "" {
			historyIDs[id] = struct{}{}
		}
		var parts []string
		for _, part := range message.Parts {
			if part.Text != "" {
				parts = append(parts, part.Text)
			}
		}
		if text := strings.Join(parts, ""); text != "" {
			history = append(history, text)
		}
	}
	return &ChatGPTWebConversationAccumulator{
		historyMessages: history,
		historyIDs:      historyIDs,
	}
}

// Apply consumes one SSE data payload and returns a new assistant delta.
func (accumulator *ChatGPTWebConversationAccumulator) Apply(payload []byte) (delta string, done bool, err error) {
	trimmed := bytes.TrimSpace(payload)
	if accumulator.eventCount >= chatGPTWebMaxConversationEvents ||
		len(trimmed) > chatGPTWebMaxConversationEventBytes-accumulator.eventBytes {
		return "", false, &ChatGPTWebResponseLimitError{
			Message: "chatgpt web conversation event stream exceeds the response limit",
		}
	}
	accumulator.eventCount++
	accumulator.eventBytes += len(trimmed)
	if bytes.Equal(trimmed, []byte("[DONE]")) {
		if accumulator.terminalError != "" {
			return "", false, errors.New(accumulator.terminalError)
		}
		if strings.TrimSpace(accumulator.text) == "" {
			return "", false, errors.New("chatgpt web conversation completed without assistant text")
		}
		return "", true, nil
	}
	var decoded any
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()
	if errDecode := decoder.Decode(&decoded); errDecode != nil {
		return "", false, fmt.Errorf("decode chatgpt web conversation event: %w", errDecode)
	}
	event, ok := decoded.(map[string]any)
	if !ok {
		return "", false, nil
	}
	if strings.EqualFold(strings.TrimSpace(stringFromAny(event["type"])), "error") || event["error"] != nil {
		return "", false, JSONStreamProtocolError("chatgpt web", trimmed)
	}
	next, messageID, fullMessage := assistantTextFromEvent(event)
	if fullMessage {
		accumulator.ignoringHistory = false
		if _, isHistory := accumulator.historyIDs[messageID]; isHistory && messageID != "" {
			accumulator.ignoringHistory = true
			accumulator.rawText = ""
			return "", false, nil
		}
	}
	if next == "" {
		if accumulator.ignoringHistory {
			return "", false, nil
		}
		next = applyChatGPTWebTextPatch(event, accumulator.rawText)
	}
	if messageID == "" && accumulator.emittedText == "" && accumulator.historyIndex < len(accumulator.historyMessages) &&
		next == accumulator.historyMessages[accumulator.historyIndex] {
		accumulator.historyIndex++
		accumulator.ignoringHistory = true
		accumulator.rawText = ""
		return "", false, nil
	}
	if status := chatGPTWebConversationTerminalError(event); status != "" {
		accumulator.terminalError = "chatgpt web conversation failed with status " + status
		return "", false, errors.New(accumulator.terminalError)
	}
	if len(next) > chatGPTWebMaxConversationTextBytes {
		return "", false, &ChatGPTWebResponseLimitError{
			Message: "chatgpt web conversation text exceeds the response limit",
		}
	}
	accumulator.rawText = next
	next = CleanChatGPTWebText(next)
	if next == accumulator.text {
		return "", false, nil
	}
	if !strings.HasPrefix(next, accumulator.emittedText) {
		return "", false, errors.New("chatgpt web rewrote already emitted assistant text")
	}
	delta = strings.TrimPrefix(next, accumulator.emittedText)
	accumulator.text = next
	accumulator.emittedText = next
	return delta, false, nil
}

func chatGPTWebConversationTerminalError(event map[string]any) string {
	var status string
	var visit func(any)
	visit = func(value any) {
		if status != "" {
			return
		}
		switch typed := value.(type) {
		case map[string]any:
			if finish, ok := typed["finish_details"].(map[string]any); ok {
				candidate := strings.ToLower(strings.TrimSpace(stringFromAny(finish["type"])))
				if chatGPTWebConversationStatusFailed(candidate) {
					status = candidate
					return
				}
			}
			if candidate := strings.ToLower(strings.TrimSpace(stringFromAny(typed["status"]))); chatGPTWebConversationStatusFailed(candidate) {
				status = candidate
				return
			}
			for _, item := range typed {
				visit(item)
			}
		case []any:
			for _, item := range typed {
				visit(item)
			}
		}
	}
	visit(event)
	return status
}

func chatGPTWebConversationStatusFailed(status string) bool {
	status = strings.ToLower(strings.TrimSpace(status))
	return strings.Contains(status, "fail") ||
		strings.Contains(status, "error") ||
		strings.Contains(status, "cancel") ||
		strings.Contains(status, "blocked") ||
		strings.Contains(status, "partial") ||
		strings.Contains(status, "incomplete") ||
		strings.Contains(status, "max_token") ||
		strings.Contains(status, "max_output_token") ||
		strings.Contains(status, "content_filter") ||
		status == "length" ||
		status == "interrupted" ||
		status == "expired"
}

// Text returns the current normalized assistant response.
func (accumulator *ChatGPTWebConversationAccumulator) Text() string {
	if accumulator == nil {
		return ""
	}
	return accumulator.text
}

func assistantTextFromEvent(event map[string]any) (text, messageID string, found bool) {
	for _, candidate := range []any{event, event["v"]} {
		object, ok := candidate.(map[string]any)
		if !ok {
			continue
		}
		message, ok := object["message"].(map[string]any)
		if !ok {
			continue
		}
		author, _ := message["author"].(map[string]any)
		if !strings.EqualFold(stringFromAny(author["role"]), "assistant") {
			continue
		}
		messageID = strings.TrimSpace(stringFromAny(message["id"]))
		if content, okContent := message["content"].(map[string]any); okContent {
			if text := textFromAny(content["parts"]); text != "" {
				return text, messageID, true
			}
			if text := stringFromAny(content["text"]); text != "" {
				return text, messageID, true
			}
		}
		return "", messageID, true
	}
	return "", "", false
}

func applyChatGPTWebTextPatch(event map[string]any, current string) string {
	if path := stringFromAny(event["p"]); path == "/message/content/parts/0" || path == "/message/content/text" {
		return applyChatGPTWebPatchOperation(event, current)
	}
	operations, ok := event["v"].([]any)
	if !ok {
		if value, okString := event["v"].(string); okString && current != "" && event["p"] == nil && event["o"] == nil {
			return current + value
		}
		return current
	}
	text := current
	for _, rawOperation := range operations {
		operation, okOperation := rawOperation.(map[string]any)
		if okOperation {
			text = applyChatGPTWebTextPatch(operation, text)
		}
	}
	return text
}

func applyChatGPTWebPatchOperation(operation map[string]any, current string) string {
	value := stringFromAny(operation["v"])
	switch strings.ToLower(strings.TrimSpace(stringFromAny(operation["o"]))) {
	case "append":
		return current + value
	case "replace":
		return value
	default:
		return current
	}
}

// CleanChatGPTWebText removes complete private annotation spans and withholds
// an incomplete trailing span until a later stream event closes it.
func CleanChatGPTWebText(value string) string {
	const (
		annotationStart = "\ue200"
		annotationEnd   = "\ue201"
	)
	for {
		start := strings.Index(value, annotationStart)
		if start < 0 {
			break
		}
		afterStart := value[start+len(annotationStart):]
		end := strings.Index(afterStart, annotationEnd)
		if end < 0 {
			value = value[:start]
			break
		}
		value = value[:start] + afterStart[end+len(annotationEnd):]
	}
	return strings.ReplaceAll(value, annotationEnd, "")
}

// ChatGPTWebImageAccumulator captures generated image IDs only from explicit
// tool output messages or tool-role patch operations.
type ChatGPTWebImageAccumulator struct {
	ConversationID string
	FileIDs        []string
	SedimentIDs    []string
	References     []ChatGPTWebImageReference
	Terminal       bool
	FailureStatus  string
	role           string
	imageTool      bool
	referenceSet   map[string]struct{}
}

// ChatGPTWebImageReference preserves the upstream order of file-service and
// sediment image outputs.
type ChatGPTWebImageReference struct {
	Kind string
	ID   string
}

// Apply consumes one image-generation SSE data payload.
func (accumulator *ChatGPTWebImageAccumulator) Apply(payload []byte) (bool, error) {
	trimmed := bytes.TrimSpace(payload)
	if bytes.Equal(trimmed, []byte("[DONE]")) {
		return true, nil
	}
	var decoded any
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		return false, fmt.Errorf("decode chatgpt web image event: %w", err)
	}
	event, ok := decoded.(map[string]any)
	if !ok {
		return false, nil
	}
	if strings.EqualFold(strings.TrimSpace(stringFromAny(event["type"])), "error") || event["error"] != nil {
		return false, JSONStreamProtocolError("chatgpt web image", trimmed)
	}
	accumulator.captureConversationID(event)
	if message := messageFromWebEvent(event); message != nil {
		role, imageTool := webMessageImageContext(message)
		accumulator.role = role
		accumulator.imageTool = imageTool
		if webMessageCanContainGeneratedImage(role) && imageTool {
			accumulator.mergeTerminalState(chatGPTWebImageConversationState(message))
			if err := accumulator.captureReferences(message); err != nil {
				return false, err
			}
		}
		return false, nil
	}
	return false, accumulator.applyImagePatch(event)
}

func (accumulator *ChatGPTWebImageAccumulator) mergeTerminalState(terminal bool, failureStatus string) {
	if failureStatus != "" {
		accumulator.FailureStatus = failureStatus
		accumulator.Terminal = true
		return
	}
	if terminal {
		accumulator.Terminal = true
	}
}

func (accumulator *ChatGPTWebImageAccumulator) captureConversationID(event map[string]any) {
	if accumulator.ConversationID == "" {
		accumulator.ConversationID = strings.TrimSpace(stringFromAny(event["conversation_id"]))
	}
	if accumulator.ConversationID == "" {
		if value, ok := event["v"].(map[string]any); ok {
			accumulator.ConversationID = strings.TrimSpace(stringFromAny(value["conversation_id"]))
		}
	}
}

func (accumulator *ChatGPTWebImageAccumulator) applyImagePatch(event map[string]any) error {
	operations, ok := event["v"].([]any)
	if strings.EqualFold(stringFromAny(event["o"]), "patch") && ok {
		for _, rawOperation := range operations {
			operation, okOperation := rawOperation.(map[string]any)
			if okOperation {
				accumulator.applyImageContextPatch(operation)
			}
		}
		if webMessageCanContainGeneratedImage(accumulator.role) &&
			(accumulator.imageTool || chatGPTWebContainsImageReference(event)) {
			accumulator.mergeTerminalState(chatGPTWebImageConversationState(event))
			return accumulator.captureReferences(event)
		}
		return nil
	}
	accumulator.applyImageContextPatch(event)
	if webMessageCanContainGeneratedImage(accumulator.role) &&
		(accumulator.imageTool || chatGPTWebContainsImageReference(event)) {
		accumulator.mergeTerminalState(chatGPTWebImageConversationState(event))
		return accumulator.captureReferences(event)
	}
	return nil
}

func (accumulator *ChatGPTWebImageAccumulator) applyImageContextPatch(event map[string]any) {
	path := strings.ToLower(strings.TrimSpace(stringFromAny(event["p"])))
	value := strings.ToLower(strings.TrimSpace(stringFromAny(event["v"])))
	if strings.Contains(path, "/author/role") {
		accumulator.role = value
	}
	if strings.Contains(path, "/metadata/async_task_type") && value == "image_gen" {
		accumulator.imageTool = true
	}
}

func (accumulator *ChatGPTWebImageAccumulator) captureReferences(value any) error {
	switch typed := value.(type) {
	case map[string]any:
		if pointer, ok := typed["asset_pointer"].(string); ok {
			if err := accumulator.appendImagePointer(pointer); err != nil {
				return err
			}
		}
		path := strings.ToLower(strings.TrimSpace(stringFromAny(typed["p"])))
		if strings.HasSuffix(path, "/asset_pointer") {
			if err := accumulator.appendImagePointer(stringFromAny(typed["v"])); err != nil {
				return err
			}
		}
		keys := make([]string, 0, len(typed))
		for key := range typed {
			if key != "asset_pointer" {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		for _, key := range keys {
			switch typed[key].(type) {
			case map[string]any, []any:
				if err := accumulator.captureReferences(typed[key]); err != nil {
					return err
				}
			}
		}
	case []any:
		for _, item := range typed {
			if err := accumulator.captureReferences(item); err != nil {
				return err
			}
		}
	}
	return nil
}

func (accumulator *ChatGPTWebImageAccumulator) appendImagePointer(pointer string) error {
	kind, id := chatGPTWebImagePointerKindID(pointer)
	if kind == "" {
		return nil
	}
	return accumulator.appendReference(kind, id)
}

func (accumulator *ChatGPTWebImageAccumulator) appendReference(kind, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil
	}
	if accumulator.referenceSet == nil {
		accumulator.referenceSet = make(map[string]struct{}, len(accumulator.References)+1)
		for _, existing := range accumulator.References {
			accumulator.referenceSet[existing.Kind+"\x00"+existing.ID] = struct{}{}
		}
	}
	key := kind + "\x00" + id
	if _, exists := accumulator.referenceSet[key]; exists {
		return nil
	}
	if len(accumulator.References) >= chatGPTWebMaxImageOutputReferences {
		return &ChatGPTWebResponseLimitError{
			Message: fmt.Sprintf("chatgpt web image output exceeds %d references", chatGPTWebMaxImageOutputReferences),
		}
	}
	accumulator.referenceSet[key] = struct{}{}
	accumulator.References = append(accumulator.References, ChatGPTWebImageReference{Kind: kind, ID: id})
	if kind == "sediment" {
		appendUniqueString(&accumulator.SedimentIDs, id)
		return nil
	}
	appendUniqueString(&accumulator.FileIDs, id)
	return nil
}

func chatGPTWebImagePointerKindID(pointer string) (string, string) {
	pointer = strings.TrimSpace(pointer)
	for _, candidate := range []struct {
		prefix string
		kind   string
	}{
		{prefix: "file-service://", kind: "file"},
		{prefix: "sediment://", kind: "sediment"},
	} {
		if strings.HasPrefix(strings.ToLower(pointer), candidate.prefix) {
			id := strings.TrimSpace(pointer[len(candidate.prefix):])
			if chatGPTWebImageReferenceIDPattern.MatchString(id) {
				return candidate.kind, id
			}
			return "", ""
		}
	}
	if chatGPTWebImageFileIDPattern.MatchString(pointer) {
		return "file", pointer
	}
	return "", ""
}

func chatGPTWebContainsImageReference(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		if pointer, ok := typed["asset_pointer"].(string); ok {
			kind, _ := chatGPTWebImagePointerKindID(pointer)
			if kind != "" {
				return true
			}
		}
		path := strings.ToLower(strings.TrimSpace(stringFromAny(typed["p"])))
		if strings.HasSuffix(path, "/asset_pointer") {
			kind, _ := chatGPTWebImagePointerKindID(stringFromAny(typed["v"]))
			if kind != "" {
				return true
			}
		}
		for key, item := range typed {
			if key != "asset_pointer" && chatGPTWebContainsImageReference(item) {
				return true
			}
		}
	case []any:
		for _, item := range typed {
			if chatGPTWebContainsImageReference(item) {
				return true
			}
		}
	}
	return false
}

// CaptureChatGPTWebImageConversation extracts image outputs from a fetched
// conversation document using the same explicit tool-output boundary.
func CaptureChatGPTWebImageConversation(payload []byte, accumulator *ChatGPTWebImageAccumulator) error {
	if accumulator == nil {
		return errors.New("chatgpt web image accumulator is nil")
	}
	var root map[string]any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&root); err != nil {
		return fmt.Errorf("decode chatgpt web conversation: %w", err)
	}
	mapping, _ := root["mapping"].(map[string]any)
	type imageMessage struct {
		id        string
		createdAt float64
		message   map[string]any
	}
	messages := make([]imageMessage, 0, len(mapping))
	for id, rawNode := range mapping {
		node, _ := rawNode.(map[string]any)
		message, _ := node["message"].(map[string]any)
		if message == nil {
			continue
		}
		role, imageTool := webMessageImageContext(message)
		if !webMessageCanContainGeneratedImage(role) || !imageTool {
			continue
		}
		messages = append(messages, imageMessage{
			id:        id,
			createdAt: numberFromAny(message["create_time"]),
			message:   message,
		})
	}
	sort.SliceStable(messages, func(i, j int) bool {
		if messages[i].createdAt == messages[j].createdAt {
			return messages[i].id < messages[j].id
		}
		return messages[i].createdAt < messages[j].createdAt
	})
	accumulator.Terminal = false
	accumulator.FailureStatus = ""
	for index, candidate := range messages {
		if err := accumulator.captureReferences(candidate.message); err != nil {
			return err
		}
		if index == len(messages)-1 {
			accumulator.Terminal, accumulator.FailureStatus = chatGPTWebImageConversationState(candidate.message)
		}
	}
	if len(messages) == 0 {
		accumulator.Terminal, accumulator.FailureStatus = chatGPTWebImageConversationState(root)
	}
	return nil
}

func chatGPTWebImageConversationState(root map[string]any) (bool, string) {
	hasCompletionMarker := false
	hasTerminalStatus := false
	failureStatus := ""
	var visit func(any)
	visit = func(value any) {
		switch typed := value.(type) {
		case map[string]any:
			for key, item := range typed {
				normalizedKey := strings.ToLower(strings.TrimSpace(key))
				switch normalizedKey {
				case "is_complete", "complete":
					if complete, ok := item.(bool); ok && complete {
						hasCompletionMarker = true
					}
				case "finish_details":
					if details, ok := item.(map[string]any); ok && len(details) > 0 {
						hasCompletionMarker = true
					}
				case "status", "state", "type":
					status := strings.ToLower(strings.TrimSpace(stringFromAny(item)))
					if failureStatus == "" && chatGPTWebFailureMessageStatus(status) {
						failureStatus = status
					}
					if chatGPTWebTerminalMessageStatus(status) {
						hasTerminalStatus = true
					}
				}
				switch item.(type) {
				case map[string]any, []any:
					visit(item)
				}
			}
		case []any:
			for _, item := range typed {
				visit(item)
			}
		}
	}
	visit(root)
	if failureStatus != "" {
		return true, failureStatus
	}
	return hasCompletionMarker && hasTerminalStatus, ""
}

func chatGPTWebTerminalMessageStatus(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "complete", "completed", "done", "finished", "finished_successfully",
		"success", "succeeded":
		return true
	default:
		return false
	}
}

func chatGPTWebFailureMessageStatus(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "blocked", "cancelled", "canceled", "error", "failed", "finished_error",
		"finished_with_error", "finished_partial_completion":
		return true
	default:
		return false
	}
}

func webMessageCanContainGeneratedImage(role string) bool {
	return role == "tool" || role == "assistant"
}

func messageFromWebEvent(event map[string]any) map[string]any {
	if message, ok := event["message"].(map[string]any); ok {
		return message
	}
	if value, ok := event["v"].(map[string]any); ok {
		message, _ := value["message"].(map[string]any)
		return message
	}
	return nil
}

func webMessageImageContext(message map[string]any) (string, bool) {
	author, _ := message["author"].(map[string]any)
	role := strings.ToLower(strings.TrimSpace(stringFromAny(author["role"])))
	metadata, _ := message["metadata"].(map[string]any)
	imageTool := strings.EqualFold(stringFromAny(metadata["async_task_type"]), "image_gen")
	if !imageTool {
		content, _ := message["content"].(map[string]any)
		imageTool = chatGPTWebContainsImageReference(content)
	}
	return role, imageTool
}

func appendUniqueString(values *[]string, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	for _, existing := range *values {
		if existing == value {
			return
		}
	}
	*values = append(*values, value)
}

func nestedString(root map[string]any, keys ...string) string {
	var current any = root
	for _, key := range keys {
		object, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current = object[key]
	}
	return stringFromAny(current)
}

func textFromAny(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []any:
		var parts []string
		for _, item := range typed {
			if text := textFromAny(item); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "")
	case map[string]any:
		if text := stringFromAny(typed["text"]); text != "" {
			return text
		}
		return textFromAny(typed["content"])
	default:
		return ""
	}
}

func stringFromAny(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return fmt.Sprint(value)
}

func numberFromAny(value any) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case json.Number:
		number, _ := typed.Float64()
		return number
	default:
		return 0
	}
}

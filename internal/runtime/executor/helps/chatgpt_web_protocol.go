package helps

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
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
	Text        string
	ImageURL    string
	ImageDetail string
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
	Model             string
	Prompt            string
	N                 int
	Images            []string
	MaskURL           string
	MaskImageIndex    int
	Size              string
	Quality           string
	Action            string
	OutputFormat      string
	OutputCompression int
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

// ChatGPTWebParseOptions controls provider-specific compatibility parsing.
type ChatGPTWebParseOptions struct {
	ForcedTool                   string
	IgnoreUnsupportedImageParams bool
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
	return ParseChatGPTWebRequestWithOptions(payload, ChatGPTWebParseOptions{})
}

// ParseChatGPTWebRequestWithForcedTool parses a canonical request while
// selecting a provider-specific tool required by the route.
func ParseChatGPTWebRequestWithForcedTool(payload []byte, forcedTool string) (ChatGPTWebRequest, error) {
	return ParseChatGPTWebRequestWithOptions(payload, ChatGPTWebParseOptions{ForcedTool: forcedTool})
}

// ParseChatGPTWebRequestWithOptions parses a canonical request with explicit compatibility options.
func ParseChatGPTWebRequestWithOptions(payload []byte, options ChatGPTWebParseOptions) (ChatGPTWebRequest, error) {
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
	if forced := normalizeChatGPTWebForcedTool(options.ForcedTool); forced != "" {
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
		if err := validateChatGPTWebImageTool(imageTool, options.IgnoreUnsupportedImageParams); err != nil {
			return ChatGPTWebRequest{}, err
		}
		request.Image, err = imageRequestFromMessages(request.Messages, imageTool)
		if err != nil {
			return ChatGPTWebRequest{}, err
		}
		if request.Image.Model == "" {
			if ChatGPTWebModelUsesImageGeneration(request.Model) {
				request.Image.Model = request.Model
			} else {
				request.Image.Model = "gpt-image-2"
			}
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

func validateChatGPTWebImageTool(tool map[string]any, ignoreUnsupportedParams bool) error {
	if value, exists := tool["n"]; exists && value != nil {
		n, ok := integerFromAny(value)
		if !ok || n < 1 {
			return &ChatGPTWebUnsupportedToolError{
				Message: "chatgpt web image_generation field \"n\" must be a positive integer",
			}
		}
	}
	if !ignoreUnsupportedParams {
		for _, candidate := range []struct {
			field        string
			defaultValue string
		}{
			{field: "background", defaultValue: "auto"},
			{field: "moderation", defaultValue: "auto"},
		} {
			if value, exists := tool[candidate.field]; exists {
				normalized := strings.ToLower(strings.TrimSpace(stringFromAny(value)))
				if normalized != "" && normalized != candidate.defaultValue {
					return &ChatGPTWebUnsupportedToolError{
						Message: fmt.Sprintf("chatgpt web does not support image_generation field %q", candidate.field),
					}
				}
			}
		}
		if value, exists := tool["input_fidelity"]; exists && strings.TrimSpace(stringFromAny(value)) != "" {
			return &ChatGPTWebUnsupportedToolError{
				Message: "chatgpt web does not support image_generation field \"input_fidelity\"",
			}
		}
		outputFormat := normalizeChatGPTWebToolOutputFormat(stringFromAny(tool["output_format"]))
		if outputFormat == "" {
			return &ChatGPTWebUnsupportedToolError{
				Message: fmt.Sprintf("chatgpt web cannot guarantee image_generation output_format %q", stringFromAny(tool["output_format"])),
			}
		}
		if value, exists := tool["output_compression"]; exists && value != nil {
			compression, ok := integerFromAny(value)
			if !ok || compression < 0 || compression > 100 || (outputFormat == "png" && compression != 100) {
				return &ChatGPTWebUnsupportedToolError{
					Message: "chatgpt web does not support this image_generation output_compression",
				}
			}
		}
		if value, exists := tool["partial_images"]; exists && strings.TrimSpace(stringFromAny(value)) != "" &&
			strings.TrimSpace(stringFromAny(value)) != "0" {
			return &ChatGPTWebUnsupportedToolError{
				Message: "chatgpt web does not support image_generation partial_images",
			}
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
				detail := strings.TrimSpace(stringFromAny(typed["detail"]))
				if detail == "" {
					if imageObject, ok := typed["image_url"].(map[string]any); ok {
						detail = strings.TrimSpace(stringFromAny(imageObject["detail"]))
					}
				}
				return []ChatGPTWebContentPart{{ImageURL: imageURL, ImageDetail: detail}}
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
		Model:             strings.TrimSpace(stringFromAny(tool["model"])),
		N:                 1,
		Size:              strings.TrimSpace(stringFromAny(tool["size"])),
		Quality:           strings.TrimSpace(stringFromAny(tool["quality"])),
		Action:            strings.ToLower(strings.TrimSpace(stringFromAny(tool["action"]))),
		OutputFormat:      strings.ToLower(strings.TrimSpace(stringFromAny(tool["output_format"]))),
		OutputCompression: 100,
		MaskImageIndex:    -1,
	}
	if n, ok := integerFromAny(tool["n"]); ok && n > 0 {
		request.N = n
	}
	if compression, ok := integerFromAny(tool["output_compression"]); ok && compression >= 0 && compression <= 100 {
		request.OutputCompression = compression
	}
	if mask, ok := tool["input_image_mask"].(map[string]any); ok {
		request.MaskURL = imageURLFromAny(mask["image_url"])
	}
	var instructions []string
	var currentText []string
	var transcript []string
	hasHistoricalText := false
	lastUserIndex := -1
	for index := range messages {
		if strings.EqualFold(strings.TrimSpace(messages[index].Role), "user") {
			lastUserIndex = index
		}
	}
	for index, message := range messages {
		role := strings.ToLower(strings.TrimSpace(message.Role))
		if role != "developer" && role != "system" && role != "user" && role != "assistant" {
			continue
		}
		messageText := make([]string, 0, len(message.Parts))
		for _, part := range message.Parts {
			if strings.TrimSpace(part.Text) != "" {
				if role == "developer" || role == "system" {
					instructions = append(instructions, strings.TrimSpace(part.Text))
				} else {
					messageText = append(messageText, strings.TrimSpace(part.Text))
					if index == lastUserIndex {
						currentText = append(currentText, strings.TrimSpace(part.Text))
					}
				}
			}
			if (role == "user" || role == "assistant") && strings.TrimSpace(part.ImageURL) != "" {
				if index == lastUserIndex && request.MaskImageIndex < 0 {
					request.MaskImageIndex = len(request.Images)
				}
				request.Images = append(request.Images, strings.TrimSpace(part.ImageURL))
			}
		}
		if len(messageText) > 0 && (role == "user" || role == "assistant") {
			transcript = append(transcript, strings.ToUpper(role[:1])+role[1:]+": "+strings.Join(messageText, "\n"))
			if index != lastUserIndex {
				hasHistoricalText = true
			}
		}
	}
	if request.MaskImageIndex < 0 {
		request.MaskImageIndex = 0
	}
	if hasHistoricalText {
		promptSections := make([]string, 0, 2)
		if len(instructions) > 0 {
			promptSections = append(promptSections, "Instructions:\n"+strings.Join(instructions, "\n\n"))
		}
		if len(transcript) > 0 {
			promptSections = append(promptSections, "Transcript:\n"+strings.Join(transcript, "\n"))
		}
		request.Prompt = strings.Join(promptSections, "\n\n")
	} else {
		promptParts := make([]string, 0, len(instructions)+len(currentText))
		promptParts = append(promptParts, instructions...)
		promptParts = append(promptParts, currentText...)
		request.Prompt = strings.Join(promptParts, "\n\n")
	}
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

func normalizeChatGPTWebToolOutputFormat(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "png":
		return "png"
	case "jpg", "jpeg":
		return "jpeg"
	case "webp":
		return "webp"
	default:
		return ""
	}
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
		switch typed := value.(type) {
		case map[string]any:
			if finish, ok := typed["finish_details"].(map[string]any); ok {
				candidate := strings.ToLower(strings.TrimSpace(stringFromAny(finish["type"])))
				if chatGPTWebConversationStatusFailed(candidate) {
					status = chatGPTWebPreferredImageStatus(status, candidate)
				}
			}
			if candidate := strings.ToLower(strings.TrimSpace(stringFromAny(typed["status"]))); chatGPTWebConversationStatusFailed(candidate) {
				status = chatGPTWebPreferredImageStatus(status, candidate)
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

// ChatGPTWebImageAccumulator captures generated image IDs from explicit tool
// output and tracks whether the current turn ended with assistant text.
type ChatGPTWebImageAccumulator struct {
	ConversationID        string
	Turn                  ChatGPTWebImageTurn
	TaskIDs               []string
	ResponseMessageIDs    []string
	FileIDs               []string
	SedimentIDs           []string
	References            []ChatGPTWebImageReference
	Terminal              bool
	StreamTerminal        bool
	FailureStatus         string
	ToolUsage             map[string]any
	PendingOutput         bool
	HiddenOutputSeen      bool
	IncompleteOutputSeen  bool
	PlaceholderOutputSeen bool
	FinalMessageSeen      bool
	role                  string
	imageTool             bool
	assistantTextValue    string
	assistantTextSeen     bool
	assistantTextPatch    bool
	assistantTerminalSeen bool
	terminalAssistantText bool
	taskSet               map[string]struct{}
	responseMessageSet    map[string]struct{}
	pendingReferenceSet   map[string]struct{}
	referenceSet          map[string]struct{}
}

// ChatGPTWebImageTurn identifies the user message that started one image
// generation turn. It prevents historical tasks and conversation messages
// from being attached to a later request that reuses the same conversation.
type ChatGPTWebImageTurn struct {
	MessageID string
	CreatedAt float64
}

// ChatGPTWebImageReference preserves the upstream order of file-service and
// sediment image outputs.
type ChatGPTWebImageReference struct {
	Kind               string
	ID                 string
	GenerationIndex    int
	HasGenerationIndex bool
}

// ChatGPTWebImageTaskState summarizes image tasks associated with one
// conversation. A terminal task result is authoritative only when every
// matching task has reached a terminal state.
type ChatGPTWebImageTaskState struct {
	Matched  int
	Terminal int
	Targets  []ChatGPTWebImageTaskTarget
}

// ChatGPTWebImageTaskTarget identifies one exact official image task stream.
type ChatGPTWebImageTaskTarget struct {
	TaskID            string
	ResponseMessageID string
	Terminal          bool
}

// AllTerminal reports whether at least one task matched and all matched tasks
// reached a terminal state.
func (state ChatGPTWebImageTaskState) AllTerminal() bool {
	return state.Matched > 0 && state.Terminal == state.Matched
}

// HasPending reports whether any matching task has not reached a terminal
// state yet.
func (state ChatGPTWebImageTaskState) HasPending() bool {
	return state.Matched > state.Terminal
}

// MergeChatGPTWebImageAccumulators combines independently observed image
// outputs while preserving the same reference bound enforced during parsing.
func MergeChatGPTWebImageAccumulators(primary, secondary *ChatGPTWebImageAccumulator) (*ChatGPTWebImageAccumulator, error) {
	if primary == nil && secondary == nil {
		return &ChatGPTWebImageAccumulator{}, nil
	}
	merged := &ChatGPTWebImageAccumulator{}
	for _, source := range []*ChatGPTWebImageAccumulator{primary, secondary} {
		if source == nil {
			continue
		}
		if merged.ConversationID == "" {
			merged.ConversationID = source.ConversationID
		}
		if merged.Turn.MessageID == "" && merged.Turn.CreatedAt == 0 {
			merged.Turn = source.Turn
		}
		merged.Terminal = merged.Terminal || source.Terminal
		merged.StreamTerminal = merged.StreamTerminal || source.StreamTerminal
		merged.FailureStatus = chatGPTWebPreferredImageFailure(merged.FailureStatus, source.FailureStatus)
		merged.HiddenOutputSeen = merged.HiddenOutputSeen || source.HiddenOutputSeen
		merged.IncompleteOutputSeen = merged.IncompleteOutputSeen || source.IncompleteOutputSeen
		merged.PlaceholderOutputSeen = merged.PlaceholderOutputSeen || source.PlaceholderOutputSeen
		merged.FinalMessageSeen = merged.FinalMessageSeen || source.FinalMessageSeen
		if len(merged.ToolUsage) == 0 && len(source.ToolUsage) > 0 {
			merged.ToolUsage = source.ToolUsage
		}
		if merged.role == "" {
			merged.role = source.role
		}
		merged.imageTool = merged.imageTool || source.imageTool
		if source.assistantTextSeen {
			if !merged.assistantTextSeen || len(source.assistantTextValue) > len(merged.assistantTextValue) {
				merged.assistantTextValue = source.assistantTextValue
			}
			merged.assistantTextSeen = true
		} else if merged.assistantTextValue == "" {
			// Preserve a control envelope only when no user-visible assistant
			// text has been observed. A longer control payload must never hide
			// a shorter real reply while task and conversation snapshots merge.
			merged.assistantTextValue = source.assistantTextValue
		}
		merged.assistantTerminalSeen = merged.assistantTerminalSeen || source.assistantTerminalSeen
		for _, taskID := range source.TaskIDs {
			if err := merged.appendTaskID(taskID); err != nil {
				return nil, err
			}
		}
		for _, messageID := range source.ResponseMessageIDs {
			if err := merged.appendResponseMessageID(messageID); err != nil {
				return nil, err
			}
		}
		for key := range source.pendingReferenceSet {
			merged.markPendingReferenceKey(key)
		}
		for _, reference := range chatGPTWebImageAccumulatorReferences(source) {
			if err := merged.appendReferenceWithMetadata(reference); err != nil {
				return nil, err
			}
		}
	}
	for _, reference := range merged.References {
		merged.clearPendingReferenceKey(reference.Kind + "\x00" + reference.ID)
	}
	merged.PendingOutput = len(merged.pendingReferenceSet) > 0
	merged.sortReferences()
	if merged.FailureStatus != "" {
		merged.assistantTerminalSeen = false
		merged.terminalAssistantText = false
	} else {
		merged.terminalAssistantText = false
		merged.updateTerminalAssistantText()
	}
	return merged, nil
}

// HasTerminalAssistantText reports whether the current image turn completed
// successfully with non-empty assistant text.
func (accumulator *ChatGPTWebImageAccumulator) HasTerminalAssistantText() bool {
	return accumulator != nil && accumulator.terminalAssistantText
}

func chatGPTWebImageAccumulatorReferences(accumulator *ChatGPTWebImageAccumulator) []ChatGPTWebImageReference {
	if accumulator == nil {
		return nil
	}
	if len(accumulator.References) > 0 {
		return accumulator.References
	}
	references := make([]ChatGPTWebImageReference, 0, len(accumulator.FileIDs)+len(accumulator.SedimentIDs))
	for _, id := range accumulator.FileIDs {
		references = append(references, ChatGPTWebImageReference{Kind: "file", ID: id})
	}
	for _, id := range accumulator.SedimentIDs {
		references = append(references, ChatGPTWebImageReference{Kind: "sediment", ID: id})
	}
	return references
}

// Apply consumes one image-generation SSE data payload.
func (accumulator *ChatGPTWebImageAccumulator) Apply(payload []byte) (bool, error) {
	trimmed := bytes.TrimSpace(payload)
	if bytes.Equal(trimmed, []byte("[DONE]")) {
		accumulator.updateTerminalAssistantText()
		accumulator.StreamTerminal = true
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
	if usage := chatGPTWebFindImageToolUsage(event, 0); len(usage) > 0 {
		accumulator.ToolUsage = usage
	}
	if err := accumulator.captureImageTaskIDs(event, 0); err != nil {
		return false, err
	}
	if chatGPTWebImageOuterModeration(event) {
		accumulator.mergeTerminalState(true, "content_filter")
		return chatGPTWebImageStreamTerminal(event), nil
	}
	if failed, detail := chatGPTWebImageOuterFailure(event); failed {
		if detail == "" {
			return false, JSONStreamProtocolError("chatgpt web image", trimmed)
		}
		return false, streamProtocolError{provider: "chatgpt web image", message: detail}
	}
	accumulator.captureConversationID(event)
	if message := messageFromWebEvent(event); message != nil {
		accumulator.FinalMessageSeen = accumulator.FinalMessageSeen || chatGPTWebEventHasFinalMessage(event)
		role, imageTool := webMessageImageContext(message)
		accumulator.role = role
		accumulator.imageTool = imageTool
		accumulator.assistantTextPatch = false
		if role == "assistant" && !imageTool {
			text := chatGPTWebImageMessageText(message)
			accumulator.replaceAssistantText(text)
		}
		streamTerminal := chatGPTWebImageStreamTerminal(event)
		accumulator.StreamTerminal = accumulator.StreamTerminal || streamTerminal
		explicitFailure, failureStatus := chatGPTWebImageStreamMessageFailure(message)
		if role == "assistant" && !imageTool && !explicitFailure {
			terminal, _ := chatGPTWebImageTerminalTextReply(message)
			accumulator.assistantTerminalSeen = accumulator.assistantTerminalSeen || terminal
			accumulator.updateTerminalAssistantText()
		}
		if webMessageCanContainGeneratedImage(role) && (imageTool || explicitFailure) {
			if explicitFailure {
				accumulator.mergeTerminalState(true, failureStatus)
			} else {
				accumulator.mergeTerminalState(chatGPTWebImageConversationState(message))
			}
			if imageTool {
				if err := accumulator.captureReferences(message); err != nil {
					return false, err
				}
				if accumulator.PendingOutput {
					accumulator.Terminal = false
				}
			}
			return streamTerminal, nil
		}
		if role == "assistant" {
			terminal, _ := chatGPTWebImageTerminalTextReply(message)
			if terminal {
				accumulator.mergeTerminalState(true, "")
			}
		}
		return streamTerminal, nil
	}
	if taskStatus := strings.ToLower(strings.TrimSpace(stringFromAny(event["task_status"]))); taskStatus != "" {
		switch {
		case chatGPTWebFailureMessageStatus(taskStatus):
			accumulator.mergeTerminalState(true, taskStatus)
		case chatGPTWebTerminalMessageStatus(taskStatus):
			accumulator.mergeTerminalState(true, "")
		}
	}
	if err := accumulator.applyImagePatch(event); err != nil {
		return false, err
	}
	streamTerminal := chatGPTWebImageStreamTerminal(event)
	if streamTerminal {
		accumulator.updateTerminalAssistantText()
	}
	accumulator.StreamTerminal = accumulator.StreamTerminal || streamTerminal
	return streamTerminal, nil
}

func chatGPTWebEventHasFinalMessage(event map[string]any) bool {
	if event == nil {
		return false
	}
	if _, ok := event["final_message"].(map[string]any); ok {
		return true
	}
	value, _ := event["v"].(map[string]any)
	_, ok := value["final_message"].(map[string]any)
	return ok
}

func (accumulator *ChatGPTWebImageAccumulator) captureAssistantPatchState(event map[string]any) {
	if accumulator == nil {
		return
	}
	if operations, ok := event["v"].([]any); ok && strings.EqualFold(stringFromAny(event["o"]), "patch") {
		for _, rawOperation := range operations {
			if operation, okOperation := rawOperation.(map[string]any); okOperation {
				accumulator.captureAssistantPatchState(operation)
			}
		}
		return
	}
	if accumulator.role != "assistant" || accumulator.imageTool {
		accumulator.assistantTextPatch = false
		return
	}

	path := strings.ToLower(strings.TrimSpace(stringFromAny(event["p"])))
	if path == "/message/content/parts/0" || path == "/message/content/text" {
		accumulator.assistantTextPatch = true
		if value, ok := event["v"].(string); ok {
			if strings.EqualFold(strings.TrimSpace(stringFromAny(event["o"])), "replace") {
				accumulator.replaceAssistantText(value)
			} else {
				accumulator.appendAssistantText(value)
			}
		}
	} else if path != "" {
		accumulator.assistantTextPatch = false
	} else if event["p"] == nil && event["o"] == nil && accumulator.assistantTextPatch {
		if value, ok := event["v"].(string); ok {
			accumulator.appendAssistantText(value)
		}
	}

	if strings.HasSuffix(path, "/status") {
		status := strings.TrimSpace(stringFromAny(event["v"]))
		switch {
		case chatGPTWebFailureMessageStatus(status):
			accumulator.mergeTerminalState(true, status)
		case chatGPTWebTerminalMessageStatus(status):
			accumulator.assistantTerminalSeen = true
		}
	}
	if strings.Contains(path, "/finish_details") {
		finishType := strings.TrimSpace(stringFromAny(event["v"]))
		if details, ok := event["v"].(map[string]any); ok {
			finishType = strings.TrimSpace(stringFromAny(details["type"]))
		}
		switch {
		case chatGPTWebFailureMessageStatus(finishType):
			accumulator.mergeTerminalState(true, finishType)
		case chatGPTWebTerminalMessageStatus(finishType):
			accumulator.assistantTerminalSeen = true
		}
	}
	if strings.HasSuffix(path, "/end_turn") && event["v"] == true {
		accumulator.assistantTerminalSeen = true
	}
	if strings.Contains(path, "/metadata") {
		if metadata, ok := event["v"].(map[string]any); ok {
			if failure := chatGPTWebConversationTerminalError(map[string]any{"metadata": metadata}); failure != "" {
				accumulator.mergeTerminalState(true, failure)
			} else if chatGPTWebImageCompletionMetadata(metadata) {
				accumulator.assistantTerminalSeen = true
			}
		}
	}
	accumulator.updateTerminalAssistantText()
}

func (accumulator *ChatGPTWebImageAccumulator) updateTerminalAssistantText() {
	if accumulator == nil || accumulator.FailureStatus != "" {
		return
	}
	if accumulator.assistantTerminalSeen {
		accumulator.recordAssistantText(accumulator.assistantTextValue)
	}
	if accumulator.assistantTextSeen && accumulator.assistantTerminalSeen {
		accumulator.terminalAssistantText = true
		accumulator.Terminal = true
	}
}

func (accumulator *ChatGPTWebImageAccumulator) replaceAssistantText(value string) {
	if accumulator == nil {
		return
	}
	accumulator.assistantTextValue = value
	accumulator.recordAssistantText(value)
}

func (accumulator *ChatGPTWebImageAccumulator) appendAssistantText(value string) {
	if accumulator == nil {
		return
	}
	accumulator.assistantTextValue += value
	accumulator.recordAssistantText(accumulator.assistantTextValue)
}

func (accumulator *ChatGPTWebImageAccumulator) recordAssistantText(value string) {
	if accumulator == nil {
		return
	}
	value = strings.TrimSpace(value)
	accumulator.assistantTextSeen = value != "" && !chatGPTWebSkippedMainlineControl(value)
}

func chatGPTWebImageCompletionMetadata(metadata map[string]any) bool {
	if metadata == nil || metadata["is_complete"] != true {
		return false
	}
	finishDetails, _ := metadata["finish_details"].(map[string]any)
	finishType := strings.ToLower(strings.TrimSpace(stringFromAny(finishDetails["type"])))
	return finishType == "stop" || chatGPTWebTerminalMessageStatus(finishType)
}

func chatGPTWebFindImageToolUsage(value any, depth int) map[string]any {
	if depth > 10 {
		return nil
	}
	switch typed := value.(type) {
	case map[string]any:
		if rawToolUsage, ok := typed["tool_usage"].(map[string]any); ok {
			for _, key := range []string{"image_gen", "image_generation"} {
				if usage, okUsage := rawToolUsage[key].(map[string]any); okUsage && len(usage) > 0 {
					return usage
				}
			}
		}
		for _, child := range typed {
			if usage := chatGPTWebFindImageToolUsage(child, depth+1); len(usage) > 0 {
				return usage
			}
		}
	case []any:
		for _, child := range typed {
			if usage := chatGPTWebFindImageToolUsage(child, depth+1); len(usage) > 0 {
				return usage
			}
		}
	}
	return nil
}

func (accumulator *ChatGPTWebImageAccumulator) mergeTerminalState(terminal bool, failureStatus string) {
	if failureStatus != "" {
		accumulator.FailureStatus = chatGPTWebPreferredImageFailure(accumulator.FailureStatus, failureStatus)
		accumulator.assistantTerminalSeen = false
		accumulator.terminalAssistantText = false
		accumulator.Terminal = true
		return
	}
	if terminal {
		accumulator.Terminal = true
	}
}

func chatGPTWebPreferredImageFailure(current, incoming string) string {
	current = strings.TrimSpace(current)
	incoming = strings.TrimSpace(incoming)
	if incoming == "" {
		return current
	}
	currentQuota := strings.EqualFold(current, "image_generation_limit_reached")
	incomingQuota := strings.EqualFold(incoming, "image_generation_limit_reached")
	if incomingQuota {
		return "image_generation_limit_reached"
	}
	if currentQuota {
		return "image_generation_limit_reached"
	}
	currentModeration := chatGPTWebImageModerationStatus(current)
	incomingModeration := chatGPTWebImageModerationStatus(incoming)
	if incomingModeration && !currentModeration {
		return "content_filter"
	}
	if currentModeration {
		return "content_filter"
	}
	if current == "" || (chatGPTWebGenericImageFailure(current) && !chatGPTWebGenericImageFailure(incoming)) {
		return incoming
	}
	if !chatGPTWebGenericImageFailure(current) && chatGPTWebGenericImageFailure(incoming) {
		return current
	}
	currentNormalized := strings.ToLower(current)
	incomingNormalized := strings.ToLower(incoming)
	if incomingNormalized < currentNormalized || incomingNormalized == currentNormalized && incoming < current {
		return incoming
	}
	return current
}

func chatGPTWebGenericImageFailure(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	if chatGPTWebFailureMessageStatus(value) {
		return true
	}
	switch value {
	case "image_tool_error", "response failed", "response incomplete",
		"content_filter", "max_tokens", "max_output_tokens", "length",
		"expired", "interrupted", "incomplete":
		return true
	default:
		return false
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
				if err := accumulator.applyImageContextPatch(operation); err != nil {
					return err
				}
			}
		}
		accumulator.captureAssistantPatchState(event)
		if webMessageCanContainGeneratedImage(accumulator.role) &&
			(accumulator.imageTool || chatGPTWebRelevantImageReference(accumulator.role, accumulator.imageTool, event)) {
			accumulator.mergeTerminalState(chatGPTWebImageConversationState(event))
			if err := accumulator.captureReferences(event); err != nil {
				return err
			}
			if accumulator.PendingOutput {
				accumulator.Terminal = false
			}
			return nil
		}
		return nil
	}
	if err := accumulator.applyImageContextPatch(event); err != nil {
		return err
	}
	accumulator.captureAssistantPatchState(event)
	if webMessageCanContainGeneratedImage(accumulator.role) &&
		(accumulator.imageTool || chatGPTWebRelevantImageReference(accumulator.role, accumulator.imageTool, event)) {
		accumulator.mergeTerminalState(chatGPTWebImageConversationState(event))
		if err := accumulator.captureReferences(event); err != nil {
			return err
		}
		if accumulator.PendingOutput {
			accumulator.Terminal = false
		}
		return nil
	}
	return nil
}

func (accumulator *ChatGPTWebImageAccumulator) applyImageContextPatch(event map[string]any) error {
	path := strings.ToLower(strings.TrimSpace(stringFromAny(event["p"])))
	value := strings.ToLower(strings.TrimSpace(stringFromAny(event["v"])))
	if strings.Contains(path, "/author/role") {
		accumulator.role = value
	}
	if strings.Contains(path, "/author/name") && value == "image_gen" {
		accumulator.imageTool = true
	}
	if strings.Contains(path, "/metadata/async_task_type") && value == "image_gen" {
		accumulator.imageTool = true
	}
	if strings.Contains(path, "/metadata/image_gen_async") ||
		strings.Contains(path, "/metadata/image_gen_multi_stream") {
		accumulator.imageTool = accumulator.imageTool || chatGPTWebTruthy(event["v"])
	}
	if strings.Contains(path, "/metadata/image_gen_task_id") && strings.TrimSpace(stringFromAny(event["v"])) != "" {
		accumulator.imageTool = true
		if err := accumulator.appendTaskID(stringFromAny(event["v"])); err != nil {
			return err
		}
	}
	return nil
}

func (accumulator *ChatGPTWebImageAccumulator) captureReferences(value any) error {
	if err := accumulator.captureReferencesAt(value, "", 0, false); err != nil {
		return err
	}
	accumulator.sortReferences()
	return nil
}

func (accumulator *ChatGPTWebImageAccumulator) captureReferencesAt(value any, field string, generationIndex int, hasGenerationIndex bool) error {
	switch typed := value.(type) {
	case map[string]any:
		if chatGPTWebImageVisuallyHidden(typed) {
			accumulator.HiddenOutputSeen = true
			return nil
		}
		if index, ok := chatGPTWebImageGenerationIndex(typed); ok {
			generationIndex = index
			hasGenerationIndex = true
		}
		if pointer, ok := typed["asset_pointer"].(string); ok {
			if chatGPTWebImageNoAuthPlaceholder(typed) {
				accumulator.PlaceholderOutputSeen = true
				accumulator.clearPendingImagePointer(pointer)
			} else if chatGPTWebImagePointerIncomplete(typed) {
				accumulator.IncompleteOutputSeen = true
				accumulator.markPendingImagePointer(pointer)
			} else if err := accumulator.appendImagePointerWithGeneration(pointer, generationIndex, hasGenerationIndex); err != nil {
				return err
			}
		}
		path := strings.ToLower(strings.TrimSpace(stringFromAny(typed["p"])))
		if strings.HasSuffix(path, "/asset_pointer") {
			if err := accumulator.appendImagePointerWithGeneration(stringFromAny(typed["v"]), generationIndex, hasGenerationIndex); err != nil {
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
			childField := ""
			if chatGPTWebImageReferenceField(key) {
				childField = strings.ToLower(strings.TrimSpace(key))
			}
			switch typed[key].(type) {
			case map[string]any, []any:
				if err := accumulator.captureReferencesAt(typed[key], childField, generationIndex, hasGenerationIndex); err != nil {
					return err
				}
			case string:
				if childField != "" {
					if err := accumulator.captureReferencesAt(typed[key], childField, generationIndex, hasGenerationIndex); err != nil {
						return err
					}
				}
			}
		}
	case []any:
		for _, item := range typed {
			if err := accumulator.captureReferencesAt(item, field, generationIndex, hasGenerationIndex); err != nil {
				return err
			}
		}
	case string:
		if kind := chatGPTWebImageReferenceKind(field); kind != "" {
			return accumulator.appendImageFieldReferenceWithGeneration(kind, typed, generationIndex, hasGenerationIndex)
		}
		if field != "" {
			return accumulator.appendImagePointerWithGeneration(typed, generationIndex, hasGenerationIndex)
		}
	}
	return nil
}

func chatGPTWebImageReferenceField(key string) bool {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "asset", "file_id", "image_id", "parts", "sediment_id":
		return true
	default:
		return false
	}
}

func chatGPTWebImageReferenceKind(field string) string {
	switch strings.ToLower(strings.TrimSpace(field)) {
	case "file_id", "image_id":
		return "file"
	case "sediment_id":
		return "sediment"
	default:
		return ""
	}
}

func (accumulator *ChatGPTWebImageAccumulator) appendImageFieldReference(kind, value string) error {
	return accumulator.appendImageFieldReferenceWithGeneration(kind, value, 0, false)
}

func (accumulator *ChatGPTWebImageAccumulator) appendImageFieldReferenceWithGeneration(kind, value string, generationIndex int, hasGenerationIndex bool) error {
	if pointerKind, id := chatGPTWebImagePointerKindID(value); pointerKind != "" {
		return accumulator.appendReferenceWithMetadata(ChatGPTWebImageReference{Kind: pointerKind, ID: id, GenerationIndex: generationIndex, HasGenerationIndex: hasGenerationIndex})
	}
	value = strings.TrimSpace(value)
	if !chatGPTWebImageReferenceIDPattern.MatchString(value) {
		return nil
	}
	return accumulator.appendReferenceWithMetadata(ChatGPTWebImageReference{Kind: kind, ID: value, GenerationIndex: generationIndex, HasGenerationIndex: hasGenerationIndex})
}

func (accumulator *ChatGPTWebImageAccumulator) appendImagePointer(pointer string) error {
	return accumulator.appendImagePointerWithGeneration(pointer, 0, false)
}

func (accumulator *ChatGPTWebImageAccumulator) appendImagePointerWithGeneration(pointer string, generationIndex int, hasGenerationIndex bool) error {
	kind, id := chatGPTWebImagePointerKindID(pointer)
	if kind == "" {
		return nil
	}
	return accumulator.appendReferenceWithMetadata(ChatGPTWebImageReference{Kind: kind, ID: id, GenerationIndex: generationIndex, HasGenerationIndex: hasGenerationIndex})
}

func (accumulator *ChatGPTWebImageAccumulator) appendReference(kind, id string) error {
	return accumulator.appendReferenceWithMetadata(ChatGPTWebImageReference{Kind: kind, ID: id})
}

func (accumulator *ChatGPTWebImageAccumulator) appendReferenceWithMetadata(reference ChatGPTWebImageReference) error {
	reference.ID = strings.TrimSpace(reference.ID)
	reference.Kind = strings.TrimSpace(reference.Kind)
	id := reference.ID
	if id == "" {
		return nil
	}
	accumulator.clearPendingReferenceKey(reference.Kind + "\x00" + id)
	if accumulator.referenceSet == nil {
		accumulator.referenceSet = make(map[string]struct{}, len(accumulator.References)+1)
		for _, existing := range accumulator.References {
			accumulator.referenceSet[existing.Kind+"\x00"+existing.ID] = struct{}{}
		}
	}
	key := reference.Kind + "\x00" + id
	if _, exists := accumulator.referenceSet[key]; exists {
		if reference.HasGenerationIndex {
			for index := range accumulator.References {
				if accumulator.References[index].Kind == reference.Kind && accumulator.References[index].ID == id && !accumulator.References[index].HasGenerationIndex {
					accumulator.References[index].GenerationIndex = reference.GenerationIndex
					accumulator.References[index].HasGenerationIndex = true
					break
				}
			}
		}
		return nil
	}
	if len(accumulator.References) >= chatGPTWebMaxImageOutputReferences {
		return &ChatGPTWebResponseLimitError{
			Message: fmt.Sprintf("chatgpt web image output exceeds %d references", chatGPTWebMaxImageOutputReferences),
		}
	}
	accumulator.referenceSet[key] = struct{}{}
	accumulator.References = append(accumulator.References, reference)
	if reference.Kind == "sediment" {
		appendUniqueString(&accumulator.SedimentIDs, id)
		return nil
	}
	appendUniqueString(&accumulator.FileIDs, id)
	return nil
}

func (accumulator *ChatGPTWebImageAccumulator) markPendingImagePointer(pointer string) {
	kind, id := chatGPTWebImagePointerKindID(pointer)
	if kind == "" {
		return
	}
	accumulator.markPendingReferenceKey(kind + "\x00" + id)
}

func (accumulator *ChatGPTWebImageAccumulator) clearPendingImagePointer(pointer string) {
	kind, id := chatGPTWebImagePointerKindID(pointer)
	if kind == "" {
		return
	}
	accumulator.clearPendingReferenceKey(kind + "\x00" + id)
}

func (accumulator *ChatGPTWebImageAccumulator) markPendingReferenceKey(key string) {
	if accumulator == nil || key == "" {
		return
	}
	if accumulator.pendingReferenceSet == nil {
		accumulator.pendingReferenceSet = make(map[string]struct{})
	}
	accumulator.pendingReferenceSet[key] = struct{}{}
	accumulator.PendingOutput = true
}

func (accumulator *ChatGPTWebImageAccumulator) clearPendingReferenceKey(key string) {
	if accumulator == nil || key == "" || accumulator.pendingReferenceSet == nil {
		return
	}
	delete(accumulator.pendingReferenceSet, key)
	accumulator.PendingOutput = len(accumulator.pendingReferenceSet) > 0
}

func (accumulator *ChatGPTWebImageAccumulator) sortReferences() {
	if accumulator == nil || len(accumulator.References) == 0 {
		return
	}
	sort.SliceStable(accumulator.References, func(left, right int) bool {
		leftReference := accumulator.References[left]
		rightReference := accumulator.References[right]
		if leftReference.HasGenerationIndex != rightReference.HasGenerationIndex {
			return leftReference.HasGenerationIndex
		}
		return leftReference.HasGenerationIndex && leftReference.GenerationIndex < rightReference.GenerationIndex
	})
	accumulator.FileIDs = accumulator.FileIDs[:0]
	accumulator.SedimentIDs = accumulator.SedimentIDs[:0]
	for _, reference := range accumulator.References {
		if reference.Kind == "sediment" {
			appendUniqueString(&accumulator.SedimentIDs, reference.ID)
		} else {
			appendUniqueString(&accumulator.FileIDs, reference.ID)
		}
	}
}

func (accumulator *ChatGPTWebImageAccumulator) captureImageTaskIDs(value any, depth int) error {
	if accumulator == nil || depth > 10 {
		return nil
	}
	switch typed := value.(type) {
	case map[string]any:
		if taskID := strings.TrimSpace(stringFromAny(typed["image_gen_task_id"])); taskID != "" {
			if err := accumulator.appendTaskID(taskID); err != nil {
				return err
			}
		}
		if typed["task_status"] != nil || typed["final_message"] != nil || typed["image_gen_message"] != nil {
			if err := accumulator.appendTaskID(stringFromAny(typed["task_id"])); err != nil {
				return err
			}
		}
		if err := accumulator.appendResponseMessageID(stringFromAny(typed["response_message_id"])); err != nil {
			return err
		}
		for _, child := range typed {
			switch child.(type) {
			case map[string]any, []any:
				if err := accumulator.captureImageTaskIDs(child, depth+1); err != nil {
					return err
				}
			}
		}
	case []any:
		for _, child := range typed {
			if err := accumulator.captureImageTaskIDs(child, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func (accumulator *ChatGPTWebImageAccumulator) appendTaskID(taskID string) error {
	return appendBoundedImageIdentity(&accumulator.TaskIDs, &accumulator.taskSet, taskID, "tasks")
}

func (accumulator *ChatGPTWebImageAccumulator) appendResponseMessageID(messageID string) error {
	return appendBoundedImageIdentity(&accumulator.ResponseMessageIDs, &accumulator.responseMessageSet, messageID, "response messages")
}

func appendBoundedImageIdentity(values *[]string, valueSet *map[string]struct{}, value, label string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	if *valueSet == nil {
		*valueSet = make(map[string]struct{}, len(*values)+1)
		for _, existing := range *values {
			(*valueSet)[existing] = struct{}{}
		}
	}
	if _, exists := (*valueSet)[value]; exists {
		return nil
	}
	if len(*values) >= chatGPTWebMaxImageOutputReferences {
		return &ChatGPTWebResponseLimitError{
			Message: fmt.Sprintf("chatgpt web image output exceeds %d %s", chatGPTWebMaxImageOutputReferences, label),
		}
	}
	(*valueSet)[value] = struct{}{}
	*values = append(*values, value)
	return nil
}

func chatGPTWebImageVisuallyHidden(value map[string]any) bool {
	if value == nil {
		return false
	}
	if value["is_visually_hidden_from_conversation"] == true {
		return true
	}
	metadata, _ := value["metadata"].(map[string]any)
	return metadata != nil && metadata["is_visually_hidden_from_conversation"] == true
}

func chatGPTWebImageGenerationIndex(value map[string]any) (int, bool) {
	if value == nil {
		return 0, false
	}
	if index, ok := chatGPTWebNonnegativeInteger(value["generation_index"]); ok {
		return index, true
	}
	metadata, _ := value["metadata"].(map[string]any)
	if metadata == nil {
		return 0, false
	}
	return chatGPTWebNonnegativeInteger(metadata["generation_index"])
}

func chatGPTWebImageNoAuthPlaceholder(value map[string]any) bool {
	if value == nil {
		return false
	}
	if value["is_no_auth_placeholder"] == true {
		return true
	}
	metadata, _ := value["metadata"].(map[string]any)
	return metadata != nil && metadata["is_no_auth_placeholder"] == true
}

func chatGPTWebImagePointerIncomplete(value map[string]any) bool {
	height, hasHeight := chatGPTWebPositiveNumber(value["height"])
	metadata, _ := value["metadata"].(map[string]any)
	generation, _ := metadata["generation"].(map[string]any)
	generatedHeight, hasGeneratedHeight := chatGPTWebPositiveNumber(generation["height"])
	return hasHeight && hasGeneratedHeight && generatedHeight < height
}

func chatGPTWebNonnegativeInteger(value any) (int, bool) {
	number, ok := chatGPTWebNumber(value)
	if !ok || number < 0 || math.Trunc(number) != number || number > float64(int(^uint(0)>>1)) {
		return 0, false
	}
	return int(number), true
}

func chatGPTWebPositiveNumber(value any) (float64, bool) {
	number, ok := chatGPTWebNumber(value)
	return number, ok && number > 0
}

func chatGPTWebNumber(value any) (float64, bool) {
	var number float64
	var err error
	switch typed := value.(type) {
	case json.Number:
		number, err = typed.Float64()
	case float64:
		number = typed
	case string:
		number, err = strconv.ParseFloat(strings.TrimSpace(typed), 64)
	default:
		return 0, false
	}
	if err != nil || math.IsNaN(number) || math.IsInf(number, 0) {
		return 0, false
	}
	return number, true
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
	return chatGPTWebContainsImageReferenceAt(value, "")
}

func chatGPTWebContainsImageReferenceAt(value any, field string) bool {
	switch typed := value.(type) {
	case string:
		if chatGPTWebImageReferenceKind(field) != "" {
			if kind, _ := chatGPTWebImagePointerKindID(typed); kind != "" {
				return true
			}
			return chatGPTWebImageReferenceIDPattern.MatchString(strings.TrimSpace(typed))
		}
		if field == "" {
			return false
		}
		kind, _ := chatGPTWebImagePointerKindID(typed)
		return kind != ""
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
			childField := ""
			if chatGPTWebImageReferenceField(key) {
				childField = strings.ToLower(strings.TrimSpace(key))
			}
			if key != "asset_pointer" && chatGPTWebContainsImageReferenceAt(item, childField) {
				return true
			}
		}
	case []any:
		for _, item := range typed {
			if chatGPTWebContainsImageReferenceAt(item, field) {
				return true
			}
		}
	}
	return false
}

func chatGPTWebRelevantImageReference(role string, imageTool bool, value any) bool {
	if !chatGPTWebContainsImageReference(value) {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(role), "assistant") || imageTool {
		return true
	}
	return chatGPTWebContainsStructuredImageReference(value)
}

func chatGPTWebContainsStructuredImageReference(value any) bool {
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
			if kind := chatGPTWebImageReferenceKind(key); kind != "" {
				value, ok := item.(string)
				if !ok {
					continue
				}
				if pointerKind, _ := chatGPTWebImagePointerKindID(value); pointerKind != "" || chatGPTWebImageReferenceIDPattern.MatchString(strings.TrimSpace(value)) {
					return true
				}
			}
		}
		for key, item := range typed {
			if key != "asset_pointer" && chatGPTWebContainsStructuredImageReference(item) {
				return true
			}
		}
	case []any:
		for _, item := range typed {
			if chatGPTWebContainsStructuredImageReference(item) {
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
	messages, turnPresent := chatGPTWebCurrentConversationTurn(root, mapping, accumulator.Turn)
	snapshot := &ChatGPTWebImageAccumulator{Turn: accumulator.Turn}
	for _, taskID := range accumulator.TaskIDs {
		if err := snapshot.appendTaskID(taskID); err != nil {
			return err
		}
	}
	for _, messageID := range accumulator.ResponseMessageIDs {
		if err := snapshot.appendResponseMessageID(messageID); err != nil {
			return err
		}
	}
	hasRelevantMessage := false
	hasImageMessage := false
	hasPendingImage := false
	imageTurnTerminal := false
	turnTerminal := false
	rootFailureDetected := false
	for _, message := range messages {
		if err := snapshot.captureImageTaskIDs(message, 0); err != nil {
			return err
		}
		role, imageTool := webMessageImageContext(message)
		terminalText, terminalTextValue := chatGPTWebImageTerminalTextReply(message)
		terminal, failureStatus := chatGPTWebImageConversationState(message)
		if !webMessageCanContainGeneratedImage(role) || (!imageTool && !terminalText && failureStatus == "") {
			continue
		}
		hasRelevantMessage = true
		if imageTool {
			imageTurnTerminal = terminal
			hasImageMessage = true
			if err := snapshot.captureReferences(message); err != nil {
				return err
			}
		}
		if failureStatus != "" {
			snapshot.FailureStatus = chatGPTWebPreferredImageFailure(snapshot.FailureStatus, failureStatus)
		} else if imageTool {
			hasPendingImage = hasPendingImage || snapshot.PendingOutput || chatGPTWebImageConversationPending(message)
		} else if terminalText {
			turnTerminal = turnTerminal || terminal
			if strings.TrimSpace(terminalTextValue) != "" {
				snapshot.replaceAssistantText(terminalTextValue)
				snapshot.assistantTerminalSeen = snapshot.assistantTerminalSeen || terminal
				snapshot.updateTerminalAssistantText()
			}
		}
	}
	if len(mapping) == 0 || turnPresent {
		rootState := make(map[string]any, len(root))
		for key, value := range root {
			if key != "mapping" {
				rootState[key] = value
			}
		}
		rootTerminal, rootFailure := chatGPTWebImageConversationState(rootState)
		if rootFailure != "" {
			snapshot.FailureStatus = chatGPTWebPreferredImageFailure(snapshot.FailureStatus, rootFailure)
			rootFailureDetected = true
		} else if !hasRelevantMessage {
			snapshot.Terminal = rootTerminal
		}
	}
	if snapshot.FailureStatus != "" {
		snapshot.assistantTerminalSeen = false
		snapshot.terminalAssistantText = false
		snapshot.Terminal = rootFailureDetected || !hasPendingImage
	} else if hasRelevantMessage {
		hasPendingImage = hasPendingImage || snapshot.PendingOutput
		snapshot.Terminal = !hasPendingImage && (turnTerminal || hasImageMessage && imageTurnTerminal)
	}
	accumulator.TaskIDs = snapshot.TaskIDs
	accumulator.ResponseMessageIDs = snapshot.ResponseMessageIDs
	accumulator.FileIDs = snapshot.FileIDs
	accumulator.SedimentIDs = snapshot.SedimentIDs
	accumulator.References = snapshot.References
	accumulator.taskSet = snapshot.taskSet
	accumulator.responseMessageSet = snapshot.responseMessageSet
	accumulator.pendingReferenceSet = snapshot.pendingReferenceSet
	accumulator.referenceSet = snapshot.referenceSet
	accumulator.Terminal = snapshot.Terminal
	accumulator.FailureStatus = snapshot.FailureStatus
	accumulator.PendingOutput = snapshot.PendingOutput
	accumulator.HiddenOutputSeen = snapshot.HiddenOutputSeen
	accumulator.IncompleteOutputSeen = snapshot.IncompleteOutputSeen
	accumulator.PlaceholderOutputSeen = snapshot.PlaceholderOutputSeen
	accumulator.FinalMessageSeen = snapshot.FinalMessageSeen
	accumulator.assistantTextValue = snapshot.assistantTextValue
	accumulator.assistantTextSeen = snapshot.assistantTextSeen
	accumulator.assistantTerminalSeen = snapshot.assistantTerminalSeen
	accumulator.terminalAssistantText = snapshot.terminalAssistantText
	return nil
}

func chatGPTWebImageConversationPending(root map[string]any) bool {
	pending := false
	var visit func(any)
	visit = func(value any) {
		if pending {
			return
		}
		switch typed := value.(type) {
		case map[string]any:
			if chatGPTWebImageGhostriderStatus(typed) == "intermediate" {
				pending = true
				return
			}
			for key, item := range typed {
				switch strings.ToLower(strings.TrimSpace(key)) {
				case "status", "state":
					switch strings.ToLower(strings.TrimSpace(stringFromAny(item))) {
					case "pending", "queued", "running", "in_progress", "processing", "started",
						"created", "intermediate", "finalizing", "undetermined", "skipping":
						pending = true
						return
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
	return pending
}

type chatGPTWebConversationMessage struct {
	id           string
	parent       string
	createdAt    float64
	hasCreatedAt bool
	message      map[string]any
}

func chatGPTWebCurrentConversationTurn(root, mapping map[string]any, turn ChatGPTWebImageTurn) ([]map[string]any, bool) {
	currentNode := strings.TrimSpace(stringFromAny(root["current_node"]))
	turnMessageID := strings.TrimSpace(turn.MessageID)
	ordered := make([]chatGPTWebConversationMessage, 0, len(mapping))
	if currentNode != "" {
		if _, ok := mapping[currentNode]; ok {
			visited := make(map[string]struct{}, len(mapping))
			for nodeID := currentNode; nodeID != "" && len(visited) < len(mapping); {
				if _, seen := visited[nodeID]; seen {
					break
				}
				visited[nodeID] = struct{}{}
				node, _ := mapping[nodeID].(map[string]any)
				if node == nil {
					break
				}
				if message, _ := node["message"].(map[string]any); message != nil {
					ordered = append(ordered, chatGPTWebConversationMessage{
						id:      nodeID,
						parent:  strings.TrimSpace(stringFromAny(node["parent"])),
						message: message,
					})
				}
				nodeID = strings.TrimSpace(stringFromAny(node["parent"]))
			}
			for left, right := 0, len(ordered)-1; left < right; left, right = left+1, right-1 {
				ordered[left], ordered[right] = ordered[right], ordered[left]
			}
			turnStart := chatGPTWebConversationTurnStart(ordered, turnMessageID)
			if turnMessageID != "" && turnStart < 0 {
				return nil, false
			}
			if turnStart < 0 {
				messages := make([]map[string]any, 0, len(ordered))
				for _, candidate := range ordered {
					messages = append(messages, candidate.message)
				}
				return messages, true
			}
			messages := make([]map[string]any, 0, len(ordered)-turnStart-1)
			for _, candidate := range ordered[turnStart+1:] {
				if chatGPTWebConversationMessageRole(candidate.message) == "user" {
					break
				}
				messages = append(messages, candidate.message)
			}
			return messages, true
		}
	}
	for id, rawNode := range mapping {
		node, _ := rawNode.(map[string]any)
		message, _ := node["message"].(map[string]any)
		if message == nil {
			continue
		}
		createdAt, hasCreatedAt := chatGPTWebConversationCreateTime(message["create_time"])
		ordered = append(ordered, chatGPTWebConversationMessage{
			id:           id,
			parent:       strings.TrimSpace(stringFromAny(node["parent"])),
			createdAt:    createdAt,
			hasCreatedAt: hasCreatedAt,
			message:      message,
		})
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].hasCreatedAt != ordered[j].hasCreatedAt {
			return !ordered[i].hasCreatedAt
		}
		if ordered[i].createdAt == ordered[j].createdAt {
			leftTarget := chatGPTWebConversationMessageID(ordered[i]) == turnMessageID
			rightTarget := chatGPTWebConversationMessageID(ordered[j]) == turnMessageID
			if leftTarget != rightTarget {
				return leftTarget
			}
			return ordered[i].id < ordered[j].id
		}
		return ordered[i].createdAt < ordered[j].createdAt
	})

	turnStart := chatGPTWebConversationTurnStart(ordered, turnMessageID)
	if turnMessageID != "" {
		if turnStart < 0 {
			return nil, false
		}
		messages := make([]map[string]any, 0, len(ordered)-1)
		orderedFallbackOpen := false
		nearestUsers := make(map[string]string, len(mapping))
		for index, candidate := range ordered {
			candidateID := chatGPTWebConversationMessageID(candidate)
			if candidateID == turnMessageID {
				orderedFallbackOpen = true
				continue
			}
			role := chatGPTWebConversationMessageRole(candidate.message)
			nearestUser := chatGPTWebConversationNearestUser(mapping, candidate.id, nearestUsers)
			if nearestUser == turnMessageID {
				messages = append(messages, candidate.message)
				continue
			}
			if index < turnStart {
				continue
			}
			if role == "user" {
				orderedFallbackOpen = false
				continue
			}
			if nearestUser == "" && orderedFallbackOpen {
				messages = append(messages, candidate.message)
			}
		}
		return messages, true
	}
	if turnStart < 0 {
		messages := make([]map[string]any, 0, len(ordered))
		for _, candidate := range ordered {
			messages = append(messages, candidate.message)
		}
		return messages, true
	}
	messages := make([]map[string]any, 0, len(ordered)-turnStart-1)
	for _, candidate := range ordered[turnStart+1:] {
		if chatGPTWebConversationMessageRole(candidate.message) == "user" {
			break
		}
		messages = append(messages, candidate.message)
	}
	return messages, true
}

func chatGPTWebConversationMessageID(candidate chatGPTWebConversationMessage) string {
	if id := strings.TrimSpace(candidate.id); id != "" {
		return id
	}
	return strings.TrimSpace(stringFromAny(candidate.message["id"]))
}

func chatGPTWebConversationNearestUser(mapping map[string]any, nodeID string, cache map[string]string) string {
	nodeID = strings.TrimSpace(nodeID)
	if nearest, exists := cache[nodeID]; exists {
		return nearest
	}
	path := make([]string, 0, 8)
	visited := make(map[string]struct{}, 8)
	nearest := ""
	for nodeID != "" && len(visited) < len(mapping) {
		if cached, exists := cache[nodeID]; exists {
			nearest = cached
			break
		}
		if _, seen := visited[nodeID]; seen {
			break
		}
		visited[nodeID] = struct{}{}
		path = append(path, nodeID)
		node, _ := mapping[nodeID].(map[string]any)
		if node == nil {
			break
		}
		message, _ := node["message"].(map[string]any)
		if chatGPTWebConversationMessageRole(message) == "user" {
			nearest = strings.TrimSpace(stringFromAny(message["id"]))
			if nearest == "" {
				nearest = nodeID
			}
			break
		}
		nodeID = strings.TrimSpace(stringFromAny(node["parent"]))
	}
	for _, pathNode := range path {
		cache[pathNode] = nearest
	}
	return nearest
}

func chatGPTWebConversationTurnStart(messages []chatGPTWebConversationMessage, messageID string) int {
	messageID = strings.TrimSpace(messageID)
	lastUser := -1
	for index, candidate := range messages {
		author, _ := candidate.message["author"].(map[string]any)
		if !strings.EqualFold(strings.TrimSpace(stringFromAny(author["role"])), "user") {
			continue
		}
		candidateID := strings.TrimSpace(candidate.id)
		if candidateID == "" {
			candidateID = strings.TrimSpace(stringFromAny(candidate.message["id"]))
		}
		if messageID != "" && candidateID == messageID {
			return index
		}
		lastUser = index
	}
	if messageID != "" {
		return -1
	}
	return lastUser
}

func chatGPTWebConversationMessageRole(message map[string]any) string {
	author, _ := message["author"].(map[string]any)
	return strings.ToLower(strings.TrimSpace(stringFromAny(author["role"])))
}

func chatGPTWebConversationCreateTime(value any) (float64, bool) {
	var number float64
	var err error
	switch typed := value.(type) {
	case float64:
		number = typed
	case json.Number:
		number, err = typed.Float64()
	case string:
		trimmed := strings.TrimSpace(typed)
		number, err = strconv.ParseFloat(trimmed, 64)
		if err != nil {
			parsed, timeErr := time.Parse(time.RFC3339Nano, trimmed)
			if timeErr != nil {
				return 0, false
			}
			number = float64(parsed.UnixNano()) / 1e9
			err = nil
		}
	default:
		return 0, false
	}
	if err != nil || number <= 0 || math.IsNaN(number) || math.IsInf(number, 0) {
		return 0, false
	}
	return number, true
}

func chatGPTWebImageOuterFailure(event map[string]any) (bool, string) {
	eventType := strings.ToLower(strings.TrimSpace(stringFromAny(event["type"])))
	response, _ := event["response"].(map[string]any)
	for _, value := range []any{event["error"], response["error"]} {
		if chatGPTWebTruthy(value) {
			return true, chatGPTWebStructuredFailureText(value)
		}
	}
	if eventType == "response.failed" || eventType == "response.incomplete" {
		for _, value := range []any{response["error"], event["error"], response["incomplete_details"], event["incomplete_details"]} {
			if detail := chatGPTWebStructuredFailureText(value); detail != "" {
				return true, detail
			}
		}
		return true, strings.ReplaceAll(eventType, ".", " ")
	}
	if eventType == "error" {
		return true, chatGPTWebStructuredFailureText(event["error"])
	}
	return false, ""
}

func chatGPTWebImageOuterModeration(event map[string]any) bool {
	eventType := strings.ToLower(strings.TrimSpace(stringFromAny(event["type"])))
	if eventType == "moderation" {
		moderation, _ := event["moderation_response"].(map[string]any)
		if moderation["blocked"] == true {
			return true
		}
	}
	response, _ := event["response"].(map[string]any)
	for _, value := range []any{event["error"], response["error"], event["incomplete_details"], response["incomplete_details"]} {
		if chatGPTWebImageStructuredModeration(value, 0) {
			return true
		}
	}
	return false
}

func chatGPTWebStructuredFailureText(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case map[string]any:
		for _, key := range []string{"message", "detail", "reason", "code", "type", "error", "incomplete_details"} {
			if detail := chatGPTWebStructuredFailureText(typed[key]); detail != "" {
				return detail
			}
		}
		if len(typed) > 0 {
			if encoded, err := json.Marshal(typed); err == nil {
				return string(encoded)
			}
		}
	case []any:
		for _, item := range typed {
			if detail := chatGPTWebStructuredFailureText(item); detail != "" {
				return detail
			}
		}
	}
	return ""
}

func chatGPTWebTruthy(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case string:
		return typed != ""
	case json.Number:
		valueFloat, err := typed.Float64()
		return err == nil && valueFloat != 0
	case float64:
		return typed != 0
	case map[string]any:
		return len(typed) > 0
	case []any:
		return len(typed) > 0
	default:
		return true
	}
}

// CaptureChatGPTWebImageTasks extracts image outputs from task records that
// belong to one conversation.
func CaptureChatGPTWebImageTasks(payload []byte, conversationID string, accumulator *ChatGPTWebImageAccumulator) (ChatGPTWebImageTaskState, error) {
	if accumulator == nil {
		return ChatGPTWebImageTaskState{}, errors.New("chatgpt web image accumulator is nil")
	}
	conversationID = strings.TrimSpace(conversationID)
	if conversationID == "" {
		return ChatGPTWebImageTaskState{}, nil
	}
	var root map[string]any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&root); err != nil {
		return ChatGPTWebImageTaskState{}, fmt.Errorf("decode chatgpt web image tasks: %w", err)
	}
	tasks, _ := root["tasks"].([]any)
	fallbackCreatedAt, hasFallbackCreatedAt := chatGPTWebImageTaskTurnBoundary(tasks, conversationID, accumulator)
	snapshot := &ChatGPTWebImageAccumulator{ConversationID: conversationID, Turn: accumulator.Turn}
	for _, taskID := range accumulator.TaskIDs {
		if err := snapshot.appendTaskID(taskID); err != nil {
			return ChatGPTWebImageTaskState{}, err
		}
	}
	for _, messageID := range accumulator.ResponseMessageIDs {
		if err := snapshot.appendResponseMessageID(messageID); err != nil {
			return ChatGPTWebImageTaskState{}, err
		}
	}
	state := ChatGPTWebImageTaskState{}
	for _, rawTask := range tasks {
		task, _ := rawTask.(map[string]any)
		if task == nil || !chatGPTWebImageTaskMatchesCurrent(task, conversationID, accumulator, fallbackCreatedAt, hasFallbackCreatedAt) {
			continue
		}
		message, imageTask := chatGPTWebImageTaskMessage(task)
		if !imageTask {
			continue
		}
		state.Matched++
		taskID := chatGPTWebImageTaskID(task)
		responseMessageID := chatGPTWebImageTaskResponseMessageID(task)
		if err := snapshot.appendTaskID(taskID); err != nil {
			return ChatGPTWebImageTaskState{}, err
		}
		if err := snapshot.appendResponseMessageID(responseMessageID); err != nil {
			return ChatGPTWebImageTaskState{}, err
		}
		taskTerminal := false
		taskFailureStatus := ""
		taskStatus := strings.ToLower(strings.TrimSpace(stringFromAny(task["status"])))
		if chatGPTWebConversationStatusFailed(taskStatus) {
			taskTerminal = true
			taskFailureStatus = taskStatus
		} else if chatGPTWebTerminalMessageStatus(taskStatus) {
			taskTerminal = true
		}
		if message == nil {
			state.appendTarget(ChatGPTWebImageTaskTarget{TaskID: taskID, ResponseMessageID: responseMessageID, Terminal: taskTerminal})
			if taskTerminal {
				state.Terminal++
				if taskFailureStatus != "" {
					snapshot.FailureStatus = chatGPTWebPreferredImageFailure(snapshot.FailureStatus, taskFailureStatus)
				}
			}
			continue
		}
		role, _ := webMessageImageContext(message)
		if role != "" && !webMessageCanContainGeneratedImage(role) {
			state.appendTarget(ChatGPTWebImageTaskTarget{TaskID: taskID, ResponseMessageID: responseMessageID, Terminal: taskTerminal})
			if taskTerminal {
				state.Terminal++
				if taskFailureStatus != "" {
					snapshot.FailureStatus = chatGPTWebPreferredImageFailure(snapshot.FailureStatus, taskFailureStatus)
				}
			}
			continue
		}
		messageSnapshot := &ChatGPTWebImageAccumulator{}
		if err := messageSnapshot.captureImageTaskIDs(message, 0); err != nil {
			return ChatGPTWebImageTaskState{}, err
		}
		if err := messageSnapshot.captureReferences(message); err != nil {
			return ChatGPTWebImageTaskState{}, err
		}
		if terminalText, terminalTextValue := chatGPTWebImageTerminalTextReply(message); terminalText && strings.TrimSpace(terminalTextValue) != "" {
			messageSnapshot.replaceAssistantText(terminalTextValue)
			messageSnapshot.assistantTerminalSeen = true
			messageSnapshot.updateTerminalAssistantText()
		}
		mergedSnapshot, err := MergeChatGPTWebImageAccumulators(snapshot, messageSnapshot)
		if err != nil {
			return ChatGPTWebImageTaskState{}, err
		}
		snapshot = mergedSnapshot
		messageTerminal, failureStatus := chatGPTWebImageConversationState(message)
		messageStatus := strings.ToLower(strings.TrimSpace(stringFromAny(message["status"])))
		if failureStatus == "" && chatGPTWebFailureMessageStatus(messageStatus) {
			failureStatus = messageStatus
			messageTerminal = true
		}
		terminal := taskTerminal
		if taskStatus == "" {
			terminal = messageTerminal
		}
		if messageSnapshot.PendingOutput || chatGPTWebImageConversationPending(message) {
			terminal = false
		}
		state.appendTarget(ChatGPTWebImageTaskTarget{TaskID: taskID, ResponseMessageID: responseMessageID, Terminal: terminal})
		if terminal {
			state.Terminal++
			if failureStatus == "" {
				failureStatus = taskFailureStatus
			}
			if failureStatus != "" {
				snapshot.FailureStatus = chatGPTWebPreferredImageFailure(snapshot.FailureStatus, failureStatus)
			}
		}
	}
	snapshot.Terminal = state.AllTerminal()
	accumulator.FileIDs = snapshot.FileIDs
	accumulator.SedimentIDs = snapshot.SedimentIDs
	accumulator.References = snapshot.References
	accumulator.TaskIDs = snapshot.TaskIDs
	accumulator.ResponseMessageIDs = snapshot.ResponseMessageIDs
	accumulator.taskSet = snapshot.taskSet
	accumulator.responseMessageSet = snapshot.responseMessageSet
	accumulator.pendingReferenceSet = snapshot.pendingReferenceSet
	accumulator.referenceSet = snapshot.referenceSet
	accumulator.ConversationID = snapshot.ConversationID
	accumulator.Terminal = snapshot.Terminal
	accumulator.FailureStatus = snapshot.FailureStatus
	accumulator.PendingOutput = snapshot.PendingOutput
	accumulator.HiddenOutputSeen = snapshot.HiddenOutputSeen
	accumulator.IncompleteOutputSeen = snapshot.IncompleteOutputSeen
	accumulator.PlaceholderOutputSeen = snapshot.PlaceholderOutputSeen
	accumulator.FinalMessageSeen = snapshot.FinalMessageSeen
	accumulator.assistantTextValue = snapshot.assistantTextValue
	accumulator.assistantTextSeen = snapshot.assistantTextSeen
	accumulator.assistantTerminalSeen = snapshot.assistantTerminalSeen
	accumulator.terminalAssistantText = snapshot.terminalAssistantText
	return state, nil
}

func (state *ChatGPTWebImageTaskState) appendTarget(target ChatGPTWebImageTaskTarget) {
	if state == nil {
		return
	}
	target.TaskID = strings.TrimSpace(target.TaskID)
	target.ResponseMessageID = strings.TrimSpace(target.ResponseMessageID)
	if target.TaskID == "" {
		return
	}
	for index := range state.Targets {
		if state.Targets[index].TaskID == target.TaskID {
			if state.Targets[index].ResponseMessageID == "" {
				state.Targets[index].ResponseMessageID = target.ResponseMessageID
			}
			if target.Terminal && !state.Targets[index].Terminal {
				state.Targets[index].Terminal = true
			}
			return
		}
	}
	state.Targets = append(state.Targets, target)
}

// ChatGPTWebImageTasksNextCursor returns the next cursor from one official
// paginated task response. An absent cursor terminates pagination.
func ChatGPTWebImageTasksNextCursor(payload []byte) (string, bool, error) {
	var root map[string]any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&root); err != nil {
		return "", false, fmt.Errorf("decode chatgpt web image task cursor: %w", err)
	}
	for _, candidate := range []map[string]any{root, mapFromAny(root["pagination"]), mapFromAny(root["page_info"])} {
		if candidate == nil {
			continue
		}
		for _, key := range []string{"next_cursor", "nextCursor", "cursor"} {
			if cursor := strings.TrimSpace(stringFromAny(candidate[key])); cursor != "" {
				return cursor, true, nil
			}
		}
	}
	return "", false, nil
}

func mapFromAny(value any) map[string]any {
	result, _ := value.(map[string]any)
	return result
}

func chatGPTWebImageTaskMessage(task map[string]any) (map[string]any, bool) {
	message, _ := task["image_gen_message"].(map[string]any)
	if message != nil {
		return message, true
	}
	for _, key := range []string{"type", "task_type"} {
		if strings.EqualFold(strings.TrimSpace(stringFromAny(task[key])), "image_gen") {
			return nil, true
		}
	}
	if chatGPTWebImageMetadataActivity(task) {
		return nil, true
	}
	metadata, _ := task["metadata"].(map[string]any)
	if chatGPTWebImageMetadataActivity(metadata) {
		return nil, true
	}
	return nil, false
}

func chatGPTWebImageTaskMatchesConversation(task map[string]any, conversationID string) bool {
	for _, key := range []string{"conversation_id", "original_conversation_id"} {
		if strings.TrimSpace(stringFromAny(task[key])) == conversationID {
			return true
		}
	}
	return false
}

func chatGPTWebImageTaskTurnBoundary(tasks []any, conversationID string, accumulator *ChatGPTWebImageAccumulator) (float64, bool) {
	if accumulator == nil || accumulator.Turn.CreatedAt <= 0 {
		return 0, false
	}
	messageID := strings.TrimSpace(accumulator.Turn.MessageID)
	// Legacy callers without a message ID can only use time. Keep the earliest
	// timestamp cohort so later task batches are not merged into the same turn.
	fallbackCreatedAt := 0.0
	hasFallbackCreatedAt := false
	for _, rawTask := range tasks {
		task, _ := rawTask.(map[string]any)
		if task == nil || !chatGPTWebImageTaskMatchesConversation(task, conversationID) ||
			chatGPTWebImageTaskHasExplicitMismatch(task, accumulator) {
			continue
		}
		if _, imageTask := chatGPTWebImageTaskMessage(task); !imageTask {
			continue
		}
		if messageID != "" {
			_, relationSeen := chatGPTWebImageTaskRelation(task, messageID)
			if relationSeen {
				continue
			}
		}
		createdAt, ok := chatGPTWebImageTaskCreatedAt(task)
		if !ok || createdAt < accumulator.Turn.CreatedAt {
			continue
		}
		if !hasFallbackCreatedAt || createdAt < fallbackCreatedAt {
			fallbackCreatedAt = createdAt
			hasFallbackCreatedAt = true
		}
	}
	return fallbackCreatedAt, hasFallbackCreatedAt
}

func chatGPTWebImageTaskMatchesCurrent(task map[string]any, conversationID string, accumulator *ChatGPTWebImageAccumulator, fallbackCreatedAt float64, hasFallbackCreatedAt bool) bool {
	if task == nil || accumulator == nil {
		return false
	}
	if taskID := chatGPTWebImageTaskID(task); taskID != "" && len(accumulator.TaskIDs) > 0 {
		return imageIdentityContains(accumulator.TaskIDs, taskID)
	}
	messageID := strings.TrimSpace(accumulator.Turn.MessageID)
	if messageID != "" {
		relationMatch, relationSeen := chatGPTWebImageTaskRelation(task, messageID)
		if relationSeen {
			return relationMatch
		}
	}
	if !chatGPTWebImageTaskMatchesConversation(task, conversationID) {
		return false
	}
	return chatGPTWebImageTaskMatchesTurn(task, accumulator.Turn, fallbackCreatedAt, hasFallbackCreatedAt)
}

func chatGPTWebImageTaskHasExplicitMismatch(task map[string]any, accumulator *ChatGPTWebImageAccumulator) bool {
	if task == nil || accumulator == nil {
		return false
	}
	if taskID := chatGPTWebImageTaskID(task); taskID != "" && len(accumulator.TaskIDs) > 0 {
		return !imageIdentityContains(accumulator.TaskIDs, taskID)
	}
	messageID := strings.TrimSpace(accumulator.Turn.MessageID)
	if messageID == "" {
		return false
	}
	matched, seen := chatGPTWebImageTaskRelation(task, messageID)
	return seen && !matched
}

func imageIdentityContains(values []string, value string) bool {
	value = strings.TrimSpace(value)
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func chatGPTWebImageTaskMatchesTurn(task map[string]any, turn ChatGPTWebImageTurn, fallbackCreatedAt float64, hasFallbackCreatedAt bool) bool {
	messageID := strings.TrimSpace(turn.MessageID)
	if messageID != "" {
		relationMatch, relationSeen := chatGPTWebImageTaskRelation(task, messageID)
		if relationSeen {
			return relationMatch
		}
	}
	if turn.CreatedAt <= 0 {
		return true
	}
	if !hasFallbackCreatedAt {
		return false
	}
	createdAt, ok := chatGPTWebImageTaskCreatedAt(task)
	return ok && createdAt == fallbackCreatedAt
}

func chatGPTWebImageTaskRelation(task map[string]any, messageID string) (bool, bool) {
	relationSeen := false
	for _, candidate := range chatGPTWebImageTaskObjects(task) {
		for _, key := range []string{"original_conversation_user_message_id", "parent_message_id", "request_message_id", "source_message_id", "original_message_id"} {
			relationID := strings.TrimSpace(stringFromAny(candidate[key]))
			if relationID == "" {
				continue
			}
			relationSeen = true
			if messageID != "" && relationID == messageID {
				return true, true
			}
		}
	}
	return false, relationSeen
}

func chatGPTWebImageTaskID(task map[string]any) string {
	for _, candidate := range chatGPTWebImageTaskObjects(task) {
		for _, key := range []string{"task_id", "image_gen_task_id"} {
			if value := strings.TrimSpace(stringFromAny(candidate[key])); value != "" {
				return value
			}
		}
	}
	return ""
}

func chatGPTWebImageTaskResponseMessageID(task map[string]any) string {
	for _, candidate := range chatGPTWebImageTaskObjects(task) {
		if value := strings.TrimSpace(stringFromAny(candidate["response_message_id"])); value != "" {
			return value
		}
	}
	return ""
}

func chatGPTWebImageTaskCreatedAt(task map[string]any) (float64, bool) {
	for _, candidate := range chatGPTWebImageTaskObjects(task) {
		for _, key := range []string{"create_time", "created_at", "created_time", "created_ts"} {
			if createdAt, ok := chatGPTWebConversationCreateTime(candidate[key]); ok {
				return createdAt, true
			}
		}
	}
	return 0, false
}

func chatGPTWebImageTaskObjects(task map[string]any) []map[string]any {
	objects := []map[string]any{task}
	if metadata, _ := task["metadata"].(map[string]any); metadata != nil {
		objects = append(objects, metadata)
	}
	message, _ := task["image_gen_message"].(map[string]any)
	if message == nil {
		return objects
	}
	objects = append(objects, message)
	if metadata, _ := message["metadata"].(map[string]any); metadata != nil {
		objects = append(objects, metadata)
	}
	return objects
}

func chatGPTWebImageConversationState(root map[string]any) (bool, string) {
	if failed, message := chatGPTWebImageMessageFailure(root); failed {
		return true, message
	}
	hasCompletionMarker := false
	hasTerminalStatus := false
	failureStatus := ""
	var visit func(any)
	visit = func(value any) {
		switch typed := value.(type) {
		case map[string]any:
			switch chatGPTWebImageGhostriderStatus(typed) {
			case "cancelled", "canceled":
				failureStatus = chatGPTWebPreferredImageStatus(failureStatus, "cancelled")
			case "final":
				hasCompletionMarker = true
				hasTerminalStatus = true
			}
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
					if chatGPTWebFailureMessageStatus(status) {
						failureStatus = chatGPTWebPreferredImageStatus(failureStatus, status)
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
	if terminal, _ := chatGPTWebImageTerminalTextReply(root); terminal {
		return true, ""
	}
	return hasCompletionMarker && hasTerminalStatus, ""
}

func chatGPTWebPreferredImageStatus(current, incoming string) string {
	current = strings.ToLower(strings.TrimSpace(current))
	incoming = strings.ToLower(strings.TrimSpace(incoming))
	if current == "" {
		return incoming
	}
	if incoming == "" {
		return current
	}
	currentRank := chatGPTWebImageStatusRank(current)
	incomingRank := chatGPTWebImageStatusRank(incoming)
	if incomingRank > currentRank || (incomingRank == currentRank && incoming < current) {
		return incoming
	}
	return current
}

func chatGPTWebImageStatusRank(value string) int {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "blocked", "content_filter":
		return 5
	case "max_tokens", "max_output_tokens", "length":
		return 4
	case "expired", "interrupted", "incomplete":
		return 3
	case "finished_partial_completion":
		return 2
	default:
		return 1
	}
}

func chatGPTWebImageStreamTerminal(event map[string]any) bool {
	return strings.EqualFold(strings.TrimSpace(stringFromAny(event["type"])), "message_stream_complete")
}

func chatGPTWebImageMessageFailure(message map[string]any) (bool, string) {
	if message == nil {
		return false, ""
	}
	if chatGPTWebImageToolQuotaFailure(message) {
		return true, "image_generation_limit_reached"
	}
	if chatGPTWebImageMessageModeration(message) {
		return true, "content_filter"
	}
	if chatGPTWebImageAssistantTextFailure(message) {
		return true, "content_filter"
	}
	metadata, _ := message["metadata"].(map[string]any)
	isError := message["is_error"] == true || metadata["is_error"] == true ||
		message["blocked"] == true || metadata["blocked"] == true ||
		chatGPTWebTruthy(message["error"]) || chatGPTWebTruthy(metadata["error"])
	if !isError {
		return false, ""
	}
	detail := ""
	for _, value := range []any{message["error"], metadata["error"]} {
		if text := chatGPTWebStructuredFailureText(value); text != "" {
			detail = text
			break
		}
	}
	if detail == "" {
		if content, ok := message["content"].(map[string]any); ok {
			detail = strings.TrimSpace(textFromAny(content["parts"]))
			if detail == "" {
				detail = strings.TrimSpace(stringFromAny(content["text"]))
			}
		}
	}
	if detail == "" {
		for _, value := range []any{message["status"], metadata["status"]} {
			if text := chatGPTWebStructuredFailureText(value); text != "" {
				detail = text
				break
			}
		}
	}
	if detail == "" {
		detail = "image_tool_error"
	}
	return true, detail
}

func chatGPTWebImageToolQuotaFailure(message map[string]any) bool {
	author, _ := message["author"].(map[string]any)
	if !strings.EqualFold(strings.TrimSpace(stringFromAny(author["role"])), "tool") {
		return false
	}
	content, _ := message["content"].(map[string]any)
	return strings.EqualFold(strings.TrimSpace(stringFromAny(content["content_type"])), "system_error") &&
		strings.EqualFold(strings.TrimSpace(stringFromAny(content["name"])), "ChatGPTAgentToolRateLimitException")
}

func chatGPTWebImageStreamMessageFailure(message map[string]any) (bool, string) {
	if failed, detail := chatGPTWebImageMessageFailure(message); failed {
		return true, detail
	}
	if status := chatGPTWebConversationTerminalError(message); status != "" {
		if chatGPTWebImageModerationStatus(status) {
			return true, "content_filter"
		}
		if detail := chatGPTWebImageMessageText(message); detail != "" && !chatGPTWebGenericImageFailure(detail) {
			return true, detail
		}
		return true, status
	}
	return false, ""
}

func chatGPTWebImageMessageModeration(message map[string]any) bool {
	metadata, _ := message["metadata"].(map[string]any)
	for _, source := range []map[string]any{message, metadata} {
		if source == nil {
			continue
		}
		if source["blocked"] == true {
			return true
		}
		for _, key := range []string{"status", "state", "code", "type", "reason", "finish_reason"} {
			if chatGPTWebImageModerationStatus(stringFromAny(source[key])) {
				return true
			}
		}
		for _, key := range []string{"error", "finish_details", "incomplete_details"} {
			if chatGPTWebImageStructuredModeration(source[key], 0) {
				return true
			}
		}
	}
	return false
}

func chatGPTWebImageAssistantTextFailure(message map[string]any) bool {
	author, _ := message["author"].(map[string]any)
	if !strings.EqualFold(strings.TrimSpace(stringFromAny(author["role"])), "assistant") {
		return false
	}
	metadata, _ := message["metadata"].(map[string]any)
	if chatGPTWebTruthy(message["error"]) || chatGPTWebTruthy(metadata["error"]) {
		return false
	}
	explicitFailure := message["is_error"] == true || metadata["is_error"] == true
	return explicitFailure && strings.TrimSpace(chatGPTWebImageMessageText(message)) != ""
}

func chatGPTWebImageStructuredModeration(value any, depth int) bool {
	if depth > 8 {
		return false
	}
	switch typed := value.(type) {
	case map[string]any:
		if typed["blocked"] == true {
			return true
		}
		for key, item := range typed {
			switch strings.ToLower(strings.TrimSpace(key)) {
			case "status", "state", "code", "type", "reason", "finish_reason":
				if chatGPTWebImageModerationStatus(stringFromAny(item)) {
					return true
				}
			}
			switch item.(type) {
			case map[string]any, []any:
				if chatGPTWebImageStructuredModeration(item, depth+1) {
					return true
				}
			}
		}
	case []any:
		for _, item := range typed {
			if chatGPTWebImageStructuredModeration(item, depth+1) {
				return true
			}
		}
	}
	return false
}

func chatGPTWebImageModerationStatus(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "blocked", "content_filter", "moderation_blocked":
		return true
	default:
		return false
	}
}

func chatGPTWebImageMessageText(message map[string]any) string {
	content, _ := message["content"].(map[string]any)
	detail := strings.TrimSpace(textFromAny(content["parts"]))
	if detail == "" {
		detail = strings.TrimSpace(stringFromAny(content["text"]))
	}
	return detail
}

func chatGPTWebImageTerminalTextReply(message map[string]any) (bool, string) {
	if message == nil {
		return false, ""
	}
	author, _ := message["author"].(map[string]any)
	if !strings.EqualFold(strings.TrimSpace(stringFromAny(author["role"])), "assistant") {
		return false, ""
	}
	content, _ := message["content"].(map[string]any)
	contentType := strings.ToLower(strings.TrimSpace(stringFromAny(content["content_type"])))
	if contentType != "" && contentType != "text" && contentType != "code" {
		return false, ""
	}
	metadata, _ := message["metadata"].(map[string]any)
	terminal := message["end_turn"] == true ||
		chatGPTWebTerminalMessageStatus(stringFromAny(message["status"])) ||
		chatGPTWebTerminalMessageStatus(stringFromAny(metadata["status"]))
	if !terminal {
		terminal = chatGPTWebImageCompletionMetadata(metadata)
	}
	if !terminal {
		return false, ""
	}
	text := chatGPTWebImageMessageText(message)
	if chatGPTWebSkippedMainlineControl(text) {
		return false, ""
	}
	return true, text
}

func chatGPTWebSkippedMainlineControl(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || !strings.Contains(value, "skipped_mainline") {
		return false
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal([]byte(value), &document); err != nil || len(document) != 1 {
		return false
	}
	raw, ok := document["skipped_mainline"]
	if !ok {
		return false
	}
	var skipped bool
	return json.Unmarshal(raw, &skipped) == nil && skipped
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
		"finished_with_error", "finished_partial_completion", "content_filter",
		"max_tokens", "max_output_tokens", "length", "expired", "interrupted", "incomplete":
		return true
	default:
		return false
	}
}

func webMessageCanContainGeneratedImage(role string) bool {
	return role == "tool" || role == "assistant"
}

func messageFromWebEvent(event map[string]any) map[string]any {
	value, _ := event["v"].(map[string]any)
	for _, candidate := range []map[string]any{event, value} {
		if candidate == nil {
			continue
		}
		if message, ok := candidate["message"].(map[string]any); ok {
			return message
		}
		if message, ok := candidate["image_gen_message"].(map[string]any); ok {
			return message
		}
		if message, ok := candidate["final_message"].(map[string]any); ok {
			return message
		}
	}
	return nil
}

func webMessageImageContext(message map[string]any) (string, bool) {
	author, _ := message["author"].(map[string]any)
	role := strings.ToLower(strings.TrimSpace(stringFromAny(author["role"])))
	metadata, _ := message["metadata"].(map[string]any)
	imageTool := chatGPTWebImageMetadataActivity(metadata) ||
		strings.EqualFold(strings.TrimSpace(stringFromAny(author["name"])), "image_gen")
	if role == "assistant" {
		recipient := strings.TrimSpace(stringFromAny(message["recipient"]))
		// User-visible replies target "all". Named recipients are tool calls,
		// including image tools whose identifiers are assigned dynamically.
		imageTool = imageTool || (recipient != "" && !strings.EqualFold(recipient, "all"))
	}
	if !imageTool {
		content, _ := message["content"].(map[string]any)
		if role == "assistant" {
			imageTool = chatGPTWebContainsStructuredImageReference(content)
		} else {
			imageTool = chatGPTWebContainsImageReference(content)
		}
	}
	return role, imageTool
}

func chatGPTWebImageMetadataActivity(metadata map[string]any) bool {
	if metadata == nil {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(stringFromAny(metadata["async_task_type"])), "image_gen") {
		return true
	}
	for _, key := range []string{"image_gen_async", "image_gen_multi_stream"} {
		if value, exists := metadata[key]; exists && chatGPTWebTruthy(value) {
			return true
		}
	}
	if strings.TrimSpace(stringFromAny(metadata["image_gen_task_id"])) != "" {
		return true
	}
	switch chatGPTWebImageGhostriderStatus(metadata) {
	case "intermediate", "final", "cancelled", "canceled":
		return true
	default:
		return false
	}
}

func chatGPTWebImageGhostriderStatus(value map[string]any) string {
	if value == nil {
		return ""
	}
	if status := strings.ToLower(strings.TrimSpace(stringFromAny(value["ghostrider_status"]))); status != "" {
		return status
	}
	ghostrider, _ := value["ghostrider"].(map[string]any)
	return strings.ToLower(strings.TrimSpace(stringFromAny(ghostrider["status"])))
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

func integerFromAny(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) || typed != math.Trunc(typed) || typed > float64(^uint(0)>>1) || typed < -float64(^uint(0)>>1)-1 {
			return 0, false
		}
		return int(typed), true
	case json.Number:
		parsed, err := strconv.ParseInt(string(typed), 10, strconv.IntSize)
		return int(parsed), err == nil
	default:
		return 0, false
	}
}

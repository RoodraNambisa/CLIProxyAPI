package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	"github.com/google/uuid"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	log "github.com/sirupsen/logrus"
)

var chatGPTWebSearchTerminalStates = map[string]struct{}{
	"finished_successfully": {},
	"completed":             {},
	"complete":              {},
	"stop":                  {},
}

var chatGPTWebSearchFailureStates = map[string]struct{}{
	"blocked":                     {},
	"cancelled":                   {},
	"canceled":                    {},
	"error":                       {},
	"failed":                      {},
	"finished_error":              {},
	"finished_with_error":         {},
	"finished_partial_completion": {},
}

const (
	chatGPTWebSearchBootstrapMaxBytes  = 32 << 20
	chatGPTWebSearchBootstrapMaxEvents = 32_768
	chatGPTWebSearchMaxPollFailures    = 4
	chatGPTWebSearchMaxPollAttempts    = 240
	chatGPTWebSearchPollMaxBytes       = 64 << 20
)

type chatGPTWebSearchExecution struct {
	response      *fhttp.Response
	headers       http.Header
	query         string
	turnMessageID string
}

func (e *ChatGPTWebExecutor) executeChatGPTWebSearch(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, prepared *chatGPTWebPreparedRequest) (chatGPTWebTextResult, http.Header, error) {
	execution, err := e.beginChatGPTWebSearch(ctx, client, credential, prepared)
	if err != nil {
		return chatGPTWebTextResult{}, nil, err
	}
	result, err := e.finishChatGPTWebSearch(ctx, client, credential, execution)
	return result, execution.headers, chatGPTWebCommittedRequestError(ctx, err)
}

func (e *ChatGPTWebExecutor) beginChatGPTWebSearch(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, prepared *chatGPTWebPreparedRequest) (*chatGPTWebSearchExecution, error) {
	query, err := chatGPTWebSearchPrompt(prepared.request.Messages)
	if err != nil {
		return nil, statusErr{
			code:           http.StatusBadRequest,
			msg:            err.Error(),
			skipAuthResult: true,
			retryOtherAuth: helps.IsChatGPTWebProviderUnsupported(err),
		}
	}
	if query == "" {
		return nil, statusErr{code: http.StatusBadRequest, msg: "web search requires a user query", skipAuthResult: true}
	}
	messages, err := e.buildChatGPTWebConversationMessages(ctx, client, credential, prepared.request.Messages)
	if err != nil {
		return nil, err
	}
	turnMessageID := applyChatGPTWebSearchMessageMetadata(messages)
	conduit, err := e.prepareChatGPTWebSearch(ctx, client, credential, query)
	if err != nil {
		return nil, err
	}
	requirements, err := e.chatGPTWebRequirements(ctx, client, credential)
	if err != nil {
		return nil, err
	}
	response, headers, err := e.startChatGPTWebSearch(ctx, client, credential, requirements, conduit, messages)
	if err != nil {
		return nil, err
	}
	return &chatGPTWebSearchExecution{
		response:      response,
		headers:       headers,
		query:         query,
		turnMessageID: turnMessageID,
	}, nil
}

func (e *ChatGPTWebExecutor) finishChatGPTWebSearch(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, execution *chatGPTWebSearchExecution) (chatGPTWebTextResult, error) {
	if execution == nil || execution.response == nil {
		return chatGPTWebTextResult{}, errors.New("chatgpt web search execution is nil")
	}
	conversationID, err := consumeChatGPTWebSearchBootstrap(ctx, execution.response.Body)
	if errClose := execution.response.Body.Close(); errClose != nil {
		log.Warnf("chatgpt web search: close bootstrap response body: %v", errClose)
	}
	if err != nil {
		return chatGPTWebTextResult{}, chatGPTWebUpstreamProtocolError(ctx, err)
	}
	if conversationID == "" {
		return chatGPTWebTextResult{}, chatGPTWebUpstreamProtocolError(
			ctx,
			errors.New("chatgpt web search stream did not return a conversation ID"),
		)
	}
	result, err := e.pollChatGPTWebSearch(ctx, client, credential, conversationID, execution.turnMessageID)
	if err != nil {
		return chatGPTWebTextResult{}, err
	}
	result.Query = execution.query
	result.Search = true
	return result, nil
}

func (e *ChatGPTWebExecutor) prepareChatGPTWebSearch(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, query string) (string, error) {
	path := "/backend-api/f/conversation/prepare"
	headers := e.chatGPTWebHeaders(credential, path, map[string]string{
		"accept":          "*/*",
		"content-type":    "application/json",
		"x-conduit-token": "no-token",
	})
	body := map[string]any{
		"action":                 "next",
		"fork_from_shared_post":  false,
		"parent_message_id":      "client-created-root",
		"model":                  chatGPTWebSearchModel,
		"client_prepare_state":   "success",
		"timezone_offset_min":    -480,
		"timezone":               "Asia/Shanghai",
		"conversation_mode":      map[string]any{"kind": "primary_assistant"},
		"system_hints":           []string{"search"},
		"partial_query":          chatGPTWebUserTextMessage(query),
		"supports_buffering":     true,
		"supported_encodings":    []string{"v1"},
		"client_contextual_info": map[string]any{"app_name": "chatgpt.com"},
	}
	_, payload, err := e.doChatGPTWebJSONWithHeaders(ctx, client, credential, path, headers, body)
	if err != nil {
		return "", err
	}
	token := gjsonString(payload, "conduit_token")
	if token == "" {
		return "", chatGPTWebUpstreamProtocolError(
			ctx,
			errors.New("chatgpt web search prepare response is missing conduit token"),
		)
	}
	return token, nil
}

func (e *ChatGPTWebExecutor) startChatGPTWebSearch(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, requirements chatGPTWebRequirements, conduit string, messages []map[string]any) (*fhttp.Response, http.Header, error) {
	path := "/backend-api/f/conversation"
	headers := chatGPTWebRequirementsHeaders(e.chatGPTWebHeaders(credential, path, nil), requirements)
	headers["accept"] = "text/event-stream"
	headers["content-type"] = "application/json"
	headers["x-conduit-token"] = conduit
	headers["x-oai-turn-trace-id"] = uuid.NewString()
	body := map[string]any{
		"action":                               "next",
		"messages":                             messages,
		"parent_message_id":                    "client-created-root",
		"model":                                chatGPTWebSearchModel,
		"client_prepare_state":                 "success",
		"timezone_offset_min":                  -480,
		"timezone":                             "Asia/Shanghai",
		"conversation_mode":                    map[string]any{"kind": "primary_assistant"},
		"enable_message_followups":             true,
		"system_hints":                         []any{},
		"supports_buffering":                   true,
		"supported_encodings":                  []string{"v1"},
		"force_use_search":                     true,
		"client_reported_search_source":        "conversation_composer_web_icon",
		"client_contextual_info":               chatGPTWebClientContext(),
		"paragen_cot_summary_display_override": "allow",
		"force_parallel_switch":                "auto",
	}
	response, err := e.doChatGPTWebJSONStream(ctx, client, credential, path, headers, body)
	if err != nil {
		return nil, nil, err
	}
	return response, cloneChatGPTWebHeaders(response.Header), nil
}

func applyChatGPTWebSearchMessageMetadata(messages []map[string]any) string {
	for index := len(messages) - 1; index >= 0; index-- {
		author, _ := messages[index]["author"].(map[string]any)
		if !strings.EqualFold(strings.TrimSpace(fmt.Sprint(author["role"])), "user") {
			continue
		}
		messages[index]["create_time"] = float64(time.Now().UnixNano()) / 1e9
		messages[index]["metadata"] = map[string]any{
			"developer_mode_connector_ids": []any{},
			"selected_github_repos":        []any{},
			"selected_all_github_repos":    false,
			"system_hints":                 []string{"search"},
			"serialization_metadata":       map[string]any{"custom_symbol_offsets": []any{}},
		}
		return strings.TrimSpace(fmt.Sprint(messages[index]["id"]))
	}
	return ""
}

func consumeChatGPTWebSearchBootstrap(ctx context.Context, body io.Reader) (string, error) {
	return consumeChatGPTWebSearchBootstrapWithLimits(
		ctx,
		body,
		chatGPTWebSearchBootstrapMaxBytes,
		chatGPTWebSearchBootstrapMaxEvents,
	)
}

func consumeChatGPTWebSearchBootstrapWithLimits(ctx context.Context, body io.Reader, maxBytes, maxEvents int) (string, error) {
	decoder := helps.NewChatGPTWebSSEDecoder(chatGPTWebSSEMaxFrameBytes)
	buffer := make([]byte, 32<<10)
	conversationID := ""
	totalBytes := 0
	eventCount := 0
	consume := func(payloads [][]byte) (bool, error) {
		for _, payload := range payloads {
			eventCount++
			if maxEvents > 0 && eventCount > maxEvents {
				return false, &helps.ChatGPTWebResponseLimitError{
					Message: "chatgpt web search bootstrap exceeds the event limit",
				}
			}
			if string(payload) == "[DONE]" {
				return true, nil
			}
			if chatGPTWebSearchBootstrapPayloadIsError(payload) {
				return false, helps.JSONStreamProtocolError("chatgpt web search", payload)
			}
			if value := findChatGPTWebJSONValue(payload, "conversation_id"); value != "" {
				conversationID = value
			}
		}
		return false, nil
	}
	for {
		count, errRead := body.Read(buffer)
		if count > 0 {
			if maxBytes > 0 && count > maxBytes-totalBytes {
				return "", &helps.ChatGPTWebResponseLimitError{
					Message: "chatgpt web search bootstrap exceeds the response limit",
				}
			}
			totalBytes += count
			payloads, err := decoder.Feed(buffer[:count], false)
			if err != nil {
				return "", err
			}
			done, errConsume := consume(payloads)
			if errConsume != nil {
				return "", errConsume
			}
			if done {
				return conversationID, nil
			}
		}
		if errRead != nil {
			if !errors.Is(errRead, io.EOF) {
				return "", errRead
			}
			payloads, err := decoder.Feed(nil, true)
			if err != nil {
				return "", err
			}
			done, errConsume := consume(payloads)
			if errConsume != nil {
				return "", errConsume
			}
			if done {
				return conversationID, nil
			}
			if ctx != nil && ctx.Err() != nil {
				return "", ctx.Err()
			}
			return "", helps.IncompleteStreamError("chatgpt web search")
		}
	}
}

func chatGPTWebSearchBootstrapPayloadIsError(payload []byte) bool {
	var event map[string]any
	if err := json.Unmarshal(payload, &event); err != nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(fmt.Sprint(event["type"])), "error") || event["error"] != nil
}

func (e *ChatGPTWebExecutor) pollChatGPTWebSearch(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, conversationID string, turnMessageIDs ...string) (chatGPTWebTextResult, error) {
	turnMessageID := ""
	if len(turnMessageIDs) > 0 {
		turnMessageID = strings.TrimSpace(turnMessageIDs[0])
	}
	transientFailures := 0
	maxPolls := e.searchMaxPolls
	if maxPolls <= 0 {
		maxPolls = chatGPTWebSearchMaxPollAttempts
	}
	responseBudget := newChatGPTWebPollResponseBudget(chatGPTWebSearchPollMaxBytes)
	for pollAttempt := 1; ; pollAttempt++ {
		path := "/backend-api/conversation/" + conversationID
		_, payload, err := e.doChatGPTWebPollGET(ctx, client, credential, path, map[string]string{
			"accept":                "*/*",
			"referer":               e.chatGPTWebBaseURL() + "/c/" + conversationID,
			"x-openai-target-route": "/backend-api/conversation/{conversation_id}",
		}, responseBudget)
		if err == nil {
			transientFailures = 0
			result, status, complete, errExtract := extractChatGPTWebSearchResult(payload, turnMessageID)
			if errExtract != nil {
				return chatGPTWebTextResult{}, statusErr{
					code:           http.StatusBadGateway,
					msg:            errExtract.Error(),
					skipAuthResult: true,
					retryOtherAuth: true,
				}
			}
			normalizedStatus := strings.ToLower(strings.TrimSpace(status))
			if _, failed := chatGPTWebSearchFailureStates[normalizedStatus]; failed || chatGPTWebSearchStatusFailed(normalizedStatus) {
				return chatGPTWebTextResult{}, statusErr{
					code:           http.StatusBadGateway,
					msg:            chatGPTWebSearchFailureMessage(normalizedStatus, result.Text),
					skipAuthResult: true,
					retryOtherAuth: true,
				}
			}
			if _, terminal := chatGPTWebSearchTerminalStates[normalizedStatus]; terminal || complete {
				if strings.TrimSpace(result.Text) == "" {
					return chatGPTWebTextResult{}, statusErr{
						code:           http.StatusBadGateway,
						msg:            "chatgpt web search completed without an answer",
						skipAuthResult: true,
						retryOtherAuth: true,
					}
				}
				result.Text = appendChatGPTWebSources(result.Text, result.Sources)
				return result, nil
			}
		} else {
			if ctx != nil && ctx.Err() != nil {
				return chatGPTWebTextResult{}, ctx.Err()
			}
			statusCode := statusCodeFromError(err)
			if statusCode == http.StatusTooManyRequests {
				return chatGPTWebTextResult{}, err
			}
			delay, retryable := chatGPTWebPollRetryDelay(err, e.searchPollInterval)
			if !retryable {
				return chatGPTWebTextResult{}, err
			}
			transientFailures++
			if transientFailures >= chatGPTWebSearchMaxPollFailures || pollAttempt >= maxPolls {
				return chatGPTWebTextResult{}, err
			}
			if errWait := waitForChatGPTWebPoll(ctx, delay); errWait != nil {
				return chatGPTWebTextResult{}, errWait
			}
			continue
		}
		if pollAttempt >= maxPolls {
			return chatGPTWebTextResult{}, statusErr{
				code:           http.StatusBadGateway,
				msg:            fmt.Sprintf("chatgpt web search remained incomplete after %d polls", maxPolls),
				skipAuthResult: true,
				retryOtherAuth: true,
			}
		}
		if errWait := waitForChatGPTWebPoll(ctx, e.searchPollInterval); errWait != nil {
			return chatGPTWebTextResult{}, errWait
		}
	}
}

func extractChatGPTWebSearchResult(payload []byte, turnMessageIDs ...string) (chatGPTWebTextResult, string, bool, error) {
	var root map[string]any
	if err := json.Unmarshal(payload, &root); err != nil {
		return chatGPTWebTextResult{}, "", false, fmt.Errorf("decode chatgpt web search conversation: %w", err)
	}
	mappingValue, exists := root["mapping"]
	if !exists {
		return chatGPTWebTextResult{}, "", false, errors.New("chatgpt web search conversation is missing mapping")
	}
	mapping, ok := mappingValue.(map[string]any)
	if !ok {
		return chatGPTWebTextResult{}, "", false, errors.New("chatgpt web search conversation mapping is invalid")
	}
	turnMessageID := ""
	if len(turnMessageIDs) > 0 {
		turnMessageID = strings.TrimSpace(turnMessageIDs[0])
	}
	latest := activeChatGPTWebSearchAssistant(root, mapping, turnMessageID)
	if latest == nil {
		return chatGPTWebTextResult{}, "", false, nil
	}
	text := chatGPTWebMessageText(latest)
	sources := collectChatGPTWebSearchSources(latest)
	metadata, _ := latest["metadata"].(map[string]any)
	finishDetails, _ := metadata["finish_details"].(map[string]any)
	statuses := []string{
		firstNonEmptyChatGPTWebString(finishDetails["type"]),
		firstNonEmptyChatGPTWebString(metadata["status"]),
		firstNonEmptyChatGPTWebString(latest["status"]),
	}
	status := firstChatGPTWebSearchStatus(statuses)
	complete := firstChatGPTWebBool(
		finishDetails["is_complete"],
		metadata["is_complete"],
		latest["is_complete"],
	)
	return chatGPTWebTextResult{Text: cleanChatGPTWebSearchText(text), Sources: sources, Search: true}, status, complete, nil
}

func activeChatGPTWebSearchAssistant(root, mapping map[string]any, turnMessageID string) map[string]any {
	currentID := strings.TrimSpace(firstNonEmptyChatGPTWebString(root["current_node"]))
	strictCurrentTurn := turnMessageID != "" && currentID != ""
	visited := make(map[string]struct{}, len(mapping))
	var currentTurnAssistant map[string]any
	for currentID != "" && len(visited) < len(mapping) {
		if _, seen := visited[currentID]; seen {
			break
		}
		visited[currentID] = struct{}{}
		node, _ := mapping[currentID].(map[string]any)
		if node == nil {
			break
		}
		message, _ := node["message"].(map[string]any)
		author, _ := message["author"].(map[string]any)
		if currentTurnAssistant == nil && strings.EqualFold(strings.TrimSpace(fmt.Sprint(author["role"])), "assistant") {
			currentTurnAssistant = message
		}
		if turnMessageID != "" && currentID == turnMessageID {
			return currentTurnAssistant
		}
		currentID = strings.TrimSpace(firstNonEmptyChatGPTWebString(node["parent"]))
	}
	if strictCurrentTurn {
		return nil
	}
	if currentTurnAssistant != nil {
		return currentTurnAssistant
	}

	ids := make([]string, 0, len(mapping))
	for id := range mapping {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var latest map[string]any
	latestTime := -1.0
	for _, id := range ids {
		rawNode := mapping[id]
		node, _ := rawNode.(map[string]any)
		message, _ := node["message"].(map[string]any)
		author, _ := message["author"].(map[string]any)
		if !strings.EqualFold(strings.TrimSpace(fmt.Sprint(author["role"])), "assistant") {
			continue
		}
		created := numberFromChatGPTWebAny(message["create_time"])
		if latest == nil || created >= latestTime {
			latest = message
			latestTime = created
		}
	}
	return latest
}

func firstChatGPTWebSearchStatus(statuses []string) string {
	for _, status := range statuses {
		normalized := strings.ToLower(strings.TrimSpace(status))
		if _, failed := chatGPTWebSearchFailureStates[normalized]; failed || chatGPTWebSearchStatusFailed(normalized) {
			return normalized
		}
	}
	for _, status := range statuses {
		normalized := strings.ToLower(strings.TrimSpace(status))
		if _, terminal := chatGPTWebSearchTerminalStates[normalized]; terminal {
			return normalized
		}
	}
	for _, status := range statuses {
		if normalized := strings.ToLower(strings.TrimSpace(status)); normalized != "" {
			return normalized
		}
	}
	return ""
}

func chatGPTWebMessageText(message map[string]any) string {
	content, ok := message["content"].(map[string]any)
	if !ok {
		return strings.TrimSpace(fmt.Sprint(message["content"]))
	}
	var parts []string
	if text := strings.TrimSpace(fmt.Sprint(content["text"])); text != "" && text != "<nil>" {
		parts = append(parts, text)
	}
	if values, ok := content["parts"].([]any); ok {
		for _, value := range values {
			switch typed := value.(type) {
			case string:
				if strings.TrimSpace(typed) != "" {
					parts = append(parts, typed)
				}
			case map[string]any:
				for _, key := range []string{"text", "summary", "content"} {
					if text := strings.TrimSpace(fmt.Sprint(typed[key])); text != "" && text != "<nil>" {
						parts = append(parts, text)
					}
				}
			}
		}
	}
	return strings.TrimSpace(strings.Join(parts, "\n"))
}

func collectChatGPTWebSearchSources(value any) []chatGPTWebSearchSource {
	message, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	seen := make(map[string]struct{})
	var sources []chatGPTWebSearchSource
	var collect func(any)
	collect = func(current any) {
		switch typed := current.(type) {
		case []any:
			for _, item := range typed {
				collect(item)
			}
		case map[string]any:
			metadata, _ := typed["metadata"].(map[string]any)
			rawURL := validChatGPTWebSearchSourceURL(firstNonEmptyChatGPTWebString(
				typed["url"],
				typed["link"],
				typed["source_url"],
				metadata["url"],
				metadata["link"],
				metadata["source_url"],
			))
			if rawURL != "" {
				if _, exists := seen[rawURL]; !exists {
					seen[rawURL] = struct{}{}
					sources = append(sources, chatGPTWebSearchSource{
						Title: firstNonEmptyChatGPTWebString(
							typed["title"],
							typed["name"],
							typed["source"],
							metadata["title"],
							metadata["name"],
							metadata["source"],
						),
						URL: rawURL,
					})
				}
			}
			for _, key := range []string{
				"items",
				"results",
				"sources",
				"citations",
				"content_references",
				"search_results",
				"references",
				"attributions",
			} {
				collect(typed[key])
			}
		}
	}
	collectContainers := func(container map[string]any) {
		for _, key := range []string{"citations", "content_references", "sources", "search_results"} {
			collect(container[key])
		}
	}
	collectContainers(message)
	if metadata, ok := message["metadata"].(map[string]any); ok {
		collectContainers(metadata)
	}
	if content, ok := message["content"].(map[string]any); ok {
		collectContainers(content)
		if parts, ok := content["parts"].([]any); ok {
			for _, rawPart := range parts {
				if part, ok := rawPart.(map[string]any); ok {
					collectContainers(part)
				}
			}
		}
	}
	sort.SliceStable(sources, func(i, j int) bool { return sources[i].URL < sources[j].URL })
	return sources
}

func validChatGPTWebSearchSourceURL(rawURL string) string {
	rawURL = strings.TrimSpace(rawURL)
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Host == "" ||
		(!strings.EqualFold(parsed.Scheme, "http") && !strings.EqualFold(parsed.Scheme, "https")) ||
		parsed.User != nil {
		return ""
	}
	if chatGPTWebSearchURLFieldsUnsafe(parsed.RawQuery) {
		return ""
	}
	if decodedFragment, err := url.QueryUnescape(parsed.Fragment); err != nil {
		return ""
	} else {
		fragmentFields, fragmentQuery, hasQuery := strings.Cut(decodedFragment, "?")
		if strings.Contains(fragmentQuery, "?") ||
			chatGPTWebSearchURLFieldsUnsafe(fragmentFields) ||
			(hasQuery && chatGPTWebSearchURLFieldsUnsafe(fragmentQuery)) {
			return ""
		}
	}
	return rawURL
}

func chatGPTWebSearchURLFieldsUnsafe(rawFields string) bool {
	if rawFields == "" {
		return false
	}
	values, err := url.ParseQuery(rawFields)
	if err != nil {
		return true
	}
	for key := range values {
		if chatGPTWebSearchURLCredentialKey(key) {
			return true
		}
	}
	return false
}

func chatGPTWebSearchURLCredentialKey(key string) bool {
	key = strings.TrimLeft(strings.TrimSpace(key), "?#")
	normalized := strings.NewReplacer("-", "", "_", "", ".", "").Replace(strings.ToLower(key))
	switch normalized {
	case "accesstoken", "apikey", "authorization", "auth", "credential", "expires", "key",
		"password", "secret", "sessiontoken", "signature", "sig", "token":
		return true
	default:
		if strings.HasSuffix(normalized, "token") ||
			strings.HasSuffix(normalized, "secret") ||
			strings.Contains(normalized, "credential") ||
			strings.Contains(normalized, "password") ||
			strings.Contains(normalized, "signature") ||
			strings.Contains(normalized, "accesskey") ||
			strings.HasPrefix(normalized, "xamz") ||
			strings.HasPrefix(normalized, "xgoog") ||
			strings.HasPrefix(normalized, "xms") {
			return true
		}
	}
	return false
}

func appendChatGPTWebSources(text string, sources []chatGPTWebSearchSource) string {
	if len(sources) == 0 {
		return text
	}
	var builder strings.Builder
	builder.WriteString(strings.TrimSpace(text))
	builder.WriteString("\n\nSources:")
	for index, source := range sources {
		builder.WriteString(fmt.Sprintf("\n%d. ", index+1))
		if source.Title != "" {
			builder.WriteString(source.Title)
			builder.WriteString(" - ")
		}
		builder.WriteString(source.URL)
	}
	return builder.String()
}

func cleanChatGPTWebSearchText(text string) string {
	return strings.TrimSpace(helps.CleanChatGPTWebText(text))
}

func chatGPTWebSearchPrompt(messages []helps.ChatGPTWebMessage) (string, error) {
	latestUser := ""
	for _, message := range messages {
		var parts []string
		for _, part := range message.Parts {
			if strings.TrimSpace(part.ImageURL) != "" {
				return "", &helps.ChatGPTWebUnsupportedRequestError{
					Message: "chatgpt web search does not support image input",
				}
			}
			if text := strings.TrimSpace(part.Text); text != "" {
				parts = append(parts, text)
			}
		}
		if len(parts) == 0 {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(message.Role), "user") {
			latestUser = strings.Join(parts, "\n")
		}
	}
	return strings.TrimSpace(latestUser), nil
}

func findChatGPTWebJSONValue(payload []byte, key string) string {
	var decoded any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return ""
	}
	var walk func(any) string
	walk = func(value any) string {
		switch typed := value.(type) {
		case map[string]any:
			if candidate := strings.TrimSpace(fmt.Sprint(typed[key])); candidate != "" && candidate != "<nil>" {
				return candidate
			}
			for _, item := range typed {
				if found := walk(item); found != "" {
					return found
				}
			}
		case []any:
			for _, item := range typed {
				if found := walk(item); found != "" {
					return found
				}
			}
		}
		return ""
	}
	return walk(decoded)
}

func firstNonEmptyChatGPTWebString(values ...any) string {
	for _, value := range values {
		text := strings.TrimSpace(fmt.Sprint(value))
		if text != "" && text != "<nil>" {
			return text
		}
	}
	return ""
}

func firstChatGPTWebBool(values ...any) bool {
	for _, value := range values {
		if flag, ok := value.(bool); ok && flag {
			return true
		}
	}
	return false
}

func chatGPTWebSearchStatusFailed(status string) bool {
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

func chatGPTWebSearchFailureMessage(status, text string) string {
	status = strings.TrimSpace(status)
	if status == "" {
		status = "failed"
	}
	text = strings.TrimSpace(text)
	if len(text) > 512 {
		text = text[:512]
	}
	if text == "" {
		return "chatgpt web search failed with status " + status
	}
	return "chatgpt web search failed with status " + status + ": " + text
}

func numberFromChatGPTWebAny(value any) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case json.Number:
		result, _ := typed.Float64()
		return result
	default:
		return 0
	}
}

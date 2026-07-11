package executor

import (
	"context"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	internalcache "github.com/router-for-me/CLIProxyAPI/v6/internal/cache"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/signature"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type xaiReasoningReplayScope struct {
	namespace  string
	modelName  string
	sessionKey string
}

var getXAIReasoningReplayItemsRequired = internalcache.GetXAIReasoningReplayItemsRequired

func (s xaiReasoningReplayScope) valid() bool {
	return strings.TrimSpace(s.namespace) != "" && strings.TrimSpace(s.modelName) != "" && strings.TrimSpace(s.sessionKey) != ""
}

func applyXAIReasoningReplayCacheRequired(ctx context.Context, replayNamespace string, from sdktranslator.Format, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, body []byte) ([]byte, xaiReasoningReplayScope, error) {
	scope := xaiReasoningReplayScopeFromRequest(ctx, replayNamespace, from, req, opts, body)
	if !scope.valid() {
		return body, scope, nil
	}
	items, ok, errReplay := getXAIReasoningReplayItemsRequired(ctx, scope.modelName, scope.cacheSessionKey())
	if errReplay != nil {
		log.Warnf("xai reasoning replay cache read failed: %v", errReplay)
		return body, scope, nil
	}
	if !ok {
		return body, scope, nil
	}
	items = filterXAIReasoningReplayItemsForInput(body, items)
	if len(items) == 0 {
		return body, scope, nil
	}
	updated, ok := insertXAIReasoningReplayItems(body, items)
	if !ok {
		return body, scope, nil
	}
	return updated, scope, nil
}

func xaiReasoningReplayScopeFromRequest(ctx context.Context, replayNamespace string, from sdktranslator.Format, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, body []byte) xaiReasoningReplayScope {
	if !xaiReasoningReplayEnabledForSource(from) {
		return xaiReasoningReplayScope{}
	}
	return xaiReasoningReplayScope{
		namespace:  strings.TrimSpace(replayNamespace),
		modelName:  thinking.ParseSuffix(req.Model).ModelName,
		sessionKey: xaiReasoningReplaySessionKey(ctx, req, opts, body),
	}
}

func (s xaiReasoningReplayScope) cacheSessionKey() string {
	if !s.valid() {
		return ""
	}
	return s.namespace + "\x00" + s.sessionKey
}

func xaiReasoningReplayEnabledForSource(from sdktranslator.Format) bool {
	return strings.EqualFold(strings.TrimSpace(from.String()), sdktranslator.FormatClaude.String())
}

func xaiInputHasValidReasoningEncryptedContent(body []byte) bool {
	input := gjson.GetBytes(body, "input")
	if !input.IsArray() {
		return false
	}
	for _, item := range input.Array() {
		if strings.TrimSpace(item.Get("type").String()) != "reasoning" {
			continue
		}
		encryptedContent := item.Get("encrypted_content")
		if encryptedContent.Type != gjson.String {
			continue
		}
		if _, err := signature.InspectGrokEncryptedContent(encryptedContent.String()); err == nil {
			return true
		}
	}
	return false
}

func filterXAIReasoningReplayItemsForInput(body []byte, items [][]byte) [][]byte {
	input := gjson.GetBytes(body, "input")
	if !input.IsArray() {
		return nil
	}

	hasInputReasoning := xaiInputHasValidReasoningEncryptedContent(body)
	existingCalls := make(map[string]bool)
	existingOutputs := make(map[string]bool)
	for _, inputItem := range input.Array() {
		itemType := strings.TrimSpace(inputItem.Get("type").String())
		if itemType == "function_call_output" || itemType == "custom_tool_call_output" {
			callID := strings.TrimSpace(inputItem.Get("call_id").String())
			if callID != "" {
				for _, candidate := range xaiReplayComparableCallIDs(callID) {
					existingOutputs[candidate] = true
				}
			}
		}
		for _, key := range xaiReplayToolCallKeys(inputItem) {
			existingCalls[key] = true
		}
	}

	filtered := make([][]byte, 0, len(items))
	for _, item := range items {
		itemResult := gjson.ParseBytes(item)
		switch strings.TrimSpace(itemResult.Get("type").String()) {
		case "reasoning":
			if hasInputReasoning {
				continue
			}
		case "function_call", "custom_tool_call":
			keys := xaiReplayToolCallKeys(itemResult)
			if len(keys) == 0 || xaiReplayAnyToolCallKeyExists(existingCalls, keys) {
				continue
			}
			hasMatchingOutput := false
			callID := strings.TrimSpace(itemResult.Get("call_id").String())
			if callID != "" {
				for _, candidate := range xaiReplayComparableCallIDs(callID) {
					if existingOutputs[candidate] {
						hasMatchingOutput = true
						break
					}
				}
			}
			if !hasMatchingOutput {
				continue
			}
			for _, key := range keys {
				existingCalls[key] = true
			}
		default:
			continue
		}
		filtered = append(filtered, item)
	}
	return filtered
}

func xaiReasoningReplaySessionKey(ctx context.Context, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, body []byte) string {
	for _, metadata := range []map[string]any{opts.Metadata, req.Metadata} {
		if value, ok := metadata[cliproxyexecutor.ExecutionSessionMetadataKey].(string); ok && strings.TrimSpace(value) != "" {
			return "execution:" + strings.TrimSpace(value)
		}
	}
	for _, payload := range [][]byte{body, req.Payload} {
		if key := xaiReplaySessionKeyFromPayload(payload); key != "" {
			return key
		}
	}
	if key := xaiReplaySessionKeyFromHeaders(opts.Headers); key != "" {
		return key
	}
	if ctx != nil {
		if ginCtx, ok := ctx.Value("gin").(*gin.Context); ok && ginCtx != nil && ginCtx.Request != nil {
			if key := xaiReplaySessionKeyFromHeaders(ginCtx.Request.Header); key != "" {
				return key
			}
		}
	}
	if sessionID := xaiClaudeCodeSessionID(req.Payload, opts.Headers); sessionID != "" {
		return "claude:" + sessionID
	}
	return ""
}

func xaiReplaySessionKeyFromPayload(payload []byte) string {
	if key := strings.TrimSpace(gjson.GetBytes(payload, "prompt_cache_key").String()); key != "" {
		return "prompt-cache:" + key
	}
	if windowID := strings.TrimSpace(gjson.GetBytes(payload, "client_metadata.x-codex-window-id").String()); windowID != "" {
		return "window:" + windowID
	}
	return ""
}

func xaiReplaySessionKeyFromHeaders(headers http.Header) string {
	if headers == nil {
		return ""
	}
	if windowID := strings.TrimSpace(headers.Get("X-Codex-Window-Id")); windowID != "" {
		return "window:" + windowID
	}
	for _, name := range []string{"X-Claude-Code-Session-Id", "Session-Id", "Session_id", "Conversation_id"} {
		if value := strings.TrimSpace(headers.Get(name)); value != "" {
			return strings.ToLower(name) + ":" + value
		}
	}
	return ""
}

func xaiReplayToolCallKeys(item gjson.Result) []string {
	itemType := strings.TrimSpace(item.Get("type").String())
	if itemType != "function_call" && itemType != "custom_tool_call" {
		return nil
	}
	ids := xaiReplayComparableCallIDs(item.Get("call_id").String())
	keys := make([]string, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, itemType+":"+id)
	}
	return keys
}

func xaiReplayComparableCallIDs(callID string) []string {
	callID = strings.TrimSpace(callID)
	if callID == "" {
		return nil
	}
	sanitized := util.SanitizeClaudeToolID(callID)
	if sanitized == "" || sanitized == callID {
		return []string{callID}
	}
	return []string{callID, sanitized}
}

func xaiReplayAnyToolCallKeyExists(existing map[string]bool, keys []string) bool {
	for _, key := range keys {
		if existing[key] {
			return true
		}
	}
	return false
}

func insertXAIReasoningReplayItems(body []byte, replayItems [][]byte) ([]byte, bool) {
	input := gjson.GetBytes(body, "input")
	if !input.IsArray() || len(replayItems) == 0 {
		return body, false
	}
	inputItems := input.Array()
	insertIndex := xaiReasoningReplayInsertIndex(inputItems, replayItems)
	items := make([]string, 0, len(inputItems)+len(replayItems))
	for index, item := range inputItems {
		if index == insertIndex {
			for _, replayItem := range replayItems {
				items = append(items, string(replayItem))
			}
		}
		items = append(items, item.Raw)
	}
	if insertIndex == len(inputItems) {
		for _, replayItem := range replayItems {
			items = append(items, string(replayItem))
		}
	}
	updated, err := sjson.SetRawBytes(body, "input", []byte("["+strings.Join(items, ",")+"]"))
	return updated, err == nil
}

func xaiReasoningReplayInsertIndex(inputItems []gjson.Result, replayItems [][]byte) int {
	replayCallIDs := make(map[string]bool)
	for _, replayItem := range replayItems {
		item := gjson.ParseBytes(replayItem)
		for _, callID := range xaiReplayComparableCallIDs(item.Get("call_id").String()) {
			replayCallIDs[callID] = true
		}
	}
	if len(replayCallIDs) > 0 {
		for index, item := range inputItems {
			itemType := strings.TrimSpace(item.Get("type").String())
			if itemType != "function_call_output" && itemType != "custom_tool_call_output" {
				continue
			}
			for _, candidate := range xaiReplayComparableCallIDs(item.Get("call_id").String()) {
				if replayCallIDs[candidate] {
					return index
				}
			}
		}
	}
	for index := len(inputItems) - 1; index >= 0; index-- {
		if inputItems[index].Get("type").String() == "message" && inputItems[index].Get("role").String() == "assistant" {
			return index
		}
	}
	for index, item := range inputItems {
		if item.Get("type").String() != "message" {
			return index
		}
		switch item.Get("role").String() {
		case "developer", "system":
			continue
		default:
			return index
		}
	}
	return len(inputItems)
}

func cacheXAIReasoningReplayFromCompleted(ctx context.Context, scope xaiReasoningReplayScope, completedData []byte) {
	if !scope.valid() {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	output := gjson.GetBytes(completedData, "response.output")
	if !output.IsArray() {
		return
	}
	items := make([][]byte, 0, len(output.Array()))
	for _, item := range output.Array() {
		switch strings.TrimSpace(item.Get("type").String()) {
		case "reasoning", "function_call", "custom_tool_call":
			items = append(items, []byte(item.Raw))
		default:
			continue
		}
	}
	if !internalcache.CacheXAIReasoningReplayItemsBestEffort(ctx, scope.modelName, scope.cacheSessionKey(), items) {
		if errDelete := internalcache.DeleteXAIReasoningReplayItemRequired(ctx, scope.modelName, scope.cacheSessionKey()); errDelete != nil {
			log.Warnf("xai reasoning replay cache delete failed after completed cache store failed: %v", errDelete)
		}
	}
}

func clearXAIReasoningReplayOnInvalidSignature(ctx context.Context, scope xaiReasoningReplayScope, statusCode int, body []byte) bool {
	if !scope.valid() || !helps.IsReasoningReplayInvalidSignatureError(statusCode, body) {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if errDelete := internalcache.DeleteXAIReasoningReplayItemRequired(ctx, scope.modelName, scope.cacheSessionKey()); errDelete != nil {
		log.Warnf("xai reasoning replay cache delete failed after invalid signature: %v", errDelete)
		return false
	}
	return true
}

package helps

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	CodexReasoningReplayCacheTTL           = time.Hour
	CodexReasoningReplayCacheMaxEntries    = 10240
	CodexReasoningReplayCacheMaxEntryBytes = 8 << 20
	CodexReasoningReplayCacheMaxTotalBytes = 256 << 20
	codexReasoningReplayPurgeInterval      = time.Minute
	maxCodexReasoningSignatureLength       = 32 * 1024 * 1024
)

var codexClaudeCodeSessionSuffixPattern = regexp.MustCompile(`_session_([a-fA-F0-9-]+)$`)

// CodexReasoningReplayScope identifies one model/session replay boundary.
type CodexReasoningReplayScope struct {
	namespace  string
	modelName  string
	sessionKey string
}

func (s CodexReasoningReplayScope) valid() bool {
	return strings.TrimSpace(s.namespace) != "" && strings.TrimSpace(s.modelName) != "" && strings.TrimSpace(s.sessionKey) != ""
}

type codexReasoningReplayEntry struct {
	items     [][]byte
	timestamp time.Time
	size      int64
}

var codexReasoningReplayStore = struct {
	sync.Mutex
	entries    map[string]codexReasoningReplayEntry
	totalBytes int64
	lastPurge  time.Time
}{entries: make(map[string]codexReasoningReplayEntry)}

// ReasoningReplayNamespace isolates process-local replay state by downstream
// access identity and selected upstream credential without retaining secrets.
func ReasoningReplayNamespace(ctx context.Context, provider, authID string) string {
	provider = strings.ToLower(strings.TrimSpace(provider))
	authID = strings.TrimSpace(authID)
	apiKey := strings.TrimSpace(APIKeyFromContext(ctx))
	if authID == "" && apiKey == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(provider + "\x00" + authID + "\x00" + apiKey))
	return hex.EncodeToString(sum[:16])
}

// SanitizeCodexReasoningEncryptedContent removes malformed encrypted_content
// values while preserving the surrounding reasoning item.
func SanitizeCodexReasoningEncryptedContent(ctx context.Context, provider string, body []byte) []byte {
	input := gjson.GetBytes(body, "input")
	if !input.IsArray() {
		return body
	}
	provider = strings.TrimSpace(provider)
	if provider == "" {
		provider = "codex executor"
	}

	updated := body
	for index, item := range input.Array() {
		if strings.TrimSpace(item.Get("type").String()) != "reasoning" {
			continue
		}
		path := fmt.Sprintf("input.%d.encrypted_content", index)
		encryptedContent := gjson.GetBytes(updated, path)
		if !encryptedContent.Exists() {
			continue
		}

		reason := ""
		switch encryptedContent.Type {
		case gjson.String:
			raw := encryptedContent.String()
			if raw != strings.TrimSpace(raw) {
				reason = "encrypted_content has leading or trailing whitespace"
			} else if err := validateCodexReasoningSignature(raw); err != nil {
				reason = err.Error()
			}
		case gjson.Null:
			reason = "encrypted_content is null"
		default:
			reason = fmt.Sprintf("encrypted_content must be a string, got %s", encryptedContent.Type.String())
		}
		if reason == "" {
			continue
		}

		next, errDelete := sjson.DeleteBytes(updated, path)
		if errDelete != nil {
			LogWithRequestID(ctx).Debugf("%s: failed to drop invalid reasoning encrypted_content at input[%d]: %v", provider, index, errDelete)
			continue
		}
		updated = next
		LogWithRequestID(ctx).Debugf("%s: dropped invalid reasoning encrypted_content at input[%d]: %s", provider, index, reason)
	}
	return updated
}

// ApplyCodexReasoningReplay injects cached Claude-origin reasoning and matching
// tool-call items into the translated Responses input.
func ApplyCodexReasoningReplay(ctx context.Context, sourceFormat, replayNamespace, modelName string, requestPayload, body []byte, requestMetadata, optionsMetadata map[string]any, headers http.Header) ([]byte, CodexReasoningReplayScope) {
	scope := codexReasoningReplayScopeFromRequest(ctx, sourceFormat, replayNamespace, modelName, requestPayload, body, requestMetadata, optionsMetadata, headers)
	if !scope.valid() {
		return body, scope
	}
	items, ok := getCodexReasoningReplayItems(scope)
	if !ok {
		return body, scope
	}
	items = filterCodexReasoningReplayItems(body, items)
	if len(items) == 0 {
		return body, scope
	}
	updated, ok := insertCodexReasoningReplayItems(body, items)
	if !ok {
		return body, scope
	}
	return updated, scope
}

// CacheCodexReasoningReplayFromCompleted stores replayable output items from a
// terminal Responses event.
func CacheCodexReasoningReplayFromCompleted(scope CodexReasoningReplayScope, completedData []byte) bool {
	if !scope.valid() {
		return false
	}
	output := gjson.GetBytes(completedData, "response.output")
	if !output.IsArray() {
		return false
	}
	items := make([][]byte, 0, len(output.Array()))
	for _, item := range output.Array() {
		switch strings.TrimSpace(item.Get("type").String()) {
		case "reasoning", "function_call", "custom_tool_call":
			items = append(items, []byte(item.Raw))
		}
	}
	normalized := normalizeCodexReasoningReplayItems(items)
	if len(normalized) == 0 {
		deleteCodexReasoningReplay(scope)
		return false
	}
	return setCodexReasoningReplayItems(scope, normalized)
}

// ClearCodexReasoningReplayOnInvalidSignature drops stale replay state after
// Codex rejects a reasoning signature.
func ClearCodexReasoningReplayOnInvalidSignature(scope CodexReasoningReplayScope, statusCode int, body []byte) bool {
	if !scope.valid() {
		return false
	}
	if !IsReasoningReplayInvalidSignatureError(statusCode, body) {
		return false
	}
	deleteCodexReasoningReplay(scope)
	return true
}

// IsReasoningReplayInvalidSignatureError reports whether an upstream response
// rejected replayed encrypted reasoning state.
func IsReasoningReplayInvalidSignatureError(statusCode int, body []byte) bool {
	if statusCode != http.StatusBadRequest && statusCode != http.StatusUnprocessableEntity {
		return false
	}
	for _, path := range []string{"error.code", "response.error.code", "code"} {
		switch strings.ToLower(strings.TrimSpace(gjson.GetBytes(body, path).String())) {
		case "invalid_encrypted_content", "invalid_signature", "thinking_signature_invalid":
			return true
		}
	}
	lower := strings.ToLower(strings.TrimSpace(string(body)))
	return strings.Contains(lower, "invalid signature in thinking block") ||
		strings.Contains(lower, "invalid_encrypted_content") ||
		strings.Contains(lower, "thinking_signature_invalid")
}

func codexReasoningReplayScopeFromRequest(ctx context.Context, sourceFormat, replayNamespace, modelName string, requestPayload, body []byte, requestMetadata, optionsMetadata map[string]any, headers http.Header) CodexReasoningReplayScope {
	if !strings.EqualFold(strings.TrimSpace(sourceFormat), "claude") {
		return CodexReasoningReplayScope{}
	}
	sessionKey := codexReplayMetadataString(optionsMetadata, "execution_session_id")
	if sessionKey == "" {
		sessionKey = codexReplayMetadataString(requestMetadata, "execution_session_id")
	}
	if sessionKey != "" {
		sessionKey = "execution:" + sessionKey
	} else if sessionKey = codexReplaySessionKeyFromPayload(body); sessionKey == "" {
		if sessionKey = codexReplaySessionKeyFromPayload(requestPayload); sessionKey == "" {
			sessionKey = codexReplaySessionKeyFromHeaders(headers)
			if sessionKey == "" && ctx != nil {
				if ginCtx, ok := ctx.Value("gin").(*gin.Context); ok && ginCtx != nil && ginCtx.Request != nil {
					sessionKey = codexReplaySessionKeyFromHeaders(ginCtx.Request.Header)
				}
			}
			if sessionKey == "" {
				sessionKey = codexClaudeSessionKeyFromPayload(requestPayload)
			}
		}
	}
	return CodexReasoningReplayScope{namespace: strings.TrimSpace(replayNamespace), modelName: strings.TrimSpace(modelName), sessionKey: sessionKey}
}

func codexReplayMetadataString(metadata map[string]any, key string) string {
	if len(metadata) == 0 {
		return ""
	}
	value, ok := metadata[key]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case []byte:
		return strings.TrimSpace(string(typed))
	default:
		return ""
	}
}

func codexReplaySessionKeyFromPayload(payload []byte) string {
	if promptCacheKey := strings.TrimSpace(gjson.GetBytes(payload, "prompt_cache_key").String()); promptCacheKey != "" {
		return "prompt-cache:" + promptCacheKey
	}
	if windowID := strings.TrimSpace(gjson.GetBytes(payload, "client_metadata.x-codex-window-id").String()); windowID != "" {
		return "window:" + windowID
	}
	if metadata := strings.TrimSpace(gjson.GetBytes(payload, "client_metadata.x-codex-turn-metadata").String()); metadata != "" {
		return codexReplaySessionKeyFromTurnMetadata(metadata)
	}
	return ""
}

func codexReplaySessionKeyFromHeaders(headers http.Header) string {
	if headers == nil {
		return ""
	}
	if metadata := strings.TrimSpace(headers.Get("X-Codex-Turn-Metadata")); metadata != "" {
		if key := codexReplaySessionKeyFromTurnMetadata(metadata); key != "" {
			return key
		}
	}
	if windowID := strings.TrimSpace(headers.Get("X-Codex-Window-Id")); windowID != "" {
		return "window:" + windowID
	}
	if value := strings.TrimSpace(headers.Get("X-Claude-Code-Session-Id")); value != "" {
		return "claude-session:" + value
	}
	for _, name := range []string{"Session-Id", "Session_id"} {
		if value := strings.TrimSpace(headers.Get(name)); value != "" {
			return "session:" + value
		}
	}
	if value := strings.TrimSpace(headers.Get("Conversation_id")); value != "" {
		return "conversation:" + value
	}
	return ""
}

func codexReplaySessionKeyFromTurnMetadata(metadata string) string {
	if promptCacheKey := strings.TrimSpace(gjson.Get(metadata, "prompt_cache_key").String()); promptCacheKey != "" {
		return "prompt-cache:" + promptCacheKey
	}
	if windowID := strings.TrimSpace(gjson.Get(metadata, "window_id").String()); windowID != "" {
		return "window:" + windowID
	}
	return ""
}

func codexClaudeSessionKeyFromPayload(payload []byte) string {
	userID := gjson.GetBytes(payload, "metadata.user_id").String()
	if matches := codexClaudeCodeSessionSuffixPattern.FindStringSubmatch(userID); len(matches) > 1 {
		return "claude:" + matches[1]
	}
	if strings.HasPrefix(userID, "{") {
		if sessionID := strings.TrimSpace(gjson.Get(userID, "session_id").String()); sessionID != "" {
			return "claude:" + sessionID
		}
	}
	return ""
}

func validateCodexReasoningSignature(signature string) error {
	if signature == "" {
		return fmt.Errorf("empty Codex reasoning signature")
	}
	if len(signature) > maxCodexReasoningSignatureLength {
		return fmt.Errorf("Codex reasoning signature exceeds maximum length")
	}
	if !strings.HasPrefix(signature, "gAAAA") {
		return fmt.Errorf("invalid Codex reasoning signature prefix")
	}
	for _, r := range signature {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '=' {
			continue
		}
		return fmt.Errorf("invalid Codex reasoning signature character")
	}
	decoded, errDecode := base64.RawURLEncoding.DecodeString(signature)
	if errDecode != nil {
		decoded, errDecode = base64.URLEncoding.DecodeString(signature)
	}
	if errDecode != nil || len(decoded) < 73 || decoded[0] != 0x80 {
		return fmt.Errorf("invalid Codex reasoning signature payload")
	}
	ciphertextLength := len(decoded) - 1 - 8 - 16 - 32
	if ciphertextLength <= 0 || ciphertextLength%16 != 0 {
		return fmt.Errorf("invalid Codex reasoning signature ciphertext")
	}
	return nil
}

func normalizeCodexReasoningReplayItems(items [][]byte) [][]byte {
	normalized := make([][]byte, 0, len(items))
	for _, item := range items {
		result := gjson.ParseBytes(item)
		var out []byte
		switch strings.TrimSpace(result.Get("type").String()) {
		case "reasoning":
			encryptedContent := result.Get("encrypted_content")
			if encryptedContent.Type != gjson.String || validateCodexReasoningSignature(encryptedContent.String()) != nil {
				continue
			}
			out = []byte(`{"type":"reasoning","summary":[],"content":null}`)
			out, _ = sjson.SetBytes(out, "encrypted_content", encryptedContent.String())
		case "function_call":
			if strings.TrimSpace(result.Get("call_id").String()) == "" || strings.TrimSpace(result.Get("name").String()) == "" || result.Get("arguments").Type != gjson.String {
				continue
			}
			out = []byte(`{"type":"function_call"}`)
			out, _ = sjson.SetBytes(out, "call_id", result.Get("call_id").String())
			out, _ = sjson.SetBytes(out, "name", result.Get("name").String())
			out, _ = sjson.SetBytes(out, "arguments", result.Get("arguments").String())
		case "custom_tool_call":
			if strings.TrimSpace(result.Get("call_id").String()) == "" || strings.TrimSpace(result.Get("name").String()) == "" || !result.Get("input").Exists() {
				continue
			}
			out = []byte(`{"type":"custom_tool_call","status":"completed"}`)
			out, _ = sjson.SetBytes(out, "call_id", result.Get("call_id").String())
			out, _ = sjson.SetBytes(out, "name", result.Get("name").String())
			if result.Get("input").Type == gjson.String {
				out, _ = sjson.SetBytes(out, "input", result.Get("input").String())
			} else {
				out, _ = sjson.SetRawBytes(out, "input", []byte(result.Get("input").Raw))
			}
		}
		if len(out) > 0 {
			normalized = append(normalized, out)
		}
	}
	return normalized
}

func filterCodexReasoningReplayItems(body []byte, items [][]byte) [][]byte {
	input := gjson.GetBytes(body, "input")
	if !input.IsArray() {
		return nil
	}
	hasReasoning := codexInputHasValidReasoning(body)
	existingCalls := make(map[string]struct{})
	existingOutputs := make(map[string]struct{})
	for _, item := range input.Array() {
		itemType := strings.TrimSpace(item.Get("type").String())
		if itemType == "function_call_output" || itemType == "custom_tool_call_output" {
			for _, callID := range comparableCodexCallIDs(item.Get("call_id").String()) {
				existingOutputs[callID] = struct{}{}
			}
		}
		for _, key := range codexReplayCallKeys(item) {
			existingCalls[key] = struct{}{}
		}
	}
	filtered := make([][]byte, 0, len(items))
	for _, item := range items {
		result := gjson.ParseBytes(item)
		switch strings.TrimSpace(result.Get("type").String()) {
		case "reasoning":
			if hasReasoning {
				continue
			}
		case "function_call", "custom_tool_call":
			keys := codexReplayCallKeys(result)
			if len(keys) == 0 || anyCodexReplayCallKeyExists(existingCalls, keys) {
				continue
			}
			matchedOutput := false
			for _, callID := range comparableCodexCallIDs(result.Get("call_id").String()) {
				if _, ok := existingOutputs[callID]; ok {
					matchedOutput = true
					break
				}
			}
			if !matchedOutput {
				continue
			}
			for _, key := range keys {
				existingCalls[key] = struct{}{}
			}
		default:
			continue
		}
		filtered = append(filtered, item)
	}
	return filtered
}

func codexInputHasValidReasoning(body []byte) bool {
	for _, item := range gjson.GetBytes(body, "input").Array() {
		if item.Get("type").String() == "reasoning" && item.Get("encrypted_content").Type == gjson.String && validateCodexReasoningSignature(item.Get("encrypted_content").String()) == nil {
			return true
		}
	}
	return false
}

func codexReplayCallKeys(item gjson.Result) []string {
	itemType := strings.TrimSpace(item.Get("type").String())
	if itemType != "function_call" && itemType != "custom_tool_call" {
		return nil
	}
	ids := comparableCodexCallIDs(item.Get("call_id").String())
	keys := make([]string, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, itemType+":"+id)
	}
	return keys
}

func comparableCodexCallIDs(callID string) []string {
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

func anyCodexReplayCallKeyExists(existing map[string]struct{}, keys []string) bool {
	for _, key := range keys {
		if _, ok := existing[key]; ok {
			return true
		}
	}
	return false
}

func insertCodexReasoningReplayItems(body []byte, replayItems [][]byte) ([]byte, bool) {
	input := gjson.GetBytes(body, "input")
	if !input.IsArray() || len(replayItems) == 0 {
		return body, false
	}
	inputItems := input.Array()
	insertIndex := codexReasoningReplayInsertIndex(inputItems, replayItems)
	replayItems = alignCodexReasoningReplayCallIDs(inputItems, replayItems)
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
	updated, errSet := sjson.SetRawBytes(body, "input", []byte("["+strings.Join(items, ",")+"]"))
	return updated, errSet == nil
}

func alignCodexReasoningReplayCallIDs(inputItems []gjson.Result, replayItems [][]byte) [][]byte {
	outputCallIDs := make(map[string]string)
	for _, item := range inputItems {
		itemType := strings.TrimSpace(item.Get("type").String())
		if itemType != "function_call_output" && itemType != "custom_tool_call_output" {
			continue
		}
		callID := strings.TrimSpace(item.Get("call_id").String())
		for _, candidate := range comparableCodexCallIDs(callID) {
			outputCallIDs[candidate] = callID
		}
	}
	if len(outputCallIDs) == 0 {
		return replayItems
	}

	aligned := make([][]byte, 0, len(replayItems))
	for _, replayItem := range replayItems {
		item := gjson.ParseBytes(replayItem)
		itemType := strings.TrimSpace(item.Get("type").String())
		if itemType != "function_call" && itemType != "custom_tool_call" {
			aligned = append(aligned, replayItem)
			continue
		}
		callID := strings.TrimSpace(item.Get("call_id").String())
		outputCallID := ""
		for _, candidate := range comparableCodexCallIDs(callID) {
			if outputCallIDs[candidate] != "" {
				outputCallID = outputCallIDs[candidate]
				break
			}
		}
		if outputCallID == "" || outputCallID == callID {
			aligned = append(aligned, replayItem)
			continue
		}
		updated, errSet := sjson.SetBytes(replayItem, "call_id", outputCallID)
		if errSet != nil {
			aligned = append(aligned, replayItem)
			continue
		}
		aligned = append(aligned, updated)
	}
	return aligned
}

func codexReasoningReplayInsertIndex(inputItems []gjson.Result, replayItems [][]byte) int {
	replayCallIDs := make(map[string]struct{})
	for _, replayItem := range replayItems {
		item := gjson.ParseBytes(replayItem)
		itemType := strings.TrimSpace(item.Get("type").String())
		if itemType != "function_call" && itemType != "custom_tool_call" {
			continue
		}
		for _, callID := range comparableCodexCallIDs(item.Get("call_id").String()) {
			replayCallIDs[callID] = struct{}{}
		}
	}
	if len(replayCallIDs) > 0 {
		for index, item := range inputItems {
			itemType := strings.TrimSpace(item.Get("type").String())
			if itemType != "function_call_output" && itemType != "custom_tool_call_output" {
				continue
			}
			callID := strings.TrimSpace(item.Get("call_id").String())
			if callID == "" {
				return index
			}
			for _, candidate := range comparableCodexCallIDs(callID) {
				if _, ok := replayCallIDs[candidate]; ok {
					return index
				}
			}
		}
	}
	for index := len(inputItems) - 1; index >= 0; index-- {
		item := inputItems[index]
		if strings.TrimSpace(item.Get("type").String()) == "message" && strings.TrimSpace(item.Get("role").String()) == "assistant" {
			return index
		}
	}
	for index, item := range inputItems {
		if strings.TrimSpace(item.Get("type").String()) != "message" {
			return index
		}
		switch strings.TrimSpace(item.Get("role").String()) {
		case "developer", "system":
			continue
		default:
			return index
		}
	}
	return len(inputItems)
}

func codexReasoningReplayKey(scope CodexReasoningReplayScope) string {
	if !scope.valid() {
		return ""
	}
	return scope.namespace + "\x00" + scope.modelName + "\x00" + scope.sessionKey
}

func setCodexReasoningReplayItems(scope CodexReasoningReplayScope, items [][]byte) bool {
	key := codexReasoningReplayKey(scope)
	if key == "" {
		return false
	}
	size := codexReplayItemsSize(items)
	if size == 0 || size > CodexReasoningReplayCacheMaxEntryBytes {
		deleteCodexReasoningReplay(scope)
		return false
	}
	cloned := cloneCodexReplayItems(items)
	now := time.Now()
	codexReasoningReplayStore.Lock()
	if codexReasoningReplayStore.lastPurge.IsZero() || now.Sub(codexReasoningReplayStore.lastPurge) >= codexReasoningReplayPurgeInterval {
		purgeExpiredCodexReasoningReplayLocked(now)
		codexReasoningReplayStore.lastPurge = now
	}
	if previous, ok := codexReasoningReplayStore.entries[key]; ok {
		codexReasoningReplayStore.totalBytes -= previous.size
	}
	codexReasoningReplayStore.entries[key] = codexReasoningReplayEntry{items: cloned, timestamp: now, size: size}
	codexReasoningReplayStore.totalBytes += size
	overEntries := len(codexReasoningReplayStore.entries) - CodexReasoningReplayCacheMaxEntries
	overBytes := codexReasoningReplayStore.totalBytes - CodexReasoningReplayCacheMaxTotalBytes
	if overEntries > 0 || overBytes > 0 {
		evictOldestCodexReasoningReplayLocked(overEntries, overBytes)
	}
	codexReasoningReplayStore.Unlock()
	return true
}

func getCodexReasoningReplayItems(scope CodexReasoningReplayScope) ([][]byte, bool) {
	key := codexReasoningReplayKey(scope)
	if key == "" {
		return nil, false
	}
	now := time.Now()
	codexReasoningReplayStore.Lock()
	entry, ok := codexReasoningReplayStore.entries[key]
	if ok && now.Sub(entry.timestamp) > CodexReasoningReplayCacheTTL {
		delete(codexReasoningReplayStore.entries, key)
		codexReasoningReplayStore.totalBytes -= entry.size
		ok = false
	} else if ok {
		entry.timestamp = now
		codexReasoningReplayStore.entries[key] = entry
	}
	codexReasoningReplayStore.Unlock()
	if !ok {
		return nil, false
	}
	return cloneCodexReplayItems(entry.items), true
}

func deleteCodexReasoningReplay(scope CodexReasoningReplayScope) {
	codexReasoningReplayStore.Lock()
	key := codexReasoningReplayKey(scope)
	if entry, ok := codexReasoningReplayStore.entries[key]; ok {
		codexReasoningReplayStore.totalBytes -= entry.size
		delete(codexReasoningReplayStore.entries, key)
	}
	codexReasoningReplayStore.Unlock()
}

func purgeExpiredCodexReasoningReplayLocked(now time.Time) {
	for key, entry := range codexReasoningReplayStore.entries {
		if now.Sub(entry.timestamp) > CodexReasoningReplayCacheTTL {
			delete(codexReasoningReplayStore.entries, key)
			codexReasoningReplayStore.totalBytes -= entry.size
		}
	}
}

func evictOldestCodexReasoningReplayLocked(count int, bytesToFree int64) {
	type candidate struct {
		key       string
		timestamp time.Time
	}
	candidates := make([]candidate, 0, len(codexReasoningReplayStore.entries))
	for key, entry := range codexReasoningReplayStore.entries {
		candidates = append(candidates, candidate{key: key, timestamp: entry.timestamp})
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].timestamp.Before(candidates[j].timestamp) })
	if count < 0 {
		count = 0
	}
	if bytesToFree < 0 {
		bytesToFree = 0
	}
	if count > len(candidates) {
		count = len(candidates)
	}
	var freed int64
	for index := 0; index < len(candidates) && (index < count || freed < bytesToFree); index++ {
		entry := codexReasoningReplayStore.entries[candidates[index].key]
		freed += entry.size
		codexReasoningReplayStore.totalBytes -= entry.size
		delete(codexReasoningReplayStore.entries, candidates[index].key)
	}
}

func codexReplayItemsSize(items [][]byte) int64 {
	var size int64
	for _, item := range items {
		size += int64(len(item))
	}
	return size
}

func cloneCodexReplayItems(items [][]byte) [][]byte {
	cloned := make([][]byte, 0, len(items))
	for _, item := range items {
		cloned = append(cloned, append([]byte(nil), item...))
	}
	return cloned
}

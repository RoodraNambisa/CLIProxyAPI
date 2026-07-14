package claude

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const antigravityWebSearchCapabilityMarker = "_cliproxy_antigravity_web_search"

type webSearchGroundingSupport struct {
	StartIndex int64
	EndIndex   int64
	Text       string
	Sources    []webSearchGroundingSource
}

type webSearchGroundingSource struct {
	URL   string
	Title string
}

type webSearchCitedTextBlock struct {
	Text      string
	Citations []map[string]any
}

const antigravityWebSearchSystemInstruction = "You are a search engine bot. You will be given a query from a user. Your task is to search the web for relevant information that will help the user. You MUST perform a web search. Do not respond or interact with the user, please respond as if they typed the query into a search bar."

// WithAntigravityWebSearchCapability marks a request using the capability of
// the already selected auth. The marker is consumed by this translator and is
// never copied into the upstream request.
func WithAntigravityWebSearchCapability(payload []byte, supported bool) []byte {
	if supported {
		updated, err := sjson.SetBytes(payload, antigravityWebSearchCapabilityMarker, true)
		if err == nil {
			return updated
		}
		return payload
	}
	updated, err := sjson.DeleteBytes(payload, antigravityWebSearchCapabilityMarker)
	if err == nil {
		return updated
	}
	return payload
}

func antigravitySupportsNativeGoogleSearch(payload []byte) bool {
	return gjson.GetBytes(payload, antigravityWebSearchCapabilityMarker).Bool()
}

func isClaudeTypedWebSearchToolType(toolType string) bool {
	return toolType == "web_search_20250305" || toolType == "web_search_20260209"
}

func hasClaudeTypedWebSearchTool(payload []byte) bool {
	tools := gjson.GetBytes(payload, "tools")
	if !tools.IsArray() {
		return false
	}
	for _, tool := range tools.Array() {
		if isClaudeTypedWebSearchToolType(tool.Get("type").String()) {
			return true
		}
	}
	return false
}

func hasOnlyClaudeTypedWebSearchTools(payload []byte) bool {
	tools := gjson.GetBytes(payload, "tools")
	if !tools.IsArray() {
		return false
	}
	hasWebSearch := false
	for _, tool := range tools.Array() {
		if isClaudeTypedWebSearchToolType(tool.Get("type").String()) {
			hasWebSearch = true
			continue
		}
		return false
	}
	return hasWebSearch
}

func allowsClaudeWebSearchToolChoice(payload []byte) bool {
	toolChoice := gjson.GetBytes(payload, "tool_choice")
	if !toolChoice.Exists() {
		return true
	}
	if toolChoice.Type == gjson.String {
		switch toolChoice.String() {
		case "", "auto", "any":
			return true
		case "none":
			return false
		default:
			return false
		}
	}
	if !toolChoice.IsObject() {
		return false
	}
	switch toolChoice.Get("type").String() {
	case "", "auto", "any":
		return true
	case "tool":
		return toolChoice.Get("name").String() == "web_search"
	default:
		return false
	}
}

func selectsClaudeWebSearchPath(payload []byte) bool {
	toolChoice := gjson.GetBytes(payload, "tool_choice")
	if !toolChoice.IsObject() {
		return false
	}
	choiceType := toolChoice.Get("type").String()
	return choiceType == "tool" && toolChoice.Get("name").String() == "web_search"
}

func shouldBuildAntigravityWebSearchRequest(model string, payload []byte) bool {
	return antigravitySupportsNativeGoogleSearch(payload) &&
		hasClaudeTypedWebSearchTool(payload) &&
		(hasOnlyClaudeTypedWebSearchTools(payload) || selectsClaudeWebSearchPath(payload)) &&
		allowsClaudeWebSearchToolChoice(payload)
}

func buildAntigravityWebSearchRequest(model string, payload []byte) []byte {
	query := extractClaudeWebSearchQuery(payload)
	maxResultCount := extractClaudeWebSearchMaxUses(payload)
	includedDomains := extractClaudeWebSearchAllowedDomains(payload)
	out := []byte(`{"model":"","requestType":"web_search","request":{"contents":[{"role":"user","parts":[{"text":""}]}],"systemInstruction":{"role":"user","parts":[{"text":""}]},"tools":[{"googleSearch":{"enhancedContent":{"imageSearch":{"maxResultCount":5}}}}],"generationConfig":{"candidateCount":1}}}`)
	out, _ = sjson.SetBytes(out, "model", model)
	out, _ = sjson.SetBytes(out, "request.contents.0.parts.0.text", query)
	out, _ = sjson.SetBytes(out, "request.systemInstruction.parts.0.text", antigravityWebSearchSystemInstruction)
	out, _ = sjson.SetBytes(out, "request.tools.0.googleSearch.enhancedContent.imageSearch.maxResultCount", maxResultCount)
	if len(includedDomains) > 0 {
		if domainsJSON, err := json.Marshal(includedDomains); err == nil {
			out, _ = sjson.SetRawBytes(out, "request.tools.0.googleSearch.includedDomains", domainsJSON)
		}
	}
	return out
}

func extractClaudeWebSearchMaxUses(payload []byte) int64 {
	const defaultMaxResultCount int64 = 5

	tools := gjson.GetBytes(payload, "tools")
	if !tools.IsArray() {
		return defaultMaxResultCount
	}
	for _, tool := range tools.Array() {
		if !isClaudeTypedWebSearchToolType(tool.Get("type").String()) {
			continue
		}
		maxUses := tool.Get("max_uses").Int()
		if maxUses > 0 {
			return maxUses
		}
	}
	return defaultMaxResultCount
}

func extractClaudeWebSearchAllowedDomains(payload []byte) []string {
	tools := gjson.GetBytes(payload, "tools")
	if !tools.IsArray() {
		return nil
	}
	for _, tool := range tools.Array() {
		if !isClaudeTypedWebSearchToolType(tool.Get("type").String()) {
			continue
		}
		allowedDomains := tool.Get("allowed_domains")
		if !allowedDomains.IsArray() {
			return nil
		}
		domains := make([]string, 0, len(allowedDomains.Array()))
		for _, domain := range allowedDomains.Array() {
			if domain.Type != gjson.String {
				continue
			}
			if trimmed := strings.TrimSpace(domain.String()); trimmed != "" {
				domains = append(domains, trimmed)
			}
		}
		return domains
	}
	return nil
}

func extractClaudeWebSearchQuery(payload []byte) string {
	messages := gjson.GetBytes(payload, "messages")
	if !messages.IsArray() {
		return ""
	}
	messageResults := messages.Array()
	for i := len(messageResults) - 1; i >= 0; i-- {
		message := messageResults[i]
		if role := message.Get("role").String(); role != "" && role != "user" {
			continue
		}
		if query := extractClaudeTextContent(message.Get("content")); query != "" {
			return query
		}
	}
	return ""
}

func extractClaudeTextContent(content gjson.Result) string {
	if content.Type == gjson.String {
		return strings.TrimSpace(content.String())
	}
	if !content.IsArray() {
		return ""
	}
	var b strings.Builder
	for _, part := range content.Array() {
		if text := strings.TrimSpace(part.Get("text").String()); text != "" {
			if b.Len() > 0 {
				b.WriteByte('\n')
			}
			b.WriteString(text)
		}
	}
	return strings.TrimSpace(b.String())
}

func hasAntigravityGoogleSearchTool(payload []byte) bool {
	tools := gjson.GetBytes(payload, "request.tools")
	if !tools.IsArray() {
		return false
	}
	for _, tool := range tools.Array() {
		if tool.Get("googleSearch").Exists() {
			return true
		}
	}
	return false
}

func shouldTranslateWebSearchGrounding(originalRequestRawJSON, requestRawJSON []byte) bool {
	return hasClaudeTypedWebSearchTool(originalRequestRawJSON) && hasAntigravityGoogleSearchTool(requestRawJSON)
}

func antigravityGroundingMetadata(root gjson.Result) gjson.Result {
	groundingMetadata := root.Get("response.candidates.0.groundingMetadata")
	if groundingMetadata.Exists() {
		return groundingMetadata
	}
	return root.Get("candidates.0.groundingMetadata")
}

func antigravityVisibleTextParts(root gjson.Result) []string {
	parts := root.Get("response.candidates.0.content.parts")
	if !parts.IsArray() {
		parts = root.Get("candidates.0.content.parts")
	}
	if !parts.IsArray() {
		return nil
	}
	results := parts.Array()
	textParts := make([]string, len(results))
	for i, part := range results {
		if part.Get("thought").Bool() || part.Get("functionCall").Exists() {
			continue
		}
		if text := part.Get("text"); text.Exists() {
			textParts[i] = text.String()
		}
	}
	return textParts
}

func appendAntigravityVisibleTextParts(current []string, root gjson.Result) []string {
	chunkParts := antigravityVisibleTextParts(root)
	if len(chunkParts) == 0 {
		return current
	}
	if len(current) == 0 {
		current = []string{""}
	}
	for _, text := range chunkParts {
		current[0] += text
	}
	return current
}

func joinWebSearchTextParts(parts []string) (string, []int64) {
	var textBuilder strings.Builder
	offsets := make([]int64, len(parts))
	for i, text := range parts {
		offsets[i] = int64(textBuilder.Len())
		textBuilder.WriteString(text)
	}
	return textBuilder.String(), offsets
}

func webSearchGroundingResults(rawMetadata []string) []gjson.Result {
	results := make([]gjson.Result, 0, len(rawMetadata))
	for _, raw := range rawMetadata {
		if strings.TrimSpace(raw) == "" || !gjson.Valid(raw) {
			continue
		}
		results = append(results, gjson.Parse(raw))
	}
	return results
}

func antigravityUsageTokens(root gjson.Result) (int64, int64) {
	usage := root.Get("response.usageMetadata")
	if !usage.Exists() {
		usage = root.Get("usageMetadata")
	}
	inputTokens := usage.Get("promptTokenCount").Int()
	outputTokens := usage.Get("candidatesTokenCount").Int() + usage.Get("thoughtsTokenCount").Int()
	if outputTokens == 0 {
		totalTokens := usage.Get("totalTokenCount").Int()
		if totalTokens > 0 {
			outputTokens = totalTokens - inputTokens
			if outputTokens < 0 {
				outputTokens = 0
			}
		}
	}
	return inputTokens, outputTokens
}

func webSearchQueryFromGrounding(groundingMetadata []gjson.Result) string {
	for _, metadata := range groundingMetadata {
		values := metadata.Get("webSearchQueries")
		if !values.IsArray() {
			continue
		}
		for _, value := range values.Array() {
			query := strings.TrimSpace(value.String())
			if query != "" {
				return query
			}
		}
	}
	return ""
}

func webSearchRequestCount(_ []gjson.Result) int64 {
	return 1
}

func webSearchResultsFromGrounding(groundingMetadata []gjson.Result) []byte {
	results := []byte(`[]`)
	seenURLs := make(map[string]struct{})
	for _, metadata := range groundingMetadata {
		groundingChunks := metadata.Get("groundingChunks")
		if !groundingChunks.IsArray() {
			continue
		}
		for _, chunk := range groundingChunks.Array() {
			web := chunk.Get("web")
			if !web.Exists() {
				continue
			}
			uri := strings.TrimSpace(web.Get("uri").String())
			if uri == "" {
				continue
			}
			if _, ok := seenURLs[uri]; ok {
				continue
			}
			seenURLs[uri] = struct{}{}

			result := []byte(`{"type":"web_search_result","page_age":null}`)
			if title := web.Get("title"); title.Exists() {
				result, _ = sjson.SetBytes(result, "title", title.String())
			}
			result, _ = sjson.SetBytes(result, "url", uri)
			results, _ = sjson.SetRawBytes(results, "-1", result)
		}
	}
	return results
}

func parseWebSearchGroundingSupports(groundingMetadata []gjson.Result, textParts []string) []webSearchGroundingSupport {
	textContent, partOffsets := joinWebSearchTextParts(textParts)
	supports := make([]webSearchGroundingSupport, 0)
	type resolvedRange struct {
		start int64
		end   int64
	}
	resolvedSegments := make(map[string]resolvedRange)
	fallbackCursor := make(map[int]int)
	supportIndexes := make(map[string]int)
	for _, metadata := range groundingMetadata {
		chunks := metadata.Get("groundingChunks").Array()
		chunkData := make([]webSearchGroundingSource, len(chunks))
		for i, chunk := range chunks {
			web := chunk.Get("web")
			if web.Exists() {
				chunkData[i].URL = strings.TrimSpace(web.Get("uri").String())
				chunkData[i].Title = web.Get("title").String()
			}
		}

		groundingSupports := metadata.Get("groundingSupports")
		if !groundingSupports.IsArray() {
			continue
		}
		for _, support := range groundingSupports.Array() {
			segment := support.Get("segment")
			if !segment.Exists() {
				continue
			}
			partIndex := int(segment.Get("partIndex").Int())
			segmentStart := segment.Get("startIndex").Int()
			segmentEnd := segment.Get("endIndex").Int()
			segmentText := segment.Get("text").String()
			segmentKey := fmt.Sprintf("%d\x00%d\x00%d\x00%s", partIndex, segmentStart, segmentEnd, segmentText)
			rangeValue, resolved := resolvedSegments[segmentKey]
			if !resolved {
				rangeValue = resolvedRange{start: -1, end: -1}
				if partIndex >= 0 && partIndex < len(partOffsets) {
					rangeValue.start = partOffsets[partIndex] + segmentStart
					rangeValue.end = partOffsets[partIndex] + segmentEnd
				}
				invalidRange := rangeValue.start < 0 || rangeValue.end < rangeValue.start || rangeValue.end > int64(len(textContent))
				if !invalidRange && segmentText != "" && textContent[rangeValue.start:rangeValue.end] != segmentText {
					invalidRange = true
				}
				if invalidRange && segmentText != "" {
					searchStart := fallbackCursor[partIndex]
					if searchStart < 0 || searchStart > len(textContent) {
						searchStart = 0
					}
					if fallbackIndex := strings.Index(textContent[searchStart:], segmentText); fallbackIndex >= 0 {
						rangeValue.start = int64(searchStart + fallbackIndex)
						rangeValue.end = rangeValue.start + int64(len(segmentText))
					}
				}
				if rangeValue.start >= 0 && rangeValue.end >= rangeValue.start && rangeValue.end <= int64(len(textContent)) {
					resolvedSegments[segmentKey] = rangeValue
					if int(rangeValue.end) > fallbackCursor[partIndex] {
						fallbackCursor[partIndex] = int(rangeValue.end)
					}
				}
			}
			if rangeValue.start < 0 || rangeValue.end < rangeValue.start || rangeValue.end > int64(len(textContent)) {
				continue
			}
			parsed := webSearchGroundingSupport{
				StartIndex: rangeValue.start,
				EndIndex:   rangeValue.end,
				Text:       segmentText,
			}
			if chunkIndices := support.Get("groundingChunkIndices"); chunkIndices.IsArray() {
				seenSources := make(map[string]struct{})
				for _, idx := range chunkIndices.Array() {
					chunkIndex := int(idx.Int())
					if chunkIndex < 0 || chunkIndex >= len(chunkData) {
						continue
					}
					source := chunkData[chunkIndex]
					if source.URL == "" {
						continue
					}
					if _, exists := seenSources[source.URL]; exists {
						continue
					}
					seenSources[source.URL] = struct{}{}
					parsed.Sources = append(parsed.Sources, source)
				}
			}
			supportKey := fmt.Sprintf("%d\x00%d\x00%s", parsed.StartIndex, parsed.EndIndex, parsed.Text)
			if existingIndex, exists := supportIndexes[supportKey]; exists {
				existing := &supports[existingIndex]
				seenSources := make(map[string]struct{}, len(existing.Sources))
				for _, source := range existing.Sources {
					seenSources[source.URL] = struct{}{}
				}
				for _, source := range parsed.Sources {
					if _, exists := seenSources[source.URL]; exists {
						continue
					}
					seenSources[source.URL] = struct{}{}
					existing.Sources = append(existing.Sources, source)
				}
				continue
			}
			supportIndexes[supportKey] = len(supports)
			supports = append(supports, parsed)
		}
	}
	sort.SliceStable(supports, func(i, j int) bool {
		if supports[i].StartIndex != supports[j].StartIndex {
			return supports[i].StartIndex < supports[j].StartIndex
		}
		return supports[i].EndIndex < supports[j].EndIndex
	})
	return supports
}

func buildWebSearchCitedTextBlocks(textContent string, supports []webSearchGroundingSupport) []webSearchCitedTextBlock {
	textBytes := []byte(textContent)
	if len(textBytes) == 0 {
		return nil
	}

	boundaries := []int64{0, int64(len(textBytes))}
	for _, support := range supports {
		start := max(support.StartIndex, int64(0))
		end := min(support.EndIndex, int64(len(textBytes)))
		if start >= end {
			continue
		}
		boundaries = append(boundaries, start, end)
	}
	sort.Slice(boundaries, func(i, j int) bool { return boundaries[i] < boundaries[j] })
	uniqueBoundaries := boundaries[:0]
	for _, boundary := range boundaries {
		if len(uniqueBoundaries) == 0 || uniqueBoundaries[len(uniqueBoundaries)-1] != boundary {
			uniqueBoundaries = append(uniqueBoundaries, boundary)
		}
	}

	blocks := make([]webSearchCitedTextBlock, 0, len(uniqueBoundaries)-1)
	for i := 0; i+1 < len(uniqueBoundaries); i++ {
		start := uniqueBoundaries[i]
		end := uniqueBoundaries[i+1]
		if start >= end {
			continue
		}
		blockText := string(textBytes[start:end])
		block := webSearchCitedTextBlock{Text: blockText}
		seenSources := make(map[string]struct{})
		for _, support := range supports {
			if support.StartIndex >= end || support.EndIndex <= start {
				continue
			}
			for _, source := range support.Sources {
				if source.URL == "" {
					continue
				}
				if _, exists := seenSources[source.URL]; exists {
					continue
				}
				seenSources[source.URL] = struct{}{}
				block.Citations = append(block.Citations, map[string]any{
					"type":       "web_search_result_location",
					"cited_text": blockText,
					"url":        source.URL,
					"title":      source.Title,
				})
			}
		}
		blocks = append(blocks, block)
	}
	return blocks
}

func buildClaudeWebSearchContent(textParts []string, groundingMetadata []gjson.Result) []byte {
	content := []byte(`[]`)
	results := webSearchResultsFromGrounding(groundingMetadata)
	toolUseID := newClaudeWebSearchToolUseID()

	serverToolUse := []byte(`{"type":"server_tool_use","id":"","name":"web_search","input":{}}`)
	serverToolUse, _ = sjson.SetBytes(serverToolUse, "id", toolUseID)
	if query := webSearchQueryFromGrounding(groundingMetadata); query != "" {
		serverToolUse, _ = sjson.SetBytes(serverToolUse, "input.query", query)
	}
	content, _ = sjson.SetRawBytes(content, "-1", serverToolUse)

	webSearchToolResult := []byte(`{"type":"web_search_tool_result","tool_use_id":"","content":[]}`)
	webSearchToolResult, _ = sjson.SetBytes(webSearchToolResult, "tool_use_id", toolUseID)
	webSearchToolResult, _ = sjson.SetRawBytes(webSearchToolResult, "content", results)
	content, _ = sjson.SetRawBytes(content, "-1", webSearchToolResult)

	textContent, _ := joinWebSearchTextParts(textParts)
	for _, block := range buildWebSearchCitedTextBlocks(textContent, parseWebSearchGroundingSupports(groundingMetadata, textParts)) {
		if block.Text == "" {
			continue
		}
		textBlock := []byte(`{"type":"text","text":""}`)
		textBlock, _ = sjson.SetBytes(textBlock, "text", block.Text)
		if len(block.Citations) > 0 {
			citationsJSON, _ := json.Marshal(block.Citations)
			textBlock, _ = sjson.SetRawBytes(textBlock, "citations", citationsJSON)
		}
		content, _ = sjson.SetRawBytes(content, "-1", textBlock)
	}

	return content
}

func appendClaudeWebSearchStreamBlocks(appendEvent func(string, string), startIndex int, textParts []string, groundingMetadata []gjson.Result) int {
	contentIndex := startIndex
	results := webSearchResultsFromGrounding(groundingMetadata)
	toolUseID := newClaudeWebSearchToolUseID()

	serverToolUseStart := fmt.Sprintf(`{"type":"content_block_start","index":%d,"content_block":{"type":"server_tool_use","id":"%s","name":"web_search","input":{}}}`,
		contentIndex, toolUseID)
	appendEvent("content_block_start", serverToolUseStart)
	if query := webSearchQueryFromGrounding(groundingMetadata); query != "" {
		queryJSON, _ := sjson.Set(`{}`, "query", query)
		inputDelta := fmt.Sprintf(`{"type":"content_block_delta","index":%d,"delta":{"type":"input_json_delta","partial_json":""}}`, contentIndex)
		inputDelta, _ = sjson.Set(inputDelta, "delta.partial_json", queryJSON)
		appendEvent("content_block_delta", inputDelta)
	}
	appendEvent("content_block_stop", fmt.Sprintf(`{"type":"content_block_stop","index":%d}`, contentIndex))
	contentIndex++

	webSearchToolResultStart := fmt.Sprintf(`{"type":"content_block_start","index":%d,"content_block":{"type":"web_search_tool_result","tool_use_id":"%s","content":[]}}`,
		contentIndex, toolUseID)
	webSearchToolResultStart, _ = sjson.SetRaw(webSearchToolResultStart, "content_block.content", string(results))
	appendEvent("content_block_start", webSearchToolResultStart)
	appendEvent("content_block_stop", fmt.Sprintf(`{"type":"content_block_stop","index":%d}`, contentIndex))
	contentIndex++

	textContent, _ := joinWebSearchTextParts(textParts)
	for _, block := range buildWebSearchCitedTextBlocks(textContent, parseWebSearchGroundingSupports(groundingMetadata, textParts)) {
		if block.Text == "" {
			continue
		}
		textBlockStart := fmt.Sprintf(`{"type":"content_block_start","index":%d,"content_block":{"type":"text","text":""}}`, contentIndex)
		if len(block.Citations) > 0 {
			textBlockStart = fmt.Sprintf(`{"type":"content_block_start","index":%d,"content_block":{"citations":[],"type":"text","text":""}}`, contentIndex)
		}
		appendEvent("content_block_start", textBlockStart)
		for _, citation := range block.Citations {
			citationJSON, _ := json.Marshal(citation)
			citationDelta := fmt.Sprintf(`{"type":"content_block_delta","index":%d,"delta":{"type":"citations_delta","citation":%s}}`, contentIndex, string(citationJSON))
			appendEvent("content_block_delta", citationDelta)
		}
		for _, chunk := range splitRunesForWebSearch(block.Text, 50) {
			textDelta := fmt.Sprintf(`{"type":"content_block_delta","index":%d,"delta":{"type":"text_delta","text":""}}`, contentIndex)
			textDelta, _ = sjson.Set(textDelta, "delta.text", chunk)
			appendEvent("content_block_delta", textDelta)
		}
		appendEvent("content_block_stop", fmt.Sprintf(`{"type":"content_block_stop","index":%d}`, contentIndex))
		contentIndex++
	}

	return contentIndex
}

func splitRunesForWebSearch(text string, chunkSize int) []string {
	if chunkSize <= 0 || text == "" {
		return nil
	}
	runes := []rune(text)
	chunks := make([]string, 0, (len(runes)+chunkSize-1)/chunkSize)
	for start := 0; start < len(runes); start += chunkSize {
		end := start + chunkSize
		if end > len(runes) {
			end = len(runes)
		}
		chunks = append(chunks, string(runes[start:end]))
	}
	return chunks
}

func newClaudeWebSearchToolUseID() string {
	return fmt.Sprintf("srvtoolu_%d", time.Now().UnixNano())
}

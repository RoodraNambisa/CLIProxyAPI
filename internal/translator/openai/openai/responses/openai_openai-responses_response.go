package responses

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	translatorcommon "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/common"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type oaiToResponsesStateReasoning struct {
	ReasoningID   string
	ReasoningData string
	OutputIndex   int
	Status        string
}
type oaiToResponsesState struct {
	Seq               int
	ResponseID        string
	Created           int64
	Started           bool
	CompletionPending bool
	CompletedEmitted  bool
	StreamEnded       bool
	ReasoningBlocks   map[int]*responsesReasoningBlock
	FinishReasons     map[int]string
	// aggregation buffers for response.output
	// Per-output message text buffers by index
	MsgTextBuf     map[int]*strings.Builder
	Reasonings     []oaiToResponsesStateReasoning
	FuncArgsBuf    map[string]*strings.Builder
	FuncNames      map[string]string
	FuncCallIDs    map[string]string
	FuncChoices    map[string]int
	FuncOutputIx   map[string]int
	FuncArgsSent   map[string]int
	FuncItemAdded  map[string]bool
	FuncItemCustom map[string]bool
	ToolIdentities map[string]responsesToolIdentity
	MsgOutputIx    map[int]int
	NextOutputIx   int
	// message item state per output index
	MsgItemAdded    map[int]bool // whether response.output_item.added emitted for message
	MsgContentAdded map[int]bool // whether response.content_part.added emitted for message
	MsgItemDone     map[int]bool // whether message done events were emitted
	MsgItemStatus   map[int]string
	// function item done state
	FuncArgsDone map[string]bool
	FuncItemDone map[string]bool
	// usage aggregation
	PromptTokens     int64
	CachedTokens     int64
	CompletionTokens int64
	TotalTokens      int64
	ReasoningTokens  int64
	UsageSeen        bool
}

// responseIDCounter provides a process-wide unique counter for synthesized response identifiers.
var responseIDCounter uint64

func emitRespEvent(event string, payload []byte) []byte {
	return translatorcommon.SSEEventData(event, payload)
}

func buildResponsesCompletedEvent(st *oaiToResponsesState, requestRawJSON []byte, nextSeq func() int) []byte {
	completed := []byte(`{"type":"response.completed","sequence_number":0,"response":{"id":"","object":"response","created_at":0,"status":"completed","background":false,"error":null}}`)
	eventType := "response.completed"
	if reason := st.incompleteReason(); reason != "" {
		eventType = "response.incomplete"
		completed, _ = sjson.SetBytes(completed, "type", eventType)
		completed, _ = sjson.SetBytes(completed, "response.status", "incomplete")
		completed, _ = sjson.SetBytes(completed, "response.incomplete_details.reason", reason)
	}
	completed, _ = sjson.SetRawBytes(completed, "response.output", []byte("[]"))
	completed, _ = sjson.SetBytes(completed, "sequence_number", nextSeq())
	completed, _ = sjson.SetBytes(completed, "response.id", st.ResponseID)
	completed, _ = sjson.SetBytes(completed, "response.created_at", st.Created)
	// Inject original request fields into response as per docs/response.completed.json
	if requestRawJSON != nil {
		req := gjson.ParseBytes(requestRawJSON)
		if v := req.Get("instructions"); v.Exists() {
			completed, _ = sjson.SetBytes(completed, "response.instructions", v.String())
		}
		if v := req.Get("max_output_tokens"); v.Exists() {
			completed, _ = sjson.SetBytes(completed, "response.max_output_tokens", v.Int())
		}
		if v := req.Get("max_tool_calls"); v.Exists() {
			completed, _ = sjson.SetBytes(completed, "response.max_tool_calls", v.Int())
		}
		if v := req.Get("model"); v.Exists() {
			completed, _ = sjson.SetBytes(completed, "response.model", v.String())
		}
		if v := req.Get("parallel_tool_calls"); v.Exists() {
			completed, _ = sjson.SetBytes(completed, "response.parallel_tool_calls", v.Bool())
		}
		if v := req.Get("previous_response_id"); v.Exists() {
			completed, _ = sjson.SetBytes(completed, "response.previous_response_id", v.String())
		}
		if v := req.Get("prompt_cache_key"); v.Exists() {
			completed, _ = sjson.SetBytes(completed, "response.prompt_cache_key", v.String())
		}
		if v := req.Get("reasoning"); v.Exists() {
			completed, _ = sjson.SetBytes(completed, "response.reasoning", v.Value())
		}
		if v := req.Get("safety_identifier"); v.Exists() {
			completed, _ = sjson.SetBytes(completed, "response.safety_identifier", v.String())
		}
		if v := req.Get("service_tier"); v.Exists() {
			completed, _ = sjson.SetBytes(completed, "response.service_tier", v.String())
		}
		if v := req.Get("store"); v.Exists() {
			completed, _ = sjson.SetBytes(completed, "response.store", v.Bool())
		}
		if v := req.Get("temperature"); v.Exists() {
			completed, _ = sjson.SetBytes(completed, "response.temperature", v.Float())
		}
		if v := req.Get("text"); v.Exists() {
			completed, _ = sjson.SetBytes(completed, "response.text", v.Value())
		}
		if v := req.Get("tool_choice"); v.Exists() {
			completed, _ = sjson.SetBytes(completed, "response.tool_choice", v.Value())
		}
		if v := req.Get("tools"); v.Exists() {
			completed, _ = sjson.SetBytes(completed, "response.tools", v.Value())
		}
		if v := req.Get("top_logprobs"); v.Exists() {
			completed, _ = sjson.SetBytes(completed, "response.top_logprobs", v.Int())
		}
		if v := req.Get("top_p"); v.Exists() {
			completed, _ = sjson.SetBytes(completed, "response.top_p", v.Float())
		}
		if v := req.Get("truncation"); v.Exists() {
			completed, _ = sjson.SetBytes(completed, "response.truncation", v.String())
		}
		if v := req.Get("user"); v.Exists() {
			completed, _ = sjson.SetBytes(completed, "response.user", v.Value())
		}
		if v := req.Get("metadata"); v.Exists() {
			completed, _ = sjson.SetBytes(completed, "response.metadata", v.Value())
		}
	}

	outputsWrapper := []byte(`{"arr":[]}`)
	type completedOutputItem struct {
		index int
		raw   []byte
	}
	outputItems := make([]completedOutputItem, 0, len(st.Reasonings)+len(st.MsgItemAdded)+len(st.FuncArgsBuf))
	if len(st.Reasonings) > 0 {
		for _, r := range st.Reasonings {
			item := []byte(`{"id":"","type":"reasoning","encrypted_content":"","summary":[{"type":"summary_text","text":""}]}`)
			item, _ = sjson.SetBytes(item, "id", r.ReasoningID)
			item, _ = sjson.SetBytes(item, "summary.0.text", r.ReasoningData)
			item, _ = sjson.SetBytes(item, "status", r.Status)
			outputItems = append(outputItems, completedOutputItem{index: r.OutputIndex, raw: item})
		}
	}
	if len(st.MsgItemAdded) > 0 {
		for i := range st.MsgItemAdded {
			txt := ""
			if b := st.MsgTextBuf[i]; b != nil {
				txt = b.String()
			}
			item := []byte(`{"id":"","type":"message","status":"completed","content":[{"type":"output_text","annotations":[],"logprobs":[],"text":""}],"role":"assistant"}`)
			item, _ = sjson.SetBytes(item, "id", fmt.Sprintf("msg_%s_%d", st.ResponseID, i))
			item, _ = sjson.SetBytes(item, "content.0.text", txt)
			item, _ = sjson.SetBytes(item, "status", st.MsgItemStatus[i])
			outputItems = append(outputItems, completedOutputItem{index: st.MsgOutputIx[i], raw: item})
		}
	}
	if len(st.FuncArgsBuf) > 0 {
		for key := range st.FuncArgsBuf {
			args := ""
			if b := st.FuncArgsBuf[key]; b != nil {
				args = b.String()
			}
			callID := st.FuncCallIDs[key]
			name := st.FuncNames[key]
			item := buildResponsesToolItem(st.ToolIdentities, name, callID, args, responsesItemStatus(st.FinishReasons[st.FuncChoices[key]]))
			outputItems = append(outputItems, completedOutputItem{index: st.FuncOutputIx[key], raw: item})
		}
	}
	sort.Slice(outputItems, func(i, j int) bool { return outputItems[i].index < outputItems[j].index })
	for _, item := range outputItems {
		outputsWrapper, _ = sjson.SetRawBytes(outputsWrapper, "arr.-1", item.raw)
	}
	if gjson.GetBytes(outputsWrapper, "arr.#").Int() > 0 {
		completed, _ = sjson.SetRawBytes(completed, "response.output", []byte(gjson.GetBytes(outputsWrapper, "arr").Raw))
	}
	if st.UsageSeen {
		completed, _ = sjson.SetBytes(completed, "response.usage.input_tokens", st.PromptTokens)
		completed, _ = sjson.SetBytes(completed, "response.usage.input_tokens_details.cached_tokens", st.CachedTokens)
		completed, _ = sjson.SetBytes(completed, "response.usage.output_tokens", st.CompletionTokens)
		if st.ReasoningTokens > 0 {
			completed, _ = sjson.SetBytes(completed, "response.usage.output_tokens_details.reasoning_tokens", st.ReasoningTokens)
		}
		total := st.TotalTokens
		if total == 0 {
			total = st.PromptTokens + st.CompletionTokens
		}
		completed, _ = sjson.SetBytes(completed, "response.usage.total_tokens", total)
	}
	return emitRespEvent(eventType, completed)
}

// ConvertOpenAIChatCompletionsResponseToOpenAIResponses converts OpenAI Chat Completions streaming chunks
// to OpenAI Responses SSE events (response.*).
func ConvertOpenAIChatCompletionsResponseToOpenAIResponses(ctx context.Context, modelName string, originalRequestRawJSON, requestRawJSON, rawJSON []byte, param *any) [][]byte {
	if *param == nil {
		*param = &oaiToResponsesState{
			FuncArgsBuf:     make(map[string]*strings.Builder),
			FuncNames:       make(map[string]string),
			FuncCallIDs:     make(map[string]string),
			FuncOutputIx:    make(map[string]int),
			FuncArgsSent:    make(map[string]int),
			FuncItemAdded:   make(map[string]bool),
			FuncItemCustom:  make(map[string]bool),
			MsgOutputIx:     make(map[int]int),
			MsgTextBuf:      make(map[int]*strings.Builder),
			MsgItemAdded:    make(map[int]bool),
			MsgContentAdded: make(map[int]bool),
			MsgItemDone:     make(map[int]bool),
			FuncArgsDone:    make(map[string]bool),
			FuncItemDone:    make(map[string]bool),
			Reasonings:      make([]oaiToResponsesStateReasoning, 0),
		}
	}
	st := (*param).(*oaiToResponsesState)
	if st.CompletedEmitted || st.StreamEnded {
		return nil
	}

	if bytes.HasPrefix(rawJSON, []byte("data:")) {
		rawJSON = bytes.TrimSpace(rawJSON[5:])
	}

	rawJSON = bytes.TrimSpace(rawJSON)
	if len(rawJSON) == 0 {
		return [][]byte{}
	}
	if bytes.Equal(rawJSON, []byte("[DONE]")) {
		st.StreamEnded = true
		if st.Started && (st.CompletionPending || len(st.MsgItemAdded) > 0 || len(st.ReasoningBlocks) > 0 || len(st.FuncArgsBuf) > 0) {
			nextSeq := func() int { st.Seq++; return st.Seq }
			var out [][]byte
			for _, choice := range st.knownChoices() {
				out = append(out, st.finishChoice(choice, nextSeq)...)
			}
			for key := range st.FuncArgsBuf {
				if !st.FuncItemDone[key] {
					return out
				}
			}
			st.CompletedEmitted = true
			return append(out, buildResponsesCompletedEvent(st, pickResponsesRequestJSON(originalRequestRawJSON, requestRawJSON), nextSeq))
		}
		return [][]byte{}
	}

	root := gjson.ParseBytes(rawJSON)
	obj := root.Get("object")
	if obj.Exists() && obj.String() != "" && obj.String() != "chat.completion.chunk" {
		return [][]byte{}
	}
	if !root.Get("choices").Exists() || !root.Get("choices").IsArray() {
		return [][]byte{}
	}

	if usage := root.Get("usage"); usage.Exists() {
		if v := usage.Get("prompt_tokens"); v.Exists() {
			st.PromptTokens = v.Int()
			st.UsageSeen = true
		}
		if v := usage.Get("prompt_tokens_details.cached_tokens"); v.Exists() {
			st.CachedTokens = v.Int()
			st.UsageSeen = true
		}
		if v := usage.Get("completion_tokens"); v.Exists() {
			st.CompletionTokens = v.Int()
			st.UsageSeen = true
		} else if v := usage.Get("output_tokens"); v.Exists() {
			st.CompletionTokens = v.Int()
			st.UsageSeen = true
		}
		if v := usage.Get("output_tokens_details.reasoning_tokens"); v.Exists() {
			st.ReasoningTokens = v.Int()
			st.UsageSeen = true
		} else if v := usage.Get("completion_tokens_details.reasoning_tokens"); v.Exists() {
			st.ReasoningTokens = v.Int()
			st.UsageSeen = true
		}
		if v := usage.Get("total_tokens"); v.Exists() {
			st.TotalTokens = v.Int()
			st.UsageSeen = true
		}
	}

	nextSeq := func() int { st.Seq++; return st.Seq }
	allocOutputIndex := func() int {
		ix := st.NextOutputIx
		st.NextOutputIx++
		return ix
	}
	var out [][]byte

	if !st.Started {
		st.ResponseID = root.Get("id").String()
		if st.ResponseID == "" {
			st.ResponseID = fmt.Sprintf("resp_%x_%d", time.Now().UnixNano(), atomic.AddUint64(&responseIDCounter, 1))
		}
		st.Created = root.Get("created").Int()
		// reset aggregation state for a new streaming response
		st.MsgTextBuf = make(map[int]*strings.Builder)
		st.ReasoningBlocks = make(map[int]*responsesReasoningBlock)
		st.FinishReasons = make(map[int]string)
		st.FuncArgsBuf = make(map[string]*strings.Builder)
		st.FuncNames = make(map[string]string)
		st.FuncCallIDs = make(map[string]string)
		st.FuncChoices = make(map[string]int)
		st.FuncOutputIx = make(map[string]int)
		st.FuncArgsSent = make(map[string]int)
		st.FuncItemAdded = make(map[string]bool)
		st.FuncItemCustom = make(map[string]bool)
		st.ToolIdentities = responsesToolIdentities(pickResponsesRequestJSON(originalRequestRawJSON, requestRawJSON))
		st.MsgOutputIx = make(map[int]int)
		st.NextOutputIx = 0
		st.MsgItemAdded = make(map[int]bool)
		st.MsgContentAdded = make(map[int]bool)
		st.MsgItemDone = make(map[int]bool)
		st.MsgItemStatus = make(map[int]string)
		st.FuncArgsDone = make(map[string]bool)
		st.FuncItemDone = make(map[string]bool)
		// Usage may already have arrived on this first chunk.
		st.CompletionPending = false
		st.CompletedEmitted = false
		// response.created
		created := []byte(`{"type":"response.created","sequence_number":0,"response":{"id":"","object":"response","created_at":0,"status":"in_progress","background":false,"error":null,"output":[]}}`)
		created, _ = sjson.SetBytes(created, "sequence_number", nextSeq())
		created, _ = sjson.SetBytes(created, "response.id", st.ResponseID)
		created, _ = sjson.SetBytes(created, "response.created_at", st.Created)
		out = append(out, emitRespEvent("response.created", created))

		inprog := []byte(`{"type":"response.in_progress","sequence_number":0,"response":{"id":"","object":"response","created_at":0,"status":"in_progress"}}`)
		inprog, _ = sjson.SetBytes(inprog, "sequence_number", nextSeq())
		inprog, _ = sjson.SetBytes(inprog, "response.id", st.ResponseID)
		inprog, _ = sjson.SetBytes(inprog, "response.created_at", st.Created)
		out = append(out, emitRespEvent("response.in_progress", inprog))
		st.Started = true
	}

	// choices[].delta content / tool_calls / reasoning_content
	if choices := root.Get("choices"); choices.Exists() && choices.IsArray() {
		choices.ForEach(func(_, choice gjson.Result) bool {
			idx, validIndex := responsesStreamIndex(choice.Get("index"), 0)
			if !validIndex {
				return true
			}
			if _, finished := st.FinishReasons[idx]; finished {
				return true
			}
			delta := choice.Get("delta")
			if delta.Exists() {
				if c := delta.Get("content"); c.Exists() && c.String() != "" {
					// Ensure the message item and its first content part are announced before any text deltas
					out = append(out, st.finishReasoning(idx, "completed", nextSeq)...)
					if _, exists := st.MsgOutputIx[idx]; !exists {
						st.MsgOutputIx[idx] = allocOutputIndex()
					}
					msgOutputIndex := st.MsgOutputIx[idx]
					if !st.MsgItemAdded[idx] {
						item := []byte(`{"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"id":"","type":"message","status":"in_progress","content":[],"role":"assistant"}}`)
						item, _ = sjson.SetBytes(item, "sequence_number", nextSeq())
						item, _ = sjson.SetBytes(item, "output_index", msgOutputIndex)
						item, _ = sjson.SetBytes(item, "item.id", fmt.Sprintf("msg_%s_%d", st.ResponseID, idx))
						out = append(out, emitRespEvent("response.output_item.added", item))
						st.MsgItemAdded[idx] = true
					}
					if !st.MsgContentAdded[idx] {
						part := []byte(`{"type":"response.content_part.added","sequence_number":0,"item_id":"","output_index":0,"content_index":0,"part":{"type":"output_text","annotations":[],"logprobs":[],"text":""}}`)
						part, _ = sjson.SetBytes(part, "sequence_number", nextSeq())
						part, _ = sjson.SetBytes(part, "item_id", fmt.Sprintf("msg_%s_%d", st.ResponseID, idx))
						part, _ = sjson.SetBytes(part, "output_index", msgOutputIndex)
						part, _ = sjson.SetBytes(part, "content_index", 0)
						out = append(out, emitRespEvent("response.content_part.added", part))
						st.MsgContentAdded[idx] = true
					}

					msg := []byte(`{"type":"response.output_text.delta","sequence_number":0,"item_id":"","output_index":0,"content_index":0,"delta":"","logprobs":[]}`)
					msg, _ = sjson.SetBytes(msg, "sequence_number", nextSeq())
					msg, _ = sjson.SetBytes(msg, "item_id", fmt.Sprintf("msg_%s_%d", st.ResponseID, idx))
					msg, _ = sjson.SetBytes(msg, "output_index", msgOutputIndex)
					msg, _ = sjson.SetBytes(msg, "content_index", 0)
					msg, _ = sjson.SetBytes(msg, "delta", c.String())
					out = append(out, emitRespEvent("response.output_text.delta", msg))
					// aggregate for response.output
					if st.MsgTextBuf[idx] == nil {
						st.MsgTextBuf[idx] = &strings.Builder{}
					}
					st.MsgTextBuf[idx].WriteString(c.String())
				}

				if text := responsesReasoningText(delta); text != "" {
					out = append(out, st.appendReasoning(idx, text, nextSeq)...)
				}

				// tool calls
				if tcs := delta.Get("tool_calls"); tcs.IsArray() && len(tcs.Array()) > 0 {
					out = append(out, st.finishReasoning(idx, "completed", nextSeq)...)
					out = append(out, st.finishMessage(idx, "completed", nextSeq)...)

					tcs.ForEach(func(index, tc gjson.Result) bool {
						toolIndex, validIndex := responsesStreamIndex(tc.Get("index"), int(index.Int()))
						if !validIndex {
							return true
						}
						key := st.acceptToolCallDelta(idx, toolIndex, tc.Get("id").String(), tc.Get("function.name").String(), tc.Get("function.arguments").String())
						if key != "" {
							out = append(out, st.emitToolEvents(key, false, nextSeq)...)
						}
						return true
					})
				}
			}

			if reason := choice.Get("finish_reason"); reason.Type == gjson.String && reason.String() != "" {
				st.FinishReasons[idx] = reason.String()
				out = append(out, st.finishChoice(idx, nextSeq)...)
				st.CompletionPending = true
			}

			return true
		})
	}

	return out
}

// ConvertOpenAIChatCompletionsResponseToOpenAIResponsesNonStream builds a single Responses JSON
// from a non-streaming OpenAI Chat Completions response.
func ConvertOpenAIChatCompletionsResponseToOpenAIResponsesNonStream(_ context.Context, _ string, originalRequestRawJSON, requestRawJSON, rawJSON []byte, _ *any) []byte {
	toolIdentities := responsesToolIdentities(pickResponsesRequestJSON(originalRequestRawJSON, requestRawJSON))
	root := gjson.ParseBytes(rawJSON)

	// Basic response scaffold
	resp := []byte(`{"id":"","object":"response","created_at":0,"status":"completed","background":false,"error":null,"incomplete_details":null,"output":[]}`)
	if reason := responsesIncompleteFromChoices(root.Get("choices")); reason != "" {
		resp, _ = sjson.SetBytes(resp, "status", "incomplete")
		resp, _ = sjson.SetBytes(resp, "incomplete_details.reason", reason)
	}

	// id: use provider id if present, otherwise synthesize
	id := root.Get("id").String()
	if id == "" {
		id = fmt.Sprintf("resp_%x_%d", time.Now().UnixNano(), atomic.AddUint64(&responseIDCounter, 1))
	}
	resp, _ = sjson.SetBytes(resp, "id", id)

	// created_at: map from chat.completion created
	created := root.Get("created").Int()
	if created == 0 {
		created = time.Now().Unix()
	}
	resp, _ = sjson.SetBytes(resp, "created_at", created)

	// Echo request fields when available (aligns with streaming path behavior)
	if request := pickResponsesRequestJSON(originalRequestRawJSON, requestRawJSON); len(request) > 0 {
		req := gjson.ParseBytes(request)
		if v := req.Get("instructions"); v.Exists() {
			resp, _ = sjson.SetBytes(resp, "instructions", v.String())
		}
		if v := req.Get("max_output_tokens"); v.Exists() {
			resp, _ = sjson.SetBytes(resp, "max_output_tokens", v.Int())
		} else {
			// Also support max_tokens from chat completion style
			if v = req.Get("max_tokens"); v.Exists() {
				resp, _ = sjson.SetBytes(resp, "max_output_tokens", v.Int())
			}
		}
		if v := req.Get("max_tool_calls"); v.Exists() {
			resp, _ = sjson.SetBytes(resp, "max_tool_calls", v.Int())
		}
		if v := req.Get("model"); v.Exists() {
			resp, _ = sjson.SetBytes(resp, "model", v.String())
		} else if v = root.Get("model"); v.Exists() {
			resp, _ = sjson.SetBytes(resp, "model", v.String())
		}
		if v := req.Get("parallel_tool_calls"); v.Exists() {
			resp, _ = sjson.SetBytes(resp, "parallel_tool_calls", v.Bool())
		}
		if v := req.Get("previous_response_id"); v.Exists() {
			resp, _ = sjson.SetBytes(resp, "previous_response_id", v.String())
		}
		if v := req.Get("prompt_cache_key"); v.Exists() {
			resp, _ = sjson.SetBytes(resp, "prompt_cache_key", v.String())
		}
		if v := req.Get("reasoning"); v.Exists() {
			resp, _ = sjson.SetBytes(resp, "reasoning", v.Value())
		}
		if v := req.Get("safety_identifier"); v.Exists() {
			resp, _ = sjson.SetBytes(resp, "safety_identifier", v.String())
		}
		if v := req.Get("service_tier"); v.Exists() {
			resp, _ = sjson.SetBytes(resp, "service_tier", v.String())
		}
		if v := req.Get("store"); v.Exists() {
			resp, _ = sjson.SetBytes(resp, "store", v.Bool())
		}
		if v := req.Get("temperature"); v.Exists() {
			resp, _ = sjson.SetBytes(resp, "temperature", v.Float())
		}
		if v := req.Get("text"); v.Exists() {
			resp, _ = sjson.SetBytes(resp, "text", v.Value())
		}
		if v := req.Get("tool_choice"); v.Exists() {
			resp, _ = sjson.SetBytes(resp, "tool_choice", v.Value())
		}
		if v := req.Get("tools"); v.Exists() {
			resp, _ = sjson.SetBytes(resp, "tools", v.Value())
		}
		if v := req.Get("top_logprobs"); v.Exists() {
			resp, _ = sjson.SetBytes(resp, "top_logprobs", v.Int())
		}
		if v := req.Get("top_p"); v.Exists() {
			resp, _ = sjson.SetBytes(resp, "top_p", v.Float())
		}
		if v := req.Get("truncation"); v.Exists() {
			resp, _ = sjson.SetBytes(resp, "truncation", v.String())
		}
		if v := req.Get("user"); v.Exists() {
			resp, _ = sjson.SetBytes(resp, "user", v.Value())
		}
		if v := req.Get("metadata"); v.Exists() {
			resp, _ = sjson.SetBytes(resp, "metadata", v.Value())
		}
	} else if v := root.Get("model"); v.Exists() {
		// Fallback model from response
		resp, _ = sjson.SetBytes(resp, "model", v.String())
	}

	// Build output list from choices[...]
	outputsWrapper := []byte(`{"arr":[]}`)
	// Detect and capture reasoning content if present
	rcText := responsesReasoningText(root.Get("choices.0.message"))
	includeReasoning := rcText != ""
	if !includeReasoning && len(requestRawJSON) > 0 {
		includeReasoning = gjson.GetBytes(requestRawJSON, "reasoning").Exists()
	}
	if includeReasoning {
		rid := id
		if strings.HasPrefix(rid, "resp_") {
			rid = strings.TrimPrefix(rid, "resp_")
		}
		// Prefer summary_text from reasoning_content; encrypted_content is optional
		reasoningItem := []byte(`{"id":"","type":"reasoning","encrypted_content":"","summary":[]}`)
		reasoningItem, _ = sjson.SetBytes(reasoningItem, "status", responsesItemStatus(root.Get("choices.0.finish_reason").String()))
		reasoningItem, _ = sjson.SetBytes(reasoningItem, "id", fmt.Sprintf("rs_%s", rid))
		if rcText != "" {
			reasoningItem, _ = sjson.SetBytes(reasoningItem, "summary.0.type", "summary_text")
			reasoningItem, _ = sjson.SetBytes(reasoningItem, "summary.0.text", rcText)
		}
		outputsWrapper, _ = sjson.SetRawBytes(outputsWrapper, "arr.-1", reasoningItem)
	}

	if choices := root.Get("choices"); choices.Exists() && choices.IsArray() {
		choices.ForEach(func(choicePosition, choice gjson.Result) bool {
			msg := choice.Get("message")
			itemStatus := responsesItemStatus(choice.Get("finish_reason").String())
			if msg.Exists() {
				if text := responsesReasoningText(msg); choicePosition.Int() > 0 && text != "" {
					item := []byte(`{"type":"reasoning","encrypted_content":"","summary":[{"type":"summary_text","text":""}]}`)
					item, _ = sjson.SetBytes(item, "id", fmt.Sprintf("rs_%s_%d", strings.TrimPrefix(id, "resp_"), choicePosition.Int()))
					item, _ = sjson.SetBytes(item, "status", itemStatus)
					item, _ = sjson.SetBytes(item, "summary.0.text", text)
					outputsWrapper, _ = sjson.SetRawBytes(outputsWrapper, "arr.-1", item)
				}
				// Text message part
				if c := msg.Get("content"); c.Exists() && c.String() != "" {
					item := []byte(`{"id":"","type":"message","status":"completed","content":[{"type":"output_text","annotations":[],"logprobs":[],"text":""}],"role":"assistant"}`)
					item, _ = sjson.SetBytes(item, "id", fmt.Sprintf("msg_%s_%d", id, int(choice.Get("index").Int())))
					item, _ = sjson.SetBytes(item, "content.0.text", c.String())
					item, _ = sjson.SetBytes(item, "status", itemStatus)
					outputsWrapper, _ = sjson.SetRawBytes(outputsWrapper, "arr.-1", item)
				}

				// Function/tool calls
				if tcs := msg.Get("tool_calls"); tcs.Exists() && tcs.IsArray() {
					tcs.ForEach(func(_, tc gjson.Result) bool {
						callID := tc.Get("id").String()
						name := tc.Get("function.name").String()
						args := tc.Get("function.arguments").String()
						item := buildResponsesToolItem(toolIdentities, name, callID, args, itemStatus)
						outputsWrapper, _ = sjson.SetRawBytes(outputsWrapper, "arr.-1", item)
						return true
					})
				}
			}
			return true
		})
	}
	if gjson.GetBytes(outputsWrapper, "arr.#").Int() > 0 {
		resp, _ = sjson.SetRawBytes(resp, "output", []byte(gjson.GetBytes(outputsWrapper, "arr").Raw))
	}

	// usage mapping
	if usage := root.Get("usage"); usage.Exists() {
		// Map common tokens
		if usage.Get("prompt_tokens").Exists() || usage.Get("completion_tokens").Exists() || usage.Get("total_tokens").Exists() {
			resp, _ = sjson.SetBytes(resp, "usage.input_tokens", usage.Get("prompt_tokens").Int())
			if d := usage.Get("prompt_tokens_details.cached_tokens"); d.Exists() {
				resp, _ = sjson.SetBytes(resp, "usage.input_tokens_details.cached_tokens", d.Int())
			}
			resp, _ = sjson.SetBytes(resp, "usage.output_tokens", usage.Get("completion_tokens").Int())
			// Reasoning tokens not available in Chat Completions; set only if present under output_tokens_details
			if d := usage.Get("output_tokens_details.reasoning_tokens"); d.Exists() {
				resp, _ = sjson.SetBytes(resp, "usage.output_tokens_details.reasoning_tokens", d.Int())
			}
			resp, _ = sjson.SetBytes(resp, "usage.total_tokens", usage.Get("total_tokens").Int())
		} else {
			// Fallback to raw usage object if structure differs
			resp, _ = sjson.SetBytes(resp, "usage", usage.Value())
		}
	}

	return resp
}

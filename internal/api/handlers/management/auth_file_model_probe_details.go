package management

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/tidwall/gjson"
)

const modelProbeDetailLimit = 128 * 1024

type modelProbeUsage struct {
	InputTokens         *int64 `json:"input_tokens,omitempty"`
	OutputTokens        *int64 `json:"output_tokens,omitempty"`
	TotalTokens         *int64 `json:"total_tokens,omitempty"`
	CachedTokens        *int64 `json:"cached_tokens,omitempty"`
	ReasoningTokens     *int64 `json:"reasoning_tokens,omitempty"`
	CacheCreationTokens *int64 `json:"cache_creation_tokens,omitempty"`
}

type modelProbeObserver struct {
	bytes     int
	finished  bool
	hasOutput bool
	text      string
	model     string
	id        string
	reason    string
	usage     *modelProbeUsage
	raw       strings.Builder
	truncated bool
}

func (o *modelProbeObserver) observe(payload []byte, stream bool) error {
	o.bytes += len(payload)
	if o.bytes > 2*1024*1024 {
		return fmt.Errorf("probe response exceeded the 2 MiB limit")
	}
	if left := modelProbeDetailLimit - o.raw.Len(); left > 0 {
		o.raw.Write(payload[:min(len(payload), left)])
		if len(payload) > left {
			o.truncated = true
		}
		if stream && o.raw.Len() < modelProbeDetailLimit {
			o.raw.WriteByte('\n')
		}
	} else {
		o.truncated = true
	}
	if !stream {
		return o.observeJSON(payload, false)
	}
	if gjson.ValidBytes(payload) {
		return o.observeJSON(payload, true)
	}
	if strings.TrimSpace(string(payload)) == "[DONE]" {
		o.finished = true
		return nil
	}
	for _, event := range helps.ParseOpenAIStreamFrame(payload) {
		if event.Err != nil {
			return event.Err
		}
		if string(event.Data) == "[DONE]" {
			o.finished = true
		} else if err := o.observeJSON(event.Data, true); err != nil {
			return err
		}
	}
	return nil
}

func probeContentText(content gjson.Result) string {
	if content.Type == gjson.String {
		return content.Str
	}
	var result strings.Builder
	for _, part := range content.Array() {
		switch part.Get("type").String() {
		case "text", "output_text":
			result.WriteString(part.Get("text").String())
		case "refusal":
			result.WriteString(part.Get("refusal").String())
		}
	}
	return result.String()
}

func (o *modelProbeObserver) observeJSON(payload []byte, stream bool) error {
	if !gjson.ValidBytes(payload) {
		return fmt.Errorf("upstream response is not valid JSON")
	}
	event := gjson.ParseBytes(payload)
	root := event
	if root.Get("response").IsObject() {
		root = root.Get("response")
	}
	if model := root.Get("model").String(); model != "" {
		o.model = model
	}
	if id := root.Get("id").String(); id != "" {
		o.id = id
	}
	o.observeUsage(root.Get("usage"))
	kind, status := event.Get("type").String(), root.Get("status").String()
	if status != "" {
		o.reason = status
	}
	text := probeContentText(root.Get("choices.0.message.content"))
	if text == "" {
		text = root.Get("choices.0.message.refusal").String()
	}
	if stream {
		text = probeContentText(root.Get("choices.0.delta.content"))
		if kind == "response.output_text.delta" {
			text = event.Get("delta").String()
		}
	}
	if text != "" {
		o.text += text
		o.hasOutput = true
	}
	var completed strings.Builder
	for _, item := range root.Get("output").Array() {
		completed.WriteString(probeContentText(item.Get("content")))
		if item.Get("type").String() != "reasoning" {
			o.hasOutput = true
		}
	}
	// Responses completion snapshots include the deltas already received.
	if completed.Len() > 0 {
		o.text = completed.String()
		o.hasOutput = true
	}
	if len(o.text) > modelProbeDetailLimit {
		o.text = o.text[:modelProbeDetailLimit]
		o.truncated = true
	}
	if root.Get("choices.0.message.tool_calls").IsArray() || root.Get("choices.0.delta.tool_calls").IsArray() {
		o.hasOutput = true
	}
	if reason := root.Get("choices.0.finish_reason"); reason.Type == gjson.String && reason.Str != "" {
		o.reason = reason.Str
		if reason.Str != "stop" && reason.Str != "tool_calls" && reason.Str != "function_call" {
			return fmt.Errorf("upstream stopped with finish_reason=%s", reason.Str)
		}
		o.finished = true
	}
	if errorValue := event.Get("error"); errorValue.Exists() && errorValue.Type != gjson.Null {
		return fmt.Errorf("upstream error: %s", errorValue.Raw)
	}
	if status == "failed" || status == "incomplete" || kind == "error" || kind == "response.failed" || kind == "response.incomplete" {
		if reason := root.Get("incomplete_details.reason").String(); reason != "" {
			o.reason = reason
		}
		return fmt.Errorf("upstream did not complete: %s", string(payload))
	}
	if status == "completed" || kind == "response.completed" || kind == "response.done" || !stream && root.Get("choices.0.message").Exists() {
		o.finished = true
	}
	return nil
}

func (o *modelProbeObserver) observeUsage(usage gjson.Result) {
	if !usage.IsObject() {
		return
	}
	if o.usage == nil {
		o.usage = &modelProbeUsage{}
	}
	for _, entry := range []struct {
		target **int64
		paths  []string
	}{
		{&o.usage.InputTokens, []string{"input_tokens", "prompt_tokens"}},
		{&o.usage.OutputTokens, []string{"output_tokens", "completion_tokens"}},
		{&o.usage.TotalTokens, []string{"total_tokens"}},
		{&o.usage.CachedTokens, []string{"input_tokens_details.cached_tokens", "prompt_tokens_details.cached_tokens", "cache_read_input_tokens"}},
		{&o.usage.ReasoningTokens, []string{"output_tokens_details.reasoning_tokens", "completion_tokens_details.reasoning_tokens", "reasoning_tokens"}},
		{&o.usage.CacheCreationTokens, []string{"cache_creation_input_tokens", "cache_creation_tokens", "prompt_tokens_details.cache_creation_tokens"}},
	} {
		for _, path := range entry.paths {
			value := usage.Get(path)
			if value.Type != gjson.Number && value.Type != gjson.String {
				continue
			}
			count, err := strconv.ParseInt(value.String(), 10, 64)
			if err != nil || count < 0 {
				continue
			}
			*entry.target = &count
			break
		}
	}
}

func (o *modelProbeObserver) fill(result *modelProbeResult) {
	result.Response, result.ResponseBody = o.text, o.raw.String()
	result.ReturnedModel, result.ResponseID, result.FinishReason = o.model, o.id, o.reason
	result.Usage = o.usage
	if o.usage != nil && o.usage.TotalTokens == nil && o.usage.InputTokens != nil && o.usage.OutputTokens != nil {
		total := *o.usage.InputTokens + *o.usage.OutputTokens
		if total >= 0 {
			o.usage.TotalTokens = &total
		}
	}
	result.DetailsTruncated = o.truncated
}

// A shared executor can report from its streaming goroutine. Retain bounded
// diagnostic fields, including Codex State when requested, never authorization headers.
type modelProbeTrace struct {
	mu         sync.Mutex
	url        string
	body       string
	model      string
	requestID  string
	status     int
	truncated  bool
	codexState *modelProbeStateResult
	stateSent  string
}

func (t *modelProbeTrace) request(url string, body []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.url = url
	t.body = string(body[:min(len(body), modelProbeDetailLimit)])
	t.truncated = len(body) > modelProbeDetailLimit
	if model := gjson.GetBytes(body, "model"); model.Type == gjson.String {
		t.model = model.Str
	}
}
func (t *modelProbeTrace) response(status int, headers http.Header) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status = status
	t.requestID = probeRequestID(headers)
	if t.codexState != nil {
		value := headers.Get("X-Codex-Turn-State")
		if validModelProbeState(value) {
			t.codexState.State = value
			t.codexState.ReturnedLength = len(value)
			t.codexState.ReturnedDigest = modelProbeStateDigest(value)
		}
	}
}
func (t *modelProbeTrace) fill(result *modelProbeResult) {
	t.mu.Lock()
	defer t.mu.Unlock()
	result.UpstreamURL, result.UpstreamRequestBody = t.url, t.body
	if t.model != "" {
		result.UpstreamModel = t.model
	}
	if t.status != 0 {
		result.StatusCode = t.status
	}
	if result.RequestID == "" {
		result.RequestID = t.requestID
	}
	result.DetailsTruncated = result.DetailsTruncated || t.truncated
	if t.codexState != nil {
		state := *t.codexState
		result.CodexState = &state
	}
}

package helps

import (
	translatorcommon "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/common"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	"github.com/tidwall/gjson"
)

func claudeUsageDetail(value translatorcommon.ClaudeUsage) usage.Detail {
	input, output, total := value.Totals()
	return usage.Detail{
		InputTokens: input, OutputTokens: output, TotalTokens: total,
		CachedTokens: value.CacheReadTokens, CacheCreationTokens: value.CacheCreationTokens,
		ReasoningTokens: value.ReasoningTokens,
	}
}

func claudeStreamUsageNode(line []byte) (gjson.Result, string) {
	payload := jsonPayload(line)
	if !gjson.ValidBytes(payload) {
		return gjson.Result{}, ""
	}
	root := gjson.ParseBytes(payload)
	kind := root.Get("type").String()
	if kind == "message_start" {
		return root.Get("message.usage"), kind
	}
	return root.Get("usage"), kind
}

// ObserveClaudeStreamUsage merges raw partial counters before deriving the
// inclusive v6 snapshot, including explicit zero corrections from the server.
func (r *UsageReporter) ObserveClaudeStreamUsage(line []byte) {
	if r == nil {
		return
	}
	node, kind := claudeStreamUsageNode(line)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.published || r.claudeUsageDone {
		return
	}
	if kind == "message_stop" {
		r.claudeUsageDone = true
		return
	}
	if kind == "message_start" {
		r.claudeUsage = translatorcommon.ClaudeUsage{}
		r.observedDetail, r.observed = usage.Detail{}, false
	}
	if r.claudeUsage.Merge(node) {
		r.observedDetail = claudeUsageDetail(r.claudeUsage)
		r.observed = true
	}
}

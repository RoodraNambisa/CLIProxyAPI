package helps

import (
	"context"
	"sort"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// CodexReasoningReplayCollector owns bounded output-item copies for one request.
// It reconstructs only the internal replay record, never the client response.
type CodexReasoningReplayCollector struct {
	scope    CodexReasoningReplayScope
	indexed  map[int64]string
	fallback []string
	bytes    int
	disabled bool
}

func NewCodexReasoningReplayCollector(scope CodexReasoningReplayScope) *CodexReasoningReplayCollector {
	if !scope.valid() || !scope.hasRequestPrefix {
		return nil
	}
	return &CodexReasoningReplayCollector{scope: scope, indexed: make(map[int64]string)}
}

func (c *CodexReasoningReplayCollector) Observe(data []byte) {
	if c == nil || c.disabled || gjson.GetBytes(data, "type").String() != "response.output_item.done" || !gjson.ValidBytes(data) {
		return
	}
	root := gjson.ParseBytes(data)
	item := root.Get("item")
	switch item.Get("type").String() {
	case "reasoning", "function_call", "custom_tool_call", "message":
	default:
		return
	}
	index := root.Get("output_index")
	var position int64
	if index.Exists() {
		var errIndex error
		position, errIndex = strconv.ParseInt(index.Raw, 10, 64)
		if index.Type != gjson.Number || errIndex != nil || position < 0 {
			return
		}
		if prior, exists := c.indexed[position]; exists {
			if prior != item.Raw {
				c.disable()
			}
			return
		}
	}
	// Include a fixed allowance per entry so tiny items cannot evade the budget.
	size := len(item.Raw) + 64
	if size > CodexReasoningReplayCacheMaxEntryBytes-c.bytes {
		c.disable()
		return
	}
	c.bytes += size
	owned := strings.Clone(item.Raw)
	if index.Exists() {
		c.indexed[position] = owned
	} else {
		c.fallback = append(c.fallback, owned)
	}
}

func (c *CodexReasoningReplayCollector) disable() {
	c.indexed, c.fallback, c.bytes, c.disabled = nil, nil, 0, true
}

func (c *CodexReasoningReplayCollector) Commit(ctx context.Context, data []byte) bool {
	if c == nil || c.disabled {
		return false
	}
	defer c.disable()
	if !gjson.ValidBytes(data) {
		return false
	}
	output := gjson.GetBytes(data, "response.output")
	if !output.Exists() || (output.IsArray() && len(output.Array()) == 0) {
		indexes := make([]int64, 0, len(c.indexed))
		for index := range c.indexed {
			indexes = append(indexes, index)
		}
		sort.Slice(indexes, func(i, j int) bool { return indexes[i] < indexes[j] })
		items := make([]string, 0, len(indexes)+len(c.fallback))
		for _, index := range indexes {
			items = append(items, c.indexed[index])
		}
		items = append(items, c.fallback...)
		var errSet error
		data, errSet = sjson.SetRawBytes(data, "response.output", []byte("["+strings.Join(items, ",")+"]"))
		if errSet != nil {
			return false
		}
	}
	return CacheCodexReasoningReplayFromCompleted(c.scope, data, ctx)
}

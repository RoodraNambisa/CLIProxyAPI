package helps

import (
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// CollectCodexOutputItemDone retains owned item bytes for one logical response.
func CollectCodexOutputItemDone(eventData []byte, indexed map[int64][]byte, fallback *[][]byte) {
	item := gjson.GetBytes(eventData, "item")
	if !item.IsObject() {
		return
	}
	if index := gjson.GetBytes(eventData, "output_index"); index.Exists() {
		value, errIndex := strconv.ParseInt(index.Raw, 10, 64)
		if index.Type != gjson.Number || errIndex != nil || value < 0 {
			return
		}
		indexed[value] = []byte(item.Raw)
		return
	}
	*fallback = append(*fallback, []byte(item.Raw))
}

// HydrateCodexOutputItemIDs fills missing IDs only when the indexed streamed item
// agrees with the terminal item's role and pairing identity.
func HydrateCodexOutputItemIDs(eventData []byte, indexed map[int64][]byte) []byte {
	output := gjson.GetBytes(eventData, "response.output")
	if !output.IsArray() || len(indexed) == 0 {
		return eventData
	}
	items := output.Array()
	used := make(map[string]bool, len(items))
	for _, item := range items {
		if id := item.Get("id"); id.Type == gjson.String && strings.TrimSpace(id.String()) != "" {
			used[id.String()] = true
		}
	}
	for index, item := range items {
		if !item.IsObject() {
			continue
		}
		id := item.Get("id")
		if id.Exists() && id.Type != gjson.Null && (id.Type != gjson.String || strings.TrimSpace(id.String()) != "") {
			continue
		}
		completed := gjson.ParseBytes(indexed[int64(index)])
		completedID := completed.Get("id")
		if !completed.IsObject() || completedID.Type != gjson.String || strings.TrimSpace(completedID.String()) == "" || used[completedID.String()] {
			continue
		}
		conflict := false
		for _, field := range []string{"type", "call_id"} {
			if left, right := item.Get(field), completed.Get(field); left.Exists() && right.Exists() && left.String() != right.String() {
				conflict = true
			}
		}
		if conflict {
			continue
		}
		updated, errSet := sjson.SetRawBytes(eventData, "response.output."+strconv.Itoa(index)+".id", []byte(completedID.Raw))
		if errSet == nil {
			eventData = updated
			used[completedID.String()] = true
		}
	}
	return eventData
}

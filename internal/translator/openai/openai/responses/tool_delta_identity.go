package responses

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
)

// toolDeltaIndex uses explicit indices or a unique call identity. Array positions
// are not stable across incremental frames containing different tool subsets.
func (st *oaiToResponsesState) toolDeltaIndex(choice int, delta gjson.Result, position, frameSize int) (int, bool) {
	if index := delta.Get("index"); index.Exists() {
		return responsesStreamIndex(index, 0)
	}
	callID := delta.Get("id").String()
	matchedIndex, matches := 0, 0
	soleIndex, count, unidentified := 0, 0, 0
	for key, owner := range st.FuncChoices {
		if owner != choice {
			continue
		}
		_, rawIndex, _ := strings.Cut(key, ":")
		index, errIndex := strconv.Atoi(rawIndex)
		if errIndex != nil {
			continue
		}
		count++
		soleIndex = index
		if st.FuncCallIDs[key] == "" {
			unidentified++
		}
		if callID != "" && st.FuncCallIDs[key] == callID {
			matchedIndex, matches = index, matches+1
		}
	}
	if matches > 0 {
		return matchedIndex, matches == 1
	}
	if callID == "" {
		if frameSize != 1 || count > 1 {
			return 0, false
		}
		if count == 1 {
			return soleIndex, true
		}
		return position, true
	}
	if unidentified > 0 {
		return soleIndex, count == 1 && frameSize == 1
	}
	for st.FuncArgsBuf[fmt.Sprintf("%d:%d", choice, position)] != nil {
		if position == int(^uint(0)>>1) {
			return 0, false
		}
		position++
	}
	return position, true
}

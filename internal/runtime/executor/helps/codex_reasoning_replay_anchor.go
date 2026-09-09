package helps

import (
	"crypto/sha256"
	"io"
	"strings"

	"github.com/tidwall/gjson"
)

// Prefix hashes are built once per input, without retaining its text.
// An anchor includes every preceding item, so repeated assistant text or call IDs
// cannot attach reasoning to a different point in the conversation.
func codexReplayPrefixIndexes(items []gjson.Result) (map[[sha256.Size]byte]int, [sha256.Size]byte) {
	indexes := make(map[[sha256.Size]byte]int, len(items)+1)
	hasher := sha256.New()
	var digest [sha256.Size]byte
	hasher.Sum(digest[:0])
	indexes[digest] = 0
	for index, item := range items {
		_, _ = io.WriteString(hasher, "\x00item\x00")
		_, _ = io.WriteString(hasher, item.Raw)
		hasher.Sum(digest[:0])
		indexes[digest] = index + 1
	}
	return indexes, digest
}

func codexReplayAssistantFingerprint(item gjson.Result) ([sha256.Size]byte, bool) {
	role, ok := codexReplayMessageRole(item)
	if !ok || role != "assistant" {
		return [sha256.Size]byte{}, false
	}
	content := item.Get("content")
	hasher := sha256.New()
	size := 0
	write := func(text string) {
		n, _ := io.WriteString(hasher, text)
		size += n
	}
	if content.Type == gjson.String {
		write(content.String())
	} else if content.IsArray() {
		for _, part := range content.Array() {
			switch strings.TrimSpace(part.Get("type").String()) {
			case "input_text", "output_text":
				if part.Get("text").Type != gjson.String {
					return [sha256.Size]byte{}, false
				}
				write(part.Get("text").String())
			default:
				return [sha256.Size]byte{}, false
			}
		}
	}
	if size == 0 {
		return [sha256.Size]byte{}, false
	}
	var digest [sha256.Size]byte
	hasher.Sum(digest[:0])
	return digest, true
}

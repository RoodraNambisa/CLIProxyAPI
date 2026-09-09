package common

import (
	"strings"

	"github.com/tidwall/gjson"
)

// RequestModelName prefers the caller's model, falling back to the translated
// request only when the original has no valid model field.
func RequestModelName(original, translated []byte) string {
	for _, body := range [][]byte{original, translated} {
		if len(body) == 0 || !gjson.ValidBytes(body) {
			continue
		}
		root := gjson.ParseBytes(body)
		for _, path := range []string{"model", "request.model"} {
			if model := root.Get(path); model.Type == gjson.String && strings.TrimSpace(model.String()) != "" {
				return model.String()
			}
		}
	}
	return ""
}

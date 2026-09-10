package helps

import (
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// SetStringIfDifferent preserves the payload when the field already has the
// requested string value. Otherwise it retains the setter's result and error.
func SetStringIfDifferent(payload []byte, path, value string) ([]byte, error) {
	current := gjson.GetBytes(payload, path)
	if current.Type == gjson.String && current.String() == value {
		return payload, nil
	}
	return sjson.SetBytes(payload, path, value)
}

// SetBoolIfDifferent keeps missing and incorrectly typed fields distinct from
// an explicit boolean, and never mutates the caller's input buffer.
func SetBoolIfDifferent(payload []byte, path string, value bool) ([]byte, error) {
	current := gjson.GetBytes(payload, path)
	if (value && current.Type == gjson.True) || (!value && current.Type == gjson.False) {
		return payload, nil
	}
	return sjson.SetBytes(payload, path, value)
}

package common

import (
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// JoinRawArray joins valid encoded items into an independently owned JSON array.
func JoinRawArray(items [][]byte) []byte {
	size := len(items) + 1
	if len(items) == 0 {
		return []byte("[]")
	}
	for _, item := range items {
		size += len(item)
	}
	out := make([]byte, 0, size)
	out = append(out, '[')
	for index, item := range items {
		if index > 0 {
			out = append(out, ',')
		}
		out = append(out, item...)
	}
	return append(out, ']')
}

// SetRawArrayItems fills an existing empty array. A single item avoids an
// intermediate joined array while preserving the input buffer's ownership.
func SetRawArrayItems(data []byte, path string, items [][]byte) []byte {
	if len(items) == 0 {
		return data
	}
	if len(items) == 1 {
		array := gjson.GetBytes(data, path)
		if array.Raw == "[]" && array.Index >= 0 && array.Index+len(array.Raw) <= len(data) {
			out := make([]byte, 0, len(data)+len(items[0]))
			out = append(out, data[:array.Index]...)
			out = append(out, '[')
			out = append(out, items[0]...)
			out = append(out, ']')
			return append(out, data[array.Index+len(array.Raw):]...)
		}
	}
	data, _ = sjson.SetRawBytes(data, path, JoinRawArray(items))
	return data
}

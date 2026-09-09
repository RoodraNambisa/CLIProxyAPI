package common

import (
	"bytes"
	"testing"

	"github.com/tidwall/gjson"
)

func TestRawArrayItemsPreserveOrderAndOwnership(t *testing.T) {
	for _, count := range []int{0, 1, 2} {
		items := [][]byte{[]byte(`{"text":"quote\" and \\ and 中文"}`), []byte(`{"value":false}`)}[:count]
		body := []byte(`{"before":true,"input":[],"after":false}`)
		original := bytes.Clone(body)
		out := SetRawArrayItems(body, "input", items)
		if !gjson.ValidBytes(out) || !bytes.Equal(body, original) || !gjson.GetBytes(out, "before").Bool() || gjson.GetBytes(out, "after").Bool() {
			t.Fatal("array construction changed its input or surrounding fields")
		}
		array := gjson.GetBytes(out, "input").Array()
		if len(array) != count {
			t.Fatalf("items=%d, want %d", len(array), count)
		}
		for index := range array {
			if array[index].Raw != string(items[index]) {
				t.Fatal("raw item order or encoding changed")
			}
		}
		if count > 0 {
			clear(items[0])
			clear(body)
			if !gjson.ValidBytes(out) || gjson.GetBytes(out, "input.0.text").String() != "quote\" and \\ and 中文" {
				t.Fatal("constructed output retained an input buffer alias")
			}
		}
	}
}

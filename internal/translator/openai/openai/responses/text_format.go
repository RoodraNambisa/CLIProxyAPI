package responses

import (
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func responsesTextFormatForChat(format gjson.Result) []byte {
	switch format.Get("type").String() {
	case "text":
		return []byte(`{"type":"text"}`)
	case "json_object":
		return []byte(`{"type":"json_object"}`)
	case "json_schema":
		result := []byte(`{"type":"json_schema","json_schema":{}}`)
		for _, field := range []string{"name", "description", "strict", "schema"} {
			if value := format.Get(field); value.Exists() {
				result, _ = sjson.SetRawBytes(result, "json_schema."+field, []byte(value.Raw))
			}
		}
		return result
	default:
		return nil
	}
}

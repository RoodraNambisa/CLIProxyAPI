package responses

import "github.com/tidwall/sjson"

func buildResponsesToolItem(identities map[string]responsesToolIdentity, name, callID, arguments, status string) []byte {
	identity, known := identities[name]
	kind, prefix, field := "function_call", "fc_", "arguments"
	if known && identity.custom {
		kind, prefix, field = "custom_tool_call", "ctc_", "input"
		arguments = unwrapCustomToolInput(arguments)
	}
	if status == "completed" && !identity.custom && arguments == "" {
		arguments = "{}"
	}
	item := []byte(`{"id":"","type":"","status":"","call_id":"","name":""}`)
	item, _ = sjson.SetBytes(item, "id", prefix+callID)
	item, _ = sjson.SetBytes(item, "type", kind)
	item, _ = sjson.SetBytes(item, "status", status)
	item, _ = sjson.SetBytes(item, "call_id", callID)
	item, _ = sjson.SetBytes(item, field, arguments)
	if known {
		name = identity.name
		if identity.namespace != "" {
			item, _ = sjson.SetBytes(item, "namespace", identity.namespace)
		}
	}
	item, _ = sjson.SetBytes(item, "name", name)
	return item
}

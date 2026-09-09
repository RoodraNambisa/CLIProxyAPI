package helps

import "github.com/tidwall/gjson"

// CodexTerminalErrorNode accepts the response envelope and its top-level alias.
func CodexTerminalErrorNode(payload []byte) gjson.Result {
	for _, path := range []string{"response.error", "error"} {
		if node := gjson.GetBytes(payload, path); node.Exists() && node.Type != gjson.Null {
			return node
		}
	}
	return gjson.Result{}
}

// CodexTerminalHTTPStatus reads explicit error statuses without inferring from
// message text. Outer transport status takes precedence over nested details.
func CodexTerminalHTTPStatus(payload []byte) int {
	if !gjson.ValidBytes(payload) {
		return 0
	}
	root := gjson.ParseBytes(payload)
	for _, path := range []string{"status", "status_code", "response.status_code", "error.status_code", "error.status", "response.error.status_code", "response.error.status", "error.code", "response.error.code", "code"} {
		value := root.Get(path)
		status := value.Int()
		if value.Type == gjson.Number && status >= 400 && status <= 599 && value.Float() == float64(status) {
			return int(status)
		}
	}
	return 0
}

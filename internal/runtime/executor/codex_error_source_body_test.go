package executor

import (
	"bytes"
	"errors"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexErrorSourceBodySurvivesNormalizationAndCopies(t *testing.T) {
	for _, mode := range []string{"terminal", "http", "websocket"} {
		t.Run(mode, func(t *testing.T) {
			body := []byte(`{"type":"response.failed","trace":"rule-fixture","response":{"status":"failed","error":{"code":"server_error","message":"temporary","details":{"number":9007199254740993}}}}`)
			var err error
			switch mode {
			case "terminal":
				err, _ = codexTerminalStreamError(body)
			case "http":
				body = []byte(`{"error":{"code":"invalid_encrypted_content","message":"invalid signature in thinking block","details":"rule-fixture"}}`)
				err = newCodexStatusErr(400, body)
			case "websocket":
				body = []byte(`{"type":"error","status":500,"trace":"rule-fixture","error":{"code":"server_error","message":"temporary"}}`)
				err, _ = parseCodexWebsocketError(body)
			}
			original := bytes.Clone(body)
			clear(body)
			var source interface{ ResponseBody() []byte }
			if !errors.As(err, &source) || !bytes.Equal(source.ResponseBody(), original) {
				t.Fatal("original rule matching body was lost or aliased the input buffer")
			}
			copy := source.ResponseBody()
			clear(copy)
			if !bytes.Equal(source.ResponseBody(), original) {
				t.Fatal("caller mutation changed the stored matching body")
			}
			if mode == "http" && gjson.Get(err.Error(), "error.code").String() != "thinking_signature_invalid" {
				t.Fatal("public compatibility classification changed")
			}
		})
	}
}

func TestCodexSanitizedWebsocketErrorReplacesMatchingBody(t *testing.T) {
	original, _ := parseCodexWebsocketError([]byte(`{"type":"error","status":500,"trace":"old fixture","error":{"message":"old"}}`))
	clean := []byte(`{"error":{"message":"sanitized"}}`)
	updated := codexWebsocketErrorWithSanitizedMessage(original, clean)
	var source interface{ ResponseBody() []byte }
	if !errors.As(updated, &source) || !bytes.Equal(source.ResponseBody(), clean) {
		t.Fatal("sanitization retained the previous matching body")
	}
}

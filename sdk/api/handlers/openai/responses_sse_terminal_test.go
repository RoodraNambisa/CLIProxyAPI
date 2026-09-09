package openai

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestResponsesSSETerminalTrackingAndTrailingFrames(t *testing.T) {
	for _, event := range []string{"response.completed", "response.done", "response.incomplete", "response.failed", "response.error", "error"} {
		for _, header := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/header=%t", event, header), func(t *testing.T) {
				payload := fmt.Sprintf(`{"type":%q,"status":429}`, event)
				frame := "data: " + payload + "\n\n"
				if header {
					frame = "event: " + event + "\ndata: {\"status\":429}\n\n"
				}
				framer := &responsesSSEFramer{maxPendingBytes: 128}
				var out bytes.Buffer
				framer.WriteChunk(&out, []byte(frame+"data: "+strings.Repeat("x", 1024)))
				first := out.String()
				framer.WriteChunk(&out, []byte("data: {\"type\":\"late\"}\n\n"))
				framer.Flush(&out)
				if !framer.terminalSeen || framer.Err() != nil || len(framer.pending) != 0 || out.String() != first || strings.Contains(first, "xxxxx") {
					t.Fatal("first terminal was lost or replaced by trailing data/size failure")
				}
				wantError := event == "error" || event == "response.error" || event == "response.failed"
				got := framer.CloseError()
				if wantError {
					if got == nil || got.StatusCode != 429 || !framer.terminalBeforeData || got != framer.terminalError {
						t.Fatal("terminal failure lost its status or original identity")
					}
				} else if got != nil {
					t.Fatalf("valid terminal failed: %v", got.Error)
				}
			})
		}
	}
}

func TestResponsesSSETerminalPayloadErrors(t *testing.T) {
	for _, payload := range []string{
		`{"error":{"status_code":429,"code":"rate_limit","message":"limited"}}`,
		`{"type":"response.completed","response":{"status_code":429,"error":{"message":"limited"}}}`,
		`{"type":"response.output_text.delta","code":"limited","message":"limited","status_code":429}`,
	} {
		framer := &responsesSSEFramer{}
		var out bytes.Buffer
		framer.WriteChunk(&out, []byte("data: {\"type\":\"response.created\"}\n\n"))
		framer.WriteChunk(&out, []byte("data: "+payload+"\n\n"))
		got := framer.CloseError()
		if got == nil || got.StatusCode != 429 || !strings.Contains(got.Error.Error(), "limited") || framer.terminalBeforeData {
			t.Fatal("embedded failure was treated as a success or an early error")
		}
	}
}

func TestResponsesSSECloseValidationPreservesTrustAndImageModes(t *testing.T) {
	for _, trust := range []bool{false, true} {
		for _, image := range []bool{false, true} {
			for _, terminal := range []string{"", "[DONE]", `{"type":"image_generation.completed"}`, `{"type":"response.incomplete","response":{"status":"incomplete"}}`} {
				t.Run(fmt.Sprintf("trust=%t/image=%t/%s", trust, image, terminal), func(t *testing.T) {
					state := &coreexecutor.ImageGenerationStreamPassthroughState{}
					state.SetEnabled(image)
					framer := &responsesSSEFramer{passthrough: trust, passthroughState: state}
					var out bytes.Buffer
					if terminal != "" {
						framer.WriteChunk(&out, []byte("data: "+terminal+"\n\n"))
					}
					framer.Flush(&out)
					got := framer.CloseError()
					valid := trust || strings.Contains(terminal, "response.incomplete") || (image && terminal != "")
					if valid && got != nil {
						t.Fatal("valid legacy mode or partial terminal was rejected")
					}
					if !valid && (got == nil || !errors.Is(got.Error, errResponsesSSEMissingTerminal)) {
						t.Fatal("missing Responses terminal completed silently")
					}
				})
			}
		}
	}
}

func TestResponsesSSETerminalProjectionKeepsOriginalMetadata(t *testing.T) {
	projected := &interfaces.ErrorMessage{StatusCode: 400, Error: errors.New("public error")}
	framer := &responsesSSEFramer{rewriteTerminalError: func([]byte) ([]byte, *interfaces.ErrorMessage, bool) {
		return []byte("event: error\ndata: public error\n\n"), projected, true
	}}
	var out bytes.Buffer
	framer.WriteChunk(&out, []byte("data: {\"type\":\"error\",\"status\":429}\n\n"))
	framer.Flush(&out)
	if framer.CloseError() != projected || framer.Err() != nil || !strings.Contains(out.String(), "public error") {
		t.Fatal("terminal projection was revalidated or lost its metadata")
	}
}

func TestResponsesSSEPayloadFailureWinsOverSuccessEventHeader(t *testing.T) {
	framer := &responsesSSEFramer{}
	var out bytes.Buffer
	framer.WriteChunk(&out, []byte("event: response.completed\ndata: {\"type\":\"error\",\"status\":429,\"message\":\"limited\"}\n\n"))
	if got := framer.CloseError(); got == nil || got.StatusCode != 429 {
		t.Fatal("success event header hid an explicit payload error")
	}
}

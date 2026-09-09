package helps

import (
	"bufio"
	"bytes"
	"errors"
	"strings"
	"testing"
	"testing/iotest"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

func TestOpenAIStreamFramesPreserveMultilineAndLegacyRecords(t *testing.T) {
	for _, ending := range []string{"\n", "\r\n", "\r"} {
		wire := strings.Join([]string{"event: message", `data: {"choices":[`, `data: {"delta":{"content":"answer"}}]}`, "", "data: [DONE]", ""}, ending)
		scanner := bufio.NewScanner(iotest.OneByteReader(strings.NewReader(wire)))
		scanner.Buffer(make([]byte, 2), 1024)
		scanner.Split(ScanSSEFrames)
		var events []OpenAIStreamEvent
		for scanner.Scan() {
			frame := scanner.Bytes()
			events = append(events, ParseOpenAIStreamFrame(frame)...)
			clear(frame)
		}
		if scanner.Err() != nil || len(events) != 2 || events[0].Err != nil || bytes.ContainsAny(events[0].Data, "\r\n") || gjson.GetBytes(events[0].Data, "choices.0.delta.content").String() != "answer" || !IsOpenAIStreamTerminal(events[1].Data) {
			t.Fatal("multiline frame, scanner ownership or line endings were lost")
		}
	}
	legacy := ParseOpenAIStreamFrame([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\ndata: {\"choices\":[{\"delta\":{\"content\":\"second\"}}]}\ndata: [DONE]\n"))
	if len(legacy) != 3 || string(legacy[2].Data) != "[DONE]" {
		t.Fatal("legacy adjacent records were merged into invalid JSON")
	}
	scanner := bufio.NewScanner(strings.NewReader("data: " + strings.Repeat("x", 128) + "\n\n"))
	scanner.Buffer(make([]byte, 4), 32)
	scanner.Split(ScanSSEFrames)
	if scanner.Scan() || scanner.Err() == nil {
		t.Fatal("scanner did not bound the complete frame")
	}
}

func TestOpenAIStreamFramesKeepErrorSemanticsAndOrder(t *testing.T) {
	for _, wire := range []string{
		"event: error\ndata: {\"code\":\"misalignment_policy_violation\",\"message\":\"denied\"}\n\n",
		"data: {\"type\":\"misalignment_policy_violation\",\"message\":\"denied\"}\nevent: response.error\n\n",
		"data: {\"type\":\"response.error\",\"code\":\"misalignment_policy_violation\",\"message\":\"denied\"}\n\n",
		"data: {\"code\":\"misalignment_policy_violation\",\"message\":\"denied\"}\n\n",
		`{"code":"misalignment_policy_violation","message":"denied"}`,
		"data: {\"error\":\ndata: {\"code\":\"misalignment_policy_violation\",\"message\":\"denied\"}}\n\n",
	} {
		source := []byte(wire)
		events := ParseOpenAIStreamFrame(source)
		clear(source)
		if len(events) != 1 || !coreauth.IsPolicyRefusalError(events[0].Err) || len(events[0].Data) != 0 {
			t.Fatal("event-level or multiline policy refusal lost its classification")
		}
		var body interface{ ResponseBody() []byte }
		if !errors.As(events[0].Err, &body) || !bytes.Contains(body.ResponseBody(), []byte("misalignment_policy_violation")) {
			t.Fatal("error body was lost or retained scanner storage")
		}
	}
	for _, wire := range []string{"event: error\n\n", "event: response.failed\ndata: [DONE]\n\n", "data: {\"unfinished\":\ndata: [DONE]\n\n", "data: {\"unfinished\":\n\n", `{"choices":[]}`} {
		events := ParseOpenAIStreamFrame([]byte(wire))
		if len(events) != 1 || events[0].Err == nil || len(events[0].Data) != 0 {
			t.Fatal("malformed or non-SSE payload was treated as success")
		}
	}
	if events := ParseOpenAIStreamFrame([]byte(": ping\nevent: message\nid: 1\n\n")); len(events) != 0 {
		t.Fatal("control frame was exposed as response data")
	}
	if events := ParseOpenAIStreamFrame([]byte("data:\n\n")); len(events) != 0 {
		t.Fatal("empty data heartbeat was treated as an error")
	}
	wire := []byte("data: {\"choices\":[]}\ndata: {\"error\":{\"message\":\"failed\"}}\ndata: [DONE]\n")
	events := ParseOpenAIStreamFrame(wire)
	if len(events) != 2 || events[0].Err != nil || events[1].Err == nil {
		t.Fatal("legacy record processing did not stop at its first failure")
	}
	events = ParseOpenAIStreamFrame([]byte("data: [DONE]\ndata: {\"error\":{\"message\":\"late\"}}\n"))
	if len(events) != 1 || events[0].Err != nil || !IsOpenAIStreamTerminal(events[0].Data) {
		t.Fatal("late error replaced the first terminal")
	}
}

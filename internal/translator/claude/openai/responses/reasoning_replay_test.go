package responses

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/encoding/protowire"
)

func claudeReplaySignature(cais bool) string {
	channel := protowire.AppendVarint(protowire.AppendTag(nil, 1, protowire.VarintType), 16)
	if cais {
		channel = protowire.AppendBytes(protowire.AppendTag(channel, 5, protowire.BytesType), []byte{1})
	} else {
		channel = protowire.AppendVarint(protowire.AppendTag(channel, 2, protowire.VarintType), 2)
	}
	channel = protowire.AppendString(protowire.AppendTag(channel, 6, protowire.BytesType), "claude-fixture")
	container := protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), channel)
	var body []byte
	if cais {
		body = protowire.AppendVarint(protowire.AppendTag(nil, 1, protowire.VarintType), 2)
	}
	body = protowire.AppendBytes(protowire.AppendTag(body, 2, protowire.BytesType), container)
	body = protowire.AppendVarint(protowire.AppendTag(body, 3, protowire.VarintType), 1)
	return base64.StdEncoding.EncodeToString(body)
}

func TestClaudeResponsesReplayKeepsThinkingToolSeparatorsAndOpaqueData(t *testing.T) {
	sig := claudeReplaySignature(true)
	raw := []byte(fmt.Sprintf(`{"input":[
		{"role":"user","content":"question"},
		{"type":"reasoning","encrypted_content":%q,"summary":[{"text":"superseded"}]},
		{"type":"reasoning","encrypted_content":%q,"summary":[{"text":"first"}],"content":[{"text":"duplicate"}]},
		{"type":"function_call","name":"lookup","call_id":"pair-1","arguments":"{\"n\":9007199254740993}"},
		{"type":"reasoning","encrypted_content":%q,"content":[{"type":"reasoning_text","text":"second"}]},
		{"type":"reasoning","encrypted_content":"claude-redacted-thinking: opaque data ","summary":[{"text":"invisible"}]},
		{"role":"assistant","content":[{"type":"output_text","text":"answer"}]},
		{"type":"custom_tool_call","namespace":"editor","name":"patch","call_id":"pair-2","input":"raw data"},
		{"type":"function_call_output","call_id":"pair-1","output":"result-1"},
		{"type":"custom_tool_call_output","call_id":"pair-2","output":"result-2"},
		{"role":"user","content":"next"}]}`, sig, sig, sig))
	before := bytes.Clone(raw)
	got := gjson.ParseBytes(ConvertOpenAIResponsesRequestToClaude("claude-fixture", raw, false))
	if got.Get("messages.#").Int() != 3 {
		t.Fatal("consecutive Responses items were not assembled into complete Claude turns")
	}
	parts := got.Get("messages.1.content").Array()
	if len(parts) != 6 || parts[0].Get("thinking").String() != "first" || parts[0].Get("signature").String() != sig ||
		parts[1].Get("id").String() != "pair-1" || parts[1].Get("input.n").Raw != "9007199254740993" ||
		parts[2].Get("thinking").String() != "second" || parts[3].Get("data").String() != " opaque data " ||
		parts[3].Get("type").String() != "redacted_thinking" || parts[4].Get("text").String() != "answer" ||
		parts[5].Get("name").String() != "editor__patch" || parts[5].Get("input.input").String() != "raw data" {
		t.Fatal("replay lost thinking, tool boundaries or opaque carrier bytes")
	}
	if got.Get("messages.2.content.0.tool_use_id").String() != "pair-1" || got.Get("messages.2.content.1.tool_use_id").String() != "pair-2" ||
		got.Get("messages.2.content.2.text").String() != "next" || !bytes.Equal(raw, before) {
		t.Fatal("tool result pairing or source immutability changed")
	}
}

func TestClaudeResponsesReplaySignaturePolicyAndTextFallback(t *testing.T) {
	native := claudeReplaySignature(false)
	for _, tc := range []struct {
		name, raw, expected string
		native, compat      bool
	}{
		{"native", fmt.Sprintf(`,"encrypted_content":%q`, native), native, true, true},
		{"wrapped", fmt.Sprintf(`,"encrypted_content":%q`, base64.StdEncoding.EncodeToString([]byte(native))), native, true, true},
		{"cais", fmt.Sprintf(`,"encrypted_content":%q`, "claude-cais#"+claudeReplaySignature(true)), claudeReplaySignature(true), true, true},
		{"missing", "", "", false, true},
		{"empty", `,"encrypted_content":""`, "", false, true},
		{"spaces", `,"encrypted_content":"  "`, "  ", false, true},
		{"opaque", `,"encrypted_content":" custom opaque "`, " custom opaque ", false, true},
		{"foreign", `,"encrypted_content":"gpt#opaque"`, "gpt#opaque", false, true},
		{"number", `,"encrypted_content":42`, "", false, false},
		{"boolean", `,"encrypted_content":false`, "", false, false},
		{"object", `,"encrypted_content":{"text":"opaque"}`, "", false, false},
		{"null", `,"encrypted_content":null`, "", false, false},
	} {
		for _, compat := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/compat=%t", tc.name, compat), func(t *testing.T) {
				raw := []byte(`{"input":[{"type":"reasoning","summary":[],"content":[{"text":"fallback"},{"text":4},{"text":{"text":"business"}},"-tail"]` + tc.raw + `},{"role":"assistant","content":"answer"},{"role":"user","content":"continue"}]}`)
				before := bytes.Clone(raw)
				out := convertOpenAIResponsesRequestToClaude("claude-fixture", raw, true, compat)
				part := gjson.GetBytes(out, "messages.0.content.0")
				want := tc.native || (compat && tc.compat)
				if (part.Get("type").String() == "thinking") != want ||
					(want && (part.Get("signature").String() != tc.expected || part.Get("thinking").String() != "fallback-tail")) ||
					!bytes.Equal(raw, before) || gjson.GetBytes(out, "messages.#").Int() != 2 || !gjson.GetBytes(out, "stream").Bool() {
					t.Fatal("signature type, compatibility policy or text fallback changed")
				}
			})
		}
	}
	for _, role := range []string{"user", "system", "developer", "model"} {
		raw := gjson.Parse(fmt.Sprintf(`{"type":"reasoning","role":%q,"encrypted_content":%q}`, role, native))
		for _, compat := range []bool{false, true} {
			if len(claudeResponsesReplayReasoning(raw, compat)) != 0 {
				t.Fatal("non-assistant history became model thinking")
			}
		}
	}
}

func TestClaudeResponsesReplayTrimsOnlyNativeFinalThinking(t *testing.T) {
	for _, compat := range []bool{false, true} {
		for _, leading := range []string{"", `{"role":"user","content":"question"},`, `{"role":"assistant","content":"answer"},`} {
			raw := []byte(fmt.Sprintf(`{"input":[%s{"type":"reasoning","encrypted_content":%q},{"type":"reasoning","encrypted_content":"claude-redacted-thinking: secret "}]}`, leading, claudeReplaySignature(true)))
			out := convertOpenAIResponsesRequestToClaude("claude-fixture", raw, false, compat)
			messages := gjson.GetBytes(out, "messages").Array()
			if len(messages) == 0 {
				t.Fatal("trimming the final thinking removed every conversational turn")
			}
			last := messages[len(messages)-1]
			if compat {
				parts := last.Get("content").Array()
				if len(parts) < 2 || parts[len(parts)-1].Get("data").String() != " secret " {
					t.Fatal("compatibility prefill lost its opaque thinking")
				}
			} else if last.Get("content").IsArray() {
				t.Fatal("native final thinking was still sent to Claude")
			}
		}
	}
	for _, value := range []string{"", "plain input"} {
		out := ConvertOpenAIResponsesRequestToClaude("claude-fixture", []byte(fmt.Sprintf(`{"input":%q}`, value)), false)
		if gjson.GetBytes(out, "messages.0.role").String() != "user" || gjson.GetBytes(out, "messages.0.content").String() != value {
			t.Fatal("string input did not produce a user turn")
		}
	}
}

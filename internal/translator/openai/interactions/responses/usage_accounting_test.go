package responses

import (
	"bytes"
	"fmt"
	"testing"

	codex "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/codex/interactions"
	chat "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/openai/interactions/chat-completions"
	"github.com/tidwall/gjson"
)

func TestInteractionsUsageAcrossOpenAIFormatsAndRoundTrips(t *testing.T) {
	for _, format := range []string{"responses", "chat", "codex"} {
		for _, stream := range []bool{false, true} {
			for _, totalKeys := range []bool{false, true} {
				for _, usageMode := range []string{"none", "base", "thoughts"} {
					t.Run(fmt.Sprintf("%s/stream=%t/total-keys=%t/%s", format, stream, totalKeys, usageMode), func(t *testing.T) {
						inputKey, outputKey, thoughtKey, toolKey, cacheKey := "input_tokens", "output_tokens", "reasoning_tokens", "tool_use_tokens", "cached_tokens"
						if totalKeys {
							inputKey, outputKey, thoughtKey, toolKey, cacheKey = "total_input_tokens", "total_output_tokens", "total_thought_tokens", "total_tool_use_tokens", "total_cached_tokens"
						}
						var thoughts, tool int64
						if usageMode == "thoughts" {
							thoughts, tool = 3, 5
						}
						usageField := ""
						if usageMode != "none" {
							usageField = fmt.Sprintf(`,"usage":{%q:10,%q:2,%q:%d,%q:%d,%q:4,"total_tokens":%d}`, inputKey, outputKey, thoughtKey, thoughts, toolKey, tool, cacheKey, 12+thoughts+tool)
						}
						body := []byte(`{"id":"fixture","object":"interaction","status":"completed","model":"fixture","steps":[{"type":"model_output","content":[{"type":"text","text":"answer"}]}]` + usageField + `}`)
						var output []byte
						if stream {
							var state any
							frame := []byte(`{"event_type":"interaction.completed","interaction":` + string(body) + `}`)
							if format == "chat" {
								output = interactionsUsageResponseFromChunks(t, chat.ConvertInteractionsResponseToOpenAI(t.Context(), "fixture", nil, nil, frame, &state), "")
							} else {
								output = interactionsUsageResponseFromChunks(t, ConvertInteractionsResponseToOpenAIResponses(t.Context(), "fixture", nil, nil, frame, &state), "response")
							}
						} else if format == "chat" {
							output = chat.ConvertInteractionsResponseToOpenAINonStream(t.Context(), "fixture", nil, nil, body, nil)
						} else {
							output = ConvertInteractionsResponseToOpenAIResponsesNonStream(t.Context(), "fixture", nil, nil, body, nil)
						}
						usage := gjson.GetBytes(output, "usage")
						if usageMode == "none" {
							if usage.Exists() {
								t.Fatal("conversion fabricated usage")
							}
							return
						}
						inputPath, outputPath, thoughtPath := "input_tokens", "output_tokens", "output_tokens_details.reasoning_tokens"
						if format == "chat" {
							inputPath, outputPath, thoughtPath = "prompt_tokens", "completion_tokens", "completion_tokens_details.reasoning_tokens"
						}
						check := func(usage gjson.Result) {
							t.Helper()
							if usage.Get(inputPath).Int() != 10+tool || usage.Get(outputPath).Int() != 2+thoughts || usage.Get(thoughtPath).Int() != thoughts || usage.Get("total_tokens").Int() != 12+thoughts+tool {
								t.Fatal("independent tool/reasoning counts were lost or counted twice")
							}
							if format != "chat" && (!usage.Get("input_tokens_details.cached_tokens").Exists() || !usage.Get("output_tokens_details.reasoning_tokens").Exists()) {
								t.Fatal("Responses usage details were omitted")
							}
						}
						check(usage)
						var reverse []byte
						if stream {
							var state any
							frame := []byte(`data: {"type":"response.completed","response":` + string(output) + `}`)
							var chunks [][]byte
							if format == "chat" {
								chunks = chat.ConvertOpenAIResponseToInteractions(t.Context(), "fixture", nil, nil, output, &state)
								chunks = append(chunks, chat.ConvertOpenAIResponseToInteractions(t.Context(), "fixture", nil, nil, []byte("[DONE]"), &state)...)
							} else if format == "codex" {
								chunks = codex.ConvertCodexResponseToInteractions(t.Context(), "fixture", nil, nil, frame, &state)
							} else {
								chunks = ConvertOpenAIResponsesResponseToInteractions(t.Context(), "fixture", nil, nil, frame, &state)
							}
							reverse = interactionsUsageResponseFromChunks(t, chunks, "interaction")
						} else if format == "chat" {
							reverse = chat.ConvertOpenAIResponseToInteractionsNonStream(t.Context(), "fixture", nil, nil, output, nil)
						} else if format == "codex" {
							reverse = codex.ConvertCodexResponseToInteractionsNonStream(t.Context(), "fixture", nil, nil, []byte(`{"response":`+string(output)+`}`), nil)
						} else {
							reverse = ConvertOpenAIResponsesResponseToInteractionsNonStream(t.Context(), "fixture", nil, nil, output, nil)
						}
						if format == "chat" {
							check(gjson.GetBytes(chat.ConvertInteractionsResponseToOpenAINonStream(t.Context(), "fixture", nil, nil, reverse, nil), "usage"))
						} else {
							check(gjson.GetBytes(ConvertInteractionsResponseToOpenAIResponsesNonStream(t.Context(), "fixture", nil, nil, reverse, nil), "usage"))
						}
					})
				}
			}
		}
	}
}

func interactionsUsageResponseFromChunks(t *testing.T, chunks [][]byte, container string) []byte {
	t.Helper()
	var response gjson.Result
	for _, chunk := range chunks {
		for _, line := range bytes.Split(chunk, []byte("\n")) {
			line = bytes.TrimSpace(bytes.TrimPrefix(bytes.TrimSpace(line), []byte("data:")))
			if !gjson.ValidBytes(line) {
				continue
			}
			node := gjson.ParseBytes(line)
			if container != "" {
				node = node.Get(container)
			}
			if node.IsObject() && (container != "" || node.Get("object").String() == "chat.completion.chunk") {
				response = node
			}
		}
	}
	if !response.IsObject() {
		t.Fatal("stream did not produce a response envelope")
	}
	return []byte(response.Raw)
}

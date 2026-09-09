package openai

import (
	"fmt"
	"testing"

	"github.com/tidwall/gjson"
)

func TestResponsesWebsocketSummaryDoesNotInheritPreviousTurn(t *testing.T) {
	previous := []byte(`{"model":"gpt-5.4","instructions":"fixture","input":[{"role":"user","content":"first"}],"reasoning":{"effort":"high","summary":"detailed"}}`)
	for _, incremental := range []bool{false, true} {
		for _, kind := range []string{"response.create", "response.append"} {
			for _, previousID := range []string{"", `,"previous_response_id":"resp_fixture"`} {
				for _, tc := range []struct{ fields, want string }{{"", ""}, {`,"reasoning":{"summary":null}`, "null"}, {`,"reasoning":{"summary":"concise"}`, `"concise"`}} {
					raw := []byte(fmt.Sprintf(`{"type":%q,"input":[{"role":"user","content":"next"}]%s%s}`, kind, previousID, tc.fields))
					normalized, next, errMsg := normalizeResponsesWebsocketRequestWithMode(raw, previous, []byte(`[]`), incremental)
					if errMsg != nil {
						t.Fatal(errMsg)
					}
					for _, body := range [][]byte{normalized, next} {
						if gjson.GetBytes(body, "reasoning.summary").Raw != tc.want || gjson.GetBytes(body, "reasoning.effort").Exists() {
							t.Fatalf("inherited previous reasoning fields: %s", body)
						}
						if gjson.GetBytes(body, "model").String() != "gpt-5.4" || gjson.GetBytes(body, "instructions").String() != "fixture" {
							t.Fatal("changed existing model/instruction inheritance")
						}
					}
				}
			}
		}
	}
}

package responses

import (
	"fmt"
	"testing"

	"github.com/tidwall/gjson"
)

func TestAntigravityResponseRequestEnvelopesAreIndependent(t *testing.T) {
	for _, responseWrapped := range []bool{false, true} {
		for _, originalWrapped := range []bool{false, true} {
			for _, translatedWrapped := range []bool{false, true} {
				t.Run(fmt.Sprintf("response=%t/original=%t/translated=%t", responseWrapped, originalWrapped, translatedWrapped), func(t *testing.T) {
					original := `{"model":"client-model","input":[],"tools":[{"type":"namespace","name":"editor","tools":[{"type":"function","name":"patch"}]}]}`
					translated := `{"model":"wire-model","input":[]}`
					response := `{"candidates":[{"content":{"parts":[{"functionCall":{"name":"editor__patch","args":{"value":9007199254740993}}}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}`
					if responseWrapped {
						response = `{"response":` + response + `}`
					}
					if originalWrapped {
						original = `{"request":` + original + `}`
					}
					if translatedWrapped {
						translated = `{"request":` + translated + `}`
					}
					out := ConvertAntigravityResponseToOpenAIResponsesNonStream(t.Context(), "wire-model", []byte(original), []byte(translated), []byte(response), nil)
					if gjson.GetBytes(out, "model").String() != "client-model" || gjson.GetBytes(out, "output.0.name").String() != "patch" || gjson.GetBytes(out, "output.0.namespace").String() != "editor" || gjson.GetBytes(out, "output.0.arguments").String() != `{"value":9007199254740993}` {
						t.Fatal("response envelope erased independent request metadata or tool identity")
					}
				})
			}
		}
	}
}

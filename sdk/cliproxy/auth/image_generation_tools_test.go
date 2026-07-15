package auth

import "testing"

func TestPayloadHasImageGenerationTool(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    bool
	}{
		{name: "native", payload: `{"tools":[{"type":"image_generation"}]}`, want: true},
		{name: "function name", payload: `{"tools":[{"type":"function","name":"image_gen.imagegen"}]}`, want: true},
		{name: "function object", payload: `{"tools":[{"type":"function","function":{"name":"image_gen.imagegen"}}]}`, want: true},
		{name: "namespace name", payload: `{"tools":[{"type":"namespace","name":"image_gen","tools":[{"type":"function","name":"imagegen"}]}]}`, want: true},
		{name: "namespace function object", payload: `{"tools":[{"type":"namespace","name":"image_gen","tools":[{"type":"function","function":{"name":"imagegen"}}]}]}`, want: true},
		{name: "other function", payload: `{"tools":[{"type":"function","name":"lookup"}]}`, want: false},
		{name: "wrong namespace", payload: `{"tools":[{"type":"namespace","name":"other","tools":[{"type":"function","name":"imagegen"}]}]}`, want: false},
		{name: "missing tools", payload: `{"input":"draw"}`, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PayloadHasImageGenerationTool([]byte(tt.payload)); got != tt.want {
				t.Fatalf("PayloadHasImageGenerationTool() = %v, want %v", got, tt.want)
			}
		})
	}
}

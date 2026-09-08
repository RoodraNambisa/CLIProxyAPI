package registry

import "testing"

func TestOpenAICatalogUsesProviderNativeTokenLimits(t *testing.T) {
	for _, tc := range []struct {
		name                                                            string
		context, completion, input, output, wantContext, wantCompletion int
	}{
		{"provider fields", 0, 0, 1048576, 65536, 1048576, 65536},
		{"existing fields win", 131072, 8192, 1048576, 65536, 131072, 8192},
		{"independent fallback", 131072, 0, 1048576, 65536, 131072, 65536},
		{"no invented capacity", 0, 0, 0, 0, 0, 0},
		{"nonpositive fields", -1, -1, -2, -2, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newTestModelRegistry()
			r.RegisterClient("google", "gemini", []*ModelInfo{{ID: "model", ContextLength: tc.context, MaxCompletionTokens: tc.completion, InputTokenLimit: tc.input, OutputTokenLimit: tc.output}})
			for _, catalog := range [][]map[string]any{r.GetAvailableModels("openai"), r.GetAvailableModelsForProviders("openai", []string{"gemini"}), r.GetOpenAIModelCatalog().Models} {
				if len(catalog) != 1 {
					t.Fatal("model was removed")
				}
				for field, want := range map[string]int{"context_length": tc.wantContext, "max_completion_tokens": tc.wantCompletion} {
					got, exists := catalog[0][field]
					if (want > 0 && got != want) || (want == 0 && exists) {
						t.Fatalf("%s = %v, want %d", field, got, want)
					}
				}
			}
			original := r.GetModelInfo("model", "gemini")
			if original.ContextLength != tc.context || original.MaxCompletionTokens != tc.completion || original.InputTokenLimit != tc.input || original.OutputTokenLimit != tc.output {
				t.Fatal("catalog mapping changed provider-native metadata")
			}
		})
	}
}

package auth

import (
	"fmt"
	"strings"
	"testing"
)

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

func TestPayloadMaySelectImageGenerationTool(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    bool
	}{
		{
			name:    "automatic",
			payload: `{"tools":[{"type":"image_generation"},{"type":"web_search_preview"}]}`,
			want:    true,
		},
		{
			name:    "explicit image",
			payload: `{"tools":[{"type":"image_generation"}],"tool_choice":{"type":"image_generation"}}`,
			want:    true,
		},
		{
			name:    "explicit image function",
			payload: `{"tools":[{"type":"function","name":"image_gen.imagegen"}],"tool_choice":{"type":"function","name":"image_gen.imagegen"}}`,
			want:    true,
		},
		{
			name:    "explicit image namespace",
			payload: `{"tools":[{"type":"namespace","name":"image_gen","tools":[{"type":"function","name":"imagegen"}]}],"tool_choice":{"type":"namespace","name":"image_gen"}}`,
			want:    true,
		},
		{
			name:    "explicit search",
			payload: `{"tools":[{"type":"image_generation"},{"type":"web_search_preview"}],"tool_choice":{"type":"web_search_preview"}}`,
			want:    false,
		},
		{
			name:    "none",
			payload: `{"tools":[{"type":"image_generation"}],"tool_choice":"none"}`,
			want:    false,
		},
		{
			name:    "allowed tools contains image",
			payload: `{"tools":[{"type":"image_generation"}],"tool_choice":{"type":"allowed_tools","mode":"auto","tools":[{"type":"image_generation"}]}}`,
			want:    true,
		},
		{
			name:    "chat completions allowed tools contains image",
			payload: `{"tools":[{"type":"function","function":{"name":"image_gen.imagegen"}}],"tool_choice":{"type":"allowed_tools","allowed_tools":{"mode":"required","tools":[{"type":"function","function":{"name":"image_gen.imagegen"}}]}}}`,
			want:    true,
		},
		{
			name:    "allowed tools excludes image",
			payload: `{"tools":[{"type":"image_generation"},{"type":"web_search_preview"}],"tool_choice":{"type":"allowed_tools","mode":"required","tools":[{"type":"web_search_preview"}]}}`,
			want:    false,
		},
		{
			name:    "chat completions allowed tools excludes image",
			payload: `{"tools":[{"type":"function","function":{"name":"image_gen.imagegen"}},{"type":"function","function":{"name":"lookup"}}],"tool_choice":{"type":"allowed_tools","allowed_tools":{"mode":"required","tools":[{"type":"function","function":{"name":"lookup"}}]}}}`,
			want:    false,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := PayloadMaySelectImageGenerationTool([]byte(testCase.payload)); got != testCase.want {
				t.Fatalf("PayloadMaySelectImageGenerationTool() = %v, want %v", got, testCase.want)
			}
		})
	}
}

func TestPayloadExplicitlySelectsImageGenerationTool(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    bool
	}{
		{
			name:    "automatic",
			payload: `{"tools":[{"type":"image_generation"}]}`,
			want:    false,
		},
		{
			name:    "explicit image",
			payload: `{"tools":[{"type":"image_generation"}],"tool_choice":{"type":"image_generation"}}`,
			want:    true,
		},
		{
			name:    "required image only",
			payload: `{"tools":[{"type":"function","name":"image_gen.imagegen"}],"tool_choice":"required"}`,
			want:    true,
		},
		{
			name:    "required with another tool",
			payload: `{"tools":[{"type":"image_generation"},{"type":"web_search_preview"}],"tool_choice":"required"}`,
			want:    false,
		},
		{
			name:    "none",
			payload: `{"tools":[{"type":"image_generation"}],"tool_choice":"none"}`,
			want:    false,
		},
		{
			name:    "required allowed image only",
			payload: `{"tools":[{"type":"image_generation"}],"tool_choice":{"type":"allowed_tools","mode":"required","tools":[{"type":"image_generation"}]}}`,
			want:    false,
		},
		{
			name:    "chat completions required allowed image only",
			payload: `{"tools":[{"type":"function","function":{"name":"image_gen.imagegen"}}],"tool_choice":{"type":"allowed_tools","allowed_tools":{"mode":"required","tools":[{"type":"function","function":{"name":"image_gen.imagegen"}}]}}}`,
			want:    false,
		},
		{
			name:    "automatic allowed image only",
			payload: `{"tools":[{"type":"image_generation"}],"tool_choice":{"type":"allowed_tools","mode":"auto","tools":[{"type":"image_generation"}]}}`,
			want:    false,
		},
		{
			name:    "required allowed mixed tools",
			payload: `{"tools":[{"type":"image_generation"},{"type":"web_search_preview"}],"tool_choice":{"type":"allowed_tools","mode":"required","tools":[{"type":"image_generation"},{"type":"web_search_preview"}]}}`,
			want:    false,
		},
		{
			name:    "image generation string",
			payload: `{"tools":[{"type":"image_generation"}],"tool_choice":"image_generation"}`,
			want:    false,
		},
		{
			name:    "mixed namespace",
			payload: `{"tools":[{"type":"namespace","name":"image_gen","tools":[{"type":"function","name":"imagegen"},{"type":"function","name":"inspect"}]}],"tool_choice":{"type":"namespace","name":"image_gen"}}`,
			want:    false,
		},
		{
			name:    "required mixed namespace",
			payload: `{"tools":[{"type":"namespace","name":"image_gen","tools":[{"type":"function","name":"imagegen"},{"type":"function","name":"inspect"}]}],"tool_choice":"required"}`,
			want:    false,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := PayloadExplicitlySelectsImageGenerationTool([]byte(testCase.payload)); got != testCase.want {
				t.Fatalf("PayloadExplicitlySelectsImageGenerationTool() = %v, want %v", got, testCase.want)
			}
		})
	}
}

func TestAdditionalImageToolsPreserveSelectionAndArrayBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, payload    string
		has, may, forced bool
	}{
		{"additional required", `{"input":[{"type":"additional_tools","tools":[{"type":"image_generation"}]}],"tool_choice":"required"}`, true, true, true},
		{"escaped root tools", `{"to\u006fls":[{"type":"image_generation"}],"tool_choice":"required"}`, true, true, true},
		{"escaped additional tools", `{"in\u0070ut":[{"type":"additional_tools","to\u006fls":[{"type":"image_generation"}]}],"tool_choice":"required"}`, true, true, true},
		{"escaped additional type", `{"input":[{"type":"additiona\u006c_tools","tools":[{"type":"image_generation"}]}],"tool_choice":"required"}`, true, true, true},
		{"first duplicate tools", `{"tools":null,"tools":[{"type":"image_generation"}],"tool_choice":"required"}`, false, false, false},
		{"additional none", `{"input":[{"type":"additional_tools","tools":[{"type":"image_generation"}]}],"tool_choice":"none"}`, true, false, false},
		{"mixed required", `{"tools":[{"type":"function","name":"read"}],"input":[{"type":"additional_tools","tools":[{"type":"image_generation"}]}],"tool_choice":"required"}`, true, true, false},
		{"invalid root tools", `{"tools":{"type":"image_generation"},"tool_choice":"required"}`, false, false, false},
		{"invalid additional tools", `{"input":[{"type":"additional_tools","tools":{"type":"image_generation"}}],"tool_choice":"required"}`, false, false, false},
		{"invalid input object", `{"input":{"type":"additional_tools","tools":[{"type":"image_generation"}]},"tool_choice":"required"}`, false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(tc.payload)
			if PayloadHasImageGenerationTool(body) != tc.has || PayloadMaySelectImageGenerationTool(body) != tc.may || PayloadExplicitlySelectsImageGenerationTool(body) != tc.forced {
				t.Fatal("additional image selection or declared array boundary changed")
			}
		})
	}
}

func TestImageToolDeclarationScanAvoidsPlainTextAllocations(t *testing.T) {
	body := []byte(`{"input":[{"role":"user","content":"` + strings.Repeat("x", 1<<20) + `"}]}`)
	for name, detect := range map[string]func([]byte) bool{
		"has":        PayloadHasImageGenerationTool,
		"may select": PayloadMaySelectImageGenerationTool,
		"forced":     PayloadExplicitlySelectsImageGenerationTool,
	} {
		t.Run(name, func(t *testing.T) {
			var image bool
			allocations := testing.AllocsPerRun(5, func() { image = detect(body) })
			if image || allocations != 0 {
				t.Fatalf("plain text selected images=%t or allocated %.0f times", image, allocations)
			}
		})
	}
}

func BenchmarkImageToolDeclarationScan(b *testing.B) {
	for _, size := range []int{128, 1 << 20, 10 << 20} {
		for _, declared := range []bool{false, true} {
			b.Run(fmt.Sprintf("bytes=%d/tools=%t", size, declared), func(b *testing.B) {
				body := []byte(`{"input":[{"role":"user","content":"` + strings.Repeat("x", size) + `"}]`)
				if declared {
					body = append(body, `,"tools":[{"type":"function","name":"read"}]`...)
				}
				body = append(body, '}')
				b.SetBytes(int64(len(body)))
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					PayloadHasImageGenerationTool(body)
				}
			})
		}
	}
}

func TestImageToolDeclarationScanSkipsHistoryWithoutAdditionalTools(t *testing.T) {
	rootOnly := []byte(`{"tools":[{"type":"function","name":"read"}]}`)
	withHistory := []byte(`{"input":[{"role":"user","content":"` + strings.Repeat("x", 1<<20) + `"}],"tools":[{"type":"function","name":"read"}]}`)
	for _, detect := range []func([]byte) bool{PayloadHasImageGenerationTool, PayloadMaySelectImageGenerationTool, PayloadExplicitlySelectsImageGenerationTool} {
		base := testing.AllocsPerRun(5, func() { detect(rootOnly) })
		withInput := testing.AllocsPerRun(5, func() {
			if detect(withHistory) {
				t.Fatal("ordinary tool history selected image generation")
			}
		})
		if withInput > base {
			t.Fatalf("history without additional tools added allocations: %g vs %g", withInput, base)
		}
	}
}

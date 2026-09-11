package codex

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
)

func TestCodexEffortNoopDoesNotCopyLargeBody(t *testing.T) {
	known := &registry.ModelInfo{ID: "fixture", Thinking: &registry.ThinkingSupport{Levels: []string{"low", "high"}}}
	for _, model := range []*registry.ModelInfo{known, nil} {
		body := []byte(`{"input":"` + strings.Repeat("x", 1<<20) + `","reasoning":{"effort":"high","summary":"auto"}}`)
		config := thinking.ThinkingConfig{Mode: thinking.ModeLevel, Level: thinking.LevelHigh}
		if allocations := testing.AllocsPerRun(5, func() {
			out, err := NewApplier().Apply(body, config, model)
			if err != nil || len(out) != len(body) || &out[0] != &body[0] {
				t.Fatal("unchanged effort rebuilt the request")
			}
		}); allocations != 0 {
			t.Fatalf("unchanged effort allocated %g times", allocations)
		}
	}
}

func TestCodexEffortUpdatePreservesFirstParentAndInvalidInputRules(t *testing.T) {
	for _, test := range []struct{ input, output string }{
		{`{"reasoning":{"effort":"low","summary":"auto"}}`, `{"reasoning":{"effort":"high","summary":"auto"}}`},
		{`{"reasoning":{"effort":"\u0068igh"}}`, `{"reasoning":{"effort":"high"}}`},
		{`{"reasoning":null,"reasoning":{"effort":"low"}}`, `{"reasoning":{"effort":"high"},"reasoning":{"effort":"low"}}`},
		{`{"reasoning":{"effort":null,"effort":"low"}}`, `{"reasoning":{"effort":"high","effort":"low"}}`},
		{`{"reasoning":{"effort":"high"}}invalid`, `{"reasoning":{"effort":"high"}}`},
		{``, `{"reasoning":{"effort":"high"}}`},
	} {
		for _, model := range []*registry.ModelInfo{{ID: "fixture", Thinking: &registry.ThinkingSupport{}}, nil} {
			body := []byte(test.input)
			out, err := NewApplier().Apply(body, thinking.ThinkingConfig{Mode: thinking.ModeLevel, Level: thinking.LevelHigh}, model)
			if err != nil || string(out) != test.output || string(body) != test.input {
				t.Fatalf("input=%s got=%s want=%s err=%v", test.input, out, test.output, err)
			}
			want := bytes.Clone(out)
			clear(body)
			if !bytes.Equal(out, want) {
				t.Fatal("changed output retained mutable source")
			}
		}
	}
}

func BenchmarkCodexEffortNoop(b *testing.B) {
	for _, size := range []int{128, 1 << 20, 10 << 20} {
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			body := []byte(`{"input":"` + strings.Repeat("x", size) + `","reasoning":{"effort":"high"}}`)
			model := &registry.ModelInfo{ID: "fixture", Thinking: &registry.ThinkingSupport{Levels: []string{"high"}}}
			config := thinking.ThinkingConfig{Mode: thinking.ModeLevel, Level: thinking.LevelHigh}
			b.ReportAllocs()
			b.SetBytes(int64(len(body)))
			b.ResetTimer()
			for range b.N {
				_, _ = NewApplier().Apply(body, config, model)
			}
		})
	}
}

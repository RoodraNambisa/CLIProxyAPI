package usage

import (
	"context"
	"testing"
)

type generateCapturePlugin struct {
	records chan Record
}

func (p generateCapturePlugin) HandleUsage(_ context.Context, record Record) { p.records <- record }

func TestGenerateDefaultsAndContextIsolation(t *testing.T) {
	if !GenerateFromContext(nil) || !GenerateFromContext(context.Background()) || !GenerateEnabled(nil) {
		t.Fatal("omitted generate must preserve legacy generation")
	}
	for _, generate := range []bool{false, true} {
		ctx := WithGenerate(nil, generate)
		if GenerateFromContext(ctx) != generate || GenerateFromContext(WithGenerate(ctx, !generate)) == generate || GenerateFromContext(ctx) != generate {
			t.Fatal("request context lost its immutable generate value")
		}
	}
}

func TestGeneratePublicationDefaultsAndOwnsOptionalFlag(t *testing.T) {
	manager := NewManager(1)
	t.Cleanup(manager.Stop)
	plugin := generateCapturePlugin{records: make(chan Record, 3)}
	manager.Register(plugin)
	for _, value := range []*bool{nil, GenerateFlag(false), GenerateFlag(true)} {
		want := GenerateEnabled(value)
		manager.Publish(t.Context(), Record{Generate: value})
		if value != nil {
			*value = !want
		}
		select {
		case record := <-plugin.records:
			if record.Generate == nil || record.Generate == value || GenerateEnabled(record.Generate) != want {
				t.Fatal("publication lost the default or retained caller-owned state")
			}
		case <-t.Context().Done():
			t.Fatal("usage record was not published")
		}
	}
}

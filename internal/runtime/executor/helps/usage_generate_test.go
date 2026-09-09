package helps

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func TestUsageReporterPinsGenerateForEveryRecord(t *testing.T) {
	for _, generate := range []bool{false, true} {
		ctx := usage.WithGenerate(t.Context(), generate)
		reporter := NewUsageReporter(ctx, "codex", "fixture", nil)
		ctx = usage.WithGenerate(ctx, !generate)
		for _, failed := range []bool{false, true} {
			for _, model := range []string{"fixture", "image-allocation"} {
				record := reporter.buildRecordForModel(model, usage.Detail{TotalTokens: 3}, failed)
				if record.Generate == nil || usage.GenerateEnabled(record.Generate) != generate || record.Failed != failed || record.Detail.TotalTokens != 3 || usage.GenerateFromContext(ctx) == generate {
					t.Fatal("usage flag drifted or altered the result/token accounting")
				}
			}
		}
	}
	var nilReporter *UsageReporter
	if !usage.GenerateEnabled(nilReporter.buildRecord(usage.Detail{}, false).Generate) || !usage.GenerateEnabled(NewUsageReporter(nil, "codex", "fixture", nil).buildRecord(usage.Detail{}, false).Generate) {
		t.Fatal("legacy reporter lost the generation default")
	}
}

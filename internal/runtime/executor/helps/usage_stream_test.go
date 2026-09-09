package helps

import (
	"context"
	"fmt"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func TestUsageReporterStreamSnapshotAndAuxiliaryRecords(t *testing.T) {
	for _, mode := range []int{-1, 0, 1} {
		for _, fallback := range []bool{false, true} {
			t.Run(fmt.Sprintf("mode=%d/fallback=%t", mode, fallback), func(t *testing.T) {
				ctx := context.Background()
				want := fallback
				if mode >= 0 {
					want = mode == 1
					ctx = usage.WithStream(ctx, want)
				}
				reporter := NewUsageReporter(ctx, "codex", "fixture", nil, fallback)
				for _, failed := range []bool{false, true} {
					if record := reporter.buildRecord(usage.Detail{}, failed); record.Stream != want || record.Failed != failed {
						t.Fatal("usage record lost its pinned logical response mode")
					}
				}
				record, ok := reporter.buildAdditionalModelRecord("gpt-image-2", usage.Detail{InputTokens: 1, TotalTokens: 1})
				if !ok || record.Stream != want || !record.Auxiliary {
					t.Fatal("auxiliary model usage changed the request's response mode")
				}
			})
		}
	}
	if NewUsageReporter(context.Background(), "codex", "legacy", nil).buildRecord(usage.Detail{}, false).Stream {
		t.Fatal("legacy reporter changed its default response mode")
	}
}

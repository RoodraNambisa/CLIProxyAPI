package usage

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func TestImageAggregationDeliveryAccountingAndRestore(t *testing.T) {
	for _, delivered := range []int{0, 9, 10} {
		t.Run(string(rune('a'+delivered)), func(t *testing.T) {
			stats := NewRequestStatistics()
			now := time.Now().UTC()
			for i := 0; i < 10; i++ {
				info := coreusage.ImageAggregation{BatchID: "same-batch", ImageIndex: i, Requested: 10, Generated: 9, Delivered: delivered, Executed: true, Role: "attempt"}
				record := coreusage.Record{Provider: "chatgpt-web", Model: "gpt-image-2", APIKey: "test-key", AuthIndex: "auth", RequestedAt: now, Failed: i == 9, ImageAggregation: info}
				if i < 9 {
					record.Detail = coreusage.Detail{InputTokens: 7, OutputTokens: 3, TotalTokens: 10}
				}
				stats.Record(context.Background(), record)
				record.ImageAggregation.Role = "delivery"
				record.Detail = coreusage.Detail{}
				record.Failed = i >= delivered
				record.FailureStage = "aggregation"
				record.ErrorCode = "image_aggregation_discarded"
				stats.Record(context.Background(), record)
			}
			check := func(s *RequestStatistics) {
				snapshot := s.Snapshot()
				if snapshot.TotalRequests != 10 || snapshot.SuccessCount != int64(delivered) || snapshot.FailureCount != int64(10-delivered) || snapshot.TotalTokens != 90 {
					t.Fatalf("bad totals: requests=%d success=%d failed=%d tokens=%d", snapshot.TotalRequests, snapshot.SuccessCount, snapshot.FailureCount, snapshot.TotalTokens)
				}
				details := s.Details(DetailQuery{ImageAggregationRole: "delivery"})
				if details.Total != 10 {
					t.Fatalf("delivery rows=%d", details.Total)
				}
				attempts := s.Details(DetailQuery{ImageAggregationRole: "attempt", ImageAggregationBatch: "same-batch", ImageAggregationIndex: new(4)})
				if attempts.Total != 1 {
					t.Fatalf("attempt lookup=%d", attempts.Total)
				}
				failures := s.FailureSummary(FailureSummaryQuery{})
				if failures.Total != int64(10-delivered) {
					t.Fatalf("failures=%d", failures.Total)
				}
				health := s.Health(HealthQuery{GroupBy: GroupByNone})
				sumSuccess, sumFailure := int64(0), int64(0)
				for _, entry := range health.Items {
					sumSuccess += entry.SuccessCount
					sumFailure += entry.FailureCount
				}
				if sumSuccess != int64(delivered) || sumFailure != int64(10-delivered) {
					t.Fatalf("health=%d/%d", sumSuccess, sumFailure)
				}
			}
			check(stats)
			raw, err := json.Marshal(stats.Snapshot())
			if err != nil {
				t.Fatal(err)
			}
			var restored StatisticsSnapshot
			if err := json.Unmarshal(raw, &restored); err != nil {
				t.Fatal(err)
			}
			clone := NewRequestStatistics()
			result := clone.MergeSnapshot(restored)
			if result.Added != 20 {
				t.Fatalf("slot identity dedup lost rows: %+v", result)
			}
			check(clone)
			if result := clone.MergeSnapshot(restored); result.Added != 0 {
				t.Fatal("duplicate import")
			}
		})
	}
}

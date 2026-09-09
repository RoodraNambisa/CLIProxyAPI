package usage

import (
	"encoding/json"
	"math"
	"path/filepath"
	"testing"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func TestUsageResponseTimingSurvivesHistoryPersistence(t *testing.T) {
	for _, format := range []string{"snapshot", "record-stream"} {
		t.Run(format, func(t *testing.T) {
			stats := NewRequestStatistics()
			now := time.Now().UTC()
			timings := [][2]time.Duration{{0, 0}, {900 * time.Millisecond, 125 * time.Millisecond}, {0, 30 * time.Millisecond}, {-time.Second, -time.Second}, {time.Duration(math.MaxInt64), time.Duration(math.MaxInt64)}, {time.Microsecond, time.Microsecond}}
			for index, timing := range timings {
				stats.Record(t.Context(), coreusage.Record{APIKey: "fixture", Model: "gpt-5.4-mini", RequestedAt: now.Add(time.Duration(index) * time.Second), Latency: 2 * time.Second, TTFT: timing[0], FirstPacketLatency: timing[1], Failed: index == 2, Detail: coreusage.Detail{TotalTokens: 1}})
			}
			path := filepath.Join(t.TempDir(), StatisticsFileName)
			if format == "snapshot" {
				if err := SaveSnapshotFile(path, stats.Snapshot()); err != nil {
					t.Fatal(err)
				}
			} else if _, err := RewriteRequestStatisticsWithPolicy(path, stats, PersistencePolicy{}); err != nil {
				t.Fatal(err)
			}
			loaded, err := LoadSnapshotFile(path)
			if err != nil {
				t.Fatal(err)
			}
			restored := NewRequestStatistics()
			restored.MergeSnapshot(loaded)
			snapshot := restored.Snapshot()
			details := snapshot.APIs["fixture"].Models["gpt-5.4-mini"].Details
			if len(details) != len(timings) || snapshot.TotalRequests != int64(len(timings)) || snapshot.TotalTokens != int64(len(timings)) {
				t.Fatal("timing persistence changed request/token totals")
			}
			for index, detail := range details {
				if detail.TTFTMs != max(int64(0), timings[index][0].Milliseconds()) || detail.FirstPacketMs != max(int64(0), timings[index][1].Milliseconds()) || detail.LatencyMs != 2000 {
					t.Fatalf("timing %d changed during save/load/merge", index)
				}
			}
		})
	}
}

func TestUsageResponseTimingKeepsLegacyJSON(t *testing.T) {
	var detail RequestDetail
	if err := json.Unmarshal([]byte(`{"latency_ms":1200,"failed":false}`), &detail); err != nil || detail.TTFTMs != 0 || detail.FirstPacketMs != 0 {
		t.Fatal("legacy record inferred timing from total latency")
	}
	payload, err := json.Marshal(detail)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"ttft_ms", "first_packet_ms"} {
		if _, present := fields[field]; present {
			t.Fatalf("legacy JSON acquired an unavailable %s", field)
		}
	}
}

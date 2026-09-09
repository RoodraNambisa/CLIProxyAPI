package usage

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func TestUsageStreamModeSurvivesHistoryPersistence(t *testing.T) {
	for _, format := range []string{"snapshot", "record-stream"} {
		t.Run(format, func(t *testing.T) {
			stats := NewRequestStatistics()
			now := time.Now().UTC()
			for index, stream := range []bool{false, true} {
				stats.Record(t.Context(), coreusage.Record{APIKey: "fixture", Model: "gpt-5.4-mini", RequestedAt: now.Add(time.Duration(index) * time.Second), Stream: stream, Detail: coreusage.Detail{TotalTokens: 1}})
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
			if len(details) != 2 || details[0].Stream || !details[1].Stream || snapshot.TotalRequests != 2 || snapshot.TotalTokens != 2 {
				t.Fatal("history restore lost response modes or changed request/token totals")
			}
		})
	}
}

func TestUsageStreamModeKeepsLegacyDetailEncoding(t *testing.T) {
	var detail RequestDetail
	if err := json.Unmarshal([]byte(`{"source":"fixture","failed":false}`), &detail); err != nil || detail.Stream {
		t.Fatal("legacy detail did not retain the default response mode")
	}
	payload, err := json.Marshal(detail)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	if _, present := fields["stream"]; present {
		t.Fatal("legacy/default mode unnecessarily changed the stored JSON shape")
	}
}

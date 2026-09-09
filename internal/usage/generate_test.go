package usage

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func TestGenerateSurvivesHistoryWithoutChangingTotals(t *testing.T) {
	for _, format := range []string{"snapshot", "record-stream"} {
		t.Run(format, func(t *testing.T) {
			stats := NewRequestStatistics()
			now := time.Now().UTC()
			for index, flag := range []*bool{nil, coreusage.GenerateFlag(false), coreusage.GenerateFlag(true)} {
				stats.Record(t.Context(), coreusage.Record{APIKey: "fixture", Model: "fixture", RequestedAt: now.Add(time.Duration(index) * time.Second), Generate: flag, Detail: coreusage.Detail{TotalTokens: 1}})
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
			details := snapshot.APIs["fixture"].Models["fixture"].Details
			if len(details) != 3 || details[0].Generate != nil || details[1].Generate == nil || coreusage.GenerateEnabled(details[1].Generate) || details[2].Generate == nil || !coreusage.GenerateEnabled(details[2].Generate) || snapshot.TotalRequests != 3 || snapshot.TotalTokens != 3 {
				t.Fatal("history changed generation intent or request/token totals")
			}
		})
	}
}

func TestGenerateHistoryOwnsInputOutputAndImportedFlags(t *testing.T) {
	stats := NewRequestStatistics()
	flag := coreusage.GenerateFlag(false)
	stats.Record(t.Context(), coreusage.Record{APIKey: "fixture", Model: "fixture", Generate: flag})
	*flag = true
	snapshot := stats.Snapshot()
	if coreusage.GenerateEnabled(snapshot.APIs["fixture"].Models["fixture"].Details[0].Generate) {
		t.Fatal("history retained a caller-owned generation flag")
	}
	restored := NewRequestStatistics()
	restored.MergeSnapshot(snapshot)
	*snapshot.APIs["fixture"].Models["fixture"].Details[0].Generate = true
	for _, store := range []*RequestStatistics{stats, restored} {
		if coreusage.GenerateEnabled(store.Snapshot().APIs["fixture"].Models["fixture"].Details[0].Generate) {
			t.Fatal("external snapshot mutation changed stored generation intent")
		}
	}
}

func TestGenerateHistoryPreservesLegacyOmission(t *testing.T) {
	var detail RequestDetail
	if err := json.Unmarshal([]byte(`{"source":"fixture"}`), &detail); err != nil || !coreusage.GenerateEnabled(detail.Generate) {
		t.Fatal("old history lost the generation default")
	}
	payload, err := json.Marshal(detail)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["generate"]; ok {
		t.Fatal("old history gained an invented explicit generation value")
	}
}

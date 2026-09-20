package usage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func TestRequestRoutesSurviveFilteringImportAndPersistence(t *testing.T) {
	stats := NewRequestStatistics()
	now := time.Now().UTC()
	for _, route := range []coreusage.RequestMetadata{
		{Method: "POST", Path: "/v1/responses"},
		{Method: "GET", Path: "/v1/responses"},
		{Method: "POST", Path: "/v1/alpha/search"},
		{Method: "POST", Path: "/backend-api/codex/alpha/search"},
		{},
	} {
		ctx := coreusage.WithRequestMetadata(t.Context(), route)
		stats.Record(ctx, coreusage.Record{APIKey: "fixture-key", Model: "same-model", RequestedAt: now, Detail: coreusage.Detail{TotalTokens: 3}})
	}
	copyStats := NewRequestStatistics()
	if got := copyStats.MergeSnapshot(stats.Snapshot()); got.Added != 5 {
		t.Fatalf("distinct request routes collapsed during import: %+v", got)
	}
	if got := copyStats.MergeSnapshot(stats.Snapshot()); got.Added != 0 || got.Skipped != 5 {
		t.Fatalf("identical requests duplicated during import: %+v", got)
	}
	path := filepath.Join(t.TempDir(), StatisticsFileName)
	if _, err := RewriteRequestStatisticsWithPolicy(path, copyStats, PersistencePolicy{}); err != nil {
		t.Fatal(err)
	}
	loaded, prepared, _, err := PrepareRequestStatistics(context.Background(), path)
	if err != nil || !loaded {
		t.Fatalf("restore failed: %v", err)
	}
	first := prepared.Details(DetailQuery{RequestPath: " /alpha/search ", Limit: 1})
	second := prepared.Details(DetailQuery{RequestPath: "/alpha/search", Limit: 1, Offset: first.NextOffset})
	if !first.RequestPathSupported || first.TotalMatched != 2 || !first.HasMore || len(first.Items) != 1 || len(second.Items) != 1 || second.HasMore {
		t.Fatalf("path filtering did not apply before pagination: first=%+v second=%+v", first, second)
	}
	if first.Items[0].RequestMethod != "POST" || first.Items[0].RequestPath == second.Items[0].RequestPath {
		t.Fatal("request route was lost or pagination repeated a row")
	}
	if prepared.Details(DetailQuery{RequestPath: "absent"}).TotalMatched != 0 {
		t.Fatal("unknown routes matched a path filter")
	}
	if snapshot := prepared.Snapshot(); snapshot.TotalRequests != 5 || snapshot.TotalTokens != 15 {
		t.Fatal("route metadata changed usage totals")
	}
}

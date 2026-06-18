package usage

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func TestRequestStatisticsRecordIncludesLatency(t *testing.T) {
	stats := NewRequestStatistics()
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC),
		Latency:     1500 * time.Millisecond,
		Detail: coreusage.Detail{
			InputTokens:  10,
			OutputTokens: 20,
			TotalTokens:  30,
		},
	})

	snapshot := stats.Snapshot()
	details := snapshot.APIs["test-key"].Models["gpt-5.4"].Details
	if len(details) != 1 {
		t.Fatalf("details len = %d, want 1", len(details))
	}
	if details[0].LatencyMs != 1500 {
		t.Fatalf("latency_ms = %d, want 1500", details[0].LatencyMs)
	}
}

func TestRequestStatisticsRecordIncludesClientIP(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name       string
		remoteAddr string
		wantIP     string
	}{
		{name: "ipv4", remoteAddr: "203.0.113.10:54321", wantIP: "203.0.113.10"},
		{name: "ipv6", remoteAddr: "[2001:db8::1]:443", wantIP: "2001:db8::1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats := NewRequestStatistics()
			recorder := httptest.NewRecorder()
			ginCtx, _ := gin.CreateTestContext(recorder)
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
			req.RemoteAddr = tt.remoteAddr
			ginCtx.Request = req

			ctx := context.WithValue(context.Background(), "gin", ginCtx)
			stats.Record(ctx, coreusage.Record{
				APIKey:      "test-key",
				Model:       "gpt-5.4",
				RequestedAt: time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC),
				Detail: coreusage.Detail{
					InputTokens:  10,
					OutputTokens: 20,
					TotalTokens:  30,
				},
			})

			snapshot := stats.Snapshot()
			details := snapshot.APIs["test-key"].Models["gpt-5.4"].Details
			if len(details) != 1 {
				t.Fatalf("details len = %d, want 1", len(details))
			}
			if details[0].ClientIP != tt.wantIP {
				t.Fatalf("client_ip = %q, want %q", details[0].ClientIP, tt.wantIP)
			}
		})
	}
}

func TestRequestStatisticsRecordMatchesHTTPLogClientIP(t *testing.T) {
	gin.SetMode(gin.TestMode)

	stats := NewRequestStatistics()
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.RemoteAddr = "203.0.113.10:54321"
	req.Header.Set("X-Forwarded-For", "198.51.100.8")
	ginCtx.Request = req

	expectedIP := logging.ResolveClientIP(ginCtx)
	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	stats.Record(ctx, coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC),
		Detail: coreusage.Detail{
			InputTokens:  10,
			OutputTokens: 20,
			TotalTokens:  30,
		},
	})

	snapshot := stats.Snapshot()
	details := snapshot.APIs["test-key"].Models["gpt-5.4"].Details
	if len(details) != 1 {
		t.Fatalf("details len = %d, want 1", len(details))
	}
	if details[0].ClientIP != expectedIP {
		t.Fatalf("client_ip = %q, want same as HTTP log %q", details[0].ClientIP, expectedIP)
	}
}

func TestRequestStatisticsMergeSnapshotDedupIgnoresLatency(t *testing.T) {
	stats := NewRequestStatistics()
	timestamp := time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC)
	first := StatisticsSnapshot{
		APIs: map[string]APISnapshot{
			"test-key": {
				Models: map[string]ModelSnapshot{
					"gpt-5.4": {
						Details: []RequestDetail{{
							Timestamp: timestamp,
							LatencyMs: 0,
							Source:    "user@example.com",
							AuthIndex: "0",
							Tokens: TokenStats{
								InputTokens:  10,
								OutputTokens: 20,
								TotalTokens:  30,
							},
						}},
					},
				},
			},
		},
	}
	second := StatisticsSnapshot{
		APIs: map[string]APISnapshot{
			"test-key": {
				Models: map[string]ModelSnapshot{
					"gpt-5.4": {
						Details: []RequestDetail{{
							Timestamp: timestamp,
							LatencyMs: 2500,
							Source:    "user@example.com",
							AuthIndex: "0",
							Tokens: TokenStats{
								InputTokens:  10,
								OutputTokens: 20,
								TotalTokens:  30,
							},
						}},
					},
				},
			},
		},
	}

	result := stats.MergeSnapshot(first)
	if result.Added != 1 || result.Skipped != 0 {
		t.Fatalf("first merge = %+v, want added=1 skipped=0", result)
	}

	result = stats.MergeSnapshot(second)
	if result.Added != 0 || result.Skipped != 1 {
		t.Fatalf("second merge = %+v, want added=0 skipped=1", result)
	}

	snapshot := stats.Snapshot()
	details := snapshot.APIs["test-key"].Models["gpt-5.4"].Details
	if len(details) != 1 {
		t.Fatalf("details len = %d, want 1", len(details))
	}
}

func TestRequestStatisticsMergeSnapshotDedupIncludesClientIP(t *testing.T) {
	stats := NewRequestStatistics()
	timestamp := time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC)
	first := StatisticsSnapshot{
		APIs: map[string]APISnapshot{
			"test-key": {
				Models: map[string]ModelSnapshot{
					"gpt-5.4": {
						Details: []RequestDetail{{
							Timestamp: timestamp,
							Source:    "user@example.com",
							ClientIP:  "203.0.113.10",
							AuthIndex: "0",
							Tokens: TokenStats{
								InputTokens:  10,
								OutputTokens: 20,
								TotalTokens:  30,
							},
						}},
					},
				},
			},
		},
	}
	second := StatisticsSnapshot{
		APIs: map[string]APISnapshot{
			"test-key": {
				Models: map[string]ModelSnapshot{
					"gpt-5.4": {
						Details: []RequestDetail{{
							Timestamp: timestamp,
							Source:    "user@example.com",
							ClientIP:  "198.51.100.8",
							AuthIndex: "0",
							Tokens: TokenStats{
								InputTokens:  10,
								OutputTokens: 20,
								TotalTokens:  30,
							},
						}},
					},
				},
			},
		},
	}

	result := stats.MergeSnapshot(first)
	if result.Added != 1 || result.Skipped != 0 {
		t.Fatalf("first merge = %+v, want added=1 skipped=0", result)
	}

	result = stats.MergeSnapshot(second)
	if result.Added != 1 || result.Skipped != 0 {
		t.Fatalf("second merge = %+v, want added=1 skipped=0", result)
	}

	snapshot := stats.Snapshot()
	details := snapshot.APIs["test-key"].Models["gpt-5.4"].Details
	if len(details) != 2 {
		t.Fatalf("details len = %d, want 2", len(details))
	}
}

func TestRequestStatisticsRemoveAuthIndexesRebuildsTotals(t *testing.T) {
	stats := NewRequestStatistics()
	timestamp := time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC)

	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: timestamp,
		Detail: coreusage.Detail{
			TotalTokens: 10,
		},
		AuthIndex: "auth-a",
	})
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: timestamp.Add(time.Minute),
		Failed:      true,
		Detail: coreusage.Detail{
			TotalTokens: 20,
		},
		AuthIndex: "auth-b",
	})

	removed := stats.RemoveAuthIndexes([]string{"auth-a"})
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}

	snapshot := stats.Snapshot()
	if snapshot.TotalRequests != 1 {
		t.Fatalf("total_requests = %d, want 1", snapshot.TotalRequests)
	}
	if snapshot.SuccessCount != 0 {
		t.Fatalf("success_count = %d, want 0", snapshot.SuccessCount)
	}
	if snapshot.FailureCount != 1 {
		t.Fatalf("failure_count = %d, want 1", snapshot.FailureCount)
	}
	if snapshot.TotalTokens != 20 {
		t.Fatalf("total_tokens = %d, want 20", snapshot.TotalTokens)
	}
	details := snapshot.APIs["test-key"].Models["gpt-5.4"].Details
	if len(details) != 1 || details[0].AuthIndex != "auth-b" {
		t.Fatalf("remaining details = %+v, want only auth-b", details)
	}
}

func TestRequestStatisticsPruneAuthIndexesRemovesStaleEntries(t *testing.T) {
	stats := NewRequestStatistics()
	timestamp := time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC)

	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: timestamp,
		Detail: coreusage.Detail{
			TotalTokens: 10,
		},
		AuthIndex: "keep-auth",
	})
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: timestamp.Add(time.Minute),
		Detail: coreusage.Detail{
			TotalTokens: 20,
		},
		AuthIndex: "stale-auth",
	})

	removed := stats.PruneAuthIndexes(map[string]struct{}{
		"keep-auth": {},
	})
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}

	snapshot := stats.Snapshot()
	if snapshot.TotalRequests != 1 || snapshot.TotalTokens != 10 {
		t.Fatalf("snapshot totals = %+v, want requests=1 tokens=10", snapshot)
	}
}

func TestRequestStatisticsSummaryOmitsDetails(t *testing.T) {
	stats := NewRequestStatistics()
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC),
		Detail: coreusage.Detail{
			InputTokens:  10,
			OutputTokens: 20,
			TotalTokens:  30,
		},
		AuthIndex: "auth-a",
	})

	summary := stats.Summary()
	if summary.TotalRequests != 1 || summary.TotalTokens != 30 {
		t.Fatalf("summary totals = %+v, want requests=1 tokens=30", summary)
	}
	model := summary.APIs["test-key"].Models["gpt-5.4"]
	if model.TotalRequests != 1 || model.TotalTokens != 30 {
		t.Fatalf("model summary = %+v, want requests=1 tokens=30", model)
	}
	raw, err := json.Marshal(summary)
	if err != nil {
		t.Fatalf("Marshal(summary) error = %v", err)
	}
	if strings.Contains(string(raw), "details") {
		t.Fatalf("summary JSON contains details: %s", raw)
	}
}

func TestRequestStatisticsAuthSummariesAndModels(t *testing.T) {
	stats := NewRequestStatistics()
	timestamp := time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC)

	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: timestamp,
		Detail: coreusage.Detail{
			InputTokens:  10,
			OutputTokens: 20,
			TotalTokens:  30,
		},
		AuthIndex: "auth-a",
	})
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.5",
		RequestedAt: timestamp.Add(time.Minute),
		Failed:      true,
		Detail: coreusage.Detail{
			InputTokens:     5,
			OutputTokens:    6,
			ReasoningTokens: 7,
			CachedTokens:    8,
			TotalTokens:     26,
		},
		AuthIndex: "auth-a",
	})

	summary, ok := stats.AuthSummary("auth-a")
	if !ok {
		t.Fatal("AuthSummary(auth-a) ok = false, want true")
	}
	if summary.TotalRequests != 2 || summary.SuccessCount != 1 || summary.FailureCount != 1 || summary.TotalTokens != 56 {
		t.Fatalf("auth summary = %+v, want requests=2 success=1 failure=1 tokens=56", summary)
	}
	if summary.Tokens.InputTokens != 15 || summary.Tokens.OutputTokens != 26 || summary.Tokens.ReasoningTokens != 7 || summary.Tokens.CachedTokens != 8 {
		t.Fatalf("auth token breakdown = %+v, want input=15 output=26 reasoning=7 cached=8", summary.Tokens)
	}

	models, ok := stats.AuthModelSummaries("auth-a")
	if !ok {
		t.Fatal("AuthModelSummaries(auth-a) ok = false, want true")
	}
	if len(models) != 2 {
		t.Fatalf("models len = %d, want 2: %+v", len(models), models)
	}
	if models[0].Model != "gpt-5.4" || models[0].TotalRequests != 1 || models[0].SuccessCount != 1 || models[0].TotalTokens != 30 {
		t.Fatalf("first model summary = %+v, want gpt-5.4 success tokens=30", models[0])
	}
	if models[1].Model != "gpt-5.5" || models[1].TotalRequests != 1 || models[1].FailureCount != 1 || models[1].TotalTokens != 26 {
		t.Fatalf("second model summary = %+v, want gpt-5.5 failure tokens=26", models[1])
	}
}

func TestRequestStatisticsMergeSnapshotRestoresAuthSummaries(t *testing.T) {
	stats := NewRequestStatistics()
	timestamp := time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC)

	result := stats.MergeSnapshot(StatisticsSnapshot{
		APIs: map[string]APISnapshot{
			"test-key": {
				Models: map[string]ModelSnapshot{
					"gpt-5.4": {
						Details: []RequestDetail{{
							Timestamp: timestamp,
							AuthIndex: "auth-a",
							Tokens: TokenStats{
								InputTokens: 10,
								TotalTokens: 10,
							},
						}},
					},
				},
			},
		},
	})
	if result.Added != 1 || result.Skipped != 0 {
		t.Fatalf("MergeSnapshot() = %+v, want added=1 skipped=0", result)
	}

	summary, ok := stats.AuthSummary("auth-a")
	if !ok {
		t.Fatal("AuthSummary(auth-a) ok = false, want true")
	}
	if summary.TotalRequests != 1 || summary.SuccessCount != 1 || summary.TotalTokens != 10 {
		t.Fatalf("auth summary = %+v, want requests=1 success=1 tokens=10", summary)
	}
}

func TestRequestStatisticsRemoveAndPruneRebuildAuthSummaries(t *testing.T) {
	stats := NewRequestStatistics()
	timestamp := time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC)

	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: timestamp,
		Detail: coreusage.Detail{
			TotalTokens: 10,
		},
		AuthIndex: "auth-a",
	})
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: timestamp.Add(time.Minute),
		Detail: coreusage.Detail{
			TotalTokens: 20,
		},
		AuthIndex: "auth-b",
	})
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: timestamp.Add(2 * time.Minute),
		Detail: coreusage.Detail{
			TotalTokens: 30,
		},
		AuthIndex: "auth-c",
	})

	if removed := stats.RemoveAuthIndexes([]string{"auth-a"}); removed != 1 {
		t.Fatalf("RemoveAuthIndexes() = %d, want 1", removed)
	}
	if _, ok := stats.AuthSummary("auth-a"); ok {
		t.Fatal("AuthSummary(auth-a) ok = true after remove, want false")
	}
	if summary, ok := stats.AuthSummary("auth-b"); !ok || summary.TotalTokens != 20 {
		t.Fatalf("AuthSummary(auth-b) = %+v, %t; want tokens=20 ok=true", summary, ok)
	}

	if removed := stats.PruneAuthIndexes(map[string]struct{}{"auth-b": {}}); removed != 1 {
		t.Fatalf("PruneAuthIndexes() = %d, want 1", removed)
	}
	if _, ok := stats.AuthSummary("auth-c"); ok {
		t.Fatal("AuthSummary(auth-c) ok = true after prune, want false")
	}
	if summary, ok := stats.AuthSummary("auth-b"); !ok || summary.TotalRequests != 1 || summary.TotalTokens != 20 {
		t.Fatalf("AuthSummary(auth-b) = %+v, %t; want one request and 20 tokens", summary, ok)
	}
}

func TestRequestStatisticsDetailsFiltersAndPaginates(t *testing.T) {
	stats := NewRequestStatistics()
	timestamp := time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC)

	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		Source:      "source-a",
		RequestedAt: timestamp,
		Detail:      coreusage.Detail{TotalTokens: 10},
		AuthIndex:   "auth-a",
	})
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		Source:      "source-a",
		RequestedAt: timestamp.Add(time.Minute),
		Detail:      coreusage.Detail{TotalTokens: 20},
		AuthIndex:   "auth-a",
	})
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.5",
		Source:      "source-b",
		RequestedAt: timestamp.Add(2 * time.Minute),
		Detail:      coreusage.Detail{TotalTokens: 30},
		AuthIndex:   "auth-b",
	})

	page := stats.Details(DetailQuery{AuthIndex: "auth-a", Limit: 1})
	if page.TotalMatched != 2 || !page.HasMore || page.NextOffset != 1 || len(page.Details) != 1 {
		t.Fatalf("first page = %+v, want two matches with one returned and next offset", page)
	}
	if page.Details[0].AuthIndex != "auth-a" || page.Details[0].Tokens.TotalTokens != 20 {
		t.Fatalf("first page detail = %+v, want newest auth-a detail with 20 tokens", page.Details[0])
	}

	page = stats.Details(DetailQuery{AuthIndex: "auth-a", Limit: 1, Offset: 1})
	if page.TotalMatched != 2 || page.HasMore || len(page.Details) != 1 {
		t.Fatalf("second page = %+v, want one final match", page)
	}
	if page.Details[0].Tokens.TotalTokens != 10 {
		t.Fatalf("second page detail tokens = %d, want 10", page.Details[0].Tokens.TotalTokens)
	}

	failed := true
	page = stats.Details(DetailQuery{AuthIndex: "auth-a", Failed: &failed})
	if page.TotalMatched != 0 || len(page.Details) != 0 {
		t.Fatalf("failed page = %+v, want no auth-a failed requests", page)
	}
}

func TestPersistAndRestoreRequestStatistics(t *testing.T) {
	dir := t.TempDir()
	path := StatisticsFilePath(&config.Config{LoggingToFile: true, AuthDir: dir})
	if filepath.Base(path) != StatisticsFileName {
		t.Fatalf("StatisticsFilePath() = %s, want base %s", path, StatisticsFileName)
	}

	stats := NewRequestStatistics()
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC),
		Detail: coreusage.Detail{
			TotalTokens: 30,
		},
		AuthIndex: "auth-a",
	})

	saved, err := PersistRequestStatistics(path, stats)
	if err != nil {
		t.Fatalf("PersistRequestStatistics() error = %v", err)
	}
	if !saved {
		t.Fatal("PersistRequestStatistics() saved = false, want true")
	}

	restored := NewRequestStatistics()
	loaded, result, err := RestoreRequestStatistics(path, restored)
	if err != nil {
		t.Fatalf("RestoreRequestStatistics() error = %v", err)
	}
	if !loaded {
		t.Fatal("RestoreRequestStatistics() loaded = false, want true")
	}
	if result.Added != 1 {
		t.Fatalf("RestoreRequestStatistics() result = %+v, want added=1", result)
	}

	snapshot := restored.Snapshot()
	if snapshot.TotalRequests != 1 || snapshot.TotalTokens != 30 {
		t.Fatalf("restored snapshot = %+v, want requests=1 tokens=30", snapshot)
	}
}

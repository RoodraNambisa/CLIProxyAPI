package usage

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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

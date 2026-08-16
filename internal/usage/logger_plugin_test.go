package usage

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
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

func TestRequestStatisticsRecordIncludesExecutionDiagnostics(t *testing.T) {
	stats := NewRequestStatistics()
	stats.Record(context.Background(), coreusage.Record{
		APIKey:                  "test-key",
		Model:                   "gpt-image-2",
		RequestedAt:             time.Now().UTC(),
		Failed:                  true,
		FailureStage:            "upstream",
		ErrorCode:               "rate_limit_exceeded",
		CredentialSelected:      true,
		UpstreamCommitted:       true,
		AuthRequestSlotConsumed: true,
	})

	details := stats.Snapshot().APIs["test-key"].Models["gpt-image-2"].Details
	if len(details) != 1 {
		t.Fatalf("details len = %d, want 1", len(details))
	}
	detail := details[0]
	if detail.FailureStage != "upstream" || detail.ErrorCode != "rate_limit_exceeded" {
		t.Fatalf("failure diagnostics = stage:%q code:%q", detail.FailureStage, detail.ErrorCode)
	}
	if !detail.CredentialSelected || !detail.UpstreamCommitted || !detail.AuthRequestSlotConsumed {
		t.Fatalf("execution ownership diagnostics = %+v", detail)
	}
}

func TestRequestStatisticsFailureSummaryAggregatesSafeDimensions(t *testing.T) {
	stats := NewRequestStatistics()
	base := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	records := []coreusage.Record{
		{
			APIKey: "api-a", Model: "gpt-image-2", Source: "chatgpt-web", RequestedAt: base,
			Failed: true, ErrorCode: "http_401", FailureStage: "upstream",
			CredentialSelected: true, UpstreamCommitted: true, AuthRequestSlotConsumed: true,
		},
		{
			APIKey: "api-a", Model: "gpt-image-2", Source: "chatgpt-web", RequestedAt: base.Add(10 * time.Minute),
			Failed: true, Auxiliary: true, ErrorCode: "http_401", FailureStage: "upstream",
			CredentialSelected: true,
		},
		{
			APIKey: "api-a", Model: "gpt-5", Source: "codex", RequestedAt: base.Add(time.Hour),
			Failed: true, ErrorCode: "auth_request_limited", FailureStage: "selection",
		},
		{APIKey: "api-a", Model: "gpt-image-2", Source: "chatgpt-web", RequestedAt: base.Add(20 * time.Minute)},
	}
	for _, record := range records {
		stats.Record(context.Background(), record)
	}

	summary := stats.FailureSummary(FailureSummaryQuery{
		API:       "api-a",
		Source:    "chatgpt-web",
		TimeRange: TimeRange{From: base, To: base.Add(time.Hour)},
	})
	if summary.Total != 2 || summary.Main != 1 || summary.Auxiliary != 1 {
		t.Fatalf("failure totals = total:%d main:%d auxiliary:%d", summary.Total, summary.Main, summary.Auxiliary)
	}
	if summary.Boundaries.CredentialSelected != 2 || summary.Boundaries.UpstreamCommitted != 1 || summary.Boundaries.AuthRequestSlotConsumed != 1 {
		t.Fatalf("failure boundaries = %+v", summary.Boundaries)
	}
	if len(summary.ByErrorCode) != 1 || summary.ByErrorCode[0].Value != "http_401" || summary.ByErrorCode[0].Count != 2 || summary.ByErrorCode[0].Percent != 100 {
		t.Fatalf("error distribution = %+v", summary.ByErrorCode)
	}
	if len(summary.ByHour) != 1 || summary.ByHour[0].Value != "2026-08-16T12:00:00Z" || summary.ByHour[0].Count != 2 {
		t.Fatalf("hour distribution = %+v", summary.ByHour)
	}
}

func TestRequestStatisticsNormalizesNegativeRecordTokens(t *testing.T) {
	stats := NewRequestStatistics()
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: time.Now().UTC(),
		Detail: coreusage.Detail{
			InputTokens:         -1,
			OutputTokens:        -2,
			ReasoningTokens:     -3,
			CachedTokens:        -4,
			CacheCreationTokens: -5,
			TotalTokens:         -6,
		},
	})
	assertUsageTokensAreZero(t, stats)
}

func TestRequestStatisticsAuxiliaryUsageKeepsTokenAndCostAttributionWithoutCountingAnotherRequest(t *testing.T) {
	stats := NewRequestStatistics()
	requestedAt := time.Now().UTC().Truncate(time.Minute).Add(-time.Second)
	primary := coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		AuthIndex:   "auth-a",
		Source:      "chatgpt-web",
		RequestedAt: requestedAt,
		Detail: coreusage.Detail{
			InputTokens:  10,
			OutputTokens: 2,
			TotalTokens:  12,
		},
	}
	auxiliary := coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-image-2",
		AuthIndex:   "auth-a",
		Source:      "chatgpt-web",
		RequestedAt: requestedAt,
		Auxiliary:   true,
		Detail: coreusage.Detail{
			InputTokens:  3,
			OutputTokens: 48,
			TotalTokens:  51,
		},
	}
	stats.Record(context.Background(), primary)
	stats.Record(context.Background(), auxiliary)

	summary := stats.Summary()
	if summary.TotalRequests != 1 || summary.SuccessCount != 1 || summary.FailureCount != 0 || summary.TotalTokens != 63 {
		t.Fatalf("summary totals = %+v, want requests=1 success=1 tokens=63", summary)
	}
	if model := summary.Models["gpt-5.4"]; model.TotalRequests != 1 || model.TotalTokens != 12 {
		t.Fatalf("primary model summary = %+v", model)
	}
	if model := summary.Models["gpt-image-2"]; model.TotalRequests != 0 || model.SuccessCount != 0 || model.TotalTokens != 51 {
		t.Fatalf("auxiliary model summary = %+v, want request counters zero and tokens=51", model)
	}
	if auth, ok := stats.AuthSummary("auth-a"); !ok || auth.TotalRequests != 1 || auth.TotalTokens != 63 {
		t.Fatalf("auth summary = %+v, ok=%t", auth, ok)
	}

	rates := stats.ratesAt(RatesQuery{WindowMinutes: 1, SparklineMinutes: 1}, requestedAt.Add(30*time.Second))
	if rates.RequestCount != 1 || rates.TokenCount != 63 || rates.RPM != 1 {
		t.Fatalf("rates = %+v, want one request and all tokens", rates)
	}

	series := stats.Series(SeriesQuery{
		TimeRange: TimeRange{From: requestedAt.Add(-time.Second), To: requestedAt.Add(time.Second)},
		Bucket:    BucketMinute,
		GroupBy:   GroupByModel,
	})
	var imageSeries *SeriesEntry
	for index := range series.Items {
		if series.Items[index].Group == "gpt-image-2" {
			imageSeries = &series.Items[index]
			break
		}
	}
	if imageSeries == nil || imageSeries.TotalRequests != 0 || imageSeries.TotalTokens != 51 {
		t.Fatalf("image model series = %+v, want requests=0 tokens=51", imageSeries)
	}

	costs := stats.Costs(CostQuery{
		TimeRange: TimeRange{From: requestedAt.Add(-time.Second), To: requestedAt.Add(time.Second)},
		Bucket:    BucketHour,
		Prices: map[string]ModelPrice{
			"gpt-5.4":     {InputPerMillion: 1, OutputPerMillion: 2},
			"gpt-image-2": {InputPerMillion: 1, OutputPerMillion: 3},
		},
	})
	if costs.Coverage.TotalRequests != 1 || costs.TotalTokens != 63 || costs.Total.AmountMicros != 161 {
		t.Fatalf("costs = %+v, want requests=1 tokens=63 amount_micros=161", costs)
	}

	snapshot := stats.Snapshot()
	imageDetails := snapshot.APIs["test-key"].Models["gpt-image-2"].Details
	if len(imageDetails) != 1 || !imageDetails[0].Auxiliary {
		t.Fatalf("image details = %+v, want persisted auxiliary marker", imageDetails)
	}
	raw, errMarshal := json.Marshal(snapshot)
	if errMarshal != nil {
		t.Fatalf("Marshal(snapshot) error = %v", errMarshal)
	}
	if !strings.Contains(string(raw), `"auxiliary":true`) {
		t.Fatalf("snapshot omitted auxiliary marker: %s", raw)
	}

	restored := NewRequestStatistics()
	var decoded StatisticsSnapshot
	if errUnmarshal := json.Unmarshal(raw, &decoded); errUnmarshal != nil {
		t.Fatalf("Unmarshal(snapshot) error = %v", errUnmarshal)
	}
	if result := restored.MergeSnapshot(decoded); result.Added != 2 {
		t.Fatalf("MergeSnapshot() = %+v, want two detail rows", result)
	}
	if restoredSummary := restored.Summary(); restoredSummary.TotalRequests != 1 || restoredSummary.TotalTokens != 63 {
		t.Fatalf("restored summary = %+v, want requests=1 tokens=63", restoredSummary)
	}
}

func TestRequestStatisticsLegacySnapshotWithoutAuxiliaryFieldStillCountsAsRequest(t *testing.T) {
	var snapshot StatisticsSnapshot
	raw := []byte(`{"apis":{"test-key":{"models":{"gpt-image-2":{"details":[{"timestamp":"2026-03-20T12:00:00Z","tokens":{"output_tokens":48,"total_tokens":48},"failed":false}]}}}}}`)
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatalf("Unmarshal(legacy snapshot) error = %v", err)
	}
	stats := NewRequestStatistics()
	if result := stats.MergeSnapshot(snapshot); result.Added != 1 {
		t.Fatalf("MergeSnapshot() = %+v, want one detail row", result)
	}
	summary := stats.Summary()
	if summary.TotalRequests != 1 || summary.SuccessCount != 1 || summary.TotalTokens != 48 {
		t.Fatalf("legacy summary = %+v, want ordinary request semantics", summary)
	}
}

func TestMergeSnapshotNormalizesNegativeTokens(t *testing.T) {
	stats := NewRequestStatistics()
	result := stats.MergeSnapshot(StatisticsSnapshot{APIs: map[string]APISnapshot{
		"test-key": {
			Models: map[string]ModelSnapshot{
				"gpt-5.4": {Details: []RequestDetail{{
					Timestamp: time.Now().UTC(),
					Tokens: TokenStats{
						InputTokens:         -1,
						OutputTokens:        -2,
						ReasoningTokens:     -3,
						CachedTokens:        -4,
						CacheCreationTokens: -5,
						TotalTokens:         -6,
					},
				}}},
			},
		},
	}})
	if result.Added != 1 {
		t.Fatalf("merge result = %+v, want one added detail", result)
	}
	assertUsageTokensAreZero(t, stats)
}

func TestRebuildNormalizesNegativeTokens(t *testing.T) {
	stats := NewRequestStatistics()
	stats.apis["test-key"] = &apiStats{Models: map[string]*modelStats{
		"gpt-5.4": {Details: []RequestDetail{{
			Timestamp: time.Now().UTC(),
			Tokens: TokenStats{
				InputTokens:         -1,
				OutputTokens:        -2,
				ReasoningTokens:     -3,
				CachedTokens:        -4,
				CacheCreationTokens: -5,
				TotalTokens:         -6,
			},
		}}},
	}}
	stats.mu.Lock()
	stats.rebuildLocked()
	stats.mu.Unlock()
	assertUsageTokensAreZero(t, stats)
}

func assertUsageTokensAreZero(t *testing.T, stats *RequestStatistics) {
	t.Helper()
	summary := stats.Summary()
	if summary.TotalRequests != 1 || summary.TotalTokens != 0 || summary.Tokens != (TokenStats{}) {
		t.Fatalf("summary = %+v, want one zero-token request", summary)
	}
	apiSummary := summary.APIs["test-key"]
	modelSummary := apiSummary.Models["gpt-5.4"]
	if apiSummary.TotalTokens != 0 || apiSummary.Tokens != (TokenStats{}) || modelSummary.TotalTokens != 0 || modelSummary.Tokens != (TokenStats{}) {
		t.Fatalf("API/model summaries contain negative tokens: api=%+v model=%+v", apiSummary, modelSummary)
	}
	details := stats.Snapshot().APIs["test-key"].Models["gpt-5.4"].Details
	if len(details) != 1 || details[0].Tokens != (TokenStats{}) {
		t.Fatalf("details = %+v, want one normalized zero-token detail", details)
	}
}

func TestRequestStatisticsAggregatesCacheCreationAndServiceTiers(t *testing.T) {
	stats := NewRequestStatistics()
	timestamp := time.Date(2026, 7, 11, 8, 0, 0, 0, time.UTC)
	stats.Record(context.Background(), coreusage.Record{
		APIKey:              "test-key",
		Model:               "gpt-5.4",
		AuthIndex:           "auth-1",
		RequestServiceTier:  "priority",
		ResponseServiceTier: "flex",
		RequestedAt:         timestamp,
		Detail: coreusage.Detail{
			InputTokens:         10,
			OutputTokens:        20,
			CacheCreationTokens: 7,
			TotalTokens:         30,
		},
	})

	detail := stats.Snapshot().APIs["test-key"].Models["gpt-5.4"].Details[0]
	if detail.RequestServiceTier != "priority" || detail.ResponseServiceTier != "flex" {
		t.Fatalf("unexpected service tiers: request=%q response=%q", detail.RequestServiceTier, detail.ResponseServiceTier)
	}
	if detail.Tokens.CacheCreationTokens != 7 {
		t.Fatalf("detail cache_creation_tokens = %d, want 7", detail.Tokens.CacheCreationTokens)
	}

	authSummary, ok := stats.AuthSummary("auth-1")
	if !ok || authSummary.Tokens.CacheCreationTokens != 7 {
		t.Fatalf("unexpected auth summary: %+v ok=%v", authSummary, ok)
	}
	series := stats.Series(SeriesQuery{TimeRange: TimeRange{From: timestamp.Add(-time.Minute), To: timestamp.Add(time.Minute)}, Bucket: BucketHour, GroupBy: GroupByAuthIndex})
	if len(series.Items) != 1 || series.Items[0].Tokens.CacheCreationTokens != 7 {
		t.Fatalf("unexpected series: %+v", series.Items)
	}
}

func TestRequestStatisticsOldJSONDefaultsNewUsageFields(t *testing.T) {
	var snapshot StatisticsSnapshot
	oldJSON := []byte(`{"apis":{"test-key":{"models":{"gpt-5.4":{"details":[{"timestamp":"2026-07-11T08:00:00Z","auth_index":"auth-1","tokens":{"input_tokens":1,"output_tokens":2,"total_tokens":3},"failed":false}]}}}}}`)
	if err := json.Unmarshal(oldJSON, &snapshot); err != nil {
		t.Fatalf("unmarshal old snapshot: %v", err)
	}
	stats := NewRequestStatistics()
	result := stats.MergeSnapshot(snapshot)
	if result.Added != 1 {
		t.Fatalf("merge result = %+v, want one added", result)
	}
	detail := stats.Snapshot().APIs["test-key"].Models["gpt-5.4"].Details[0]
	if detail.Tokens.CacheCreationTokens != 0 || detail.RequestServiceTier != "" || detail.ResponseServiceTier != "" {
		t.Fatalf("new fields should use zero values for old JSON: %+v", detail)
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

func TestRequestStatisticsAsyncRecordFlushesBeforeRead(t *testing.T) {
	stats := newRequestStatistics(true)
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC),
		Detail:      coreusage.Detail{TotalTokens: 17},
		AuthIndex:   "auth-a",
	})

	summary := stats.Summary()
	if summary.TotalRequests != 1 || summary.TotalTokens != 17 {
		t.Fatalf("summary = %+v, want one request and 17 tokens", summary)
	}
	runtime := stats.batch.snapshot()
	if runtime.QueueDepth != 0 || runtime.ProcessedRecords != 1 || runtime.Flushes == 0 {
		t.Fatalf("batch runtime = %+v, want flushed record", runtime)
	}
}

func TestRequestStatisticsRemoveAuthIndexesPreservesDerivedViews(t *testing.T) {
	stats := NewRequestStatistics()
	base := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Minute)
	records := []coreusage.Record{
		{APIKey: "api-a", Model: "model-a", Source: "source-a", RequestedAt: base, Detail: coreusage.Detail{InputTokens: 3, OutputTokens: 7, TotalTokens: 10}, AuthIndex: "remove"},
		{APIKey: "api-a", Model: "model-a", Source: "source-a", RequestedAt: base.Add(time.Minute), Failed: true, Detail: coreusage.Detail{InputTokens: 4, OutputTokens: 16, TotalTokens: 20}, AuthIndex: "keep"},
		{APIKey: "api-b", Model: "model-b", Source: "source-b", RequestedAt: base.Add(2 * time.Minute), Detail: coreusage.Detail{InputTokens: 5, OutputTokens: 25, TotalTokens: 30}, AuthIndex: "remove"},
		{APIKey: "api-b", Model: "model-b", Source: "source-b", RequestedAt: base.Add(3 * time.Minute), Detail: coreusage.Detail{InputTokens: 6, OutputTokens: 34, TotalTokens: 40}, AuthIndex: "keep"},
	}
	for _, record := range records {
		stats.Record(context.Background(), record)
	}
	expected := NewRequestStatistics()
	for _, record := range records {
		if record.AuthIndex == "keep" {
			expected.Record(context.Background(), record)
		}
	}

	if removed := stats.RemoveAuthIndexes([]string{"remove"}); removed != 2 {
		t.Fatalf("RemoveAuthIndexes() = %d, want 2", removed)
	}
	if got, want := stats.Snapshot(), expected.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot mismatch after incremental removal\ngot:  %+v\nwant: %+v", got, want)
	}
	gotSummary, wantSummary := stats.Summary(), expected.Summary()
	wantSummary.Version = gotSummary.Version
	if !reflect.DeepEqual(gotSummary, wantSummary) {
		t.Fatalf("summary mismatch after incremental removal\ngot:  %+v\nwant: %+v", gotSummary, wantSummary)
	}
	gotDetails := stats.Details(DetailQuery{Limit: 10})
	wantDetails := expected.Details(DetailQuery{Limit: 10})
	if !reflect.DeepEqual(gotDetails, wantDetails) {
		t.Fatalf("details mismatch after incremental removal\ngot:  %+v\nwant: %+v", gotDetails, wantDetails)
	}
}

func TestRequestStatisticsRemoveAuthIndexesPreservesZeroTokenAuxiliaryViews(t *testing.T) {
	stats := NewRequestStatistics()
	base := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Minute)
	records := []coreusage.Record{
		{APIKey: "api-a", Model: "model-a", Source: "source-a", RequestedAt: base, Auxiliary: true, AuthIndex: "keep"},
		{APIKey: "api-a", Model: "model-a", Source: "source-a", RequestedAt: base.Add(time.Minute), Auxiliary: true, AuthIndex: "remove"},
	}
	for _, record := range records {
		stats.Record(context.Background(), record)
	}
	expected := NewRequestStatistics()
	expected.Record(context.Background(), records[0])

	if removed := stats.RemoveAuthIndexes([]string{"remove"}); removed != 1 {
		t.Fatalf("RemoveAuthIndexes() = %d, want 1", removed)
	}
	gotSummary, wantSummary := stats.Summary(), expected.Summary()
	wantSummary.Version = gotSummary.Version
	if !reflect.DeepEqual(gotSummary, wantSummary) {
		t.Fatalf("summary mismatch after zero-token auxiliary removal\ngot:  %+v\nwant: %+v", gotSummary, wantSummary)
	}
	if got, want := stats.Details(DetailQuery{Limit: 10}), expected.Details(DetailQuery{Limit: 10}); !reflect.DeepEqual(got, want) {
		t.Fatalf("details mismatch after zero-token auxiliary removal\ngot:  %+v\nwant: %+v", got, want)
	}
}

func TestRequestStatisticsClearResetsStatsAndBumpsVersion(t *testing.T) {
	stats := NewRequestStatistics()
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC),
		Detail:      coreusage.Detail{TotalTokens: 10},
		AuthIndex:   "auth-a",
	})
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.5",
		RequestedAt: time.Date(2026, 3, 20, 12, 1, 0, 0, time.UTC),
		Failed:      true,
		Detail:      coreusage.Detail{TotalTokens: 20},
		AuthIndex:   "auth-b",
	})
	_, versionBefore, _ := stats.SnapshotWithState()

	previous := stats.Clear()
	if previous.TotalRequests != 2 || previous.SuccessCount != 1 || previous.FailureCount != 1 || previous.TotalTokens != 30 {
		t.Fatalf("previous snapshot = %+v, want requests=2 success=1 failure=1 tokens=30", previous)
	}

	snapshot, versionAfter, _ := stats.SnapshotWithState()
	if versionAfter <= versionBefore {
		t.Fatalf("version after clear = %d, want > %d", versionAfter, versionBefore)
	}
	if snapshot.TotalRequests != 0 || snapshot.SuccessCount != 0 || snapshot.FailureCount != 0 || snapshot.TotalTokens != 0 {
		t.Fatalf("snapshot after clear = %+v, want zero totals", snapshot)
	}
	if len(snapshot.APIs) != 0 || len(snapshot.RequestsByDay) != 0 || len(snapshot.RequestsByHour) != 0 || len(snapshot.TokensByDay) != 0 || len(snapshot.TokensByHour) != 0 {
		t.Fatalf("snapshot after clear still has aggregates: %+v", snapshot)
	}
	if auths := stats.AuthSummaries(); len(auths) != 0 {
		t.Fatalf("auth summaries after clear = %+v, want empty", auths)
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
	snapshot := stats.Snapshot()
	details := snapshot.APIs["test-key"].Models["gpt-5.4"].Details
	if len(details) != 1 || details[0].AuthIndex != "auth-b" {
		t.Fatalf("remaining details = %+v, want only auth-b", details)
	}
	stats.mu.RLock()
	for detailID, location := range stats.detailLocations {
		modelStatsValue := stats.apis[location.API].Models[location.Model]
		if location.Offset < 0 || location.Offset >= len(modelStatsValue.Details) || modelStatsValue.Details[location.Offset].internalID != detailID {
			stats.mu.RUnlock()
			t.Fatalf("detail location %d = %+v is inconsistent", detailID, location)
		}
	}
	stats.mu.RUnlock()
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

func TestRequestStatisticsSummaryForRange(t *testing.T) {
	stats := NewRequestStatistics()
	base := time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC)
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "api-a",
		Model:       "model-a",
		RequestedAt: base,
		Detail:      coreusage.Detail{TotalTokens: 10},
		AuthIndex:   "auth-a",
	})
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "api-a",
		Model:       "model-b",
		RequestedAt: base.Add(time.Hour),
		Failed:      true,
		Detail:      coreusage.Detail{TotalTokens: 20},
		AuthIndex:   "auth-b",
	})
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "api-b",
		Model:       "model-c",
		RequestedAt: base.Add(2 * time.Hour),
		Detail:      coreusage.Detail{TotalTokens: 30},
		AuthIndex:   "auth-c",
	})

	summary := stats.SummaryForRange(TimeRange{
		From: base.Add(30 * time.Minute),
		To:   base.Add(2 * time.Hour),
	})
	if summary.TotalRequests != 1 || summary.SuccessCount != 0 || summary.FailureCount != 1 || summary.TotalTokens != 20 {
		t.Fatalf("range summary = %+v, want one failed request with 20 tokens", summary)
	}
	if got := summary.APIs["api-a"].Models["model-b"].TotalRequests; got != 1 {
		t.Fatalf("range model requests = %d, want 1", got)
	}
	if _, ok := summary.APIs["api-b"]; ok {
		t.Fatalf("range summary includes upper-bound api-b: %+v", summary.APIs)
	}
}

func TestRequestStatisticsDetailsAliasesAndSorts(t *testing.T) {
	stats := NewRequestStatistics()
	base := time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC)
	records := []coreusage.Record{
		{APIKey: "api-b", Model: "model-c", RequestedAt: base, Detail: coreusage.Detail{TotalTokens: 30}, AuthIndex: "auth-b"},
		{APIKey: "api-a", Model: "model-b", RequestedAt: base.Add(time.Minute), Detail: coreusage.Detail{TotalTokens: 10}, AuthIndex: "auth-c"},
		{APIKey: "api-c", Model: "model-a", RequestedAt: base.Add(2 * time.Minute), Detail: coreusage.Detail{TotalTokens: 20}, AuthIndex: "auth-a"},
	}
	for _, record := range records {
		stats.Record(context.Background(), record)
	}

	page := stats.Details(DetailQuery{Limit: 2, SortBy: SortByCreatedAt, SortOrder: SortOrderDesc})
	if page.Total != 3 || page.TotalMatched != 3 || !page.HasMore || page.NextOffset != 2 {
		t.Fatalf("page metadata = %+v, want total aliases and next offset", page)
	}
	if len(page.Items) != 2 || len(page.Details) != 2 {
		t.Fatalf("page item aliases len = items:%d details:%d, want 2", len(page.Items), len(page.Details))
	}
	if page.Items[0].Tokens.TotalTokens != 20 || page.Details[0].Tokens.TotalTokens != 20 {
		t.Fatalf("page first aliases = items:%+v details:%+v, want newest request", page.Items[0], page.Details[0])
	}

	tests := []struct {
		name      string
		sortBy    string
		sortOrder string
		wantFirst func(DetailEntry) bool
	}{
		{name: "created asc", sortBy: SortByCreatedAt, sortOrder: SortOrderAsc, wantFirst: func(item DetailEntry) bool { return item.Tokens.TotalTokens == 30 }},
		{name: "tokens asc", sortBy: SortByTokens, sortOrder: SortOrderAsc, wantFirst: func(item DetailEntry) bool { return item.Tokens.TotalTokens == 10 }},
		{name: "model asc", sortBy: SortByModel, sortOrder: SortOrderAsc, wantFirst: func(item DetailEntry) bool { return item.Model == "model-a" }},
		{name: "api desc", sortBy: SortByAPI, sortOrder: SortOrderDesc, wantFirst: func(item DetailEntry) bool { return item.API == "api-c" }},
		{name: "auth asc", sortBy: SortByAuthIndex, sortOrder: SortOrderAsc, wantFirst: func(item DetailEntry) bool { return item.AuthIndex == "auth-a" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stats.Details(DetailQuery{SortBy: tt.sortBy, SortOrder: tt.sortOrder})
			if len(got.Items) != 3 {
				t.Fatalf("items len = %d, want 3", len(got.Items))
			}
			if !tt.wantFirst(got.Items[0]) {
				t.Fatalf("first item = %+v, did not match sort expectation", got.Items[0])
			}
		})
	}

	filtered := stats.Details(DetailQuery{
		TimeRange: TimeRange{From: base.Add(time.Minute), To: base.Add(2 * time.Minute)},
	})
	if filtered.Total != 1 || filtered.Items[0].Tokens.TotalTokens != 10 {
		t.Fatalf("filtered page = %+v, want only middle request", filtered)
	}
}

func TestRequestStatisticsAuthSummariesForQuery(t *testing.T) {
	stats := NewRequestStatistics()
	base := time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC)
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "api-a",
		Model:       "model-a",
		RequestedAt: base,
		Detail:      coreusage.Detail{TotalTokens: 10},
		AuthIndex:   "auth-a",
	})
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "api-a",
		Model:       "model-a",
		RequestedAt: base.Add(time.Hour),
		Detail:      coreusage.Detail{TotalTokens: 20},
		AuthIndex:   "auth-b",
	})
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "api-a",
		Model:       "model-a",
		RequestedAt: base.Add(2 * time.Hour),
		Detail:      coreusage.Detail{TotalTokens: 30},
		AuthIndex:   "auth-c",
	})

	summaries := stats.AuthSummariesForQuery(AuthUsageQuery{
		TimeRange:   TimeRange{From: base.Add(30 * time.Minute), To: base.Add(2 * time.Hour)},
		AuthIndexes: []string{"auth-a", "auth-b"},
	})
	if len(summaries) != 1 {
		t.Fatalf("summaries len = %d, want 1: %+v", len(summaries), summaries)
	}
	if summaries[0].AuthIndex != "auth-b" || summaries[0].TotalTokens != 20 {
		t.Fatalf("summary = %+v, want only auth-b with 20 tokens", summaries[0])
	}
}

func TestRequestStatisticsSeriesBucketsAndGroups(t *testing.T) {
	stats := NewRequestStatistics()
	base := time.Date(2026, 3, 20, 12, 34, 45, 0, time.UTC)
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "api-a",
		Model:       "model-a",
		Source:      "source-a",
		RequestedAt: base,
		Detail:      coreusage.Detail{TotalTokens: 10},
		AuthIndex:   "auth-a",
	})
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "api-a",
		Model:       "model-a",
		Source:      "source-b",
		RequestedAt: base.Add(time.Minute),
		Failed:      true,
		Detail:      coreusage.Detail{TotalTokens: 20},
		AuthIndex:   "auth-b",
	})
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "api-b",
		Model:       "model-b",
		Source:      "source-a",
		RequestedAt: base.Add(31 * time.Minute),
		Detail:      coreusage.Detail{TotalTokens: 30},
		AuthIndex:   "auth-a",
	})

	hourly := stats.Series(SeriesQuery{
		TimeRange: TimeRange{From: base.Add(-time.Minute), To: base.Add(time.Hour)},
		Bucket:    BucketHour,
		GroupBy:   GroupByModel,
	})
	if hourly.Bucket != BucketHour || hourly.GroupBy != GroupByModel || len(hourly.Items) != 2 {
		t.Fatalf("hourly series = %+v, want two model groups", hourly)
	}
	if hourly.Items[0].Group != "model-a" || hourly.Items[0].TotalRequests != 2 || hourly.Items[0].FailureCount != 1 || hourly.Items[0].TotalTokens != 30 {
		t.Fatalf("first hourly item = %+v, want model-a aggregate", hourly.Items[0])
	}

	minuteFailed := stats.Series(SeriesQuery{
		TimeRange: TimeRange{From: base.Add(-time.Minute), To: base.Add(2 * time.Minute)},
		Bucket:    BucketMinute,
		GroupBy:   GroupByFailed,
	})
	if len(minuteFailed.Items) != 2 {
		t.Fatalf("minute failed len = %d, want 2: %+v", len(minuteFailed.Items), minuteFailed.Items)
	}
	if minuteFailed.Items[0].Bucket != base.Truncate(time.Minute) || minuteFailed.Items[0].Group != "success" {
		t.Fatalf("first minute item = %+v, want success at base minute", minuteFailed.Items[0])
	}
	if minuteFailed.Items[1].Group != "failed" || minuteFailed.Items[1].FailureCount != 1 {
		t.Fatalf("second minute item = %+v, want failed aggregate", minuteFailed.Items[1])
	}
	if minuteFailed.Items[1].TotalTokens != 20 || minuteFailed.Items[1].Tokens.TotalTokens != 20 {
		t.Fatalf("failed token aggregate = %+v, want 20 tokens", minuteFailed.Items[1])
	}

	dailyAuth := stats.Series(SeriesQuery{
		TimeRange: TimeRange{From: base.Add(-time.Minute), To: base.Add(time.Hour)},
		Bucket:    BucketDay,
		GroupBy:   GroupByAuthIndex,
	})
	if len(dailyAuth.Items) != 2 {
		t.Fatalf("daily auth len = %d, want 2: %+v", len(dailyAuth.Items), dailyAuth.Items)
	}
	if dailyAuth.Items[0].Bucket.Hour() != 0 || dailyAuth.Items[0].Bucket.Location() != time.UTC {
		t.Fatalf("daily bucket = %s, want UTC midnight", dailyAuth.Items[0].Bucket)
	}
}

func TestRequestStatisticsSeriesGroupsMissingAuthAsUnknown(t *testing.T) {
	stats := NewRequestStatistics()
	base := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "api-a",
		Model:       "model-a",
		RequestedAt: base.Add(time.Minute),
	})
	result := stats.Series(SeriesQuery{
		TimeRange: TimeRange{From: base, To: base.Add(time.Hour)},
		Bucket:    BucketHour,
		GroupBy:   GroupByAuthIndex,
	})
	if len(result.Items) != 1 || result.Items[0].Group != "unknown" || result.Items[0].TotalRequests != 1 {
		t.Fatalf("auth series = %+v, want unknown group", result.Items)
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
		APIKey:              "test-key",
		Model:               "gpt-5.4",
		RequestServiceTier:  "priority",
		ResponseServiceTier: "flex",
		RequestedAt:         time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC),
		Detail: coreusage.Detail{
			CacheCreationTokens: 7,
			TotalTokens:         30,
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
	detail := snapshot.APIs["test-key"].Models["gpt-5.4"].Details[0]
	if detail.Tokens.CacheCreationTokens != 7 || detail.RequestServiceTier != "priority" || detail.ResponseServiceTier != "flex" {
		t.Fatalf("restored detail lost new usage fields: %+v", detail)
	}
}

func TestClearAndPersistRequestStatistics(t *testing.T) {
	path := filepath.Join(t.TempDir(), StatisticsFileName)
	stats := NewRequestStatistics()
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC),
		Detail:      coreusage.Detail{TotalTokens: 30},
		AuthIndex:   "auth-a",
	})
	if _, err := PersistRequestStatistics(path, stats); err != nil {
		t.Fatalf("PersistRequestStatistics() error = %v", err)
	}

	previous, err := ClearAndPersistRequestStatistics(path, stats)
	if err != nil {
		t.Fatalf("ClearAndPersistRequestStatistics() error = %v", err)
	}
	if previous.TotalRequests != 1 || previous.TotalTokens != 30 {
		t.Fatalf("previous snapshot = %+v, want one request and 30 tokens", previous)
	}
	if stats.HasPendingPersistence() {
		t.Fatal("cleared statistics unexpectedly remain pending")
	}

	persisted, err := LoadSnapshotFile(path)
	if err != nil {
		t.Fatalf("LoadSnapshotFile() error = %v", err)
	}
	if persisted.TotalRequests != 0 || persisted.TotalTokens != 0 || len(persisted.APIs) != 0 {
		t.Fatalf("persisted snapshot = %+v, want empty usage", persisted)
	}

	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: time.Date(2026, 3, 20, 12, 1, 0, 0, time.UTC),
	})
	if !stats.HasPendingPersistence() {
		t.Fatal("request recorded after clear should remain pending")
	}
}

func TestClearAndPersistSerializesWithActivePersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), StatisticsFileName)
	stats := NewRequestStatistics()
	stats.Record(context.Background(), coreusage.Record{
		APIKey: "test-key",
		Model:  "gpt-5.4",
	})

	persistStarted := make(chan struct{})
	releasePersist := make(chan struct{})
	persistDone := make(chan error, 1)
	go func() {
		_, err := persistRequestStatisticsWithSave(path, stats, func(path string, snapshot StatisticsSnapshot) error {
			close(persistStarted)
			<-releasePersist
			return SaveSnapshotFile(path, snapshot)
		})
		persistDone <- err
	}()
	<-persistStarted

	clearBeforeLock := make(chan struct{})
	clearLockAcquired := make(chan struct{})
	clearSaveStarted := make(chan struct{})
	clearDone := make(chan error, 1)
	go func() {
		_, err := clearAndPersistRequestStatisticsWithHooks(
			path,
			stats,
			func(path string, snapshot StatisticsSnapshot) error {
				close(clearSaveStarted)
				return SaveSnapshotFile(path, snapshot)
			},
			func() { clearBeforeLock <- struct{}{} },
			func() { close(clearLockAcquired) },
		)
		clearDone <- err
	}()
	<-clearBeforeLock
	select {
	case <-clearLockAcquired:
		t.Fatal("clear acquired persistence lock while prior persistence was active")
	case <-time.After(100 * time.Millisecond):
	}
	close(releasePersist)
	if err := <-persistDone; err != nil {
		t.Fatalf("persistRequestStatisticsWithSave() error = %v", err)
	}
	select {
	case <-clearLockAcquired:
	case <-time.After(time.Second):
		t.Fatal("clear did not acquire lock after prior persistence completed")
	}
	select {
	case <-clearSaveStarted:
	case <-time.After(time.Second):
		t.Fatal("clear save did not start after acquiring persistence lock")
	}
	if err := <-clearDone; err != nil {
		t.Fatalf("ClearAndPersistRequestStatistics() error = %v", err)
	}

	persisted, err := LoadSnapshotFile(path)
	if err != nil {
		t.Fatalf("LoadSnapshotFile() error = %v", err)
	}
	if persisted.TotalRequests != 0 || len(persisted.APIs) != 0 {
		t.Fatalf("persisted snapshot = %+v, want clear to win", persisted)
	}
}

func TestMergePersistedSnapshotDoesNotHideExistingPendingChanges(t *testing.T) {
	stats := NewRequestStatistics()
	stats.Record(context.Background(), coreusage.Record{
		APIKey: "live-key",
		Model:  "gpt-5.4",
	})
	snapshot := StatisticsSnapshot{
		APIs: map[string]APISnapshot{
			"restored-key": {
				Models: map[string]ModelSnapshot{
					"gpt-5.4": {
						Details: []RequestDetail{{Timestamp: time.Now().UTC()}},
					},
				},
			},
		},
	}
	if result := stats.mergePersistedSnapshot(snapshot); result.Added != 1 {
		t.Fatalf("merge result = %+v, want added=1", result)
	}
	if !stats.HasPendingPersistence() {
		t.Fatal("pre-existing live record was incorrectly marked persisted")
	}
}

func TestClearAndPersistKeepsConcurrentRecordPending(t *testing.T) {
	path := filepath.Join(t.TempDir(), StatisticsFileName)
	stats := NewRequestStatistics()
	stats.Record(context.Background(), coreusage.Record{
		APIKey: "old-key",
		Model:  "gpt-5.4",
	})

	saveStarted := make(chan struct{})
	releaseSave := make(chan struct{})
	clearDone := make(chan error, 1)
	go func() {
		_, err := clearAndPersistRequestStatisticsWithSave(path, stats, func(path string, snapshot StatisticsSnapshot) error {
			close(saveStarted)
			<-releaseSave
			return SaveSnapshotFile(path, snapshot)
		})
		clearDone <- err
	}()
	<-saveStarted

	stats.Record(context.Background(), coreusage.Record{
		APIKey: "new-key",
		Model:  "gpt-5.4",
	})
	close(releaseSave)
	if err := <-clearDone; err != nil {
		t.Fatalf("clearAndPersistRequestStatisticsWithSave() error = %v", err)
	}
	if !stats.HasPendingPersistence() {
		t.Fatal("record written during clear persistence should remain pending")
	}
	if snapshot := stats.Snapshot(); snapshot.TotalRequests != 1 {
		t.Fatalf("in-memory snapshot = %+v, want only concurrent record", snapshot)
	}
	persisted, err := LoadSnapshotFile(path)
	if err != nil {
		t.Fatalf("LoadSnapshotFile() error = %v", err)
	}
	if persisted.TotalRequests != 0 {
		t.Fatalf("persisted snapshot = %+v, want empty clear state", persisted)
	}
}

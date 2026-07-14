package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func TestUsageMetaAndSummaryHandlers(t *testing.T) {
	handler, stats, _ := newUsageHandlerForTest(t)
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC),
		Detail:      coreusage.Detail{TotalTokens: 30},
		AuthIndex:   "auth-a",
	})

	ctx, recorder := newUsageRequestContext("/v0/management/usage/meta")
	handler.GetUsageMeta(ctx)
	var metaBody struct {
		Usage          usage.MetaSnapshot `json:"usage"`
		FailedRequests int64              `json:"failed_requests"`
	}
	decodeUsageResponse(t, recorder, &metaBody)
	if metaBody.Usage.Version == 0 || metaBody.Usage.TotalRequests != 1 || metaBody.Usage.TotalTokens != 30 {
		t.Fatalf("meta usage = %+v, want version and totals", metaBody.Usage)
	}
	if !metaBody.Usage.Available || metaBody.Usage.AsOf.IsZero() || metaBody.Usage.OldestAt == nil || metaBody.Usage.NewestAt == nil {
		t.Fatalf("meta availability = %+v, want timestamps and available=true", metaBody.Usage)
	}
	if metaBody.FailedRequests != 0 {
		t.Fatalf("failed_requests = %d, want 0", metaBody.FailedRequests)
	}

	ctx, recorder = newUsageRequestContext("/v0/management/usage/summary")
	handler.GetUsageSummary(ctx)
	if strings.Contains(recorder.Body.String(), "details") {
		t.Fatalf("summary response contains details: %s", recorder.Body.String())
	}
	var summaryBody struct {
		Usage usage.SummarySnapshot `json:"usage"`
	}
	decodeUsageResponse(t, recorder, &summaryBody)
	model := summaryBody.Usage.APIs["test-key"].Models["gpt-5.4"]
	if model.TotalRequests != 1 || model.SuccessCount != 1 || model.FailureCount != 0 || model.TotalTokens != 30 || model.Tokens.TotalTokens != 30 {
		t.Fatalf("model summary = %+v, want requests=1 tokens=30", model)
	}
	if summaryBody.Usage.Models["gpt-5.4"].TotalRequests != 1 {
		t.Fatalf("global model summary = %+v", summaryBody.Usage.Models)
	}
}

func TestUsageHealthRatesAndTokensHandlers(t *testing.T) {
	handler, stats, _ := newUsageHandlerForTest(t)
	now := time.Now().UTC()
	base := now.Truncate(time.Hour).Add(-2 * time.Hour)
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "api-a",
		Model:       "model-a",
		AuthIndex:   "auth-a",
		Source:      "source-a",
		RequestedAt: base.Add(time.Minute),
		Detail: coreusage.Detail{
			InputTokens:         10,
			OutputTokens:        5,
			ReasoningTokens:     2,
			CachedTokens:        3,
			CacheCreationTokens: 4,
			TotalTokens:         24,
		},
	})
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "api-rates",
		Model:       "model-rates",
		AuthIndex:   "auth-rates",
		Source:      "source-rates",
		RequestedAt: now.Add(-time.Minute),
		Detail:      coreusage.Detail{InputTokens: 60, TotalTokens: 60},
	})

	from := base.Format(time.RFC3339)
	to := base.Add(time.Hour).Format(time.RFC3339)
	ctx, recorder := newUsageRequestContext("/v0/management/usage/health?from=" + from + "&to=" + to + "&bucket=15m&group_by=none")
	handler.GetUsageHealth(ctx)
	var health usage.HealthResult
	decodeUsageResponse(t, recorder, &health)
	if len(health.Items) != 4 || health.Items[0].State != usage.HealthStateHealthy || health.Items[1].State != usage.HealthStateNoData {
		t.Fatalf("health response = %+v", health)
	}

	ctx, recorder = newUsageRequestContext("/v0/management/usage/rates?window_minutes=30&sparkline_minutes=60")
	handler.GetUsageRates(ctx)
	var rates usage.RatesResult
	decodeUsageResponse(t, recorder, &rates)
	if rates.WindowMinutes != 30 || rates.SparklineMinutes != 60 || len(rates.Items) != 60 || rates.RequestCount < 1 || rates.TokenCount < 60 || rates.RPM <= 0 || rates.TPM <= 0 {
		t.Fatalf("rates response = %+v", rates)
	}

	ctx, recorder = newUsageRequestContext("/v0/management/usage/tokens?from=" + from + "&to=" + to + "&bucket=hour&group_by=model")
	handler.GetUsageTokens(ctx)
	var tokens usage.TokenResult
	decodeUsageResponse(t, recorder, &tokens)
	if len(tokens.Items) != 1 || tokens.TotalTokens != 24 || tokens.Items[0].Group != "model-a" || tokens.Items[0].Tokens.CacheCreationTokens != 4 {
		t.Fatalf("tokens response = %+v", tokens)
	}

	ctx, recorder = newUsageRequestContext("/v0/management/usage/health?from=" + from + "&to=" + to + "&bucket=15m&group_by=auth_index&auth_index=auth-a,missing")
	handler.GetUsageHealth(ctx)
	decodeUsageResponse(t, recorder, &health)
	if len(health.Items) != 8 || health.Items[0].Group != "auth-a" || health.Items[1].Group != "missing" {
		t.Fatalf("grouped health response = %+v", health)
	}

	ctx, recorder = newUsageRequestContext("/v0/management/usage/health?from=" + from + "&to=" + to + "&bucket=15m&group_by=source&source=source-a,missing")
	handler.GetUsageHealth(ctx)
	decodeUsageResponse(t, recorder, &health)
	if len(health.Items) != 8 || health.Items[0].Group != "missing" || health.Items[1].Group != "source-a" {
		t.Fatalf("source health response = %+v", health)
	}
}

func TestUsageCostsHandlerUsesSharedPrices(t *testing.T) {
	handler, stats, _ := newUsageHandlerForTest(t)
	base := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	handler.cfg.UsagePricing = config.UsagePricingConfig{Models: map[string]config.UsageModelPrice{
		"model-a": {
			InputPerMillion:       2,
			OutputPerMillion:      10,
			CachedInputPerMillion: 0.5,
		},
	}}
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "api-a",
		Model:       "model-a",
		RequestedAt: base.Add(time.Minute),
		Detail: coreusage.Detail{
			InputTokens:  1000,
			CachedTokens: 200,
			OutputTokens: 500,
			TotalTokens:  1700,
		},
	})

	from := base.Format(time.RFC3339)
	to := base.Add(time.Hour).Format(time.RFC3339)
	ctx, recorder := newUsageRequestContext("/v0/management/usage/costs?from=" + from + "&to=" + to + "&bucket=hour")
	handler.GetUsageCosts(ctx)
	var result usage.CostResult
	decodeUsageResponse(t, recorder, &result)
	if result.Total.AmountMicros != 6700 || len(result.ByModel) != 1 || len(result.ByAPI) != 1 || len(result.Series) != 1 {
		t.Fatalf("cost result = %+v", result)
	}
}

func TestUsageAnalyticsHandlersRejectInvalidParameters(t *testing.T) {
	handler, _, _ := newUsageHandlerForTest(t)
	tests := []struct {
		target string
		call   func(*gin.Context)
	}{
		{target: "/v0/management/usage/health?bucket=minute", call: handler.GetUsageHealth},
		{target: "/v0/management/usage/health?group_by=model", call: handler.GetUsageHealth},
		{target: "/v0/management/usage/rates?window_minutes=0", call: handler.GetUsageRates},
		{target: "/v0/management/usage/rates?sparkline_minutes=1441", call: handler.GetUsageRates},
		{target: "/v0/management/usage/tokens?bucket=minute", call: handler.GetUsageTokens},
		{target: "/v0/management/usage/tokens?group_by=source", call: handler.GetUsageTokens},
		{target: "/v0/management/usage/costs?bucket=minute", call: handler.GetUsageCosts},
	}
	for _, test := range tests {
		t.Run(test.target, func(t *testing.T) {
			ctx, recorder := newUsageRequestContext(test.target)
			test.call(ctx)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestUsageHealthRejectsOversizedFilterLists(t *testing.T) {
	handler, _, _ := newUsageHandlerForTest(t)
	values := make([]string, maxUsageHealthFilterValues+1)
	for index := range values {
		values[index] = "auth-" + strconv.Itoa(index)
	}
	ctx, recorder := newUsageRequestContext("/v0/management/usage/health?auth_index=" + strings.Join(values, ","))
	handler.GetUsageHealth(ctx)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("oversized auth filter status = %d, want 400: %s", recorder.Code, recorder.Body.String())
	}

	ctx, recorder = newUsageRequestContext("/v0/management/usage/health?source=" + strings.Repeat("x", maxUsageHealthFilterBytes+1))
	handler.GetUsageHealth(ctx)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("oversized source filter status = %d, want 400: %s", recorder.Code, recorder.Body.String())
	}

	values = values[:15]
	ctx, recorder = newUsageRequestContext("/v0/management/usage/health?group_by=auth_index&auth_index=" + strings.Join(values, ","))
	handler.GetUsageHealth(ctx)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("oversized health matrix status = %d, want 400: %s", recorder.Code, recorder.Body.String())
	}
}

func TestUsageTokenQueryDefaultsToRecentThirtyDays(t *testing.T) {
	ctx, recorder := newUsageRequestContext("/v0/management/usage/tokens")
	query, ok := parseUsageTokenQuery(ctx)
	if !ok {
		t.Fatalf("default token query rejected: %s", recorder.Body.String())
	}
	if query.TimeRange.From.IsZero() || query.TimeRange.To.IsZero() {
		t.Fatalf("default token range = %+v", query.TimeRange)
	}
	if got := query.TimeRange.To.Sub(query.TimeRange.From); got != usage.DefaultTokenRange {
		t.Fatalf("default token range = %s, want %s", got, usage.DefaultTokenRange)
	}
}

func TestUsageCostQueryDefaultsToRecentThirtyDays(t *testing.T) {
	ctx, recorder := newUsageRequestContext("/v0/management/usage/costs")
	query, ok := parseUsageCostQuery(ctx)
	if !ok {
		t.Fatalf("default cost query rejected: %s", recorder.Body.String())
	}
	if got := query.TimeRange.To.Sub(query.TimeRange.From); got != usage.DefaultTokenRange {
		t.Fatalf("default cost range = %s, want %s", got, usage.DefaultTokenRange)
	}
	if query.Bucket != usage.BucketDay {
		t.Fatalf("default cost bucket = %q, want day", query.Bucket)
	}
}

func TestUsageDetailsHandlerFiltersAndPaginates(t *testing.T) {
	handler, stats, _ := newUsageHandlerForTest(t)
	timestamp := time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC)
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: timestamp,
		Detail:      coreusage.Detail{TotalTokens: 10},
		AuthIndex:   "auth-a",
	})
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: timestamp.Add(time.Minute),
		Detail:      coreusage.Detail{TotalTokens: 20},
		AuthIndex:   "auth-a",
	})
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.5",
		RequestedAt: timestamp.Add(2 * time.Minute),
		Detail:      coreusage.Detail{TotalTokens: 30},
		AuthIndex:   "auth-b",
	})

	ctx, recorder := newUsageRequestContext("/v0/management/usage/details?auth_index=auth-a&limit=1")
	handler.GetUsageDetails(ctx)
	var page usage.DetailPage
	decodeUsageResponse(t, recorder, &page)
	if page.TotalMatched != 2 || !page.HasMore || page.NextOffset != 1 || len(page.Details) != 1 {
		t.Fatalf("details page = %+v, want two matches with one returned", page)
	}
	if page.Details[0].AuthIndex != "auth-a" || page.Details[0].Tokens.TotalTokens != 20 {
		t.Fatalf("details[0] = %+v, want newest auth-a request", page.Details[0])
	}
}

func TestClearUsageStatisticsHandler(t *testing.T) {
	handler, stats, _ := newUsageHandlerForTest(t)
	logDir := t.TempDir()
	t.Setenv("WRITABLE_PATH", logDir)
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC),
		Detail:      coreusage.Detail{TotalTokens: 30},
		AuthIndex:   "auth-a",
	})

	ctx, recorder := newUsageRequestContext("/v0/management/usage")
	handler.ClearUsageStatistics(ctx)
	var body struct {
		Cleared              bool   `json:"cleared"`
		Version              uint64 `json:"version"`
		TotalRequestsBefore  int64  `json:"total_requests_before"`
		FailedRequestsBefore int64  `json:"failed_requests_before"`
		TotalRequestsAfter   int64  `json:"total_requests_after"`
		FailedRequestsAfter  int64  `json:"failed_requests_after"`
	}
	decodeUsageResponse(t, recorder, &body)
	if !body.Cleared {
		t.Fatal("cleared = false, want true")
	}
	if body.Version == 0 {
		t.Fatal("version = 0, want changed version")
	}
	if body.TotalRequestsBefore != 1 || body.FailedRequestsBefore != 0 {
		t.Fatalf("before counters = %+v, want one successful request", body)
	}
	if body.TotalRequestsAfter != 0 || body.FailedRequestsAfter != 0 {
		t.Fatalf("after counters = %+v, want zero counters", body)
	}

	meta := stats.Meta()
	if meta.TotalRequests != 0 || meta.SuccessCount != 0 || meta.FailureCount != 0 || meta.TotalTokens != 0 {
		t.Fatalf("meta after clear = %+v, want zero", meta)
	}
	persisted, err := usage.LoadSnapshotFile(handler.usageStatisticsFilePath())
	if err != nil {
		t.Fatalf("LoadSnapshotFile() error = %v", err)
	}
	if persisted.TotalRequests != 0 || persisted.SuccessCount != 0 || persisted.FailureCount != 0 || persisted.TotalTokens != 0 {
		t.Fatalf("persisted usage after clear = %+v, want zero", persisted)
	}
}

func TestClearUsageStatisticsWaitsForQueuedRecords(t *testing.T) {
	stats := usage.GetRequestStatistics()
	if err := coreusage.DefaultManager().Barrier(context.Background()); err != nil {
		t.Fatalf("initial Barrier() error = %v", err)
	}
	stats.Clear()
	t.Cleanup(func() {
		_ = coreusage.DefaultManager().Barrier(context.Background())
		stats.Clear()
	})

	blocker := &blockingUsagePlugin{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	coreusage.RegisterPlugin(blocker)
	coreusage.PublishRecord(context.Background(), coreusage.Record{APIKey: "first", Model: "model-a"})
	select {
	case <-blocker.entered:
	case <-time.After(time.Second):
		t.Fatal("blocking usage plugin was not entered")
	}
	coreusage.PublishRecord(context.Background(), coreusage.Record{APIKey: "second", Model: "model-a"})

	handler, _, _ := newUsageHandlerForTest(t)
	handler.SetUsageStatistics(stats)
	t.Setenv("WRITABLE_PATH", t.TempDir())
	ctx, recorder := newUsageRequestContext("/v0/management/usage")
	done := make(chan struct{})
	go func() {
		handler.ClearUsageStatistics(ctx)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("clear completed before queued usage was released")
	case <-time.After(100 * time.Millisecond):
	}
	close(blocker.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("clear did not complete after queued usage was released")
	}

	var body struct {
		TotalRequestsBefore int64 `json:"total_requests_before"`
		TotalRequestsAfter  int64 `json:"total_requests_after"`
	}
	decodeUsageResponse(t, recorder, &body)
	if body.TotalRequestsBefore != 2 || body.TotalRequestsAfter != 0 {
		t.Fatalf("clear response = %+v, want both queued records cleared", body)
	}
	if err := coreusage.DefaultManager().Barrier(context.Background()); err != nil {
		t.Fatalf("final Barrier() error = %v", err)
	}
	if got := stats.Meta().TotalRequests; got != 0 {
		t.Fatalf("usage revived after clear: total_requests=%d", got)
	}
}

func TestClearUsageStatisticsCanceledBarrierPreservesData(t *testing.T) {
	handler, stats, _ := newUsageHandlerForTest(t)
	stats.Record(context.Background(), coreusage.Record{APIKey: "test-key", Model: "model-a"})
	ctx, recorder := newUsageRequestContext("/v0/management/usage")
	requestContext, cancel := context.WithCancel(ctx.Request.Context())
	cancel()
	ctx.Request = ctx.Request.WithContext(requestContext)

	handler.ClearUsageStatistics(ctx)
	if recorder.Code != http.StatusRequestTimeout {
		t.Fatalf("status = %d, want 408: %s", recorder.Code, recorder.Body.String())
	}
	if got := stats.Meta().TotalRequests; got != 1 {
		t.Fatalf("canceled clear changed usage: total_requests=%d", got)
	}
}

type blockingUsagePlugin struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (p *blockingUsagePlugin) HandleUsage(context.Context, coreusage.Record) {
	p.once.Do(func() {
		close(p.entered)
		<-p.release
	})
}

func TestClearUsageStatisticsHandlerReportsPersistenceFailure(t *testing.T) {
	handler, stats, _ := newUsageHandlerForTest(t)
	blockedPath := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blockedPath, []byte("blocked"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	t.Setenv("WRITABLE_PATH", blockedPath)
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC),
	})

	ctx, recorder := newUsageRequestContext("/v0/management/usage")
	handler.ClearUsageStatistics(ctx)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusInternalServerError, recorder.Body.String())
	}
	var body struct {
		Cleared bool   `json:"cleared"`
		Error   string `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("json.Unmarshal(%s) error = %v", recorder.Body.String(), err)
	}
	if !body.Cleared || body.Error == "" {
		t.Fatalf("response = %+v, want cleared persistence error", body)
	}
	if stats.Meta().TotalRequests != 0 {
		t.Fatalf("usage was not cleared after persistence failure: %+v", stats.Meta())
	}
	if !stats.HasPendingPersistence() {
		t.Fatal("failed clear persistence should remain pending")
	}
}

func TestUsageBarrierErrorResponse(t *testing.T) {
	status, message := usageBarrierErrorResponse(context.Canceled)
	if status != http.StatusRequestTimeout || message != "usage statistics clear canceled" {
		t.Fatalf("canceled response = (%d, %q)", status, message)
	}
	status, message = usageBarrierErrorResponse(coreusage.ErrManagerClosed)
	if status != http.StatusServiceUnavailable || message != "usage statistics queue unavailable" {
		t.Fatalf("closed response = (%d, %q)", status, message)
	}
}

func TestUsageStatisticsFilePathUsesHotReloadedAuthDir(t *testing.T) {
	workDir := t.TempDir()
	t.Chdir(workDir)
	if err := os.WriteFile(filepath.Join(workDir, "logs"), []byte("blocked"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	t.Setenv("WRITABLE_PATH", "")
	t.Setenv("writable_path", "")

	handler, _, _ := newUsageHandlerForTest(t)
	newAuthDir := t.TempDir()
	handler.SetConfig(&config.Config{AuthDir: newAuthDir})
	want := filepath.Join(newAuthDir, "logs", usage.StatisticsFileName)
	if got := handler.usageStatisticsFilePath(); got != want {
		t.Fatalf("usageStatisticsFilePath() = %q, want hot-reloaded path %q", got, want)
	}
}

func TestUsageAuthSummariesIncludesCurrentZeroUsageAndStale(t *testing.T) {
	handler, stats, manager := newUsageHandlerForTest(t)
	usedIndex := registerUsageAuthForTest(t, manager, "used-auth", "used.json", "codex", "Used", "used@example.com")
	zeroIndex := registerUsageAuthForTest(t, manager, "zero-auth", "zero.json", "codex", "Zero", "zero@example.com")

	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC),
		Detail:      coreusage.Detail{TotalTokens: 30},
		AuthIndex:   usedIndex,
	})
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: time.Date(2026, 3, 20, 12, 1, 0, 0, time.UTC),
		Detail:      coreusage.Detail{TotalTokens: 40},
		AuthIndex:   "stale-auth",
	})

	ctx, recorder := newUsageRequestContext("/v0/management/usage/auths")
	handler.GetUsageAuthSummaries(ctx)
	var body struct {
		Auths []usageAuthSummaryForTest `json:"auths"`
	}
	decodeUsageResponse(t, recorder, &body)
	byIndex := usageAuthsByIndexForTest(body.Auths)

	used := byIndex[usedIndex]
	if used.TotalRequests != 1 || used.TotalTokens != 30 || used.Email != "used@example.com" || used.Stale || used.LastUsedAt == nil || used.ModelCount != 1 {
		t.Fatalf("used auth = %+v, want current usage with email", used)
	}
	zero := byIndex[zeroIndex]
	if zero.AuthIndex == "" || zero.TotalRequests != 0 || zero.Email != "zero@example.com" || zero.Stale {
		t.Fatalf("zero auth = %+v, want current zero-usage auth", zero)
	}
	stale := byIndex["stale-auth"]
	if stale.TotalRequests != 1 || stale.TotalTokens != 40 || !stale.Stale {
		t.Fatalf("stale auth = %+v, want stale usage summary", stale)
	}
}

func TestUsageAuthSummaryNotFound(t *testing.T) {
	handler, _, _ := newUsageHandlerForTest(t)
	ctx, recorder := newUsageRequestContext("/v0/management/usage/auths/missing")
	ctx.Params = gin.Params{{Key: "auth_index", Value: "missing"}}

	handler.GetUsageAuthSummary(ctx)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestUsageAuthModelSummaries(t *testing.T) {
	handler, stats, manager := newUsageHandlerForTest(t)
	authIndex := registerUsageAuthForTest(t, manager, "used-auth", "used.json", "codex", "Used", "used@example.com")
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC),
		Detail:      coreusage.Detail{TotalTokens: 30},
		AuthIndex:   authIndex,
	})
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.5",
		RequestedAt: time.Date(2026, 3, 20, 12, 1, 0, 0, time.UTC),
		Failed:      true,
		Detail:      coreusage.Detail{TotalTokens: 40},
		AuthIndex:   authIndex,
	})

	ctx, recorder := newUsageRequestContext("/v0/management/usage/auths/" + authIndex + "/models")
	ctx.Params = gin.Params{{Key: "auth_index", Value: authIndex}}
	handler.GetUsageAuthModelSummaries(ctx)

	var body struct {
		Auth   usageAuthSummaryForTest        `json:"auth"`
		Models []usage.AuthModelUsageSnapshot `json:"models"`
	}
	decodeUsageResponse(t, recorder, &body)
	if body.Auth.AuthIndex != authIndex || body.Auth.TotalRequests != 2 || body.Auth.TotalTokens != 70 {
		t.Fatalf("auth = %+v, want aggregate totals", body.Auth)
	}
	if len(body.Models) != 2 {
		t.Fatalf("models len = %d, want 2: %+v", len(body.Models), body.Models)
	}
	if body.Models[0].Model != "gpt-5.4" || body.Models[0].SuccessCount != 1 || body.Models[0].TotalTokens != 30 {
		t.Fatalf("first model = %+v, want gpt-5.4 success tokens=30", body.Models[0])
	}
	if body.Models[1].Model != "gpt-5.5" || body.Models[1].FailureCount != 1 || body.Models[1].TotalTokens != 40 {
		t.Fatalf("second model = %+v, want gpt-5.5 failure tokens=40", body.Models[1])
	}
}

func TestUsageSummaryHandlerFiltersTimeRangeAndRejectsInvalid(t *testing.T) {
	handler, stats, _ := newUsageHandlerForTest(t)
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

	target := "/v0/management/usage/summary?from=" + base.Add(30*time.Minute).Format(time.RFC3339) + "&to=" + base.Add(2*time.Hour).Format(time.RFC3339)
	ctx, recorder := newUsageRequestContext(target)
	handler.GetUsageSummary(ctx)
	var body struct {
		Usage usage.SummarySnapshot `json:"usage"`
	}
	decodeUsageResponse(t, recorder, &body)
	if body.Usage.TotalRequests != 1 || body.Usage.FailureCount != 1 || body.Usage.TotalTokens != 20 {
		t.Fatalf("summary = %+v, want only second request", body.Usage)
	}

	ctx, recorder = newUsageRequestContext("/v0/management/usage/summary?from=not-time")
	handler.GetUsageSummary(ctx)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("invalid from status = %d, want 400", recorder.Code)
	}
}

func TestUsageDetailsHandlerAliasesAndSortValidation(t *testing.T) {
	handler, stats, _ := newUsageHandlerForTest(t)
	base := time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC)
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "api-b",
		Model:       "model-b",
		RequestedAt: base,
		Detail:      coreusage.Detail{TotalTokens: 30},
		AuthIndex:   "auth-b",
	})
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "api-a",
		Model:       "model-a",
		RequestedAt: base.Add(time.Minute),
		Detail:      coreusage.Detail{TotalTokens: 10},
		AuthIndex:   "auth-a",
	})

	ctx, recorder := newUsageRequestContext("/v0/management/usage/details?sort_by=tokens&sort_order=asc&limit=1")
	handler.GetUsageDetails(ctx)
	var page usage.DetailPage
	decodeUsageResponse(t, recorder, &page)
	if page.Total != 2 || page.TotalMatched != 2 || !page.HasMore || page.NextOffset != 1 {
		t.Fatalf("page metadata = %+v, want new and legacy aliases", page)
	}
	if len(page.Items) != 1 || len(page.Details) != 1 || page.Items[0].Tokens.TotalTokens != 10 || page.Details[0].Tokens.TotalTokens != 10 {
		t.Fatalf("page aliases = %+v, want one lowest-token item", page)
	}

	ctx, recorder = newUsageRequestContext("/v0/management/usage/details?sort_by=bad")
	handler.GetUsageDetails(ctx)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("invalid sort_by status = %d, want 400", recorder.Code)
	}
	ctx, recorder = newUsageRequestContext("/v0/management/usage/details?sort_order=bad")
	handler.GetUsageDetails(ctx)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("invalid sort_order status = %d, want 400", recorder.Code)
	}
}

func TestUsageAuthSummariesBatchFilter(t *testing.T) {
	handler, stats, manager := newUsageHandlerForTest(t)
	usedIndex := registerUsageAuthForTest(t, manager, "used-auth", "used.json", "codex", "Used", "used@example.com")
	zeroIndex := registerUsageAuthForTest(t, manager, "zero-auth", "zero.json", "codex", "Zero", "zero@example.com")
	otherIndex := registerUsageAuthForTest(t, manager, "other-auth", "other.json", "codex", "Other", "other@example.com")
	base := time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC)
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "api-a",
		Model:       "model-a",
		RequestedAt: base,
		Detail:      coreusage.Detail{TotalTokens: 10},
		AuthIndex:   usedIndex,
	})
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "api-a",
		Model:       "model-a",
		RequestedAt: base,
		Detail:      coreusage.Detail{TotalTokens: 20},
		AuthIndex:   otherIndex,
	})
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "api-a",
		Model:       "model-a",
		RequestedAt: base,
		Detail:      coreusage.Detail{TotalTokens: 30},
		AuthIndex:   "stale-auth",
	})

	target := "/v0/management/usage/auths?auth_index=" + usedIndex + "," + zeroIndex + ",stale-auth,missing"
	ctx, recorder := newUsageRequestContext(target)
	handler.GetUsageAuthSummaries(ctx)
	var body struct {
		Auths []usageAuthSummaryForTest `json:"auths"`
	}
	decodeUsageResponse(t, recorder, &body)
	if len(body.Auths) != 3 {
		t.Fatalf("auths len = %d, want 3: %+v", len(body.Auths), body.Auths)
	}
	byIndex := usageAuthsByIndexForTest(body.Auths)
	if byIndex[usedIndex].TotalTokens != 10 {
		t.Fatalf("used auth = %+v, want 10 tokens", byIndex[usedIndex])
	}
	if byIndex[zeroIndex].AuthIndex == "" || byIndex[zeroIndex].TotalRequests != 0 {
		t.Fatalf("zero auth = %+v, want current zero usage", byIndex[zeroIndex])
	}
	if !byIndex["stale-auth"].Stale || byIndex["stale-auth"].TotalTokens != 30 {
		t.Fatalf("stale auth = %+v, want stale 30 tokens", byIndex["stale-auth"])
	}
	if _, ok := byIndex[otherIndex]; ok {
		t.Fatalf("batch response includes unrequested auth %s: %+v", otherIndex, body.Auths)
	}
}

func TestUsageSeriesHandlerDefaultsAndValidation(t *testing.T) {
	handler, stats, _ := newUsageHandlerForTest(t)
	now := time.Now().UTC()
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "api-a",
		Model:       "recent-model",
		RequestedAt: now.Add(-time.Hour),
		Detail:      coreusage.Detail{TotalTokens: 10},
		AuthIndex:   "auth-a",
	})
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "api-a",
		Model:       "old-model",
		RequestedAt: now.Add(-48 * time.Hour),
		Detail:      coreusage.Detail{TotalTokens: 20},
		AuthIndex:   "auth-b",
	})

	ctx, recorder := newUsageRequestContext("/v0/management/usage/series")
	handler.GetUsageSeries(ctx)
	var result usage.SeriesResult
	decodeUsageResponse(t, recorder, &result)
	if result.Bucket != usage.BucketHour || result.GroupBy != usage.GroupByModel {
		t.Fatalf("series defaults = %+v, want hour/model", result)
	}
	if len(result.Items) != 1 || result.Items[0].Group != "recent-model" || result.Items[0].TotalTokens != 10 {
		t.Fatalf("series items = %+v, want only recent model", result.Items)
	}

	ctx, recorder = newUsageRequestContext("/v0/management/usage/series?bucket=bad")
	handler.GetUsageSeries(ctx)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("invalid bucket status = %d, want 400", recorder.Code)
	}
	ctx, recorder = newUsageRequestContext("/v0/management/usage/series?group_by=bad")
	handler.GetUsageSeries(ctx)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("invalid group_by status = %d, want 400", recorder.Code)
	}
}

type usageAuthSummaryForTest struct {
	AuthIndex     string           `json:"auth_index"`
	ID            string           `json:"id"`
	Name          string           `json:"name"`
	Provider      string           `json:"provider"`
	Label         string           `json:"label"`
	Status        string           `json:"status"`
	Disabled      bool             `json:"disabled"`
	AccountType   string           `json:"account_type"`
	Account       string           `json:"account"`
	Email         string           `json:"email"`
	Stale         bool             `json:"stale"`
	TotalRequests int64            `json:"total_requests"`
	SuccessCount  int64            `json:"success_count"`
	FailureCount  int64            `json:"failure_count"`
	TotalTokens   int64            `json:"total_tokens"`
	Tokens        usage.TokenStats `json:"tokens"`
	LastUsedAt    *time.Time       `json:"last_used_at"`
	ModelCount    int              `json:"model_count"`
}

func newUsageHandlerForTest(t *testing.T) (*Handler, *usage.RequestStatistics, *coreauth.Manager) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	manager := coreauth.NewManager(&memoryAuthStore{}, nil, nil)
	stats := usage.NewRequestStatistics()
	handler := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	handler.SetUsageStatistics(stats)
	return handler, stats, manager
}

func registerUsageAuthForTest(t *testing.T, manager *coreauth.Manager, id, fileName, provider, label, email string) string {
	t.Helper()
	auth := &coreauth.Auth{
		ID:       id,
		FileName: fileName,
		Provider: provider,
		Label:    label,
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"email": email},
	}
	authIndex := auth.EnsureIndex()
	if authIndex == "" {
		t.Fatalf("EnsureIndex() = empty for %s", id)
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register(%s) error = %v", id, err)
	}
	return authIndex
}

func newUsageRequestContext(target string) (*gin.Context, *httptest.ResponseRecorder) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, target, nil)
	return ctx, recorder
}

func decodeUsageResponse(t *testing.T, recorder *httptest.ResponseRecorder, target any) {
	t.Helper()
	if recorder.Code < http.StatusOK || recorder.Code >= http.StatusMultipleChoices {
		t.Fatalf("unexpected status %d: %s", recorder.Code, recorder.Body.String())
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), target); err != nil {
		t.Fatalf("json.Unmarshal(%s) error = %v", recorder.Body.String(), err)
	}
}

func usageAuthsByIndexForTest(items []usageAuthSummaryForTest) map[string]usageAuthSummaryForTest {
	out := make(map[string]usageAuthSummaryForTest, len(items))
	for _, item := range items {
		out[item.AuthIndex] = item
	}
	return out
}

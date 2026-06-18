package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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
	if model.TotalRequests != 1 || model.TotalTokens != 30 {
		t.Fatalf("model summary = %+v, want requests=1 tokens=30", model)
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
	if used.TotalRequests != 1 || used.TotalTokens != 30 || used.Email != "used@example.com" || used.Stale {
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

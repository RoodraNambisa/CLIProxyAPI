package usage

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

// legacyQueryDetails is the pre-index implementation retained as an independent
// compatibility oracle and allocation baseline.
func legacyQueryDetails(s *RequestStatistics, query DetailQuery) DetailPage {
	query = normalizeDetailQuery(query)
	s.flushPending()
	s.mu.RLock()
	matches := make([]DetailEntry, 0)
	for api, stats := range s.apis {
		if query.API != "" && query.API != api {
			continue
		}
		for model, values := range stats.Models {
			if query.Model != "" && query.Model != model {
				continue
			}
			for _, detail := range values.Details {
				if detailMatchesQuery(detail, query) {
					matches = append(matches, DetailEntry{API: api, Model: model, RequestDetail: publicRequestDetail(detail)})
				}
			}
		}
	}
	s.mu.RUnlock()
	sortDetailEntries(matches, query.SortBy, query.SortOrder)
	total := len(matches)
	if query.Offset >= total {
		return newDetailPage([]DetailEntry{}, total, query.Offset, query.Limit)
	}
	return newDetailPage(matches[query.Offset:min(total, query.Offset+query.Limit)], total, query.Offset, query.Limit)
}

func queryPerformanceFixture(n int) *RequestStatistics {
	s := NewRequestStatistics()
	base := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	for i := 0; i < n; i++ {
		s.recordPreparedLocked(preparedUsageRecord{
			apiName: fmt.Sprint("api-", i%3), modelName: fmt.Sprint("model-", i%2),
			detail: RequestDetail{
				Timestamp: base.Add(time.Duration(i) * time.Millisecond),
				AuthIndex: fmt.Sprint("auth-", i%32), Source: fmt.Sprint("source-", i%2),
				RequestPath: "/v1/images/generations", ClientIP: "127.0.0.1",
				Failed: i%3 != 0, Auxiliary: i%11 == 0,
				ErrorCode: "http_404", FailureStage: "upstream",
				ErrorResponse:      `{"detail":"Model not found"}`,
				CredentialSelected: true, UpstreamCommitted: i%5 == 0,
				Tokens: TokenStats{TotalTokens: int64(i % 101)},
			},
		}, base.Add(time.Hour))
	}
	return s
}

func TestIndexedDetailsMatchesLegacyQuery(t *testing.T) {
	s := queryPerformanceFixture(2048)
	for _, order := range []string{SortOrderAsc, SortOrderDesc} {
		for _, by := range []string{SortByCreatedAt, SortByAPI, SortByModel, SortByAuthIndex, SortByTokens} {
			for _, offset := range []int{0, 29, 1500, 3000} {
				for _, filter := range []DetailQuery{{}, {API: "api-1"}, {Model: "model-1"}, {AuthIndex: "auth-3"}, {Source: "source-1"}, {Failed: new(true)}, {RequestPath: "images", ClientIP: "127.0.0.1"}, {TimeRange: TimeRange{From: s.oldestAt.Add(time.Second), To: s.newestAt}}} {
					filter.SortBy, filter.SortOrder, filter.Offset, filter.Limit = by, order, offset, 23
					got, want := s.Details(filter), legacyQueryDetails(s, filter)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("query %+v differs from legacy: got total %d/%d rows, want %d/%d", filter, got.Total, len(got.Items), want.Total, len(want.Items))
					}
				}
			}
		}
	}
}

func BenchmarkUsageQueryHistory(b *testing.B) {
	for _, n := range []int{100000, 1000000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			s := queryPerformanceFixture(n)
			for _, implementation := range []struct {
				name  string
				query func(*RequestStatistics, DetailQuery) DetailPage
			}{{"legacy", legacyQueryDetails}, {"indexed", (*RequestStatistics).Details}} {
				for _, page := range []struct {
					name  string
					query DetailQuery
				}{
					{"first_page", DetailQuery{Limit: 50}},
					{"recent", DetailQuery{Limit: 50, TimeRange: TimeRange{From: s.newestAt.Add(-time.Second)}}},
					{"auth", DetailQuery{Limit: 50, AuthIndex: "auth-3"}},
				} {
					b.Run(implementation.name+"/"+page.name, func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							if got := implementation.query(s, page.query); len(got.Items) != 50 {
								b.Fatal("missing rows")
							}
						}
					})
				}
			}
			b.Run("failure_summary", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if s.FailureSummary(FailureSummaryQuery{}).Total == 0 {
						b.Fatal("missing failures")
					}
				}
			})
		})
	}
}

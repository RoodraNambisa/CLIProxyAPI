package auth

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

// This benchmark intentionally uses APIs that also exist in the pre-change
// tree, so the identical fixture can measure the two revisions in one machine.
func BenchmarkSelectionWithUsageQueries(b *testing.B) {
	for _, rows := range []int{100000, 1000000} {
		b.Run(fmt.Sprint(rows), func(b *testing.B) {
			fixed := time.Now().Truncate(time.Minute).Add(time.Second)
			auths := make([]*Auth, 100000)
			for i := range auths {
				auths[i] = &Auth{ID: fmt.Sprint("joint-", i), Provider: "test"}
			}
			scheduler := newSchedulerForTest(&RandomSelector{}, auths...)
			scheduler.setRoutingConfig(config.RoutingConfig{PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 1})
			scheduler.requestLimiter.now = func() time.Time { return fixed }
			window, _ := authRequestWindowAt(fixed, 1)
			for _, a := range auths {
				scheduler.requestLimiter.counts[a.ID] = authRequestWindowCount{window: window, windowMinutes: 1, limit: 1, count: 1}
			}
			// Build the route and observe the saturated membership before measuring.
			scheduler.pickSingle(b.Context(), "test", "", core.Options{}, nil)
			stats := usage.NewRequestStatistics()
			for i := range rows {
				stats.Record(context.Background(), coreusage.Record{APIKey: "joint", Model: "test", RequestedAt: fixed.Add(time.Duration(i) * time.Microsecond), Failed: true, ErrorCode: "http_404", FailureStage: "upstream"})
			}
			stats.Details(usage.DetailQuery{Limit: 50})
			var selections, queries, writes []int64
			var selectionMu sync.Mutex
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var wg sync.WaitGroup
				start := make(chan struct{})
				for range 100 {
					wg.Go(func() {
						<-start
						began := time.Now()
						_, err := scheduler.pickSingle(b.Context(), "test", "", core.Options{}, nil)
						elapsed := time.Since(began).Nanoseconds()
						if !isAuthRequestLimitedError(err) {
							b.Error("expected saturated limit")
						}
						selectionMu.Lock()
						selections = append(selections, elapsed)
						selectionMu.Unlock()
					})
				}
				wg.Go(func() {
					<-start
					for range 20 {
						began := time.Now()
						if page := stats.Details(usage.DetailQuery{Limit: 50}); len(page.Items) != 50 {
							b.Error("missing query page")
						}
						stats.FailureSummary(usage.FailureSummaryQuery{})
						queries = append(queries, time.Since(began).Nanoseconds())
					}
				})
				wg.Go(func() {
					<-start
					for range 100 {
						began := time.Now()
						stats.Record(context.Background(), coreusage.Record{APIKey: "joint", Model: "test", RequestedAt: fixed})
						writes = append(writes, time.Since(began).Nanoseconds())
					}
				})
				close(start)
				wg.Wait()
			}
			b.StopTimer()
			for name, values := range map[string][]int64{"selection": selections, "query": queries, "write": writes} {
				slices.Sort(values)
				for _, p := range []int{50, 95, 99} {
					b.ReportMetric(float64(values[(len(values)*p+99)/100-1]), fmt.Sprintf("%s_p%d_ns", name, p))
				}
			}
		})
	}
}

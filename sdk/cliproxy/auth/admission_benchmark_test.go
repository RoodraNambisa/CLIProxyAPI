package auth

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/admission"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

// The mutex mirrors the provider scheduler's critical section. Pre-existing
// executions hold the unavailable fraction, while measured attempts use leases.
func BenchmarkAdmissionConcurrency(b *testing.B) {
	for _, strategy := range []schedulerStrategy{schedulerStrategyRoundRobin, schedulerStrategyRandom} {
		for _, size := range []int{10000, 100000} {
			for _, ready := range []int{100, 10, 1, 0} {
				for _, workers := range []int{1, 100, 500} {
					b.Run(fmt.Sprintf("strategy_%d/%d/ready_%d/workers_%d", strategy, size, ready, workers), func(b *testing.B) {
						view := &readyView{}
						limiter := newAuthRequestWindowLimiter()
						limiter.traffic = admission.NewController()
						defer limiter.traffic.Close()
						now := time.Now()
						for i := 0; i < size; i++ {
							id := fmt.Sprint("admission-", i)
							entry := &scheduledAuth{auth: &Auth{ID: id}}
							view.flat = append(view.flat, entry)
							entity, _ := limiter.traffic.Traffic.Acquire("credential", id, 1)
							if i < size*ready/100 {
								entity.Release()
							}
						}
						index := view.requestLimitIndex(limiter, now)
						for i := size * ready / 100; i < size; i++ {
							index.exclude(view.flat[i], authRequestLimitBlock{concurrency: 1, resetIn: 24 * time.Hour}, now)
						}
						var mu sync.Mutex
						latencies := make([]int64, 0, workers*b.N)
						var waits int64
						before := SelectionMetricsSnapshot().CandidateChecks
						b.ReportAllocs()
						b.ResetTimer()
						for range b.N {
							var wg sync.WaitGroup
							for range workers {
								wg.Go(func() {
									slot := &core.AuthRequestSlot{}
									policy := func(*Auth) authRequestLimitPolicy { return authRequestLimitPolicy{concurrency: 1, requestSlot: slot} }
									start := time.Now()
									mu.Lock()
									waits += time.Since(start).Nanoseconds()
									entry, _ := view.pickWithRequestLimit(strategy, nil, policy, limiter)
									if (entry != nil) != (ready > 0) {
										b.Error("incorrect capacity")
									}
									slot.Commit()
									slot.FinishReservation()()
									latencies = append(latencies, time.Since(start).Nanoseconds())
									mu.Unlock()
								})
							}
							wg.Wait()
						}
						b.StopTimer()
						slices.Sort(latencies)
						for _, p := range []int{95, 99} {
							b.ReportMetric(float64(latencies[(len(latencies)*p+99)/100-1]), fmt.Sprintf("p%d_ns", p))
						}
						b.ReportMetric(float64(SelectionMetricsSnapshot().CandidateChecks-before)/float64(workers*b.N), "checks/request")
						b.ReportMetric(float64(waits)/float64(workers*b.N), "lock_wait_ns/request")
						b.ReportMetric(float64(workers*b.N)/b.Elapsed().Seconds(), "requests/s")
					})
				}
			}
		}
	}
}

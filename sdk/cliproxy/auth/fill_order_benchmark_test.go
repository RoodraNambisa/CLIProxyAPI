package auth

import (
	"fmt"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/admission"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"slices"
	"sync"
	"testing"
	"time"
)

func BenchmarkFillCapacity(b *testing.B) {
	for _, size := range []int{10000, 100000} {
		for _, ready := range []int{100, 10, 1, 0} {
			for _, width := range []int{1, 8} {
				for _, workers := range []int{1, 100, 500} {
					b.Run(fmt.Sprintf("n_%d/ready_%d/group_%d/workers_%d", size, ready, width, workers), func(b *testing.B) {
						limiter := newAuthRequestWindowLimiter()
						limiter.traffic = admission.NewController()
						defer limiter.traffic.Close()
						shard := &modelScheduler{entries: make(map[string]*scheduledAuth, size)}
						for i := range size {
							id := fmt.Sprintf("fill-%06d", i)
							a := &Auth{ID: id}
							shard.entries[id] = &scheduledAuth{auth: a, meta: &scheduledAuthMeta{auth: a}, state: scheduledStateReady, expiryIndex: -1}
							lease, _ := limiter.traffic.Traffic.Acquire("credential", id, 1)
							if i >= size-size*ready/100 {
								lease.Release()
							}
						}
						shard.rebuildIndexesLocked()
						// One cold pass populates exclusions. Repeated measurements exercise the hot path.
						slot := &core.AuthRequestSlot{}
						policy := func(*Auth) authRequestLimitPolicy { return authRequestLimitPolicy{concurrency: 1, requestSlot: slot} }
						shard.pickFillFirstAtPriorityLocked(false, 0, width, 0, nil, policy, limiter, nil, nil)
						slot.FinishReservation()()
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
									a, _, _ := shard.pickFillFirstAtPriorityLocked(false, 0, width, 0, nil, policy, limiter, nil, nil)
									if (a != nil) != (ready > 0) {
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
						b.ReportMetric(float64(waits)/float64(workers*b.N), "lock_wait_ns/request")
						b.ReportMetric(float64(SelectionMetricsSnapshot().CandidateChecks-before)/float64(workers*b.N), "checks/request")
						b.ReportMetric(float64(workers*b.N)/b.Elapsed().Seconds(), "requests/s")
					})
				}
			}
		}
	}
}

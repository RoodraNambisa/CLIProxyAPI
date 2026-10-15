package auth

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"
)

func benchmarkLimitedReadyView(n, availablePercent int) (*readyView, *authRequestWindowLimiter, func(*Auth) authRequestLimitPolicy) {
	view := &readyView{}
	limiter := newAuthRequestWindowLimiter()
	now := time.Now().Truncate(time.Minute).Add(time.Second)
	limiter.now = func() time.Time { return now }
	window, _ := authRequestWindowAt(now, 1)
	blocked := make(map[string]bool)
	for i := 0; i < n; i++ {
		id := fmt.Sprint("candidate-", i)
		view.flat = append(view.flat, &scheduledAuth{auth: &Auth{ID: id}})
		if i >= n*availablePercent/100 {
			blocked[id] = true
			limiter.counts[id] = authRequestWindowCount{window: window, windowMinutes: 1, limit: 1, count: 1}
		}
	}
	policy := func(a *Auth) authRequestLimitPolicy {
		if blocked[a.ID] {
			return authRequestLimitPolicy{limit: 1, windowMinutes: 1}
		}
		return authRequestLimitPolicy{windowMinutes: 1}
	}
	return view, limiter, policy
}

// Keep the pre-index random picker as a same-process contention baseline.
func legacyRandomRequestLimitPick(view *readyView, limiter *authRequestWindowLimiter, policy func(*Auth) authRequestLimitPolicy) *scheduledAuth {
	now := limiter.nowTime()
	try := func(entry *scheduledAuth) bool {
		selectionRuntime.candidates.Add(1)
		ok, _ := limiter.tryAcquireAt(entry.auth.ID, policy(entry.auth), now)
		return ok
	}
	for range 8 {
		entry := view.flat[rand.IntN(len(view.flat))]
		if try(entry) {
			return entry
		}
	}
	candidates := append([]*scheduledAuth(nil), view.flat...)
	for len(candidates) > 0 {
		i := rand.IntN(len(candidates))
		if try(candidates[i]) {
			return candidates[i]
		}
		candidates[i] = candidates[len(candidates)-1]
		candidates = candidates[:len(candidates)-1]
	}
	return nil
}

func BenchmarkRandomRequestLimitComparison(b *testing.B) {
	for _, size := range []int{10000, 100000} {
		for _, ready := range []int{100, 10, 1, 0} {
			for _, workers := range []int{1, 100, 500} {
				for _, mode := range []string{"legacy", "indexed_cold", "indexed_warm"} {
					b.Run(fmt.Sprintf("%d/ready_%d/workers_%d/%s", size, ready, workers, mode), func(b *testing.B) {
						view, limiter, policy := benchmarkLimitedReadyView(size, ready)
						if mode == "indexed_warm" {
							// Prime with authoritative limiter results, not invented exclusions.
							for _, entry := range view.flat {
								if ok, block := limiter.tryAcquireAt(entry.auth.ID, policy(entry.auth), limiter.nowTime()); !ok {
									view.requestLimitIndex(limiter, limiter.nowTime()).exclude(entry, block, limiter.nowTime())
								}
							}
						}
						var mu sync.Mutex
						latencies := make([]int64, 0, workers*b.N)
						var waitNanos int64
						before := SelectionMetricsSnapshot().CandidateChecks
						b.ReportAllocs()
						b.ResetTimer()
						for range b.N {
							var wg sync.WaitGroup
							for range workers {
								wg.Go(func() {
									start := time.Now()
									mu.Lock()
									waitNanos += time.Since(start).Nanoseconds()
									if mode == "legacy" {
										legacyRandomRequestLimitPick(view, limiter, policy)
									} else {
										view.pickWithRequestLimit(schedulerStrategyRandom, nil, policy, limiter)
									}
									latencies = append(latencies, time.Since(start).Nanoseconds())
									mu.Unlock()
								})
							}
							wg.Wait()
						}
						b.StopTimer()
						slices.Sort(latencies)
						for _, p := range []int{50, 95, 99} {
							b.ReportMetric(float64(latencies[(len(latencies)*p+99)/100-1]), fmt.Sprintf("p%d_ns", p))
						}
						b.ReportMetric(float64(SelectionMetricsSnapshot().CandidateChecks-before)/float64(workers*b.N), "checks/request")
						b.ReportMetric(float64(waitNanos)/float64(workers*b.N), "lock_wait_ns/request")
					})
				}
			}
		}
	}
}

func BenchmarkRandomRequestLimitSelection(b *testing.B) {
	for _, n := range []int{10000, 100000} {
		for _, available := range []int{100, 10, 1, 0} {
			b.Run(fmt.Sprintf("%d/ready_%d", n, available), func(b *testing.B) {
				view, limiter, policy := benchmarkLimitedReadyView(n, available)
				b.ReportAllocs()
				for b.Loop() {
					entry, _ := view.pickWithRequestLimit(schedulerStrategyRandom, nil, policy, limiter)
					if (entry != nil) != (available > 0) {
						b.Fatal("incorrect eligibility")
					}
				}
			})
		}
	}
}

func BenchmarkRandomRequestLimitContention(b *testing.B) {
	for _, workers := range []int{1, 100, 500} {
		b.Run(fmt.Sprint(workers), func(b *testing.B) {
			view, limiter, policy := benchmarkLimitedReadyView(100000, 0)
			var providerMu sync.Mutex
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				for j := 0; j < workers; j++ {
					wg.Go(func() {
						providerMu.Lock()
						view.pickWithRequestLimit(schedulerStrategyRandom, nil, policy, limiter)
						providerMu.Unlock()
					})
				}
				wg.Wait()
			}
		})
	}
}

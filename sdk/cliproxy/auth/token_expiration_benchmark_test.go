package auth

import (
	"context"
	"fmt"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func benchmarkTokenExpiryShard(b *testing.B, count int) (*modelScheduler, time.Time) {
	b.Helper()
	now := time.Now()
	shard := &modelScheduler{entries: make(map[string]*scheduledAuth, count)}
	for i := 0; i < count; i++ {
		expiry := now.Add(time.Hour + time.Duration(i)*time.Second)
		auth := &Auth{ID: fmt.Sprintf("web-%06d", i), Provider: "chatgpt-web", Metadata: map[string]any{
			"access_token": expiryTestToken(fmt.Sprint(expiry.Unix())),
		}}
		shard.entries[auth.ID] = buildScheduledAuth(buildScheduledAuthMetaWithSupportedModels(auth, nil), "", now)
	}
	shard.rebuildIndexesLocked()
	return shard, now
}

func BenchmarkChatGPTWebTokenExpiry(b *testing.B) {
	for _, count := range []int{1000, 100000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			shard, now := benchmarkTokenExpiryShard(b, count)
			b.Run("IdleCheck", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					shard.promoteExpiredLocked(now)
				}
			})
			for _, strategy := range []schedulerStrategy{schedulerStrategyRoundRobin, schedulerStrategyRandom} {
				name := "RoundRobinPick"
				if strategy == schedulerStrategyRandom {
					name = "RandomPick"
				}
				b.Run(name, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						auth, err := shard.pickReadyLocked(false, func(int) schedulerStrategy { return strategy }, nil, nil, nil, nil, nil, 0, nil, nil, "chatgpt-web", "")
						if err != nil || auth == nil {
							b.Fatal("failed to pick", err)
						}
					}
				})
				b.Run(name+"IndexedScheduler", func(b *testing.B) {
					scheduler := newAuthScheduler(&RoundRobinSelector{})
					scheduler.strategy = strategy
					provider := scheduler.ensureProviderLocked("chatgpt-web")
					provider.modelShards[""] = shard
					b.ReportAllocs()
					for b.Loop() {
						auth, err := scheduler.pickSingle(context.Background(), "chatgpt-web", "", cliproxyexecutor.Options{}, nil)
						if err != nil || auth == nil {
							b.Fatal("failed scheduler pick", err)
						}
					}
				})
			}
		})
	}
}

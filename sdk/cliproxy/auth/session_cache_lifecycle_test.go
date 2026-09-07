package auth

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSessionCacheConcurrentStopPreservesLiveBindings(t *testing.T) {
	for range 100 {
		cache := NewSessionCache(time.Hour)
		cache.Set("session", "auth")
		start := make(chan struct{})
		var panics atomic.Int32
		var workers sync.WaitGroup
		for range 32 {
			workers.Go(func() {
				defer func() {
					if recover() != nil {
						panics.Add(1)
					}
				}()
				<-start
				cache.Stop()
			})
		}
		close(start)
		workers.Wait()
		if panics.Load() != 0 {
			t.Fatal("concurrent cleanup shutdown panicked")
		}
		if value, ok := cache.Get("session"); !ok || value != "auth" {
			t.Fatal("stopping cleanup invalidated a live request's binding")
		}
	}
}

func TestSessionCacheSmallPositiveTTLCleanupDoesNotPanic(t *testing.T) {
	cache := &SessionCache{ttl: time.Nanosecond, stopCh: make(chan struct{})}
	cache.Stop()
	cache.cleanupLoop()
}

package auth

import (
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestRoutingUpdatesKeepPublishedPolicyAndSchedulerConsistent(t *testing.T) {
	for range 100 {
		manager := NewManager(nil, nil, nil)
		start := make(chan struct{})
		var workers sync.WaitGroup
		for index := range 16 {
			workers.Go(func() {
				<-start
				manager.SetSelector(&FillFirstSelector{Range: index + 1})
				manager.SetConfig(&config.Config{Routing: config.RoutingConfig{FillFirstRange: 2 * (index + 1)}})
			})
		}
		close(start)
		workers.Wait()
		policy := manager.selectionPolicy()
		manager.scheduler.mu.RLock()
		strategy, fillRange := manager.scheduler.strategy, manager.scheduler.globalFillFirstRange
		manager.scheduler.mu.RUnlock()
		if policy.strategy != strategy || policy.fillRange != fillRange {
			t.Fatal("concurrent setters left scheduler and request policy at different updates")
		}
	}
}

func TestRoutingSelectorReplacementStopsOnlyUnusedCacheMaintenance(t *testing.T) {
	old := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &RoundRobinSelector{}, TTL: time.Hour})
	defer old.Stop()
	manager := NewManager(nil, old, nil)
	old.cache.Set("session", "auth")
	ctx := manager.WithRoutingPolicySnapshot(t.Context())
	manager.SetSelector(old)
	select {
	case <-old.cache.stopCh:
		t.Fatal("reusing the same selector stopped its maintenance")
	default:
	}
	wrapper := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: old, TTL: time.Hour})
	defer wrapper.Stop()
	manager.SetSelector(wrapper)
	select {
	case <-old.cache.stopCh:
		t.Fatal("a retained fallback selector was stopped")
	default:
	}
	manager.SetSelector(&RoundRobinSelector{})
	for _, cache := range []*SessionCache{old.cache, wrapper.cache} {
		select {
		case <-cache.stopCh:
		default:
			t.Fatal("replaced selector leaked its cleanup goroutine")
		}
	}
	if manager.selectorForContext(ctx) != old {
		t.Fatal("replacement changed a captured selector")
	}
	if value, ok := old.cache.Get("session"); !ok || value != "auth" {
		t.Fatal("cleanup shutdown invalidated an in-flight binding")
	}
}

type routingOpaqueSelector struct{ Selector }

func TestRoutingSelectorReplacementPreservesSharedAndOpaqueCaches(t *testing.T) {
	old := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &RoundRobinSelector{}, TTL: time.Hour})
	defer old.Stop()
	manager := NewManager(nil, old, nil)
	shared := &SessionAffinitySelector{fallback: &RoundRobinSelector{}, cache: old.cache}
	manager.SetSelector(shared)
	select {
	case <-old.cache.stopCh:
		t.Fatal("shared current cache was stopped")
	default:
	}
	manager.SetSelector(&routingOpaqueSelector{Selector: shared})
	select {
	case <-old.cache.stopCh:
		t.Fatal("opaque caller-owned chain was stopped")
	default:
	}
}

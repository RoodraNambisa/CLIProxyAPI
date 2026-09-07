package auth

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"
)

func TestBootstrapRetryBudgetCannotBeRefilledByNestedSetup(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	for _, initial := range []int{-1, 0, 1, 2} {
		ctx := manager.WithRequestRetryBudget(t.Context(), initial)
		for range max(0, initial) {
			if !ConsumeRequestRetryBudget(ctx) {
				t.Fatal("initial budget unavailable")
			}
		}
		for _, next := range []int{0, 1, 20} {
			nested := manager.WithRequestRetryBudgetForProviders(ctx, []string{"codex"}, next)
			if nested != ctx || ConsumeRequestRetryBudget(nested) {
				t.Fatalf("nested setup refilled an exhausted budget: initial=%d next=%d", initial, next)
			}
		}
	}
}

func TestBootstrapRetryBudgetDoesNotNarrowGoIntegers(t *testing.T) {
	large := int64(math.MaxInt32) + 1
	if int64(int(large)) != large {
		t.Skip("Go int is 32 bits")
	}
	manager := NewManager(nil, nil, nil)
	ctx := manager.WithRequestRetryBudget(nil, int(large))
	if !ConsumeRequestRetryBudget(ctx) || int64(requestRetryBudgetFromContext(ctx).remaining.Load()) != large-1 {
		t.Fatal("bootstrap retry budget overflowed")
	}
}

func TestBootstrapRetryBudgetCancellationDoesNotSpendRemaining(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	parent := manager.WithRequestRetryBudget(t.Context(), 1)
	canceled, cancel := context.WithCancel(parent)
	cancel()
	if ConsumeRequestRetryBudget(canceled) {
		t.Fatal("canceled request consumed a retry")
	}
	if !ConsumeRequestRetryBudget(parent) {
		t.Fatal("canceled child changed the shared budget")
	}
}

func TestBootstrapRetryBudgetConcurrentConsumptionIsBounded(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	ctx := manager.WithRequestRetryBudget(t.Context(), 73)
	var count atomic.Int64
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			for range 32 {
				if ConsumeRequestRetryBudget(ctx) {
					count.Add(1)
				}
			}
		})
	}
	workers.Wait()
	if count.Load() != 73 || requestRetryBudgetFromContext(ctx).remaining.Load() != 0 {
		t.Fatal("concurrent consumers exceeded or lost the retry budget")
	}
}

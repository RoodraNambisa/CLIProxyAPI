package auth

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func weightedTestAuth(id string, weight int) *Auth {
	return &Auth{ID: id, Provider: "codex", Attributes: map[string]string{AttributeWeight: fmt.Sprint(weight)}}
}

type weightedObservedContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func (c *weightedObservedContext) Err() error {
	err := c.Context.Err()
	c.once.Do(func() { close(c.observed) })
	return err
}

func TestWeightedSelectorCancellationWhileWaitingDoesNotAdvance(t *testing.T) {
	selector := &WeightedRoundRobinSelector{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	observed := &weightedObservedContext{Context: ctx, observed: make(chan struct{})}
	selector.mu.Lock()
	done := make(chan error, 1)
	go func() {
		_, err := selector.Pick(observed, "codex", "model", core.Options{}, []*Auth{weightedTestAuth("a", 1)})
		done <- err
	}()
	select {
	case <-observed.observed:
	case <-time.After(time.Second):
		selector.mu.Unlock()
		t.Fatal("selection did not check cancellation")
	}
	cancel()
	selector.mu.Unlock()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting selection error=%v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled selection did not finish")
	}
	if len(selector.states) != 0 {
		t.Fatal("canceled selection created or advanced weight state")
	}
}

func TestWeightedSelectorActiveWeightEditAndBoundedModelState(t *testing.T) {
	selector := &WeightedRoundRobinSelector{maxKeys: 2}
	pool := []*Auth{weightedTestAuth("a", 5), weightedTestAuth("b", 1), weightedTestAuth("c", 1)}
	if _, err := selector.Pick(t.Context(), "codex", "model", core.Options{}, pool); err != nil {
		t.Fatal(err)
	}
	pool[0].Attributes[AttributeWeight] = "1"
	for _, id := range []string{"a", "b", "c"} {
		picked, err := selector.Pick(t.Context(), "codex", "model", core.Options{}, pool)
		if err != nil || picked == nil || picked.ID != id {
			t.Fatal("edited weights retained obsolete credits")
		}
	}
	for _, model := range []string{"other", "third", "fourth"} {
		if _, err := selector.Pick(t.Context(), "codex", model, core.Options{}, pool); err != nil {
			t.Fatal(err)
		}
		if len(selector.states) > 2 {
			t.Fatal("model churn exceeded the configured state bound")
		}
	}
}

func TestWeightedSelectorCredentialChurnBoundsStaleCredits(t *testing.T) {
	selector := &WeightedRoundRobinSelector{}
	for index := range 2*maxSmoothWeightedStateEntries + 2 {
		_, err := selector.Pick(t.Context(), "codex", "model", core.Options{}, []*Auth{weightedTestAuth(fmt.Sprint(index), 1)})
		if err != nil {
			t.Fatal(err)
		}
		state := selector.states["codex:model"]
		if len(state.current) > maxSmoothWeightedStateEntries || len(state.weights) > maxSmoothWeightedStateEntries {
			t.Fatal("credential churn retained unbounded stale credits")
		}
	}
}

func TestWeightedSelectorProportionsAndOrdinarySelectorCompatibility(t *testing.T) {
	pool := []*Auth{weightedTestAuth("a", 5), weightedTestAuth("b", 1), weightedTestAuth("c", 1), weightedTestAuth("zero", 0)}
	selector := &WeightedRoundRobinSelector{}
	var sequence []string
	for range 7 {
		picked, err := selector.Pick(t.Context(), "codex", "model", core.Options{}, pool)
		if err != nil {
			t.Fatal(err)
		}
		sequence = append(sequence, picked.ID)
	}
	if !reflect.DeepEqual(sequence, []string{"a", "a", "b", "a", "c", "a", "a"}) {
		t.Fatalf("unexpected weighted sequence: %v", sequence)
	}
	rr := &RoundRobinSelector{}
	seen := make(map[string]bool)
	for range 4 {
		picked, err := rr.Pick(t.Context(), "codex", "model", core.Options{}, pool)
		if err != nil {
			t.Fatal(err)
		}
		seen[picked.ID] = true
	}
	if !seen["zero"] || len(seen) != 4 {
		t.Fatal("ordinary round-robin started interpreting weights")
	}
}

func TestWeightedSelectorKeepsCreditsForTransientSubsetsAndRejectedSlots(t *testing.T) {
	pool := []*Auth{weightedTestAuth("a", 1), weightedTestAuth("b", 1), weightedTestAuth("c", 1)}
	selector := &WeightedRoundRobinSelector{}
	first, _ := selector.Pick(t.Context(), "codex", "model", core.Options{}, pool)
	if first.ID != "a" {
		t.Fatal("unstable initial tie")
	}
	_, _ = selector.Pick(t.Context(), "codex", "model", core.Options{}, pool[:1])
	next, _ := selector.Pick(t.Context(), "codex", "model", core.Options{}, pool)
	if next.ID != "b" {
		t.Fatal("temporary filtering reset pending credits")
	}
	before := selector.states["codex:model"].clone()
	_, err := selector.pickAccepted(t.Context(), "codex", "model", core.Options{}, pool, func(*Auth) bool { return false })
	if err == nil || !reflect.DeepEqual(before.current, selector.states["codex:model"].current) {
		t.Fatal("failed reservations consumed weighted credits")
	}
	next, err = selector.pickAccepted(t.Context(), "codex", "model", core.Options{}, pool, func(a *Auth) bool { return a.ID != "c" })
	if err != nil || next.ID == "c" {
		t.Fatal("failed reservation prevented another eligible credential")
	}
}

func TestWeightedSelectorZeroPriorityModelScopeAndOverflow(t *testing.T) {
	zero := weightedTestAuth("zero", 0)
	zero.Attributes["priority"] = "9"
	pool := []*Auth{zero, weightedTestAuth("a", 1), weightedTestAuth("b", 1)}
	selector := &WeightedRoundRobinSelector{}
	for _, model := range []string{"first", "second"} {
		opts := core.Options{Metadata: map[string]any{core.RequestedModelMetadataKey: model + "(high)"}}
		picked, err := selector.Pick(t.Context(), "codex", "", opts, pool)
		if err != nil || picked.ID != "a" {
			t.Fatal("zero top-priority credential blocked selection or model scopes collided")
		}
	}
	if saturatingAddInt64(math.MaxInt64, 1) != math.MaxInt64 || saturatingAddInt64(math.MinInt64, -1) != math.MinInt64 {
		t.Fatal("weighted accumulator overflowed")
	}
}

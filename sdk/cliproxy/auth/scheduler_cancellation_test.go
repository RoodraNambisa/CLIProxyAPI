package auth

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestSchedulerCancellationWhileWaitingForLocks(t *testing.T) {
	for _, mode := range []string{"single", "mixed", "pinned", "mixed-cursor"} {
		t.Run(mode, func(t *testing.T) {
			first, second := weightedTestAuth(schedulerTestID(t, "a"), 1), weightedTestAuth(schedulerTestID(t, "b"), 1)
			providers := []string{"codex"}
			if mode != "single" {
				second.Provider = "openai"
				providers = append(providers, "openai")
			}
			for _, credential := range []*Auth{first, second} {
				registerSchedulerModels(t, credential.Provider, "cancel-model", credential.ID)
			}
			scheduler := newSchedulerForTest(&WeightedRoundRobinSelector{}, first, second)
			opts := core.Options{}
			if mode == "pinned" {
				opts.Metadata = map[string]any{core.PinnedAuthMetadataKey: first.ID}
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var selectedContext context.Context = ctx
			started := make(chan struct{})
			var allowed func(*Auth) bool
			var unlock func()
			if mode == "mixed-cursor" {
				scheduler.mixedCursorMu.Lock()
				unlock = scheduler.mixedCursorMu.Unlock
				var once sync.Once
				allowed = func(*Auth) bool { once.Do(func() { close(started) }); return true }
			} else {
				scheduler.providers["codex"].mu.Lock()
				unlock = scheduler.providers["codex"].mu.Unlock
				selectedContext = &weightedObservedContext{Context: ctx, observed: started}
			}
			var unlockOnce sync.Once
			release := func() { unlockOnce.Do(unlock) }
			defer release()
			done := make(chan error, 1)
			go func() {
				_, _, err := scheduler.pickMixed(selectedContext, providers, "cancel-model", opts, nil, allowed)
				done <- err
			}()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("selection did not reach the controlled lock wait")
			}
			cancel()
			release()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatal("queued scheduler request ignored cancellation")
				}
			case <-time.After(time.Second):
				t.Fatal("queued scheduler did not return after releasing the lock")
			}
			picked, _, err := scheduler.pickMixed(t.Context(), providers, "cancel-model", opts, nil)
			if err != nil || picked == nil || picked.ID != first.ID {
				t.Fatal("canceled lock waiter advanced weighted credits")
			}
		})
	}
}

func TestSchedulerCancellationDoesNotSelectOrAdvance(t *testing.T) {
	for name, selector := range map[string]Selector{"round-robin": &RoundRobinSelector{}, "weighted": &WeightedRoundRobinSelector{}} {
		for _, mode := range []string{"single", "mixed", "pinned"} {
			t.Run(name+"/"+mode, func(t *testing.T) {
				first, second := weightedTestAuth(schedulerTestID(t, "a"), 1), weightedTestAuth(schedulerTestID(t, "b"), 1)
				providers := []string{"codex"}
				if mode != "single" {
					second.Provider = "openai"
					providers = append(providers, "openai")
				}
				for _, credential := range []*Auth{first, second} {
					registerSchedulerModels(t, credential.Provider, "cancel-model", credential.ID)
				}
				scheduler := newSchedulerForTest(selector, first, second)
				opts := core.Options{}
				if mode == "pinned" {
					opts.Metadata = map[string]any{core.PinnedAuthMetadataKey: first.ID}
				}
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				picked, _, err := scheduler.pickMixed(ctx, providers, "cancel-model", opts, nil)
				if !errors.Is(err, context.Canceled) || picked != nil {
					t.Fatal("canceled scheduler request selected a credential")
				}
				picked, _, err = scheduler.pickMixed(t.Context(), providers, "cancel-model", opts, nil)
				if err != nil || picked == nil || picked.ID != first.ID {
					t.Fatal("canceled request changed the next valid selection")
				}
			})
		}
	}
}

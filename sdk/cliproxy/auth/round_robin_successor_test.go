package auth

import (
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestRoundRobinContinuesAfterFilteringThePreviouslyPickedCredential(t *testing.T) {
	selector := &RoundRobinSelector{}
	for _, step := range []struct{ ids, want string }{{"abcd", "a"}, {"bcd", "b"}, {"acd", "c"}, {"abcd", "d"}, {"bc", "b"}, {"acd", "c"}} {
		var candidates []*Auth
		for _, id := range step.ids {
			candidates = append(candidates, &Auth{ID: string(id), Status: StatusActive})
		}
		picked, err := selector.Pick(t.Context(), "codex", "gpt-5.4", executor.Options{}, candidates)
		if err != nil || picked.ID != step.want {
			t.Fatalf("candidates %s: got %v, want %s", step.ids, picked, step.want)
		}
	}
}

func TestSchedulerRoundRobinRebuildKeepsGeneralAndWebsocketSuccessors(t *testing.T) {
	model := &modelScheduler{entries: map[string]*scheduledAuth{}}
	for _, id := range []string{"a", "b", "c", "d"} {
		auth := &Auth{ID: id, Status: StatusActive}
		model.entries[id] = &scheduledAuth{auth: auth, meta: &scheduledAuthMeta{auth: auth, priority: 10, websocketEnabled: id == "b" || id == "d"}, state: scheduledStateReady}
	}
	model.rebuildIndexesLocked()
	if model.readyByPriority[10].all.pickRoundRobin(nil).auth.ID != "a" || model.readyByPriority[10].ws.pickRoundRobin(nil).auth.ID != "b" {
		t.Fatal("incorrect initial rings")
	}
	delete(model.entries, "a")
	model.rebuildIndexesLocked()
	if model.readyByPriority[10].all.pickRoundRobin(nil).auth.ID != "b" || model.readyByPriority[10].ws.pickRoundRobin(nil).auth.ID != "d" {
		t.Fatal("removal reset a ready view to its numeric offset")
	}
	model.entries["c"].state = scheduledStateCooldown
	model.entries["c"].nextRetryAt = time.Now().Add(time.Minute)
	model.rebuildIndexesLocked()
	if model.readyByPriority[10].all.pickRoundRobin(nil).auth.ID != "d" {
		t.Fatal("cooldown skipped the remaining successor")
	}
	model.entries["c"].state = scheduledStateReady
	model.rebuildIndexesLocked()
	if model.readyByPriority[10].all.pickRoundRobin(nil).auth.ID != "b" {
		t.Fatal("recovery did not wrap the sorted ring")
	}
	if model.readyByPriority[10].all.pickRoundRobin(func(entry *scheduledAuth) bool { return entry.auth.ID == "d" }).auth.ID != "d" {
		t.Fatal("predicate filtering lost the valid next credential")
	}
}

func TestRoundRobinConcurrentEqualPoolIsFair(t *testing.T) {
	selector := &RoundRobinSelector{}
	pool := []*Auth{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	counts := map[string]int{}
	var mu sync.Mutex
	var work sync.WaitGroup
	for range 8 {
		work.Go(func() {
			for range 75 {
				picked, err := selector.Pick(t.Context(), "codex", "gpt-5.4", executor.Options{}, pool)
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				counts[picked.ID]++
				mu.Unlock()
			}
		})
	}
	work.Wait()
	for _, count := range counts {
		if count != 200 {
			t.Fatal("concurrent selection is not equally distributed")
		}
	}
}

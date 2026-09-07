package auth

import (
	"reflect"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestWeightedSchedulerSingleAndMixedProportions(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		pool := []*Auth{weightedTestAuth(schedulerTestID(t, "a"), 5), weightedTestAuth(schedulerTestID(t, "b"), 1), weightedTestAuth(schedulerTestID(t, "c"), 1)}
		providers := []string{"codex"}
		if mixed {
			pool[2].Provider = "openai"
			providers = append(providers, "openai")
		}
		for _, credential := range pool {
			registerSchedulerModels(t, credential.Provider, "weighted-model", credential.ID)
		}
		scheduler := newSchedulerForTest(&WeightedRoundRobinSelector{}, pool...)
		counts := map[string]int{}
		for range 70 {
			picked, _, err := scheduler.pickMixed(t.Context(), providers, "weighted-model", core.Options{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			counts[picked.ID]++
		}
		if counts[pool[0].ID] != 50 || counts[pool[1].ID] != 10 || counts[pool[2].ID] != 10 {
			t.Fatal("scheduler lost configured proportions")
		}
	}
}

func TestWeightedSchedulerPriorityOverridesDoNotDisableOtherStrategies(t *testing.T) {
	zero, positive := weightedTestAuth(schedulerTestID(t, "zero"), 0), weightedTestAuth(schedulerTestID(t, "positive"), 1)
	zero.Attributes["priority"] = "10"
	registerSchedulerModels(t, "codex", "weighted-model", zero.ID, positive.ID)
	scheduler := newSchedulerForTest(&WeightedRoundRobinSelector{}, zero, positive)
	picked, err := scheduler.pickSingle(t.Context(), "codex", "weighted-model", core.Options{}, nil)
	if err != nil || picked.ID != positive.ID {
		t.Fatal("zero-weight priority blocked positive credentials")
	}
	scheduler.setRoutingPriorityOverrides([]config.RoutingPriorityOverride{{Priority: 10, Strategy: "round-robin"}})
	picked, err = scheduler.pickSingle(t.Context(), "codex", "weighted-model", core.Options{}, nil)
	if err != nil || picked.ID != zero.ID {
		t.Fatal("non-weighted priority override interpreted weight")
	}
	scheduler.setSelector(&RoundRobinSelector{})
	scheduler.setRoutingPriorityOverrides([]config.RoutingPriorityOverride{{Priority: 10, Strategy: "weighted-round-robin"}})
	picked, err = scheduler.pickSingle(t.Context(), "codex", "weighted-model", core.Options{}, nil)
	if err != nil || picked.ID != positive.ID {
		t.Fatal("weighted priority override did not exclude zero")
	}
}

func TestWeightedReadyViewKeepsCreditsAcrossRebuildAndFiltering(t *testing.T) {
	model := &modelScheduler{entries: map[string]*scheduledAuth{}}
	for _, id := range []string{"a", "b", "c"} {
		credential := weightedTestAuth(id, 1)
		model.entries[id] = &scheduledAuth{auth: credential, meta: &scheduledAuthMeta{auth: credential, priority: 0}, state: scheduledStateReady}
	}
	model.rebuildIndexesLocked()
	pick := func(predicate func(*scheduledAuth) bool) string {
		t.Helper()
		entry, _ := model.readyByPriority[0].all.pickWithRequestLimit(schedulerStrategyWeightedRoundRobin, predicate, nil, nil)
		if entry == nil {
			t.Fatal("missing weighted candidate")
		}
		return entry.auth.ID
	}
	if pick(nil) != "a" {
		t.Fatal("initial tie is not deterministic")
	}
	before := model.readyByPriority[0].all.weighted.clone()
	model.entries["a"].state = scheduledStateCooldown
	model.entries["a"].nextRetryAt = time.Now().Add(time.Minute)
	model.rebuildIndexesLocked()
	if !reflect.DeepEqual(before.current, model.readyByPriority[0].all.weighted.current) {
		t.Fatal("cooldown rebuild discarded pending credits")
	}
	if pick(nil) != "b" {
		t.Fatal("rebuild restarted weighted order")
	}
	if pick(func(entry *scheduledAuth) bool { return entry.auth.ID == "b" }) != "b" {
		t.Fatal("filtered pick failed")
	}
	model.entries["a"].state = scheduledStateReady
	model.rebuildIndexesLocked()
	if pick(nil) != "c" {
		t.Fatal("filtered requests reset another credential's credit")
	}
}

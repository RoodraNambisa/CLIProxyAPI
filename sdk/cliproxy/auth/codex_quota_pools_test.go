package auth

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestCodexQuotaPoolsRetainSharedQuotaAndDeduplicateActivePool(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	installed, err := manager.Register(t.Context(), &Auth{ID: "quota", Provider: "codex"})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Unix(100, 0)
	first := http.Header{
		"X-Codex-Active-Limit":         {"premium"},
		"X-Codex-Primary-Used-Percent": {"50"}, "X-Codex-Primary-Window-Minutes": {"10080"},
		"X-Codex-Primary-Reset-After-Seconds":    {"3600"},
		"X-Codex-Bengalfox-Limit-Name":           {"GPT-5.3-Codex-Spark"},
		"X-Codex-Bengalfox-Primary-Used-Percent": {"0"}, "X-Codex-Bengalfox-Primary-Window-Minutes": {"300"},
		"X-Codex-Bengalfox-Secondary-Used-Percent": {"2"}, "X-Codex-Bengalfox-Secondary-Window-Minutes": {"10080"},
	}
	manager.recordCodexQuotaObservation(installed.ID, installed.RuntimeInstanceID(), "http", first, at)
	second := first.Clone()
	second.Set("X-Codex-Active-Limit", "codex_bengalfox")
	second.Set("X-Codex-Primary-Used-Percent", "0")
	second.Set("X-Codex-Primary-Window-Minutes", "300")
	second.Set("X-Codex-Secondary-Used-Percent", "2")
	second.Set("X-Codex-Secondary-Window-Minutes", "10080")
	manager.recordCodexQuotaObservation(installed.ID, installed.RuntimeInstanceID(), "http", second, at.Add(time.Minute))
	current, _ := manager.GetByID(installed.ID)
	snapshot := current.CodexQuotaSnapshot()
	if len(snapshot.Pools) != 2 {
		t.Fatalf("duplicate or lost pool: %#v", snapshot.Pools)
	}
	pools := make(map[string]CodexQuotaPool)
	for _, pool := range snapshot.Pools {
		pools[pool.ID] = pool
	}
	shared, spark := pools["codex"], pools["codex_bengalfox"]
	if shared.Signals["X-Codex-Primary-Used-Percent"] != "50" || !shared.ObservedAt.Equal(at) || shared.Signals["X-Codex-Primary-Reset-After-Seconds"] != "3600" {
		t.Fatal("a model-specific response overwrote or redated the shared quota")
	}
	if spark.Name != "GPT-5.3-Codex-Spark" || spark.Signals["X-Codex-Secondary-Used-Percent"] != "2" || !spark.ObservedAt.Equal(at.Add(time.Minute)) {
		t.Fatal("active pool alias did not resolve to the named pool")
	}
	shared.Signals["X-Codex-Primary-Used-Percent"] = "reader"
	current, _ = manager.GetByID(installed.ID)
	for _, pool := range current.CodexQuotaSnapshot().Pools {
		if pool.Signals["X-Codex-Primary-Used-Percent"] == "reader" {
			t.Fatal("a reader mutated the retained quota pool")
		}
	}
	// A later event for the same pool replaces its windows; names may persist.
	manager.recordCodexQuotaObservation(installed.ID, installed.RuntimeInstanceID(), "websocket", http.Header{
		"X-Codex-Active-Limit": {"codex_bengalfox"}, "X-Codex-Primary-Used-Percent": {"10"},
	}, at.Add(2*time.Minute))
	current, _ = manager.GetByID(installed.ID)
	for _, pool := range current.CodexQuotaSnapshot().Pools {
		if pool.ID == "codex_bengalfox" && (pool.Name != spark.Name || len(pool.Signals) != 1 || pool.Signals["X-Codex-Primary-Used-Percent"] != "10") {
			t.Fatal("a partial update inherited stale fields from its previous snapshot")
		}
	}
	if current.CloneWithoutRuntimeInstance().CodexQuotaSnapshot() != nil {
		t.Fatal("retained pools escaped the credential runtime instance")
	}
}

func TestCodexQuotaPoolsUseIdentityRatherThanPercentages(t *testing.T) {
	observation := newCodexQuotaObservation(http.Header{
		"X-Codex-Active-Limit": {"codex_other"}, "X-Codex-Primary-Used-Percent": {"2"},
		"X-Codex-Bengalfox-Primary-Used-Percent":        {"2"},
		"X-Codex-Additional-Spark-Limit-Id":             {"codex_bengalfox"},
		"X-Codex-Additional-Spark-Limit-Name":           {"Spark"},
		"X-Codex-Additional-Spark-Primary-Used-Percent": {"2"},
	}, "http", time.Unix(100, 0))
	pools, _ := mergeCodexQuotaPools(nil, observation)
	if len(pools) != 2 {
		t.Fatalf("pool identity was lost: %#v", pools)
	}
	// Quota-free observations update the latest raw diagnostics without erasing pools.
	next := newCodexQuotaObservation(http.Header{"Retry-After": {"10"}}, "http", time.Unix(200, 0))
	observation.Pools = pools
	retained, _ := mergeCodexQuotaPools(observation, next)
	if len(retained) != 2 {
		t.Fatal("retry-only observation erased independently scoped quota")
	}
}

func TestCodexQuotaPoolsEvictOldestWithinBound(t *testing.T) {
	var previous *CodexQuotaObservation
	for index := range maxCodexQuotaPools + 3 {
		current := newCodexQuotaObservation(http.Header{
			"X-Codex-Active-Limit": {fmt.Sprintf("codex_pool_%d", index)}, "X-Codex-Primary-Used-Percent": {"0"},
		}, "http", time.Unix(int64(index+100), 0))
		current.Pools, _ = mergeCodexQuotaPools(previous, current)
		previous = current
	}
	if len(previous.Pools) != maxCodexQuotaPools || previous.Pools[len(previous.Pools)-1].ObservedAt.Unix() != 103 {
		t.Fatal("retained pool count or eviction order was unbounded")
	}
}

func TestCodexQuotaPoolsAcceptOutOfOrderIndependentPools(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	installed, err := manager.Register(t.Context(), &Auth{ID: "quota", Provider: "codex"})
	if err != nil {
		t.Fatal(err)
	}
	spark := http.Header{"X-Codex-Active-Limit": {"codex_bengalfox"}, "X-Codex-Primary-Used-Percent": {"2"}}
	shared := http.Header{"X-Codex-Active-Limit": {"premium"}, "X-Codex-Primary-Used-Percent": {"50"}}
	manager.recordCodexQuotaObservation(installed.ID, installed.RuntimeInstanceID(), "http", spark, time.Unix(200, 0))
	if !manager.recordCodexQuotaObservation(installed.ID, installed.RuntimeInstanceID(), "http", shared, time.Unix(100, 0)) {
		t.Fatal("an independent shared-quota response was lost due to publication ordering")
	}
	if manager.recordCodexQuotaObservation(installed.ID, installed.RuntimeInstanceID(), "http", spark, time.Unix(150, 0)) {
		t.Fatal("an older response replaced an already newer pool")
	}
	current, _ := manager.GetByID(installed.ID)
	snapshot := current.CodexQuotaSnapshot()
	if snapshot.ObservedAt.Unix() != 200 || snapshot.Signals["X-Codex-Active-Limit"] != "codex_bengalfox" || len(snapshot.Pools) != 2 {
		t.Fatal("pool publication changed the latest raw observation")
	}
}

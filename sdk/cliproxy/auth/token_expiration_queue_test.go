package auth

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

func TestTokenExpiryQueueTracksRefreshRemovalAndCooldown(t *testing.T) {
	now := time.Unix(1800000000, 0)
	shard := &modelScheduler{entries: make(map[string]*scheduledAuth)}
	random := rand.New(rand.NewPCG(1, 2))
	for step := 0; step < 1000; step++ {
		id := fmt.Sprintf("web-%02d", random.IntN(50))
		if step%13 == 0 {
			shard.removeEntryLocked(id)
		} else {
			auth := &Auth{ID: id, Provider: "chatgpt-web", Metadata: map[string]any{
				"access_token": expiryTestToken(fmt.Sprint(now.Add(time.Duration(random.IntN(100)-20) * time.Second).Unix())),
			}}
			if step%7 == 0 {
				auth.Metadata["access_token"] = "opaque"
			}
			if step%9 == 0 {
				auth.Disabled = true
			}
			if step%11 == 0 {
				auth.Unavailable = true
				auth.NextRetryAfter = now.Add(2 * time.Second)
				auth.Quota.Exceeded = true
			}
			shard.upsertEntryLocked(buildScheduledAuthMetaWithSupportedModels(auth, nil), now)
		}
		now = now.Add(time.Second)
		shard.promoteExpiredLocked(now)
		readyKnown := 0
		for _, entry := range shard.entries {
			blocked, _, _ := isAuthBlockedForModel(entry.auth, "", now)
			if (entry.state == scheduledStateReady) == blocked {
				t.Fatalf("step %d: indexed state diverged for %s", step, entry.auth.ID)
			}
			if entry.state == scheduledStateReady && entry.meta.hasAccessTokenExpiry {
				readyKnown++
				if entry.expiryIndex < 0 || entry.expiryIndex >= len(shard.expiring) || shard.expiring[entry.expiryIndex] != entry {
					t.Fatalf("step %d: lost expiry index", step)
				}
			}
		}
		if len(shard.expiring) != readyKnown {
			t.Fatal("stale expiry entries accumulated")
		}
		for i, entry := range shard.expiring {
			if entry.expiryIndex != i || shard.entries[entry.auth.ID] != entry {
				t.Fatal("removed or duplicate credential retained by expiry queue")
			}
			if i > 0 && shard.expiring.Less(i, (i-1)/2) {
				t.Fatal("expiry heap order broken")
			}
		}
	}
}

func TestTokenExpiryQueueRefreshMovesBothEarlierAndLater(t *testing.T) {
	now := time.Unix(1800000000, 0)
	shard := &modelScheduler{entries: make(map[string]*scheduledAuth)}
	upsert := func(id string, seconds int64) {
		auth := &Auth{ID: id, Provider: "chatgpt-web", Metadata: map[string]any{
			"access_token": expiryTestToken(fmt.Sprint(now.Unix() + seconds)),
		}}
		shard.upsertEntryLocked(buildScheduledAuthMetaWithSupportedModels(auth, nil), now)
	}
	upsert("first", 10)
	upsert("second", 20)
	upsert("first", 30)
	shard.promoteExpiredLocked(now.Add(10 * time.Second))
	if len(shard.expiring) != 2 || shard.expiring[0].auth.ID != "second" {
		t.Fatal("refresh retained old expiry")
	}
	upsert("first", 15)
	shard.promoteExpiredLocked(now.Add(15 * time.Second))
	if shard.entries["first"].state != scheduledStateBlocked || len(shard.expiring) != 1 {
		t.Fatal("earlier replacement was not expired")
	}
	upsert("first", 40)
	shard.promoteExpiredLocked(now.Add(20 * time.Second))
	if shard.entries["first"].state != scheduledStateReady || shard.entries["second"].state != scheduledStateBlocked {
		t.Fatal("restored credential or expiry order incorrect")
	}
}

func TestIndexedCandidatePrioritiesMatchEntryClassification(t *testing.T) {
	now := time.Now()
	shard := &modelScheduler{entries: make(map[string]*scheduledAuth)}
	for i := 0; i < 150; i++ {
		auth := &Auth{ID: fmt.Sprint(i), Provider: "chatgpt-web", Metadata: map[string]any{"access_token": "opaque"}}
		meta := &scheduledAuthMeta{auth: auth, priority: i % 7, websocketEnabled: i%3 == 0}
		entry := buildScheduledAuth(meta, "", now)
		switch i % 5 {
		case 0:
			entry.state, entry.nextRetryAt = scheduledStateCooldown, now.Add(time.Hour)
		case 1:
			entry.state = scheduledStateBlocked
		case 2:
			entry.state = scheduledStateDisabled
		}
		shard.entries[auth.ID] = entry
	}
	shard.rebuildIndexesLocked()
	for _, predicate := range []func(*scheduledAuth) bool{nil, func(e *scheduledAuth) bool { return len(e.auth.ID)%2 == 0 }, func(*scheduledAuth) bool { return false }} {
		for _, websocket := range []bool{false, true} {
			want := make(map[int]struct{})
			for _, entry := range shard.entries {
				if (!websocket || entry.meta.websocketEnabled) && entryCandidateForPriority(entry, predicate) {
					want[entry.meta.priority] = struct{}{}
				}
			}
			got := shard.candidatePrioritiesFromIndexesLocked(websocket, predicate)
			if !slices.Equal(got, sortedPrioritySet(want)) {
				t.Fatalf("candidate priorities changed: got %v want %v", got, sortedPrioritySet(want))
			}
		}
	}
}

func TestLargeWebPoolDoesNotScanEveryCredentialForSelection(t *testing.T) {
	now := time.Now()
	shard := &modelScheduler{entries: make(map[string]*scheduledAuth, 100000)}
	for i := 0; i < 100000; i++ {
		auth := &Auth{ID: fmt.Sprintf("web-%06d", i), Provider: "chatgpt-web", Metadata: map[string]any{"access_token": "opaque"}}
		meta := &scheduledAuthMeta{auth: auth, hasAccessTokenExpiry: true, accessTokenExpiresAt: now.Add(time.Hour)}
		if i < 90000 {
			meta.accessTokenExpiresAt = now.Add(-time.Hour)
		}
		shard.entries[auth.ID] = buildScheduledAuth(meta, "", now)
	}
	shard.rebuildIndexesLocked()
	for _, strategy := range []schedulerStrategy{schedulerStrategyRoundRobin, schedulerStrategyRandom} {
		calls := 0
		predicate := func(*scheduledAuth) bool { calls++; return true }
		got, err := shard.pickReadyLocked(false, func(int) schedulerStrategy { return strategy }, nil, nil, nil, nil, nil, 0, predicate, predicate, "chatgpt-web", "")
		if err != nil || got == nil || shard.entries[got.ID].state != scheduledStateReady {
			t.Fatalf("large pool selection failed: %v", err)
		}
		if calls > 4 {
			t.Fatalf("selection scanned credentials: %d predicate calls", calls)
		}
	}
	if len(shard.expiring) != 10000 {
		t.Fatal("expired credentials retained in the ready expiry queue")
	}
}

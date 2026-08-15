package auth

import (
	"fmt"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestAuthSchedulerRoutingDiagnosticsMixedStates(t *testing.T) {
	now := time.Date(2026, time.August, 14, 12, 34, 56, 0, time.UTC)
	resetAt := now.Add(2 * time.Hour)
	auths := []*Auth{
		routingDiagnosticsTestAuth("eligible-high", "chatgpt-web", 10),
		routingDiagnosticsTestAuth("limited-high", "chatgpt-web", 10),
		routingDiagnosticsTestAuth("quota-high", "chatgpt-web", 10),
		routingDiagnosticsTestAuth("cooldown-high", "chatgpt-web", 10),
		routingDiagnosticsTestAuth("unavailable-high", "chatgpt-web", 10),
		routingDiagnosticsTestAuth("eligible-low", "chatgpt-web", 0),
	}
	auths[2].Metadata["quota_state"] = "exhausted"
	auths[2].Metadata["image_quota_remaining"] = 0
	auths[2].Metadata["image_quota_reset_at"] = resetAt
	auths[3].Unavailable = true
	auths[3].CooldownScope = cooldownScopeAuth
	auths[3].NextRetryAfter = now.Add(time.Hour)
	auths[3].Quota = QuotaState{Exceeded: true, Reason: "quota", NextRecoverAt: now.Add(time.Hour)}
	auths[4].Metadata["lifecycle_state"] = LifecycleStateReauthRequired

	scheduler := newRoutingDiagnosticsTestScheduler("chatgpt-web", "gpt-image-2", now, auths)
	scheduler.setRoutingConfig(internalconfig.RoutingConfig{
		PerAuthRequestLimit:         1,
		PerAuthRequestWindowMinutes: 5,
	})
	policy := scheduler.requestLimitPolicyForAuth(auths[1])
	reservation, acquired, block := scheduler.requestLimiter.reserveAt(auths[1].ID, policy, now)
	if !acquired || block.limited() {
		t.Fatalf("reserve limited auth: acquired=%v block=%+v", acquired, block)
	}
	if !reservation.Commit() {
		t.Fatal("commit limited auth reservation")
	}

	snapshot := scheduler.RoutingDiagnostics("chatgpt-web", "gpt-image-2", now)
	if snapshot.Provider != "chatgpt-web" || snapshot.Model != "gpt-image-2" {
		t.Fatalf("snapshot target = %q/%q", snapshot.Provider, snapshot.Model)
	}
	_, expectedResetAt := authRequestWindowAt(now, 5)
	tests := []struct {
		priority         int
		total            int
		quotaExhausted   int
		cooldown         int
		unavailable      int
		readyBeforeLimit int
		requestLimited   int
		eligibleNow      int
		expectedResetAt  *time.Time
	}{
		{
			priority:         10,
			total:            5,
			quotaExhausted:   1,
			cooldown:         1,
			unavailable:      1,
			readyBeforeLimit: 2,
			requestLimited:   1,
			eligibleNow:      1,
			expectedResetAt:  &expectedResetAt,
		},
		{
			priority:         0,
			total:            1,
			readyBeforeLimit: 1,
			eligibleNow:      1,
		},
	}
	if len(snapshot.Priorities) != len(tests) {
		t.Fatalf("priority groups = %d, want %d: %+v", len(snapshot.Priorities), len(tests), snapshot.Priorities)
	}
	for index, test := range tests {
		got := snapshot.Priorities[index]
		if got.Priority != test.priority ||
			got.Total != test.total ||
			got.QuotaExhausted != test.quotaExhausted ||
			got.Cooldown != test.cooldown ||
			got.Unavailable != test.unavailable ||
			got.ReadyBeforeRequestLimit != test.readyBeforeLimit ||
			got.RequestLimited != test.requestLimited ||
			got.EligibleNow != test.eligibleNow {
			t.Errorf("priority %d diagnostics = %+v", test.priority, got)
		}
		if test.expectedResetAt == nil {
			if got.EarliestRequestLimitResetAt != nil {
				t.Errorf("priority %d reset = %v, want nil", test.priority, got.EarliestRequestLimitResetAt)
			}
		} else if got.EarliestRequestLimitResetAt == nil || !got.EarliestRequestLimitResetAt.Equal(*test.expectedResetAt) {
			t.Errorf("priority %d reset = %v, want %v", test.priority, got.EarliestRequestLimitResetAt, test.expectedResetAt)
		}
		if got.RequestCapacity.Mode != "limited" || got.RequestCapacity.LimitedCredentials != test.readyBeforeLimit || got.RequestCapacity.UnlimitedCredentials != 0 {
			t.Errorf("priority %d request capacity = %+v", test.priority, got.RequestCapacity)
			continue
		}
		if got.RequestCapacity.ConfiguredSlots == nil || *got.RequestCapacity.ConfiguredSlots != int64(test.readyBeforeLimit) {
			t.Errorf("priority %d configured slots = %v", test.priority, got.RequestCapacity.ConfiguredSlots)
		}
		if got.RequestCapacity.RemainingSlots == nil || *got.RequestCapacity.RemainingSlots != int64(test.eligibleNow) {
			t.Errorf("priority %d remaining slots = %v", test.priority, got.RequestCapacity.RemainingSlots)
		}
		wantRPM := float64(test.readyBeforeLimit) / 5
		if got.RequestCapacity.ConfiguredRPM == nil || *got.RequestCapacity.ConfiguredRPM != wantRPM {
			t.Errorf("priority %d configured RPM = %v, want %v", test.priority, got.RequestCapacity.ConfiguredRPM, wantRPM)
		}
	}
}

func TestAuthSchedulerRoutingDiagnosticsReportsUnlimitedCapacityExplicitly(t *testing.T) {
	now := time.Date(2026, time.August, 14, 12, 34, 56, 0, time.UTC)
	auths := []*Auth{
		routingDiagnosticsTestAuth("unlimited-a", "test", 0),
		routingDiagnosticsTestAuth("unlimited-b", "test", 0),
	}
	scheduler := newRoutingDiagnosticsTestScheduler("test", "diag-model", now, auths)
	scheduler.setRoutingConfig(internalconfig.RoutingConfig{PerAuthRequestLimit: 0})

	snapshot := scheduler.RoutingDiagnostics("test", "diag-model", now)
	if len(snapshot.Priorities) != 1 {
		t.Fatalf("priority groups = %d, want 1", len(snapshot.Priorities))
	}
	capacity := snapshot.Priorities[0].RequestCapacity
	if capacity.Mode != "unlimited" || capacity.UnlimitedCredentials != 2 || capacity.LimitedCredentials != 0 {
		t.Fatalf("capacity = %+v, want explicit unlimited mode", capacity)
	}
	if capacity.ConfiguredSlots != nil || capacity.RemainingSlots != nil || capacity.ConfiguredRPM != nil {
		t.Fatalf("unlimited capacity totals = %+v, want null totals", capacity)
	}
}

func TestAuthSchedulerRoutingDiagnosticsReportsFiniteTotalsForMixedCapacity(t *testing.T) {
	now := time.Date(2026, time.August, 14, 12, 34, 56, 0, time.UTC)
	auths := []*Auth{
		routingDiagnosticsTestAuth("limited", "test", 0),
		routingDiagnosticsTestAuth("unlimited", "test", 0),
	}
	auths[1].Attributes["plan_type"] = "unlimited"
	disabled := 0
	scheduler := newRoutingDiagnosticsTestScheduler("test", "diag-model", now, auths)
	scheduler.setRoutingConfig(internalconfig.RoutingConfig{
		PerAuthRequestLimit:         2,
		PerAuthRequestWindowMinutes: 5,
		PriorityOverrides: []internalconfig.RoutingPriorityOverride{{
			Priority: 0,
			SubscriptionOverrides: []internalconfig.RoutingSubscriptionOverride{{
				PlanTypes:           []string{"unlimited"},
				PerAuthRequestLimit: &disabled,
			}},
		}},
	})

	snapshot := scheduler.RoutingDiagnostics("test", "diag-model", now)
	if len(snapshot.Priorities) != 1 {
		t.Fatalf("priority groups = %d, want 1", len(snapshot.Priorities))
	}
	capacity := snapshot.Priorities[0].RequestCapacity
	if capacity.Mode != "mixed" || capacity.LimitedCredentials != 1 || capacity.UnlimitedCredentials != 1 {
		t.Fatalf("capacity = %+v, want one limited and one unlimited credential", capacity)
	}
	if capacity.ConfiguredSlots == nil || *capacity.ConfiguredSlots != 2 {
		t.Fatalf("configured slots = %v, want 2", capacity.ConfiguredSlots)
	}
	if capacity.RemainingSlots == nil || *capacity.RemainingSlots != 2 {
		t.Fatalf("remaining slots = %v, want 2", capacity.RemainingSlots)
	}
	if capacity.ConfiguredRPM == nil || *capacity.ConfiguredRPM != 0.4 {
		t.Fatalf("configured RPM = %v, want 0.4", capacity.ConfiguredRPM)
	}
}

func TestAuthSchedulerRoutingDiagnosticsTenThousandEntries(t *testing.T) {
	now := time.Date(2026, time.August, 14, 12, 34, 56, 0, time.UTC)
	auths := make([]*Auth, 10_000)
	for index := range auths {
		priority := 0
		if index%2 == 0 {
			priority = 10
		}
		auths[index] = routingDiagnosticsTestAuth(fmt.Sprintf("auth-%05d", index), "test", priority)
	}
	scheduler := newRoutingDiagnosticsTestScheduler("test", "diag-model", now, auths)

	snapshot := scheduler.RoutingDiagnostics("test", "diag-model", now)
	if len(snapshot.Priorities) != 2 {
		t.Fatalf("priority groups = %d, want 2", len(snapshot.Priorities))
	}
	for _, got := range snapshot.Priorities {
		if got.Total != 5_000 || got.ReadyBeforeRequestLimit != 5_000 || got.EligibleNow != 5_000 {
			t.Errorf("priority %d diagnostics = %+v", got.Priority, got)
		}
		if got.QuotaExhausted != 0 || got.Cooldown != 0 || got.Unavailable != 0 || got.RequestLimited != 0 || got.EarliestRequestLimitResetAt != nil {
			t.Errorf("priority %d unexpected blocked diagnostics = %+v", got.Priority, got)
		}
	}
}

func TestAuthSchedulerRoutingDiagnosticsDoesNotCreateMissingModelShards(t *testing.T) {
	now := time.Date(2026, time.August, 14, 12, 34, 56, 0, time.UTC)
	auths := []*Auth{routingDiagnosticsTestAuth("known-model-auth", "test", 0)}
	scheduler := newRoutingDiagnosticsTestScheduler("test", "known-model", now, auths)
	providerState := scheduler.providers["test"]
	before := len(providerState.modelShards)

	for index := 0; index < 100; index++ {
		snapshot := scheduler.RoutingDiagnostics("test", fmt.Sprintf("missing-model-%d", index), now)
		if len(snapshot.Priorities) != 0 {
			t.Fatalf("missing model %d priorities = %+v", index, snapshot.Priorities)
		}
	}
	if after := len(providerState.modelShards); after != before {
		t.Fatalf("model shard count = %d, want unchanged %d", after, before)
	}

	snapshot := scheduler.RoutingDiagnostics("test", "known-model", now)
	if len(snapshot.Priorities) != 1 || snapshot.Priorities[0].EligibleNow != 1 {
		t.Fatalf("known model diagnostics = %+v", snapshot.Priorities)
	}
}

func TestAuthSchedulerRoutingDiagnosticsUsesEphemeralSupportedModelShard(t *testing.T) {
	now := time.Date(2026, time.August, 14, 12, 34, 56, 0, time.UTC)
	auth := routingDiagnosticsTestAuth("multi-model-auth", "test", 0)
	scheduler := newAuthScheduler(&RoundRobinSelector{})
	providerState := &providerScheduler{
		providerKey: "test",
		auths:       make(map[string]*scheduledAuthMeta, 1),
		modelShards: make(map[string]*modelScheduler),
	}
	providerState.auths[auth.ID] = buildScheduledAuthMetaWithSupportedModels(auth, map[string]struct{}{
		"known-model":    {},
		"unopened-model": {},
	})
	scheduler.providers["test"] = providerState
	scheduler.authProviders[auth.ID] = "test"

	before := len(providerState.modelShards)
	snapshot := scheduler.RoutingDiagnostics("test", "unopened-model", now)
	if len(snapshot.Priorities) != 1 || snapshot.Priorities[0].Total != 1 || snapshot.Priorities[0].EligibleNow != 1 {
		t.Fatalf("supported unopened model diagnostics = %+v", snapshot.Priorities)
	}
	if after := len(providerState.modelShards); after != before {
		t.Fatalf("model shard count = %d, want unchanged %d", after, before)
	}

	unknown := scheduler.RoutingDiagnostics("test", "unknown-model", now)
	if len(unknown.Priorities) != 0 {
		t.Fatalf("unsupported model diagnostics = %+v", unknown.Priorities)
	}
	if after := len(providerState.modelShards); after != before {
		t.Fatalf("model shard count after unsupported query = %d, want unchanged %d", after, before)
	}
}

func routingDiagnosticsTestAuth(id, provider string, priority int) *Auth {
	return &Auth{
		ID:       id,
		Provider: provider,
		Attributes: map[string]string{
			"priority": fmt.Sprintf("%d", priority),
		},
		Metadata: map[string]any{
			"lifecycle_state": LifecycleStateActive,
		},
	}
}

func newRoutingDiagnosticsTestScheduler(provider, model string, now time.Time, auths []*Auth) *authScheduler {
	scheduler := newAuthScheduler(&RoundRobinSelector{})
	providerKey := provider
	modelKey := canonicalModelKey(model)
	providerState := &providerScheduler{
		providerKey: providerKey,
		auths:       make(map[string]*scheduledAuthMeta, len(auths)),
		modelShards: make(map[string]*modelScheduler, 1),
	}
	shard := &modelScheduler{
		modelKey:        modelKey,
		entries:         make(map[string]*scheduledAuth, len(auths)),
		readyByPriority: make(map[int]*readyBucket),
	}
	supportedModels := map[string]struct{}{modelKey: {}}
	for _, auth := range auths {
		meta := buildScheduledAuthMetaWithSupportedModels(auth, supportedModels)
		providerState.auths[auth.ID] = meta
		entry := buildScheduledAuth(meta, modelKey, now)
		shard.entries[auth.ID] = entry
		scheduler.authProviders[auth.ID] = providerKey
	}
	shard.rebuildIndexesLocked()
	providerState.modelShards[modelKey] = shard
	scheduler.providers[providerKey] = providerState
	return scheduler
}

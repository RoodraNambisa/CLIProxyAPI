package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func uploadOptions() core.Options {
	return core.Options{Metadata: map[string]any{core.ChatGPTWebUploadRequiredMetadataKey: true}}
}

func TestChatGPTWebUploadCooldownIsIndependent(t *testing.T) {
	store := newChatGPTWebDependencyTestStore()
	m := NewManager(store, &RoundRobinSelector{}, nil)
	defer func() { _ = m.CloseExecutors() }()
	auth, err := m.Register(t.Context(), &Auth{ID: t.Name(), Provider: "chatgpt-web", Metadata: map[string]any{"lifecycle_state": "active", "image_quota_remaining": 8}})
	if err != nil {
		t.Fatal(err)
	}
	registerSchedulerModels(t, "chatgpt-web", "image-model", auth.ID)
	wait := 23 * time.Hour
	m.MarkResult(t.Context(), Result{AuthID: auth.ID, Provider: auth.Provider, Model: "image-model", Error: &Error{Code: "chatgpt_web_upload_rate_limit", HTTPStatus: 429}, RetryAfter: &wait})
	current, _ := m.GetByID(auth.ID)
	until := ChatGPTWebUploadCooldownUntil(current)
	waitForResultPersistenceCondition(t, 3*time.Second, "upload cooldown not persisted", func() bool {
		saved, errSaved := store.List(t.Context())
		return errSaved == nil && len(saved) == 1 && ChatGPTWebUploadCooldownUntil(saved[0]).Equal(until)
	})
	if until.Before(time.Now().Add(22*time.Hour)) || current.Unavailable || len(current.ModelStates) != 0 || current.Disabled {
		t.Fatalf("upload limit changed model/auth availability: %+v", current)
	}
	if *metadataInt(current.Metadata["image_quota_remaining"]) != 8 {
		t.Fatal("upload limit spent image quota")
	}
	m.MarkResult(t.Context(), Result{AuthID: auth.ID, Provider: auth.Provider, Model: "image-model", Success: true})
	current, _ = m.GetByID(auth.ID)
	if !ChatGPTWebUploadCooldownUntil(current).Equal(until) {
		t.Fatal("unrelated success cleared upload cooldown")
	}
	s := newSchedulerForTest(&RoundRobinSelector{}, current)
	for _, opts := range []core.Options{{}, uploadOptions()} {
		picked, errPick := s.pickSingle(t.Context(), auth.Provider, "image-model", opts, nil)
		if core.ChatGPTWebUploadRequired(opts) {
			if picked != nil || statusCodeFromError(errPick) != http.StatusTooManyRequests {
				t.Fatalf("upload pick = %v, %v", picked, errPick)
			}
		} else if errPick != nil || picked == nil {
			t.Fatalf("no-upload pick = %v, %v", picked, errPick)
		}
	}
	// Both snapshots must survive a metadata round trip without synthetic model state.
	reloaded := current.Clone()
	raw, errMarshal := json.Marshal(current.Metadata)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	reloaded.Metadata = nil
	if err := json.Unmarshal(raw, &reloaded.Metadata); err != nil {
		t.Fatal(err)
	}
	reloaded.ModelStates = nil
	s.upsertAuth(reloaded)
	if got, err := s.pickSingle(t.Context(), auth.Provider, "image-model", uploadOptions(), nil); got != nil || err == nil {
		t.Fatal("reload lost upload block")
	}
	if !ClearChatGPTWebUploadCooldown(reloaded) {
		t.Fatal("explicit reset did not clear upload cooldown")
	}
	s.upsertAuth(reloaded)
	if got, err := s.pickSingle(t.Context(), auth.Provider, "image-model", uploadOptions(), nil); got == nil || err != nil {
		t.Fatalf("reset did not restore upload: %v", err)
	}
}

type uploadRequirement bool

func (r uploadRequirement) RequiresUpload() bool { return bool(r) }

type uploadPreflightExecutor struct{ schedulerProviderTestExecutor }

func (e *uploadPreflightExecutor) PrepareProviderRequest(_ context.Context, req core.Request, _ core.Options, _ core.RequestOperation) (any, error) {
	return uploadRequirement(string(req.Payload) == "upload"), nil
}

func TestChatGPTWebUploadPreflightSelection(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprint(legacy), func(t *testing.T) {
			var selector Selector = &RoundRobinSelector{}
			if legacy {
				selector = NewSessionAffinitySelector(&RoundRobinSelector{})
			}
			m := NewManager(nil, selector, nil)
			m.RegisterExecutor(&uploadPreflightExecutor{schedulerProviderTestExecutor{provider: "chatgpt-web"}})
			auth, err := m.Register(t.Context(), &Auth{ID: t.Name(), Provider: "chatgpt-web", Metadata: map[string]any{"lifecycle_state": "active", chatGPTWebUploadResetKey: time.Now().Add(time.Hour).Format(time.RFC3339Nano)}})
			if err != nil {
				t.Fatal(err)
			}
			for _, payload := range []string{"upload", "text"} {
				req := core.Request{Payload: []byte(payload)}
				providers, opts, err := m.prepareProviderRequests(t.Context(), []string{"chatgpt-web"}, req, core.Options{}, core.RequestOperationExecute)
				if err != nil {
					t.Fatal(err)
				}
				picked, _, _, err := m.pickNextMixed(t.Context(), providers, "", opts, nil)
				if payload == "upload" {
					if picked != nil || statusCodeFromError(err) != 429 {
						t.Fatalf("upload selected cooling credential: %v", err)
					}
				} else if err != nil || picked == nil || picked.ID != auth.ID || picked.selectionUploadRequired {
					t.Fatalf("text unavailable: %v", err)
				}
			}
		})
	}
}

func TestChatGPTWebUploadCooldownPolicies(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  *config.Config
		wait *time.Duration
		want time.Duration
	}{
		{"default backoff", &config.Config{}, nil, quotaBackoffBase},
		{"skip status", &config.Config{NoCooldownStatusCodes: []int{429}}, nil, 0},
		{"fixed", &config.Config{FixedErrorCooldowns: []config.FixedErrorCooldownRule{{StatusCode: 429, CooldownSeconds: 120, Scope: "auth"}}}, nil, 2 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := NewManager(nil, nil, nil)
			m.SetConfig(tc.cfg)
			auth := &Auth{Provider: "chatgpt-web"}
			now := time.Now()
			m.applyChatGPTWebUploadCooldown(t.Context(), auth, Result{Error: &Error{HTTPStatus: 429, Code: "chatgpt_web_upload_rate_limit"}, RetryAfter: tc.wait}, now)
			until := ChatGPTWebUploadCooldownUntil(auth)
			if tc.want == 0 {
				if !until.IsZero() {
					t.Fatal("skip ignored")
				}
			} else if !until.Equal(now.Add(tc.want)) {
				t.Fatalf("until=%v", until)
			}
			if auth.Unavailable || len(auth.ModelStates) != 0 {
				t.Fatal("upload policy blocked unrelated work")
			}
		})
	}
}

func TestChatGPTWebUploadSelectionExpiryAndOtherProviders(t *testing.T) {
	now := time.Now()
	web := &Auth{ID: "web", Provider: "chatgpt-web", Metadata: map[string]any{"lifecycle_state": "active", chatGPTWebUploadResetKey: now.Add(time.Hour).Format(time.RFC3339Nano)}}
	other := web.Clone()
	other.ID = "codex"
	other.Provider = "codex"
	s := newSchedulerForTest(&RoundRobinSelector{}, web, other)
	legacy := cloneAuthForRequestSelection(web, uploadOptions())
	if blocked, _, _ := isAuthBlockedForModel(legacy, "", now); !blocked {
		t.Fatal("legacy selector allowed upload")
	}
	if blocked, _, _ := isAuthBlockedForModel(cloneAuthForRequestSelection(web, core.Options{}), "", now); blocked {
		t.Fatal("legacy selector blocked text")
	}
	if blocked, _, _ := isAuthBlockedForModel(cloneSelectedAuth(legacy), "", now); blocked {
		t.Fatal("selection flag leaked into execution")
	}
	p := s.providers["chatgpt-web"]
	p.mu.Lock()
	uploadShard := p.ensureModelLocked("", now, true)
	if uploadShard.entries[web.ID].state != scheduledStateCooldown {
		t.Fatal("upload shard missing cooldown")
	}
	uploadShard.promoteExpiredLocked(now.Add(2 * time.Hour))
	if uploadShard.entries[web.ID].state != scheduledStateReady {
		t.Fatal("expiry did not restore upload shard")
	}
	p.mu.Unlock()
	if picked, _, err := s.pickMixed(t.Context(), []string{"codex"}, "", uploadOptions(), nil); picked == nil || err != nil {
		t.Fatalf("other provider affected: %v", err)
	}
}

func TestChatGPTWebUploadCooldownRefreshMergeAndDisable(t *testing.T) {
	m := NewManager(nil, nil, nil)
	base := &Auth{ID: t.Name(), Provider: "chatgpt-web", Metadata: map[string]any{}}
	current := base.Clone()
	refreshed := base.Clone()
	next := refreshed.Clone()
	wait := time.Hour
	result := Result{Error: &Error{Code: "chatgpt_web_upload_rate_limit", HTTPStatus: 429}, RetryAfter: &wait}
	m.applyChatGPTWebUploadCooldown(t.Context(), current, result, time.Now())
	carryForwardConcurrentRefreshMetadata(base, current, refreshed, next)
	if !ChatGPTWebUploadCooldownUntil(next).Equal(ChatGPTWebUploadCooldownUntil(current)) {
		t.Fatal("refresh discarded concurrent upload cooldown")
	}
	ClearChatGPTWebUploadCooldown(current)
	carryForwardConcurrentRefreshMetadata(base, current, refreshed, next)
	if !ChatGPTWebUploadCooldownUntil(next).IsZero() {
		t.Fatal("refresh restored cleared upload cooldown")
	}
	current.Metadata["disable_cooling"] = true
	m.applyChatGPTWebUploadCooldown(t.Context(), current, result, time.Now())
	if !ChatGPTWebUploadCooldownUntil(current).IsZero() {
		t.Fatal("disable cooling ignored")
	}
}

func BenchmarkChatGPTWebUploadScheduler100K(b *testing.B) {
	now := time.Now()
	p := &providerScheduler{providerKey: "chatgpt-web", auths: make(map[string]*scheduledAuthMeta), modelShards: make(map[string]*modelScheduler)}
	for i := 0; i < 100000; i++ {
		auth := &Auth{ID: fmt.Sprint(i), Provider: "chatgpt-web", Metadata: map[string]any{"lifecycle_state": "active", chatGPTWebUploadResetKey: now.Add(time.Hour).Format(time.RFC3339Nano)}}
		p.auths[auth.ID] = buildScheduledAuthMetaWithSupportedModels(auth, nil)
	}
	p.auths["ready"] = buildScheduledAuthMetaWithSupportedModels(&Auth{ID: "ready", Provider: "chatgpt-web", Metadata: map[string]any{"lifecycle_state": "active"}}, nil)
	p.ensureModelLocked("", now, true)
	s := newAuthScheduler(&RoundRobinSelector{})
	s.providers["chatgpt-web"] = p
	b.ResetTimer()
	for b.Loop() {
		picked, err := s.pickSingle(b.Context(), "chatgpt-web", "", uploadOptions(), nil)
		if err != nil || picked == nil || picked.ID != "ready" {
			b.Fatalf("unexpected pick: %v", err)
		}
	}
}

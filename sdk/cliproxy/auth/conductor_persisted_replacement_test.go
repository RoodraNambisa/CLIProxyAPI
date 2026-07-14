package auth

import (
	"context"
	"sync"
	"testing"
	"time"
)

type replacementCaptureStore struct {
	mu    sync.Mutex
	saved []*Auth
}

func (*replacementCaptureStore) List(context.Context) ([]*Auth, error) { return nil, nil }

func (s *replacementCaptureStore) Save(_ context.Context, auth *Auth) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saved = append(s.saved, auth.Clone())
	return "", nil
}

func (*replacementCaptureStore) Delete(context.Context, string) error { return nil }

func (s *replacementCaptureStore) lastSaved() *Auth {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.saved) == 0 {
		return nil
	}
	return s.saved[len(s.saved)-1].Clone()
}

func TestManagerReplacementPersistsInheritedAvailabilityState(t *testing.T) {
	for _, operation := range []struct {
		name    string
		replace func(*Manager, *Auth) (*Auth, error)
	}{
		{name: "register", replace: func(manager *Manager, auth *Auth) (*Auth, error) {
			return manager.Register(t.Context(), auth)
		}},
		{name: "update", replace: func(manager *Manager, auth *Auth) (*Auth, error) {
			return manager.Update(t.Context(), auth)
		}},
	} {
		t.Run(operation.name, func(t *testing.T) {
			store := &replacementCaptureStore{}
			manager := NewManager(store, nil, nil)
			retryAt := time.Now().Add(time.Hour).Round(time.Second)
			initial := &Auth{
				ID:               "persisted-replacement",
				Provider:         "codex",
				NextRetryAfter:   retryAt,
				CooldownScope:    cooldownScopeAuth,
				ModelStates:      map[string]*ModelState{"gpt-5": {Unavailable: true, NextRetryAfter: retryAt}},
				Metadata:         map[string]any{"type": "codex"},
				Attributes:       map[string]string{SourceHashAttributeKey: "same-source"},
				LastRefreshedAt:  time.Now(),
				NextRefreshAfter: retryAt,
			}
			if _, errRegister := manager.Register(t.Context(), initial); errRegister != nil {
				t.Fatalf("register initial auth: %v", errRegister)
			}
			if _, errReplace := operation.replace(manager, &Auth{
				ID:       initial.ID,
				Provider: "codex",
				Metadata: map[string]any{"type": "codex", "access_token": "replacement"},
				Attributes: map[string]string{
					SourceHashAttributeKey: "same-source",
				},
			}); errReplace != nil {
				t.Fatalf("replace auth: %v", errReplace)
			}

			persisted := store.lastSaved()
			if persisted == nil {
				t.Fatal("replacement was not persisted")
			}
			if persisted.CooldownScope != cooldownScopeAuth || !persisted.NextRetryAfter.Equal(retryAt) {
				t.Fatalf("persisted auth cooldown = %q/%v, want auth/%v", persisted.CooldownScope, persisted.NextRetryAfter, retryAt)
			}
			state := persisted.ModelStates["gpt-5"]
			if state == nil || !state.Unavailable || !state.NextRetryAfter.Equal(retryAt) {
				t.Fatalf("persisted model state = %#v, want inherited cooldown", state)
			}
		})
	}
}

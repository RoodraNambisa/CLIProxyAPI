package auth

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

type deleteOutcomeStore struct {
	deleteErr   error
	deleteCount atomic.Int32
}

type lifecycleRoundTripperProvider struct {
	evicted chan string
	calls   atomic.Int32
	closed  atomic.Bool
}

func (p *lifecycleRoundTripperProvider) RoundTripperFor(*Auth) http.RoundTripper {
	p.calls.Add(1)
	return nil
}

func (p *lifecycleRoundTripperProvider) EvictAuth(authID string) { p.evicted <- authID }

func (p *lifecycleRoundTripperProvider) CloseIdleConnections() { p.closed.Store(true) }

func TestManagerReleasesRoundTripperProviderLifecycle(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	provider := &lifecycleRoundTripperProvider{evicted: make(chan string, 1)}
	manager.SetRoundTripperProvider(provider)
	const authID = "transport-lifecycle-auth"
	selected, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "codex"})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	if errDelete := manager.Delete(WithSkipPersist(t.Context()), authID); errDelete != nil {
		t.Fatalf("Delete() error = %v", errDelete)
	}
	select {
	case evicted := <-provider.evicted:
		if evicted != authID {
			t.Fatalf("evicted auth = %q, want %q", evicted, authID)
		}
	case <-time.After(time.Second):
		t.Fatal("auth transport was not evicted")
	}
	if transport := manager.roundTripperFor(selected); transport != nil {
		t.Fatal("removed auth received a transport")
	}
	if calls := provider.calls.Load(); calls != 0 {
		t.Fatalf("removed auth reached provider %d times, want 0", calls)
	}
	if errClose := manager.CloseExecutors(); errClose != nil {
		t.Fatalf("CloseExecutors() error = %v", errClose)
	}
	if !provider.closed.Load() {
		t.Fatal("round tripper provider was not closed")
	}
}

func TestDeleteWithOperationRollbackUsesLatestRuntimeState(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	const authID = "delete-rollback-latest-state"
	if _, errRegister := manager.Register(t.Context(), &Auth{
		ID:               authID,
		Provider:         "codex",
		NextRefreshAfter: time.Now().Add(time.Hour),
		Metadata:         map[string]any{"type": "codex"},
	}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- manager.DeleteWithOperation(t.Context(), authID, func(context.Context) error {
			close(started)
			<-release
			return NewDeleteOutcomeError(DeleteOutcomeRolledBack, errors.New("rolled back"))
		})
	}()
	<-started
	manager.mu.Lock()
	manager.auths[authID].NextRefreshAfter = time.Time{}
	manager.mu.Unlock()
	close(release)
	if errDelete := <-done; errDelete == nil {
		t.Fatal("DeleteWithOperation() error = nil, want rollback error")
	}
	restored, ok := manager.GetByID(authID)
	if !ok || restored == nil {
		t.Fatal("rolled back auth was not restored")
	}
	if !restored.NextRefreshAfter.IsZero() {
		t.Fatalf("restored NextRefreshAfter = %v, want latest cleared state", restored.NextRefreshAfter)
	}
}

func TestDeleteIfSerializesConcurrentReplacement(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	const authID = "conditional-delete-auth"
	if _, errRegister := manager.Register(t.Context(), &Auth{
		ID:         authID,
		Provider:   "codex",
		Attributes: map[string]string{SourceHashAttributeKey: "old"},
	}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}

	predicateStarted := make(chan struct{})
	releasePredicate := make(chan struct{})
	deleteDone := make(chan error, 1)
	go func() {
		deleted, errDelete := manager.DeleteIf(t.Context(), authID, func(current *Auth) bool {
			close(predicateStarted)
			<-releasePredicate
			return current != nil && current.Attributes[SourceHashAttributeKey] == "old"
		})
		if errDelete == nil && !deleted {
			errDelete = errors.New("conditional delete did not remove the matched auth")
		}
		deleteDone <- errDelete
	}()
	<-predicateStarted

	updateDone := make(chan error, 1)
	go func() {
		_, errUpdate := manager.Update(t.Context(), &Auth{
			ID:         authID,
			Provider:   "codex",
			Attributes: map[string]string{SourceHashAttributeKey: "new"},
		})
		updateDone <- errUpdate
	}()
	select {
	case errUpdate := <-updateDone:
		t.Fatalf("Update() bypassed conditional delete lock: %v", errUpdate)
	case <-time.After(50 * time.Millisecond):
	}
	close(releasePredicate)
	if errDelete := <-deleteDone; errDelete != nil {
		t.Fatalf("DeleteIf() error = %v", errDelete)
	}
	if errUpdate := <-updateDone; errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	current, ok := manager.GetByID(authID)
	if !ok || current == nil || current.Attributes[SourceHashAttributeKey] != "new" {
		t.Fatalf("current auth = %#v, %t; want concurrent replacement", current, ok)
	}
}

func (*deleteOutcomeStore) List(context.Context) ([]*Auth, error) { return nil, nil }

func (*deleteOutcomeStore) Save(context.Context, *Auth) (string, error) { return "", nil }

func (s *deleteOutcomeStore) Delete(context.Context, string) error {
	s.deleteCount.Add(1)
	return s.deleteErr
}

func TestDeleteHandlesStoreDeleteOutcomes(t *testing.T) {
	storeErr := errors.New("store delete failed")
	tests := []struct {
		name        string
		deleteErr   error
		wantErr     bool
		wantRemoved bool
	}{
		{name: "success", wantRemoved: true},
		{name: "committed", deleteErr: NewDeleteOutcomeError(DeleteOutcomeCommitted, storeErr), wantRemoved: true},
		{name: "uncertain", deleteErr: NewDeleteOutcomeError(DeleteOutcomeUncertain, storeErr), wantErr: true, wantRemoved: true},
		{name: "rolled back", deleteErr: NewDeleteOutcomeError(DeleteOutcomeRolledBack, storeErr), wantErr: true},
		{name: "ordinary failure", deleteErr: storeErr, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &deleteOutcomeStore{deleteErr: tt.deleteErr}
			manager := NewManager(store, nil, nil)
			const authID = "delete-outcome-auth"
			selected, errRegister := manager.Register(t.Context(), &Auth{
				ID:       authID,
				Provider: "codex",
				Metadata: map[string]any{"type": "codex"},
			})
			if errRegister != nil {
				t.Fatalf("Register() error = %v", errRegister)
			}
			cleanupDone := selected.RuntimeInstanceCleanupDone()

			errDelete := manager.Delete(t.Context(), authID)
			if tt.wantErr {
				if !errors.Is(errDelete, storeErr) {
					t.Fatalf("Delete() error = %v, want %v", errDelete, storeErr)
				}
			} else if errDelete != nil {
				t.Fatalf("Delete() error = %v, want nil", errDelete)
			}
			if got := store.deleteCount.Load(); got != 1 {
				t.Fatalf("Store.Delete() calls = %d, want 1", got)
			}

			current, ok := manager.GetByID(authID)
			if tt.wantRemoved {
				if ok || current != nil {
					t.Fatalf("GetByID() = (%#v, %t), want removed", current, ok)
				}
				if !selected.RuntimeInstanceRetired() {
					t.Fatal("removed auth runtime instance was not retired")
				}
				select {
				case <-cleanupDone:
				default:
					t.Fatal("removed auth runtime cleanup did not complete")
				}
				if _, _, active := selected.BeginRuntimeExecution(t.Context()); active {
					t.Fatal("removed auth runtime instance accepted a new execution")
				}
				return
			}

			if !ok || current == nil {
				t.Fatal("known delete failure did not retain runtime auth")
			}
			if selected.RuntimeInstanceRetired() {
				t.Fatal("retained auth runtime instance was retired")
			}
			select {
			case <-cleanupDone:
				t.Fatal("retained auth runtime cleanup unexpectedly completed")
			default:
			}
			_, release, active := selected.BeginRuntimeExecution(t.Context())
			if !active || release == nil {
				t.Fatal("retained auth runtime instance rejected a new execution")
			}
			release()
		})
	}
}

package auth

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	log "github.com/sirupsen/logrus"
)

func TestManagerAuthMutationContextRetainsOuterOwnershipAndRejectsStaleToken(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	auth := &Auth{
		ID:       "same.json",
		Provider: "chatgpt-web",
		Metadata: map[string]any{"type": "chatgpt-web", "email": "same@example.com"},
	}
	lockedCtx, unlockOuter, errLock := manager.LockAuthMutation(t.Context(), auth)
	if errLock != nil {
		t.Fatalf("LockAuthMutation() error: %v", errLock)
	}
	_, unlockNested, errNested := manager.lockAuthMutationContext(lockedCtx, auth)
	if errNested != nil {
		unlockOuter()
		t.Fatalf("nested lockAuthMutationContext() error: %v", errNested)
	}
	unlockNested()

	competingAcquired := make(chan func(), 1)
	go func() {
		_, unlock, errAcquire := manager.LockAuthMutation(context.Background(), auth)
		if errAcquire != nil {
			competingAcquired <- nil
			return
		}
		competingAcquired <- unlock
	}()
	select {
	case unlock := <-competingAcquired:
		if unlock != nil {
			unlock()
		}
		unlockOuter()
		t.Fatal("nested unlock released the outer mutation lock")
	case <-time.After(20 * time.Millisecond):
	}
	unlockOuter()
	var unlockCompeting func()
	select {
	case unlockCompeting = <-competingAcquired:
		if unlockCompeting == nil {
			t.Fatal("competing LockAuthMutation() failed")
		}
	case <-time.After(time.Second):
		t.Fatal("competing LockAuthMutation() did not acquire after outer unlock")
	}

	staleAcquired := make(chan func(), 1)
	go func() {
		_, unlock, errAcquire := manager.LockAuthMutation(lockedCtx, auth)
		if errAcquire != nil {
			staleAcquired <- nil
			return
		}
		staleAcquired <- unlock
	}()
	select {
	case unlock := <-staleAcquired:
		if unlock != nil {
			unlock()
		}
		unlockCompeting()
		t.Fatal("released mutation token bypassed the active lock")
	case <-time.After(20 * time.Millisecond):
	}
	unlockCompeting()
	select {
	case unlock := <-staleAcquired:
		if unlock == nil {
			t.Fatal("LockAuthMutation() with stale context failed")
		}
		unlock()
	case <-time.After(time.Second):
		t.Fatal("LockAuthMutation() with stale context did not reacquire")
	}
}

func TestManagerAuthMutationContextSerializesConcurrentNestedUse(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	auth := &Auth{
		ID:       "same.json",
		Provider: "chatgpt-web",
		Metadata: map[string]any{"type": "chatgpt-web", "email": "same@example.com"},
	}
	lockedCtx, unlockOuter, errLock := manager.LockAuthMutation(t.Context(), auth)
	if errLock != nil {
		t.Fatalf("LockAuthMutation() error: %v", errLock)
	}
	defer unlockOuter()

	_, unlockFirst, errFirst := manager.lockAuthMutationContext(lockedCtx, auth)
	if errFirst != nil {
		t.Fatalf("first nested lock error: %v", errFirst)
	}
	secondAcquired := make(chan func(), 1)
	go func() {
		_, unlockSecond, errSecond := manager.lockAuthMutationContext(lockedCtx, auth)
		if errSecond != nil {
			secondAcquired <- nil
			return
		}
		secondAcquired <- unlockSecond
	}()
	select {
	case unlockSecond := <-secondAcquired:
		if unlockSecond != nil {
			unlockSecond()
		}
		unlockFirst()
		t.Fatal("concurrent nested mutation reused the same ownership")
	case <-time.After(20 * time.Millisecond):
	}

	unlockFirst()
	select {
	case unlockSecond := <-secondAcquired:
		if unlockSecond == nil {
			t.Fatal("second nested mutation failed")
		}
		unlockSecond()
	case <-time.After(time.Second):
		t.Fatal("second nested mutation did not acquire after first release")
	}
}

func TestManagerAuthMutationContextRejectsIdentityDrift(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	auth := &Auth{
		ID:       "same.json",
		Provider: "chatgpt-web",
		Metadata: map[string]any{"type": "chatgpt-web", "email": "old@example.com"},
	}
	lockedCtx, unlockOuter, errLock := manager.LockAuthMutation(t.Context(), auth)
	if errLock != nil {
		t.Fatalf("LockAuthMutation() error: %v", errLock)
	}
	defer unlockOuter()

	changed := auth.Clone()
	changed.Metadata["email"] = "new@example.com"
	_, unlockChanged, errChanged := manager.lockAuthMutationContext(lockedCtx, changed)
	if unlockChanged != nil {
		unlockChanged()
	}
	if !errors.Is(errChanged, ErrAuthMutationIdentityChanged) {
		t.Fatalf("identity drift error = %v, want ErrAuthMutationIdentityChanged", errChanged)
	}
}

func TestManagerAuthMutationContextRejectsCancellationAfterNestedAcquire(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	auth := &Auth{ID: "same.json", Provider: "codex"}
	baseCtx, cancel := context.WithCancel(t.Context())
	lockedCtx, unlockOuter, errLock := manager.LockAuthMutation(baseCtx, auth)
	if errLock != nil {
		t.Fatalf("LockAuthMutation() error: %v", errLock)
	}
	defer unlockOuter()
	cancel()

	_, unlockNested, errNested := manager.lockAuthMutationContext(lockedCtx, auth)
	if unlockNested != nil {
		unlockNested()
	}
	if !errors.Is(errNested, context.Canceled) {
		t.Fatalf("nested canceled error = %v, want context canceled", errNested)
	}
}

func TestManagerAuthMutationContextAssignsIDBeforeOuterLock(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	auth := &Auth{Provider: "codex", Metadata: map[string]any{"type": "codex"}}
	lockedCtx, unlockOuter, errLock := manager.LockAuthMutation(t.Context(), auth)
	if errLock != nil {
		t.Fatalf("LockAuthMutation() error: %v", errLock)
	}
	defer unlockOuter()
	if strings.TrimSpace(auth.ID) == "" {
		t.Fatal("LockAuthMutation() did not assign an auth ID")
	}
	registered, errRegister := manager.Register(lockedCtx, auth)
	if errRegister != nil {
		t.Fatalf("Register() error: %v", errRegister)
	}
	if registered == nil || registered.ID != auth.ID {
		t.Fatalf("registered auth ID = %v, want %q", registered, auth.ID)
	}
}

func TestManagerAuthMutationContextNormalizesIDBeforeReuse(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	auth := &Auth{ID: " account.json ", Provider: "codex", Metadata: map[string]any{"type": "codex"}}
	lockedCtx, unlockOuter, errLock := manager.LockAuthMutation(t.Context(), auth)
	if errLock != nil {
		t.Fatalf("LockAuthMutation() error: %v", errLock)
	}
	defer unlockOuter()
	if auth.ID != "account.json" {
		t.Fatalf("normalized auth ID = %q, want account.json", auth.ID)
	}

	registered, errRegister := manager.Register(lockedCtx, auth)
	if errRegister != nil {
		t.Fatalf("Register() error: %v", errRegister)
	}
	if registered == nil || registered.ID != "account.json" {
		t.Fatalf("registered auth = %#v, want normalized ID", registered)
	}
	if _, ok := manager.GetByID(" account.json "); ok {
		t.Fatal("manager retained a whitespace auth ID alias")
	}
}

func TestManagerDeleteReusesActiveMutationToken(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	auth := &Auth{ID: "delete.json", Provider: "codex"}
	if _, errRegister := manager.Register(t.Context(), auth); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	lockedCtx, unlockOuter, errLock := manager.LockAuthMutation(t.Context(), auth)
	if errLock != nil {
		t.Fatalf("LockAuthMutation() error = %v", errLock)
	}
	defer unlockOuter()

	deleted := make(chan error, 1)
	go func() { deleted <- manager.Delete(lockedCtx, auth.ID) }()
	select {
	case errDelete := <-deleted:
		if errDelete != nil {
			t.Fatalf("Delete() error = %v", errDelete)
		}
	case <-time.After(time.Second):
		t.Fatal("Delete() deadlocked while reusing an active mutation token")
	}
	if _, exists := manager.GetByID(auth.ID); exists {
		t.Fatal("deleted auth remains registered")
	}
}

func TestManagerMutationContextRetainsTokensForMultipleManagers(t *testing.T) {
	first := NewManager(nil, nil, nil)
	second := NewManager(nil, nil, nil)
	firstAuth := &Auth{ID: "first.json", Provider: "codex"}
	secondAuth := &Auth{ID: "second.json", Provider: "codex"}
	firstCtx, unlockFirst, errFirst := first.LockAuthMutation(t.Context(), firstAuth)
	if errFirst != nil {
		t.Fatalf("first LockAuthMutation() error = %v", errFirst)
	}
	defer unlockFirst()
	secondCtx, unlockSecond, errSecond := second.LockAuthMutation(firstCtx, secondAuth)
	if errSecond != nil {
		t.Fatalf("second LockAuthMutation() error = %v", errSecond)
	}
	defer unlockSecond()

	registered := make(chan error, 1)
	go func() {
		_, errRegister := first.Register(secondCtx, firstAuth)
		registered <- errRegister
	}()
	select {
	case errRegister := <-registered:
		if errRegister != nil {
			t.Fatalf("Register() error = %v", errRegister)
		}
	case <-time.After(time.Second):
		t.Fatal("first manager token was lost after nesting a second manager")
	}
}

func TestManagerAuthMutationContextDoesNotReacquireBarrierBetweenIdentityKeys(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	auth := &Auth{
		ID:       "same.json",
		Provider: "chatgpt-web",
		Metadata: map[string]any{"type": "chatgpt-web", "email": "same@example.com"},
	}
	emailKey := "\x00chatgpt-web-email:same@example.com"
	raw, _ := manager.persistLocks.LoadOrStore(emailKey, newAuthPersistLock())
	emailLock := raw.(*authPersistLock)
	<-emailLock.semaphore

	acquired := make(chan func(), 1)
	go func() {
		_, unlock, errLock := manager.LockAuthMutation(context.Background(), auth)
		if errLock != nil {
			acquired <- nil
			return
		}
		acquired <- unlock
	}()

	deadline := time.Now().Add(time.Second)
	for manager.persistBarrier.TryLock() {
		manager.persistBarrier.Unlock()
		if time.Now().After(deadline) {
			emailLock.semaphore <- struct{}{}
			t.Fatal("mutation did not acquire the persistence barrier read lock")
		}
		time.Sleep(time.Millisecond)
	}

	writerDone := make(chan struct{})
	go func() {
		manager.persistBarrier.Lock()
		manager.persistBarrier.Unlock()
		close(writerDone)
	}()
	deadline = time.Now().Add(time.Second)
	for manager.persistBarrier.TryRLock() {
		manager.persistBarrier.RUnlock()
		if time.Now().After(deadline) {
			emailLock.semaphore <- struct{}{}
			t.Fatal("writer did not queue behind the mutation")
		}
		time.Sleep(time.Millisecond)
	}

	emailLock.semaphore <- struct{}{}
	select {
	case unlock := <-acquired:
		if unlock == nil {
			t.Fatal("LockAuthMutation() failed")
		}
		unlock()
	case <-time.After(time.Second):
		t.Fatal("mutation deadlocked while a persistence-barrier writer was queued")
	}
	select {
	case <-writerDone:
	case <-time.After(time.Second):
		t.Fatal("queued persistence-barrier writer did not finish")
	}
}

func TestManagerUpdateNormalizesID(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	if _, errRegister := manager.Register(t.Context(), &Auth{ID: "account.json", Provider: "codex"}); errRegister != nil {
		t.Fatalf("Register() error: %v", errRegister)
	}
	updated, errUpdate := manager.Update(t.Context(), &Auth{ID: " account.json ", Provider: "codex"})
	if errUpdate != nil {
		t.Fatalf("Update() error: %v", errUpdate)
	}
	if updated == nil || updated.ID != "account.json" {
		t.Fatalf("updated auth = %#v, want normalized ID", updated)
	}
	if auths := manager.List(); len(auths) != 1 || auths[0].ID != "account.json" {
		t.Fatalf("manager auths = %#v, want one normalized entry", auths)
	}
}

func TestManagerUpdateRejectsWhitespaceID(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	updated, errUpdate := manager.Update(t.Context(), &Auth{ID: "   ", Provider: "codex"})
	if errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	if updated != nil {
		t.Fatalf("Update() auth = %#v, want nil", updated)
	}
	if auths := manager.List(); len(auths) != 0 {
		t.Fatalf("manager auth count = %d, want 0", len(auths))
	}
}

type registerIfAbsentTestStore struct {
	saveIfAbsentErr error
	saveCalls       atomic.Int32
	listAuths       []*Auth
	listErr         error
}

func (s *registerIfAbsentTestStore) List(context.Context) ([]*Auth, error) {
	return s.listAuths, s.listErr
}

func (*registerIfAbsentTestStore) Save(context.Context, *Auth) (string, error) { return "", nil }

func (s *registerIfAbsentTestStore) SaveIfAbsent(context.Context, *Auth) (string, error) {
	s.saveCalls.Add(1)
	return "", s.saveIfAbsentErr
}

func (*registerIfAbsentTestStore) Delete(context.Context, string) error { return nil }

type sharedRegisterIfAbsentTestStore struct {
	mu    sync.Mutex
	auths map[string]*Auth
}

func newSharedRegisterIfAbsentTestStore() *sharedRegisterIfAbsentTestStore {
	return &sharedRegisterIfAbsentTestStore{auths: make(map[string]*Auth)}
}

func (s *sharedRegisterIfAbsentTestStore) List(context.Context) ([]*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	auths := make([]*Auth, 0, len(s.auths))
	for _, auth := range s.auths {
		auths = append(auths, auth.Clone())
	}
	return auths, nil
}

func (s *sharedRegisterIfAbsentTestStore) Save(_ context.Context, auth *Auth) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.auths[auth.ID] = auth.Clone()
	return auth.ID, nil
}

func (s *sharedRegisterIfAbsentTestStore) SaveIfAbsent(_ context.Context, auth *Auth) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.auths[auth.ID]; exists {
		return "", ErrAuthAlreadyExists
	}
	s.auths[auth.ID] = auth.Clone()
	return auth.ID, nil
}

func (s *sharedRegisterIfAbsentTestStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	delete(s.auths, id)
	s.mu.Unlock()
	return nil
}

func TestManagerRegisterIfAbsentIsAtomic(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	start := make(chan struct{})
	var successes atomic.Int32
	var conflicts atomic.Int32
	var workers sync.WaitGroup
	for index := range 2 {
		workers.Add(1)
		go func(label string) {
			defer workers.Done()
			<-start
			_, errRegister := manager.RegisterIfAbsent(context.Background(), &Auth{
				ID:       "same-id",
				Provider: "chatgpt-web",
				Label:    label,
				Metadata: map[string]any{"type": "chatgpt-web", "email": label + "@example.com"},
			})
			switch {
			case errRegister == nil:
				successes.Add(1)
			case errors.Is(errRegister, ErrAuthAlreadyExists):
				conflicts.Add(1)
			default:
				t.Errorf("RegisterIfAbsent() error = %v", errRegister)
			}
		}(string(rune('a' + index)))
	}
	close(start)
	workers.Wait()
	if successes.Load() != 1 || conflicts.Load() != 1 {
		t.Fatalf("successes/conflicts = %d/%d, want 1/1", successes.Load(), conflicts.Load())
	}
	if auth, ok := manager.GetByID("same-id"); !ok || auth == nil {
		t.Fatal("winning auth is not installed")
	}
}

func TestManagerRegisterIfAbsentRejectsConcurrentDuplicateChatGPTWebEmail(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	start := make(chan struct{})
	var successes atomic.Int32
	var conflicts atomic.Int32
	var workers sync.WaitGroup
	for index := range 2 {
		workers.Add(1)
		go func(id string) {
			defer workers.Done()
			<-start
			_, errRegister := manager.RegisterIfAbsent(t.Context(), &Auth{
				ID:       id,
				Provider: "chatgpt-web",
				Metadata: map[string]any{"type": "chatgpt-web", "email": "same@example.com"},
			})
			switch {
			case errRegister == nil:
				successes.Add(1)
			case errors.Is(errRegister, ErrChatGPTWebEmailAlreadyExists):
				conflicts.Add(1)
			default:
				t.Errorf("RegisterIfAbsent() error = %v", errRegister)
			}
		}(fmt.Sprintf("chatgpt-web-%d.json", index))
	}
	close(start)
	workers.Wait()
	if successes.Load() != 1 || conflicts.Load() != 1 {
		t.Fatalf("successes/conflicts = %d/%d, want 1/1", successes.Load(), conflicts.Load())
	}
}

func TestManagerRegisterIfAbsentRejectsDuplicateChatGPTWebEmailAcrossManagers(t *testing.T) {
	store := newSharedRegisterIfAbsentTestStore()
	first := NewManager(store, nil, nil)
	second := NewManager(store, nil, nil)
	firstAuth := &Auth{
		ID:       "first.json",
		Provider: "chatgpt-web",
		Metadata: map[string]any{"type": "chatgpt-web", "email": "same@example.com"},
	}
	secondAuth := firstAuth.Clone()
	secondAuth.ID = "second.json"

	start := make(chan struct{})
	results := make(chan error, 2)
	for _, registration := range []struct {
		manager *Manager
		auth    *Auth
	}{{first, firstAuth}, {second, secondAuth}} {
		go func(manager *Manager, auth *Auth) {
			<-start
			_, errRegister := manager.RegisterIfAbsent(context.Background(), auth)
			results <- errRegister
		}(registration.manager, registration.auth)
	}
	close(start)
	successes := 0
	conflicts := 0
	for range 2 {
		errRegister := <-results
		switch {
		case errRegister == nil:
			successes++
		case errors.Is(errRegister, ErrAuthAlreadyExists), errors.Is(errRegister, ErrChatGPTWebEmailAlreadyExists):
			conflicts++
		default:
			t.Fatalf("RegisterIfAbsent() error = %v", errRegister)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes/conflicts = %d/%d, want 1/1", successes, conflicts)
	}
	wantID := chatgptwebauth.CredentialFileName("same@example.com")
	if firstAuth.ID != wantID || secondAuth.ID != wantID || firstAuth.FileName != wantID || secondAuth.FileName != wantID {
		t.Fatalf("canonical auth identities = %#v and %#v, want %q", firstAuth, secondAuth, wantID)
	}
	if auths, errList := store.List(t.Context()); errList != nil || len(auths) != 1 || auths[0].ID != wantID {
		t.Fatalf("stored auths = %#v, error = %v", auths, errList)
	}
}

func TestManagerRegisterCanonicalizesNewPersistedChatGPTWebAuth(t *testing.T) {
	store := newSharedRegisterIfAbsentTestStore()
	manager := NewManager(store, nil, nil)
	auth := &Auth{
		ID:         "custom-name.json",
		Provider:   "chatgpt-web",
		FileName:   "custom-name.json",
		Attributes: map[string]string{"path": "nested/custom-name.json"},
		Metadata:   map[string]any{"type": "chatgpt-web", "email": " Person@Example.com "},
	}
	wantID := chatgptwebauth.CredentialFileName("person@example.com")
	registered, errRegister := manager.Register(t.Context(), auth)
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	if registered == nil || registered.ID != wantID || registered.FileName != wantID {
		t.Fatalf("registered auth = %#v, want canonical ID %q", registered, wantID)
	}
	if _, exists := registered.Attributes["path"]; exists {
		t.Fatalf("registered auth retained a non-canonical path: %#v", registered.Attributes)
	}
	if auths, errList := store.List(t.Context()); errList != nil || len(auths) != 1 || auths[0].ID != wantID {
		t.Fatalf("stored auths = %#v, error = %v", auths, errList)
	}
}

func TestManagerRegisterDoesNotOverwriteCanonicalChatGPTWebStorageKey(t *testing.T) {
	store := newSharedRegisterIfAbsentTestStore()
	wantID := chatgptwebauth.CredentialFileName("person@example.com")
	existing := &Auth{ID: wantID, Provider: "codex", FileName: wantID, Metadata: map[string]any{"type": "codex"}}
	if _, errSave := store.SaveIfAbsent(t.Context(), existing); errSave != nil {
		t.Fatalf("seed canonical key: %v", errSave)
	}
	manager := NewManager(store, nil, nil)
	_, errRegister := manager.Register(t.Context(), &Auth{
		ID:       "custom-name.json",
		Provider: "chatgpt-web",
		Metadata: map[string]any{"type": "chatgpt-web", "email": "person@example.com"},
	})
	if !errors.Is(errRegister, ErrAuthAlreadyExists) {
		t.Fatalf("Register() error = %v, want ErrAuthAlreadyExists", errRegister)
	}
	auths, errList := store.List(t.Context())
	if errList != nil || len(auths) != 1 || auths[0].Provider != "codex" {
		t.Fatalf("stored auths = %#v, error = %v", auths, errList)
	}
}

func TestManagerLoadPrefersCanonicalChatGPTWebCredential(t *testing.T) {
	store := newSharedRegisterIfAbsentTestStore()
	wantID := chatgptwebauth.CredentialFileName("person@example.com")
	for _, auth := range []*Auth{
		{ID: "legacy-name.json", Provider: "chatgpt-web", FileName: "legacy-name.json", Metadata: map[string]any{"type": "chatgpt-web", "email": "person@example.com"}},
		{ID: wantID, Provider: "chatgpt-web", FileName: wantID, Metadata: map[string]any{"type": "chatgpt-web", "email": "Person@Example.com"}},
	} {
		if _, errSave := store.Save(t.Context(), auth); errSave != nil {
			t.Fatalf("Save() error = %v", errSave)
		}
	}
	manager := NewManager(store, nil, nil)
	if errLoad := manager.Load(t.Context()); errLoad != nil {
		t.Fatalf("Load() error = %v", errLoad)
	}
	auths := manager.List()
	if len(auths) != 1 || auths[0].ID != wantID {
		t.Fatalf("loaded auths = %#v, want canonical %q", auths, wantID)
	}
}

func TestManagerRegisterIfAbsentRejectsPersistedDuplicateChatGPTWebEmail(t *testing.T) {
	store := &registerIfAbsentTestStore{listAuths: []*Auth{{
		ID:       "legacy-name.json",
		Provider: "chatgpt-web",
		Metadata: map[string]any{"type": "chatgpt-web", "email": "same@example.com"},
	}}}
	manager := NewManager(store, nil, nil)
	_, errRegister := manager.RegisterIfAbsent(t.Context(), &Auth{
		ID:       "chatgpt-web-deterministic.json",
		Provider: "chatgpt-web",
		Metadata: map[string]any{"type": "chatgpt-web", "email": "same@example.com"},
	})
	if !errors.Is(errRegister, ErrChatGPTWebEmailAlreadyExists) {
		t.Fatalf("RegisterIfAbsent() error = %v, want duplicate email", errRegister)
	}
	if calls := store.saveCalls.Load(); calls != 0 {
		t.Fatalf("SaveIfAbsent() calls = %d, want 0", calls)
	}
}

func TestManagerUpdateRejectsDuplicateChatGPTWebEmail(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	first, errFirst := manager.Register(t.Context(), &Auth{
		ID:       "first.json",
		Provider: "chatgpt-web",
		Metadata: map[string]any{"type": "chatgpt-web", "email": "first@example.com"},
	})
	if errFirst != nil {
		t.Fatalf("register first auth: %v", errFirst)
	}
	second, errSecond := manager.Register(t.Context(), &Auth{
		ID:       "second.json",
		Provider: "chatgpt-web",
		Metadata: map[string]any{"type": "chatgpt-web", "email": "second@example.com"},
	})
	if errSecond != nil {
		t.Fatalf("register second auth: %v", errSecond)
	}

	updated := second.Clone()
	updated.Metadata["email"] = "first@example.com"
	if _, errUpdate := manager.Update(t.Context(), updated); !errors.Is(errUpdate, ErrChatGPTWebEmailAlreadyExists) {
		t.Fatalf("Update() error = %v, want duplicate email", errUpdate)
	}
	if _, current, errUpdate := manager.UpdateIfCurrent(t.Context(), second, updated); !errors.Is(errUpdate, ErrChatGPTWebEmailAlreadyExists) || current {
		t.Fatalf("UpdateIfCurrent() = current %t, error %v; want duplicate email", current, errUpdate)
	}
	if got, ok := manager.GetByID(second.ID); !ok || chatGPTWebRegistrationEmail(got) != "second@example.com" {
		t.Fatalf("second auth after rejected update = %#v", got)
	}
	if got, ok := manager.GetByID(first.ID); !ok || chatGPTWebRegistrationEmail(got) != "first@example.com" {
		t.Fatalf("first auth after rejected update = %#v", got)
	}
}

func TestManagerUpdateRejectsChatGPTWebEmailChange(t *testing.T) {
	manager := NewManager(newSharedRegisterIfAbsentTestStore(), nil, nil)
	original, errRegister := manager.Register(t.Context(), &Auth{
		ID:       "account.json",
		Provider: "chatgpt-web",
		Metadata: map[string]any{"type": "chatgpt-web", "email": "first@example.com"},
	})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	changed := original.Clone()
	changed.Metadata["email"] = "second@example.com"
	if _, errUpdate := manager.Update(t.Context(), changed); !errors.Is(errUpdate, ErrChatGPTWebEmailImmutable) {
		t.Fatalf("Update() error = %v, want immutable email", errUpdate)
	}
	current, ok := manager.GetByID(original.ID)
	if !ok || chatGPTWebRegistrationEmail(current) != "first@example.com" {
		t.Fatalf("current auth = %#v", current)
	}
}

func TestManagerRegisterIfAbsentMarksUnknownStoreErrorUncertain(t *testing.T) {
	wantErr := errors.New("store result unknown")
	manager := NewManager(&registerIfAbsentTestStore{saveIfAbsentErr: wantErr}, nil, nil)
	_, errRegister := manager.RegisterIfAbsent(t.Context(), &Auth{
		ID:       "uncertain",
		Provider: "chatgpt-web",
		Metadata: map[string]any{"type": "chatgpt-web", "email": "uncertain@example.com"},
	})
	if !errors.Is(errRegister, wantErr) {
		t.Fatalf("RegisterIfAbsent() error = %v, want %v", errRegister, wantErr)
	}
	if outcome, ok := SaveOutcomeFromError(errRegister); !ok || outcome != SaveOutcomeUncertain {
		t.Fatalf("RegisterIfAbsent() outcome = %v, %t; want uncertain", outcome, ok)
	}
	if _, exists := manager.GetByID("uncertain"); exists {
		t.Fatal("uncertain auth was installed")
	}
}

func TestManagerRegisterIfAbsentInstallsCommittedSaveWithCleanupWarning(t *testing.T) {
	wantWarning := errors.New("unlock failed after durable save: refresh-token-secret")
	var logs bytes.Buffer
	logger := log.StandardLogger()
	previousOutput := logger.Out
	logger.SetOutput(&logs)
	t.Cleanup(func() { logger.SetOutput(previousOutput) })
	manager := NewManager(&registerIfAbsentTestStore{
		saveIfAbsentErr: NewSaveOutcomeError(SaveOutcomeCommitted, wantWarning),
	}, nil, nil)
	wantID := chatgptwebauth.CredentialFileName("committed@example.com")
	installed, errRegister := manager.RegisterIfAbsent(t.Context(), &Auth{
		ID:       "committed",
		Provider: "chatgpt-web",
		Metadata: map[string]any{"type": "chatgpt-web", "email": "committed@example.com"},
	})
	if errRegister != nil {
		t.Fatalf("RegisterIfAbsent() error = %v, want nil", errRegister)
	}
	if installed == nil || installed.ID != wantID {
		t.Fatalf("RegisterIfAbsent() auth = %#v, want committed auth", installed)
	}
	current, exists := manager.GetByID(wantID)
	if !exists || current == nil || current.ID != wantID {
		t.Fatalf("installed manager auth = %#v, exists=%t", current, exists)
	}
	if strings.Contains(logs.String(), "refresh-token-secret") {
		t.Fatalf("committed warning leaked store error: %q", logs.String())
	}
}

func TestManagerRegisterIfAbsentStopsWhenContextIsAlreadyCanceled(t *testing.T) {
	store := &registerIfAbsentTestStore{}
	manager := NewManager(store, nil, nil)
	for index := range 100 {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		id := fmt.Sprintf("canceled-%d", index)
		_, errRegister := manager.RegisterIfAbsent(ctx, &Auth{
			ID:       id,
			Provider: "chatgpt-web",
			Metadata: map[string]any{"type": "chatgpt-web"},
		})
		if !errors.Is(errRegister, context.Canceled) {
			t.Fatalf("RegisterIfAbsent() error = %v, want context canceled", errRegister)
		}
		if _, exists := manager.GetByID(id); exists {
			t.Fatalf("canceled auth %q was installed", id)
		}
	}
	if calls := store.saveCalls.Load(); calls != 0 {
		t.Fatalf("SaveIfAbsent() calls = %d, want 0", calls)
	}
}

package watcher

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
)

func TestWatcherPersistenceCallbacksCancelPathWait(t *testing.T) {
	tests := []struct {
		name string
		run  func(*Watcher, string, string)
	}{
		{"replacement-completion", func(w *Watcher, path, normalized string) {
			w.completeAuthPersistenceAttempt(path, normalized, authFileVersion{}, 0, 0, errors.New("test failure"))
		}},
		{"removal-completion", func(w *Watcher, path, normalized string) {
			w.completeAuthRemovalAttempt(path, normalized, 0, nil, 0, errors.New("test failure"))
		}},
		{"retired-removal-completion", func(w *Watcher, path, normalized string) {
			w.completeRetiredDeleteAttempt(path, normalized, 0, nil, authfileguard.RetiredSnapshot{}, 0, errors.New("test failure"))
		}},
		{"removal-retry", func(w *Watcher, path, normalized string) {
			w.retryAuthRemovalPersistence(path, normalized, "test removal", 0, nil, 0, 0, false)
		}},
		{"retired-removal-retry", func(w *Watcher, path, normalized string) {
			w.retryRetiredAuthRemovalPersistence(path, normalized, "test removal", 0, nil, authfileguard.RetiredSnapshot{}, 0, 0)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				w, path, _ := newAuthEventFixture(t)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				w.authWorkContext, w.authWorkCancel = ctx, cancel
				// Use a stale generation so releasing the test lock after a
				// regression cannot start persistence or modify the fixture.
				w.retiredDeletes = map[string]uint64{w.normalizeAuthPath(path): 1}
				unlock := authfileguard.Lock(path)
				defer unlock()
				done := make(chan struct{})
				go func() {
					defer close(done)
					tt.run(w, path, w.normalizeAuthPath(path))
				}()
				synctest.Wait()
				w.stopped.Store(true)
				cancel()
				synctest.Wait()
				select {
				case <-done:
				default:
					t.Error("canceled persistence callback still waits for a save-owned path lock")
				}
				unlock()
				<-done
			})
		})
	}
}

func TestWatcherRemovalGraceCanBeCanceled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, path, _ := newAuthEventFixture(t)
		if errRemove := os.Remove(path); errRemove != nil {
			t.Fatal(errRemove)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan struct{})
		go func() {
			defer close(done)
			w.handleEventContext(ctx, fsnotify.Event{Name: path, Op: fsnotify.Remove})
		}()
		synctest.Wait()
		cancel()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Error("cancellation waited for the replacement grace period")
		}
		<-done
		if !w.isKnownAuthFile(path) || len(w.currentAuths) != 1 {
			t.Fatal("canceled removal changed the credential snapshot")
		}
		w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Remove})
		if w.isKnownAuthFile(path) {
			t.Fatal("canceled grace period hid a later genuine deletion")
		}
	})
}

func TestWatcherCanceledInitialReplayWithoutEventLoop(t *testing.T) {
	w, path, _ := newAuthEventFixture(t)
	w.beginInitialSyncEventBuffer()
	w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Write})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got := w.replayInitialSyncEvents(ctx); got != 0 {
		t.Fatalf("canceled replay counted %d events", got)
	}
	if w.initialSyncActive || len(w.initialSyncAuthPaths) != 0 {
		t.Fatal("canceled replay retained the initial event buffer")
	}
}

func TestWatcherStoppingRetriesCancelsRunningCallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, path, _ := newAuthEventFixture(t)
		normalized := w.normalizeAuthPath(path)
		w.retiredDeletes = map[string]uint64{normalized: 1}
		w.authRetryBase = time.Millisecond
		unlock := authfileguard.Lock(path)
		defer unlock()
		entered := make(chan struct{})
		w.scheduleAuthPersistenceRetry(normalized, 1, 0, errors.New("test failure"), func(attempt int) {
			close(entered)
			w.completeAuthPersistenceAttempt(path, normalized, authFileVersion{}, 1, attempt, errors.New("test failure"))
		})
		time.Sleep(2 * time.Millisecond)
		<-entered
		synctest.Wait()
		w.stopped.Store(true)
		done := make(chan struct{})
		go func() {
			defer close(done)
			w.stopAuthPersistenceRetryTimers()
			w.stopAuthPersistenceTasks()
		}()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Error("shutdown waited for a retry before canceling its path-lock wait")
		}
		unlock()
		<-done
		if w.authWorkContext == nil || w.authWorkContext.Err() == nil || len(w.authRetryTimers) != 0 {
			t.Fatal("persistence shutdown left live work or retry timers")
		}
		if w.startAuthPersistenceTask(func(context.Context) { t.Error("work ran after shutdown") }) {
			t.Fatal("shutdown accepted new persistence work")
		}
	})
}

func TestWatcherPersistenceRejectedDuringStopDoesNotRelockPath(t *testing.T) {
	w, path, _ := newAuthEventFixture(t)
	persister := &stubStore{}
	w.storePersister = persister
	t.Cleanup(func() { authfileguard.ClearQuarantined(path) })
	// Simulate shutdown after the event has acquired its path lock and
	// quarantined the update, but before it can start persistence work.
	w.SetAuthUpdateObserver(func(AuthUpdate) { w.stopped.Store(true) })
	if errWrite := os.WriteFile(path, []byte(`{"type":"chatgpt-web","email":"fixture@example.com","session_token":"rotated"}`), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	done := make(chan struct{})
	go func() {
		w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Write})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("event re-locked its own path after shutdown rejected persistence")
	}
	if atomic.LoadInt32(&persister.authPersisted) != 0 || len(w.currentAuths) != 0 || !authfileguard.IsQuarantined(path) {
		t.Fatal("shutdown admitted an unpersisted credential or started new persistence")
	}
	unlock, errLock := authfileguard.LockContext(t.Context(), path)
	if errLock != nil {
		t.Fatal(errLock)
	}
	unlock()
}

func TestWatcherEventLoopStopCancelsPathWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, path, _ := newAuthEventFixture(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		events := make(chan fsnotify.Event)
		w.watcher = &fsnotify.Watcher{Events: events, Errors: make(chan error)}
		done := make(chan struct{})
		w.eventCancel, w.eventDone = cancel, done
		unlock := authfileguard.Lock(path)
		defer unlock()
		go func() { defer close(done); w.processEvents(ctx) }()
		events <- fsnotify.Event{Name: path, Op: fsnotify.Write}
		synctest.Wait()
		stopped := make(chan struct{})
		go func() { w.stopEventLoop(); close(stopped) }()
		synctest.Wait()
		select {
		case <-stopped:
		default:
			t.Error("stopping the event loop still waits for a save-owned path lock")
		}
		unlock()
		<-stopped
	})
}

func TestWatcherDrainAndInitialReplayCancelPathWait(t *testing.T) {
	for _, replay := range []bool{false, true} {
		name := "drain"
		if replay {
			name = "initial-replay"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				w, path, _ := newAuthEventFixture(t)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				event := fsnotify.Event{Name: path, Op: fsnotify.Write}
				if replay {
					w.beginInitialSyncEventBuffer()
					w.handleEvent(event)
				} else {
					events := make(chan fsnotify.Event, 1)
					events <- event
					w.watcher = &fsnotify.Watcher{Events: events, Errors: make(chan error)}
				}
				unlock := authfileguard.Lock(path)
				defer unlock()
				done := make(chan struct{})
				go func() {
					defer close(done)
					if replay {
						w.replayInitialSyncEvents(ctx)
					} else {
						w.drainPendingEvents(ctx)
					}
				}()
				synctest.Wait()
				cancel()
				synctest.Wait()
				select {
				case <-done:
				default:
					t.Error("canceled event processing still waits for a save-owned path lock")
				}
				unlock()
				<-done
			})
		})
	}
}

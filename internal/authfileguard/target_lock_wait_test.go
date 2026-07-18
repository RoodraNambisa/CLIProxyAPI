package authfileguard

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func waitForPersistentProcessWriter(t *testing.T, root *os.Root, lockPath string) {
	t.Helper()
	key, errIdentity := persistentProcessLockIdentity(root, lockPath)
	if errIdentity != nil {
		t.Fatal(errIdentity)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		persistentProcessLocks.Lock()
		entry := persistentProcessLocks.entries[key]
		persistentProcessLocks.Unlock()
		if entry != nil {
			entry.mu.Lock()
			writer := entry.writer || entry.waitingWriters > 0
			entry.mu.Unlock()
			if writer {
				return
			}
		}
		time.Sleep(lockRetryInterval)
	}
	t.Fatal("persistent process writer did not begin waiting")
}

func waitForPersistentProcessReferences(t *testing.T, root *os.Root, lockPath string, want int) {
	t.Helper()
	if errWait := waitForPersistentProcessState(root, lockPath, 2*time.Second, func(state persistentProcessLockTestState) bool {
		return state.refs >= want
	}); errWait != nil {
		t.Fatal(errWait)
	}
}

func waitForPersistentProcessReaders(root *os.Root, lockPath string, want int, timeout time.Duration) error {
	return waitForPersistentProcessState(root, lockPath, timeout, func(state persistentProcessLockTestState) bool {
		return state.readers >= want
	})
}

type persistentProcessLockTestState struct {
	readers        int
	writer         bool
	waitingWriters int
	refs           int
}

func waitForPersistentProcessState(root *os.Root, lockPath string, timeout time.Duration, ready func(persistentProcessLockTestState) bool) error {
	key, errIdentity := persistentProcessLockIdentity(root, lockPath)
	if errIdentity != nil {
		return errIdentity
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		persistentProcessLocks.Lock()
		entry := persistentProcessLocks.entries[key]
		state := persistentProcessLockTestState{}
		if entry != nil {
			state.refs = entry.refs
		}
		persistentProcessLocks.Unlock()
		if entry != nil {
			entry.mu.Lock()
			state.readers = entry.readers
			state.writer = entry.writer
			state.waitingWriters = entry.waitingWriters
			entry.mu.Unlock()
			if ready(state) {
				return nil
			}
		}
		time.Sleep(lockRetryInterval)
	}
	return fmt.Errorf("persistent process lock %q did not reach expected state", lockPath)
}

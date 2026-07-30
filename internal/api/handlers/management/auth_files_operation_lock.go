package management

import (
	"context"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

type managedAuthOperationLock struct {
	token chan struct{}
	refs  int
}

var managedAuthOperationLocks = struct {
	sync.Mutex
	entries map[string]*managedAuthOperationLock
}{entries: make(map[string]*managedAuthOperationLock)}

func lockManagedAuthFileOperationContext(ctx context.Context, path string) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	key := filepath.Clean(strings.TrimSpace(path))
	if key == "." || key == "" {
		return func() {}, nil
	}
	if runtime.GOOS == "windows" {
		key = strings.ToLower(key)
	}
	managedAuthOperationLocks.Lock()
	entry := managedAuthOperationLocks.entries[key]
	if entry == nil {
		entry = &managedAuthOperationLock{token: make(chan struct{}, 1)}
		entry.token <- struct{}{}
		managedAuthOperationLocks.entries[key] = entry
	}
	entry.refs++
	managedAuthOperationLocks.Unlock()

	releaseReference := func() {
		managedAuthOperationLocks.Lock()
		entry.refs--
		if entry.refs == 0 && managedAuthOperationLocks.entries[key] == entry {
			delete(managedAuthOperationLocks.entries, key)
		}
		managedAuthOperationLocks.Unlock()
	}
	select {
	case <-ctx.Done():
		releaseReference()
		return nil, ctx.Err()
	case <-entry.token:
	}
	if errContext := ctx.Err(); errContext != nil {
		entry.token <- struct{}{}
		releaseReference()
		return nil, errContext
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			entry.token <- struct{}{}
			releaseReference()
		})
	}, nil
}

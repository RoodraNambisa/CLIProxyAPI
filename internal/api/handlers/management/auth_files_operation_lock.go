package management

import (
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

type managedAuthOperationLock struct {
	mu   sync.Mutex
	refs int
}

var managedAuthOperationLocks = struct {
	sync.Mutex
	entries map[string]*managedAuthOperationLock
}{entries: make(map[string]*managedAuthOperationLock)}

func lockManagedAuthFileOperation(path string) func() {
	key := filepath.Clean(strings.TrimSpace(path))
	if key == "." || key == "" {
		return func() {}
	}
	if runtime.GOOS == "windows" {
		key = strings.ToLower(key)
	}
	managedAuthOperationLocks.Lock()
	entry := managedAuthOperationLocks.entries[key]
	if entry == nil {
		entry = &managedAuthOperationLock{}
		managedAuthOperationLocks.entries[key] = entry
	}
	entry.refs++
	managedAuthOperationLocks.Unlock()
	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		managedAuthOperationLocks.Lock()
		entry.refs--
		if entry.refs == 0 && managedAuthOperationLocks.entries[key] == entry {
			delete(managedAuthOperationLocks.entries, key)
		}
		managedAuthOperationLocks.Unlock()
	}
}

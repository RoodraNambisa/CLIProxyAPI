package authfileguard

import (
	"context"
	"strings"
	"sync"
)

type managerOwnedPersistenceContextKey struct{}

type managerPersistedGeneration struct {
	id   uint64
	hash string
}

var managerPersistedGenerations = struct {
	sync.Mutex
	nextID uint64
	byPath map[string]managerPersistedGeneration
}{byPath: make(map[string]managerPersistedGeneration)}

// WithManagerOwnedPersistence marks a save as owned by the runtime manager.
func WithManagerOwnedPersistence(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, managerOwnedPersistenceContextKey{}, true)
}

// ManagerOwnedPersistence reports whether the runtime manager owns a save and
// will install the resulting generation without watcher assistance.
func ManagerOwnedPersistence(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	owned, _ := ctx.Value(managerOwnedPersistenceContextKey{}).(bool)
	return owned
}

// MarkManagerPersistedGeneration records one file generation written by the
// runtime manager so the filesystem watcher can adopt its persistence echo.
func MarkManagerPersistedGeneration(path, hash string) {
	hash = strings.ToLower(strings.TrimSpace(hash))
	keys := pathIdentityKeys(path)
	if hash == "" || len(keys) == 0 {
		return
	}
	managerPersistedGenerations.Lock()
	replaced := make(map[uint64]struct{}, len(keys))
	for _, key := range keys {
		if marker, ok := managerPersistedGenerations.byPath[key]; ok {
			replaced[marker.id] = struct{}{}
		}
	}
	clearManagerPersistedGenerationIDsLocked(replaced)
	managerPersistedGenerations.nextID++
	marker := managerPersistedGeneration{id: managerPersistedGenerations.nextID, hash: hash}
	for _, key := range keys {
		managerPersistedGenerations.byPath[key] = marker
	}
	managerPersistedGenerations.Unlock()
}

// ConsumeManagerPersistedGeneration reports whether path currently matches a
// generation written by the runtime manager. Observing any generation consumes
// the marker for that path; a different hash means the manager write was
// superseded before the watcher observed it.
func ConsumeManagerPersistedGeneration(path, hash string) bool {
	hash = strings.ToLower(strings.TrimSpace(hash))
	keys := pathIdentityKeys(path)
	if hash == "" || len(keys) == 0 {
		return false
	}
	managerPersistedGenerations.Lock()
	var matched managerPersistedGeneration
	observed := make(map[uint64]struct{}, len(keys))
	for _, key := range keys {
		marker, ok := managerPersistedGenerations.byPath[key]
		if !ok {
			continue
		}
		observed[marker.id] = struct{}{}
		if marker.hash == hash {
			matched = marker
		}
	}
	clearManagerPersistedGenerationIDsLocked(observed)
	managerPersistedGenerations.Unlock()
	return matched.id != 0
}

// ClearManagerPersistedGeneration invalidates any manager-owned marker for
// path, including markers recorded through an equivalent path alias.
func ClearManagerPersistedGeneration(path string) {
	keys := pathIdentityKeys(path)
	if len(keys) == 0 {
		return
	}
	managerPersistedGenerations.Lock()
	generations := make(map[uint64]struct{}, len(keys))
	for _, key := range keys {
		if marker, ok := managerPersistedGenerations.byPath[key]; ok {
			generations[marker.id] = struct{}{}
		}
	}
	clearManagerPersistedGenerationIDsLocked(generations)
	managerPersistedGenerations.Unlock()
}

func clearManagerPersistedGenerationIDsLocked(generations map[uint64]struct{}) {
	if len(generations) == 0 {
		return
	}
	for key, marker := range managerPersistedGenerations.byPath {
		if _, clear := generations[marker.id]; clear {
			delete(managerPersistedGenerations.byPath, key)
		}
	}
}

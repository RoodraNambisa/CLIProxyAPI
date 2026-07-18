package authfileguard

import (
	"context"
	"sort"
	"sync"
)

type pathLock struct {
	token chan struct{}
	refs  int
}

var pathLocks = struct {
	sync.Mutex
	entries map[string]*pathLock
}{entries: make(map[string]*pathLock)}

type retiredMarker struct {
	keys []string
}

// RetiredSnapshot identifies the exact retired marker generations observed at
// the start of a delete operation.
type RetiredSnapshot struct {
	generations []uint64
}

var retiredPaths = struct {
	sync.RWMutex
	nextGeneration uint64
	byKey          map[string]map[uint64]struct{}
	byGeneration   map[uint64]retiredMarker
}{
	byKey:        make(map[string]map[uint64]struct{}),
	byGeneration: make(map[uint64]retiredMarker),
}

var quarantinedPaths = struct {
	sync.RWMutex
	keys map[string]struct{}
}{keys: make(map[string]struct{})}

// Lock serializes in-process mutations to one auth file path and its current
// resolved filesystem entity.
func Lock(path string) func() {
	unlock, _ := LockContext(context.Background(), path)
	return unlock
}

// LockContext is Lock with cancellable in-process lock waiting.
func LockContext(ctx context.Context, path string) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if errContext := ctx.Err(); errContext != nil {
		return nil, errContext
	}
	lexicalKey := pathLexicalKey(path)
	if lexicalKey == "" {
		return func() {}, nil
	}

	entityKey := resolvedPathKey(lexicalKey)
	for {
		unlock, errLock := lockPathKeysContext(ctx, []string{lexicalKey, entityKey})
		if errLock != nil {
			return nil, errLock
		}
		refreshedEntityKey := resolvedPathKey(lexicalKey)
		if refreshedEntityKey == entityKey {
			return unlock, nil
		}
		unlock()
		entityKey = refreshedEntityKey
	}
}

func lockPathKeysContext(ctx context.Context, keys []string) (func(), error) {
	keys = sortedUniqueKeys(keys)
	if len(keys) == 0 {
		return func() {}, nil
	}

	pathLocks.Lock()
	entries := make([]*pathLock, len(keys))
	for i, key := range keys {
		entry := pathLocks.entries[key]
		if entry == nil {
			entry = &pathLock{token: make(chan struct{}, 1)}
			entry.token <- struct{}{}
			pathLocks.entries[key] = entry
		}
		entry.refs++
		entries[i] = entry
	}
	pathLocks.Unlock()

	releaseReferences := func() {
		pathLocks.Lock()
		for i, entry := range entries {
			entry.refs--
			if entry.refs == 0 && pathLocks.entries[keys[i]] == entry {
				delete(pathLocks.entries, keys[i])
			}
		}
		pathLocks.Unlock()
	}
	acquired := 0
	for _, entry := range entries {
		if errContext := ctx.Err(); errContext != nil {
			for i := acquired - 1; i >= 0; i-- {
				entries[i].token <- struct{}{}
			}
			releaseReferences()
			return nil, errContext
		}
		select {
		case <-entry.token:
			acquired++
		case <-ctx.Done():
			for i := acquired - 1; i >= 0; i-- {
				entries[i].token <- struct{}{}
			}
			releaseReferences()
			return nil, ctx.Err()
		}
	}
	if errContext := ctx.Err(); errContext != nil {
		for i := acquired - 1; i >= 0; i-- {
			entries[i].token <- struct{}{}
		}
		releaseReferences()
		return nil, errContext
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			for i := len(entries) - 1; i >= 0; i-- {
				entries[i].token <- struct{}{}
			}
			releaseReferences()
		})
	}, nil
}

// MarkRetired records that a path has held a retired credential. The marker is
// cleared only after a confirmed deletion of that path.
func MarkRetired(path string) (RetiredSnapshot, bool) {
	keys := pathIdentityKeys(path)
	if len(keys) == 0 {
		return RetiredSnapshot{}, false
	}
	retiredPaths.Lock()
	matchingGenerations := retiredGenerationsForKeysLocked(keys)
	allKeys := append([]string(nil), keys...)
	for generation := range matchingGenerations {
		allKeys = append(allKeys, retiredPaths.byGeneration[generation].keys...)
	}
	allKeys = sortedUniqueKeys(allKeys)
	if len(matchingGenerations) == 1 {
		for generation := range matchingGenerations {
			if equalKeys(retiredPaths.byGeneration[generation].keys, allKeys) {
				retiredPaths.Unlock()
				return RetiredSnapshot{generations: []uint64{generation}}, false
			}
		}
	}
	removeRetiredGenerationsLocked(matchingGenerations)
	retiredPaths.nextGeneration++
	generation := retiredPaths.nextGeneration
	retiredPaths.byGeneration[generation] = retiredMarker{keys: allKeys}
	for _, key := range allKeys {
		generations := retiredPaths.byKey[key]
		if generations == nil {
			generations = make(map[uint64]struct{})
			retiredPaths.byKey[key] = generations
		}
		generations[generation] = struct{}{}
	}
	retiredPaths.Unlock()
	return RetiredSnapshot{generations: []uint64{generation}}, true
}

// IsRetired reports whether a path is locked to management-only use.
func IsRetired(path string) bool {
	keys := pathIdentityKeys(path)
	if len(keys) == 0 {
		return false
	}
	retiredPaths.RLock()
	defer retiredPaths.RUnlock()
	for _, key := range keys {
		if len(retiredPaths.byKey[key]) != 0 {
			return true
		}
	}
	return false
}

// MarkQuarantined prevents a durable pending deletion from being admitted by
// the runtime manager while the watcher resumes or confirms the transaction.
func MarkQuarantined(path string) {
	keys := pathIdentityKeys(path)
	quarantinedPaths.Lock()
	for _, key := range keys {
		quarantinedPaths.keys[key] = struct{}{}
	}
	quarantinedPaths.Unlock()
}

// IsQuarantined reports whether a path has a durable pending deletion.
func IsQuarantined(path string) bool {
	keys := pathIdentityKeys(path)
	quarantinedPaths.RLock()
	defer quarantinedPaths.RUnlock()
	for _, key := range keys {
		if _, ok := quarantinedPaths.keys[key]; ok {
			return true
		}
	}
	return false
}

// ClearQuarantined releases a path after its pending deletion or replacement
// persistence has been durably confirmed.
func ClearQuarantined(path string) {
	keys := pathIdentityKeys(path)
	quarantinedPaths.Lock()
	for _, key := range keys {
		delete(quarantinedPaths.keys, key)
	}
	quarantinedPaths.Unlock()
}

// ClearRetired releases a retired path after its deletion is confirmed.
func ClearRetired(path string) {
	keys := pathIdentityKeys(path)
	if len(keys) == 0 {
		return
	}
	retiredPaths.Lock()
	removeRetiredGenerationsLocked(retiredGenerationsForKeysLocked(keys))
	retiredPaths.Unlock()
}

// CaptureRetired records the current retired marker generations for path.
func CaptureRetired(path string) RetiredSnapshot {
	keys := pathIdentityKeys(path)
	if len(keys) == 0 {
		return RetiredSnapshot{}
	}
	retiredPaths.RLock()
	generations := retiredGenerationsForKeysLocked(keys)
	retiredPaths.RUnlock()
	result := make([]uint64, 0, len(generations))
	for generation := range generations {
		result = append(result, generation)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return RetiredSnapshot{generations: result}
}

// ClearRetiredSnapshot releases only marker generations captured before a
// confirmed delete, preserving markers created while the delete was running.
func ClearRetiredSnapshot(snapshot RetiredSnapshot) {
	if len(snapshot.generations) == 0 {
		return
	}
	generations := make(map[uint64]struct{}, len(snapshot.generations))
	for _, generation := range snapshot.generations {
		generations[generation] = struct{}{}
	}
	retiredPaths.Lock()
	removeRetiredGenerationsLocked(generations)
	retiredPaths.Unlock()
}

func retiredGenerationsForKeysLocked(keys []string) map[uint64]struct{} {
	generations := make(map[uint64]struct{})
	for _, key := range keys {
		for generation := range retiredPaths.byKey[key] {
			generations[generation] = struct{}{}
		}
	}
	return generations
}

func removeRetiredGenerationsLocked(generations map[uint64]struct{}) {
	for generation := range generations {
		marker, ok := retiredPaths.byGeneration[generation]
		if !ok {
			continue
		}
		delete(retiredPaths.byGeneration, generation)
		for _, key := range marker.keys {
			indexedGenerations := retiredPaths.byKey[key]
			delete(indexedGenerations, generation)
			if len(indexedGenerations) == 0 {
				delete(retiredPaths.byKey, key)
			}
		}
	}
}

func sortedUniqueKeys(keys []string) []string {
	unique := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if key != "" {
			unique[key] = struct{}{}
		}
	}
	keys = keys[:0]
	for key := range unique {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func equalKeys(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

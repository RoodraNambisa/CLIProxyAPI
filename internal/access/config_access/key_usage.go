package configaccess

import (
	"crypto/sha256"
	"sync"
	"sync/atomic"
	"time"
)

// apiKeyUsage keeps only configured key digests and survives provider replacement.
// Retired providers may update old counters, but cannot recreate deleted entries.
type apiKeyUsage struct {
	mu      sync.RWMutex
	entries map[[sha256.Size]byte]*atomic.Int64
}

var configuredKeyUsage apiKeyUsage

func (usage *apiKeyUsage) reconcile(keys []string) map[string]*atomic.Int64 {
	usage.mu.Lock()
	defer usage.mu.Unlock()
	next := make(map[[sha256.Size]byte]*atomic.Int64, len(keys))
	counters := make(map[string]*atomic.Int64, len(keys))
	for _, key := range keys {
		digest := sha256.Sum256([]byte(key))
		counter := usage.entries[digest]
		if counter == nil {
			counter = &atomic.Int64{}
		}
		next[digest] = counter
		counters[key] = counter
	}
	usage.entries = next
	return counters
}

func noteAPIKeyUse(counter *atomic.Int64, now time.Time) {
	if counter == nil {
		return
	}
	stamp := now.UnixNano()
	for previous := counter.Load(); previous < stamp; previous = counter.Load() {
		if counter.CompareAndSwap(previous, stamp) {
			return
		}
	}
}

func (usage *apiKeyUsage) snapshot(keys []string) map[string]string {
	usage.mu.RLock()
	defer usage.mu.RUnlock()
	result := make(map[string]string, len(keys))
	for _, key := range keys {
		if counter := usage.entries[sha256.Sum256([]byte(key))]; counter != nil {
			if stamp := counter.Load(); stamp > 0 {
				result[key] = time.Unix(0, stamp).UTC().Format(time.RFC3339Nano)
			}
		}
	}
	return result
}

// APIKeyLastUsed returns successful authentication times without persisting them.
func APIKeyLastUsed(keys []string) map[string]string {
	return configuredKeyUsage.snapshot(keys)
}

package helps

import (
	"sort"
	"strings"
	"sync"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
)

// ChatGPTWebAccountInfoRawQuotaResponses keeps the latest raw quota response per credential.
type ChatGPTWebAccountInfoRawQuotaResponses struct {
	mu           sync.Mutex
	enabled      bool
	records      map[string]chatgptwebauth.AccountInfoRawQuotaResponseRecord
	totalBytes   int
	evictedCount uint64
}

// NewChatGPTWebAccountInfoRawQuotaResponses creates an empty bounded capture store.
func NewChatGPTWebAccountInfoRawQuotaResponses(enabled bool) *ChatGPTWebAccountInfoRawQuotaResponses {
	return &ChatGPTWebAccountInfoRawQuotaResponses{
		enabled: enabled,
		records: make(map[string]chatgptwebauth.AccountInfoRawQuotaResponseRecord),
	}
}

// SetEnabled changes collection without removing existing records.
func (responses *ChatGPTWebAccountInfoRawQuotaResponses) SetEnabled(enabled bool) {
	if responses == nil {
		return
	}
	responses.mu.Lock()
	responses.enabled = enabled
	responses.mu.Unlock()
}

// Record replaces the latest response for one credential.
func (responses *ChatGPTWebAccountInfoRawQuotaResponses) Record(event chatgptwebauth.AccountInfoRawQuotaResponseEvent) {
	if responses == nil {
		return
	}
	responses.mu.Lock()
	enabled := responses.enabled
	responses.mu.Unlock()
	if !enabled {
		return
	}
	authIndex := strings.TrimSpace(event.AuthIndex)
	if authIndex == "" || (event.HTTPStatus == 0 && len(event.Body) == 0) {
		return
	}
	capturedAt := event.CapturedAt.UTC()
	if capturedAt.IsZero() {
		capturedAt = time.Now().UTC()
	}
	body := append([]byte(nil), event.Body...)
	truncated := event.Truncated
	if len(body) > chatgptwebauth.AccountInfoRawQuotaResponseMaxBytes {
		body = body[:chatgptwebauth.AccountInfoRawQuotaResponseMaxBytes]
		truncated = true
	}
	record := chatgptwebauth.AccountInfoRawQuotaResponseRecord{
		AuthIndex:     authIndex,
		CapturedAt:    capturedAt,
		Attempt:       max(0, event.Attempt),
		HTTPStatus:    event.HTTPStatus,
		ContentType:   strings.TrimSpace(event.ContentType),
		ResponseBytes: len(event.Body),
		Truncated:     truncated,
		ParseError:    strings.TrimSpace(event.ParseError),
		Body:          string(body),
	}
	if event.ParsedQuota != nil {
		record.ParsedQuota = &chatgptwebauth.AccountInfoRawQuotaParsed{
			FeaturePresent: event.ParsedQuota.FeaturePresent,
			Present:        event.ParsedQuota.Present,
			Remaining:      event.ParsedQuota.Remaining,
		}
		if !event.ParsedQuota.ResetAt.IsZero() {
			resetAt := event.ParsedQuota.ResetAt
			record.ParsedQuota.ResetAt = &resetAt
		}
	}

	responses.mu.Lock()
	defer responses.mu.Unlock()
	if !responses.enabled {
		return
	}
	if previous, exists := responses.records[authIndex]; exists {
		responses.totalBytes -= len(previous.Body)
	}
	responses.records[authIndex] = record
	responses.totalBytes += len(record.Body)
	for len(responses.records) > chatgptwebauth.AccountInfoRawQuotaResponseCapacity ||
		responses.totalBytes > chatgptwebauth.AccountInfoRawQuotaResponseMaxBytes {
		if !responses.evictOldestLocked(authIndex) {
			break
		}
	}
}

// Snapshot returns detached records sorted by newest first.
func (responses *ChatGPTWebAccountInfoRawQuotaResponses) Snapshot() chatgptwebauth.AccountInfoRawQuotaResponsesSnapshot {
	if responses == nil {
		return chatgptwebauth.AccountInfoRawQuotaResponsesSnapshot{
			Capacity: chatgptwebauth.AccountInfoRawQuotaResponseCapacity,
			MaxBytes: chatgptwebauth.AccountInfoRawQuotaResponseMaxBytes,
		}
	}
	responses.mu.Lock()
	defer responses.mu.Unlock()
	return responses.snapshotLocked()
}

// Clear removes captured responses while preserving the switch.
func (responses *ChatGPTWebAccountInfoRawQuotaResponses) Clear() chatgptwebauth.AccountInfoRawQuotaResponsesSnapshot {
	if responses == nil {
		return chatgptwebauth.AccountInfoRawQuotaResponsesSnapshot{
			Capacity: chatgptwebauth.AccountInfoRawQuotaResponseCapacity,
			MaxBytes: chatgptwebauth.AccountInfoRawQuotaResponseMaxBytes,
		}
	}
	responses.mu.Lock()
	defer responses.mu.Unlock()
	clear(responses.records)
	responses.totalBytes = 0
	responses.evictedCount = 0
	return responses.snapshotLocked()
}

func (responses *ChatGPTWebAccountInfoRawQuotaResponses) snapshotLocked() chatgptwebauth.AccountInfoRawQuotaResponsesSnapshot {
	records := make([]chatgptwebauth.AccountInfoRawQuotaResponseRecord, 0, len(responses.records))
	for _, record := range responses.records {
		cloned := record
		if record.ParsedQuota != nil {
			parsed := *record.ParsedQuota
			if record.ParsedQuota.ResetAt != nil {
				resetAt := *record.ParsedQuota.ResetAt
				parsed.ResetAt = &resetAt
			}
			cloned.ParsedQuota = &parsed
		}
		records = append(records, cloned)
	}
	sort.Slice(records, func(left, right int) bool {
		if records[left].CapturedAt.Equal(records[right].CapturedAt) {
			return records[left].AuthIndex < records[right].AuthIndex
		}
		return records[left].CapturedAt.After(records[right].CapturedAt)
	})
	return chatgptwebauth.AccountInfoRawQuotaResponsesSnapshot{
		Enabled:      responses.enabled,
		Capacity:     chatgptwebauth.AccountInfoRawQuotaResponseCapacity,
		MaxBytes:     chatgptwebauth.AccountInfoRawQuotaResponseMaxBytes,
		TotalBytes:   responses.totalBytes,
		EvictedCount: responses.evictedCount,
		Records:      records,
	}
}

func (responses *ChatGPTWebAccountInfoRawQuotaResponses) evictOldestLocked(preferredKeep string) bool {
	oldestKey := ""
	var oldest time.Time
	for key, record := range responses.records {
		if key == preferredKeep && len(responses.records) > 1 {
			continue
		}
		if oldestKey == "" || record.CapturedAt.Before(oldest) ||
			(record.CapturedAt.Equal(oldest) && key < oldestKey) {
			oldestKey = key
			oldest = record.CapturedAt
		}
	}
	if oldestKey == "" {
		return false
	}
	responses.totalBytes -= len(responses.records[oldestKey].Body)
	delete(responses.records, oldestKey)
	responses.evictedCount++
	return true
}

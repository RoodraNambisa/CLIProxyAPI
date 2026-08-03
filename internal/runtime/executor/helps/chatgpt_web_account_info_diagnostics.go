package helps

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
)

// ChatGPTWebAccountInfoDiagnostics keeps a bounded, de-duplicated in-memory
// view of safe account-info failures.
type ChatGPTWebAccountInfoDiagnostics struct {
	mu           sync.Mutex
	enabled      bool
	records      map[string]*chatgptwebauth.AccountInfoDiagnosticRecord
	totalCount   uint64
	evictedCount uint64
}

// NewChatGPTWebAccountInfoDiagnostics creates an empty diagnostics store.
func NewChatGPTWebAccountInfoDiagnostics(enabled bool) *ChatGPTWebAccountInfoDiagnostics {
	return &ChatGPTWebAccountInfoDiagnostics{
		enabled: enabled,
		records: make(map[string]*chatgptwebauth.AccountInfoDiagnosticRecord),
	}
}

// SetEnabled changes collection without removing existing records.
func (diagnostics *ChatGPTWebAccountInfoDiagnostics) SetEnabled(enabled bool) {
	if diagnostics == nil {
		return
	}
	diagnostics.mu.Lock()
	diagnostics.enabled = enabled
	diagnostics.mu.Unlock()
}

// Enabled reports whether new events are collected.
func (diagnostics *ChatGPTWebAccountInfoDiagnostics) Enabled() bool {
	if diagnostics == nil {
		return false
	}
	diagnostics.mu.Lock()
	defer diagnostics.mu.Unlock()
	return diagnostics.enabled
}

// Record merges one safe diagnostic event into the bounded store.
func (diagnostics *ChatGPTWebAccountInfoDiagnostics) Record(event chatgptwebauth.AccountInfoDiagnosticEvent) {
	if diagnostics == nil {
		return
	}
	signature := chatGPTWebAccountInfoDiagnosticSignature(event)
	if signature == "" {
		return
	}
	now := event.OccurredAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	diagnostics.mu.Lock()
	defer diagnostics.mu.Unlock()
	if !diagnostics.enabled {
		return
	}
	diagnostics.totalCount++
	if existing := diagnostics.records[signature]; existing != nil {
		chatGPTWebMergeAccountInfoDiagnostic(existing, event, now)
		return
	}
	if len(diagnostics.records) >= chatgptwebauth.AccountInfoDiagnosticsCapacity {
		diagnostics.evictOldestLocked()
	}
	record := chatGPTWebNewAccountInfoDiagnostic(signature, event, now)
	diagnostics.records[signature] = &record
}

// Snapshot returns a detached copy sorted by most recent occurrence.
func (diagnostics *ChatGPTWebAccountInfoDiagnostics) Snapshot() chatgptwebauth.AccountInfoDiagnosticsSnapshot {
	if diagnostics == nil {
		return chatgptwebauth.AccountInfoDiagnosticsSnapshot{Capacity: chatgptwebauth.AccountInfoDiagnosticsCapacity}
	}
	diagnostics.mu.Lock()
	defer diagnostics.mu.Unlock()
	return diagnostics.snapshotLocked()
}

// Clear removes records and cumulative counters while preserving the switch.
func (diagnostics *ChatGPTWebAccountInfoDiagnostics) Clear() chatgptwebauth.AccountInfoDiagnosticsSnapshot {
	if diagnostics == nil {
		return chatgptwebauth.AccountInfoDiagnosticsSnapshot{Capacity: chatgptwebauth.AccountInfoDiagnosticsCapacity}
	}
	diagnostics.mu.Lock()
	defer diagnostics.mu.Unlock()
	clear(diagnostics.records)
	diagnostics.totalCount = 0
	diagnostics.evictedCount = 0
	return diagnostics.snapshotLocked()
}

func (diagnostics *ChatGPTWebAccountInfoDiagnostics) snapshotLocked() chatgptwebauth.AccountInfoDiagnosticsSnapshot {
	records := make([]chatgptwebauth.AccountInfoDiagnosticRecord, 0, len(diagnostics.records))
	for _, record := range diagnostics.records {
		if record == nil {
			continue
		}
		cloned := *record
		cloned.ImageQuotaFeaturePresent = cloneChatGPTWebDiagnosticBool(record.ImageQuotaFeaturePresent)
		cloned.LastRemaining = cloneChatGPTWebDiagnosticInt64(record.LastRemaining)
		cloned.MinRemaining = cloneChatGPTWebDiagnosticInt64(record.MinRemaining)
		cloned.MaxRemaining = cloneChatGPTWebDiagnosticInt64(record.MaxRemaining)
		records = append(records, cloned)
	}
	sort.Slice(records, func(left, right int) bool {
		if records[left].LastSeen.Equal(records[right].LastSeen) {
			return records[left].ID < records[right].ID
		}
		return records[left].LastSeen.After(records[right].LastSeen)
	})
	return chatgptwebauth.AccountInfoDiagnosticsSnapshot{
		Enabled:      diagnostics.enabled,
		Capacity:     chatgptwebauth.AccountInfoDiagnosticsCapacity,
		UniqueCount:  len(records),
		TotalCount:   diagnostics.totalCount,
		EvictedCount: diagnostics.evictedCount,
		Records:      records,
	}
}

func (diagnostics *ChatGPTWebAccountInfoDiagnostics) evictOldestLocked() {
	oldestSignature := ""
	var oldest time.Time
	oldestID := ""
	for signature, record := range diagnostics.records {
		if record == nil {
			oldestSignature = signature
			break
		}
		if oldestSignature == "" || record.LastSeen.Before(oldest) ||
			(record.LastSeen.Equal(oldest) && record.ID < oldestID) {
			oldestSignature = signature
			oldest = record.LastSeen
			oldestID = record.ID
		}
	}
	if oldestSignature == "" {
		return
	}
	delete(diagnostics.records, oldestSignature)
	diagnostics.evictedCount++
}

func chatGPTWebNewAccountInfoDiagnostic(
	signature string,
	event chatgptwebauth.AccountInfoDiagnosticEvent,
	now time.Time,
) chatgptwebauth.AccountInfoDiagnosticRecord {
	sum := sha256.Sum256([]byte(signature))
	record := chatgptwebauth.AccountInfoDiagnosticRecord{
		ID:                       hex.EncodeToString(sum[:8]),
		Phase:                    event.Phase,
		Stage:                    event.Stage,
		Reason:                   event.Reason,
		ErrorType:                event.ErrorType,
		HTTPStatus:               event.HTTPStatus,
		ContentType:              event.ContentType,
		Cloudflare:               event.Cloudflare,
		BodyKind:                 event.BodyKind,
		AccountsKind:             event.AccountsKind,
		LimitsProgressKind:       event.LimitsProgressKind,
		LimitsProgressCount:      event.LimitsProgressCount,
		ImageQuotaFeaturePresent: cloneChatGPTWebDiagnosticBool(event.ImageQuotaFeaturePresent),
		ImageQuotaRemainingKind:  event.ImageQuotaRemainingKind,
		ImageQuotaResetAfter:     event.ImageQuotaResetAfter,
		ErrorEnvelopeKind:        event.ErrorEnvelopeKind,
		ResponseBytes:            event.ResponseBytes,
		ContentLength:            max(0, event.ContentLength),
		UpstreamErrorCode:        event.UpstreamErrorCode,
		Count:                    1,
		FirstSeen:                now,
		LastSeen:                 now,
		LastAuthIndex:            event.AuthIndex,
		LastAttempt:              event.Attempt,
	}
	chatGPTWebMergeAccountInfoDiagnosticRemaining(&record, event.ImageQuotaRemaining, true)
	return record
}

func chatGPTWebMergeAccountInfoDiagnostic(
	record *chatgptwebauth.AccountInfoDiagnosticRecord,
	event chatgptwebauth.AccountInfoDiagnosticEvent,
	now time.Time,
) {
	record.Count++
	if now.Before(record.FirstSeen) {
		record.FirstSeen = now
	}
	isLatest := !now.Before(record.LastSeen)
	if isLatest {
		record.LastSeen = now
		record.ErrorType = event.ErrorType
		record.ImageQuotaResetAfter = event.ImageQuotaResetAfter
		record.ResponseBytes = event.ResponseBytes
		record.ContentLength = max(0, event.ContentLength)
		record.LastAuthIndex = event.AuthIndex
		record.LastAttempt = event.Attempt
	}
	chatGPTWebMergeAccountInfoDiagnosticRemaining(record, event.ImageQuotaRemaining, isLatest)
}

func chatGPTWebMergeAccountInfoDiagnosticRemaining(
	record *chatgptwebauth.AccountInfoDiagnosticRecord,
	remaining *int64,
	updateLast bool,
) {
	if record == nil || remaining == nil {
		return
	}
	value := *remaining
	if updateLast {
		record.LastRemaining = &value
	}
	if record.MinRemaining == nil || value < *record.MinRemaining {
		minimum := value
		record.MinRemaining = &minimum
	}
	if record.MaxRemaining == nil || value > *record.MaxRemaining {
		maximum := value
		record.MaxRemaining = &maximum
	}
}

func chatGPTWebAccountInfoDiagnosticSignature(event chatgptwebauth.AccountInfoDiagnosticEvent) string {
	remainingClass := "missing"
	if event.ImageQuotaRemaining != nil {
		switch {
		case *event.ImageQuotaRemaining < 0:
			remainingClass = "negative"
		case *event.ImageQuotaRemaining == 0:
			remainingClass = "zero"
		default:
			remainingClass = "positive"
		}
	}
	signature := struct {
		Phase               string `json:"phase"`
		Stage               string `json:"stage"`
		Reason              string `json:"reason"`
		HTTPStatus          int    `json:"http_status"`
		ContentType         string `json:"content_type"`
		Cloudflare          bool   `json:"cloudflare"`
		BodyKind            string `json:"body_kind"`
		AccountsKind        string `json:"accounts_kind"`
		LimitsProgressKind  string `json:"limits_progress_kind"`
		LimitsProgressCount int    `json:"limits_progress_count"`
		FeaturePresent      string `json:"feature_present"`
		RemainingKind       string `json:"remaining_kind"`
		RemainingClass      string `json:"remaining_class"`
		ErrorEnvelopeKind   string `json:"error_envelope_kind"`
		UpstreamErrorCode   string `json:"upstream_error_code"`
	}{
		Phase:               strings.TrimSpace(event.Phase),
		Stage:               strings.TrimSpace(event.Stage),
		Reason:              strings.TrimSpace(event.Reason),
		HTTPStatus:          event.HTTPStatus,
		ContentType:         strings.TrimSpace(event.ContentType),
		Cloudflare:          event.Cloudflare,
		BodyKind:            strings.TrimSpace(event.BodyKind),
		AccountsKind:        strings.TrimSpace(event.AccountsKind),
		LimitsProgressKind:  strings.TrimSpace(event.LimitsProgressKind),
		LimitsProgressCount: event.LimitsProgressCount,
		FeaturePresent:      chatGPTWebDiagnosticBoolClass(event.ImageQuotaFeaturePresent),
		RemainingKind:       strings.TrimSpace(event.ImageQuotaRemainingKind),
		RemainingClass:      remainingClass,
		ErrorEnvelopeKind:   strings.TrimSpace(event.ErrorEnvelopeKind),
		UpstreamErrorCode:   strings.TrimSpace(event.UpstreamErrorCode),
	}
	if signature.Phase == "" || signature.Stage == "" || signature.Reason == "" {
		return ""
	}
	payload, errMarshal := json.Marshal(signature)
	if errMarshal != nil {
		return ""
	}
	return string(payload)
}

func chatGPTWebDiagnosticBoolClass(value *bool) string {
	if value == nil {
		return "unknown"
	}
	if *value {
		return "true"
	}
	return "false"
}

func cloneChatGPTWebDiagnosticBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneChatGPTWebDiagnosticInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

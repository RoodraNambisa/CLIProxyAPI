package helps

import (
	"fmt"
	"sync"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
)

func TestChatGPTWebAccountInfoDiagnosticsDeduplicatesNegativeRemaining(t *testing.T) {
	diagnostics := NewChatGPTWebAccountInfoDiagnostics(true)
	start := time.Date(2026, time.August, 3, 10, 0, 0, 0, time.UTC)
	first := int64(-1)
	second := int64(-2)
	diagnostics.Record(chatgptwebauth.AccountInfoDiagnosticEvent{
		OccurredAt: start, Phase: "quota", Stage: "parse", Reason: "quota_remaining_invalid",
		ErrorType: "*errors.errorString",
		BodyKind:  "json_object", LimitsProgressKind: "array", LimitsProgressCount: 4,
		ImageQuotaRemainingKind: "number", ImageQuotaRemaining: &first,
		ImageQuotaResetAfter: "2026-08-04T00:00:00Z", AuthIndex: "first", Attempt: 1,
	})
	diagnostics.Record(chatgptwebauth.AccountInfoDiagnosticEvent{
		OccurredAt: start.Add(time.Minute), Phase: "quota", Stage: "parse", Reason: "quota_remaining_invalid",
		ErrorType: "*fmt.wrapError",
		BodyKind:  "json_object", LimitsProgressKind: "array", LimitsProgressCount: 4,
		ImageQuotaRemainingKind: "number", ImageQuotaRemaining: &second,
		ImageQuotaResetAfter: "2026-08-05T00:00:00Z", AuthIndex: "second", Attempt: 2,
		ErrorMessage: "latest upstream error", ResponseBody: `{"error":"latest body"}`, ResponseBodyTruncated: true,
	})

	snapshot := diagnostics.Snapshot()
	if snapshot.UniqueCount != 1 || snapshot.TotalCount != 2 || len(snapshot.Records) != 1 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	record := snapshot.Records[0]
	if record.Count != 2 || record.LastRemaining == nil || *record.LastRemaining != -2 ||
		record.MinRemaining == nil || *record.MinRemaining != -2 ||
		record.MaxRemaining == nil || *record.MaxRemaining != -1 ||
		record.ErrorType != "*fmt.wrapError" ||
		record.LastAuthIndex != "second" || record.LastAttempt != 2 ||
		record.ImageQuotaResetAfter != "2026-08-05T00:00:00Z" {
		t.Fatalf("record = %+v", record)
	}
	if record.ErrorMessage != "latest upstream error" || record.ResponseBody != `{"error":"latest body"}` || !record.ResponseBodyTruncated {
		t.Fatalf("latest diagnostic content = %+v", record)
	}
}

func TestChatGPTWebAccountInfoDiagnosticsEvictsLeastRecentlySeen(t *testing.T) {
	diagnostics := NewChatGPTWebAccountInfoDiagnostics(true)
	start := time.Date(2026, time.August, 3, 10, 0, 0, 0, time.UTC)
	for index := 0; index < chatgptwebauth.AccountInfoDiagnosticsCapacity; index++ {
		diagnostics.Record(chatgptwebauth.AccountInfoDiagnosticEvent{
			OccurredAt: start.Add(time.Duration(index) * time.Second),
			Phase:      "quota", Stage: "parse", Reason: fmt.Sprintf("reason_%03d", index),
		})
	}
	diagnostics.Record(chatgptwebauth.AccountInfoDiagnosticEvent{
		OccurredAt: start.Add(time.Hour), Phase: "quota", Stage: "parse", Reason: "reason_000",
	})
	diagnostics.Record(chatgptwebauth.AccountInfoDiagnosticEvent{
		OccurredAt: start.Add(2 * time.Hour), Phase: "profile", Stage: "request", Reason: "new_reason",
	})

	snapshot := diagnostics.Snapshot()
	if snapshot.UniqueCount != chatgptwebauth.AccountInfoDiagnosticsCapacity ||
		snapshot.TotalCount != uint64(chatgptwebauth.AccountInfoDiagnosticsCapacity+2) ||
		snapshot.EvictedCount != 1 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	foundRefreshed := false
	foundEvicted := false
	for _, record := range snapshot.Records {
		foundRefreshed = foundRefreshed || record.Reason == "reason_000"
		foundEvicted = foundEvicted || record.Reason == "reason_001"
	}
	if !foundRefreshed || foundEvicted {
		t.Fatalf("records did not preserve LRU order: refreshed=%t evicted_present=%t", foundRefreshed, foundEvicted)
	}
}

func TestChatGPTWebAccountInfoDiagnosticsKeepsLatestFieldsForOutOfOrderEvents(t *testing.T) {
	diagnostics := NewChatGPTWebAccountInfoDiagnostics(true)
	latestTime := time.Date(2026, time.August, 3, 10, 0, 0, 0, time.UTC)
	latestRemaining := int64(-1)
	olderRemaining := int64(-3)
	base := chatgptwebauth.AccountInfoDiagnosticEvent{
		Phase: "quota", Stage: "parse", Reason: "quota_remaining_invalid",
		ImageQuotaRemainingKind: "number",
	}
	latest := base
	latest.OccurredAt = latestTime
	latest.ImageQuotaRemaining = &latestRemaining
	latest.AuthIndex = "latest"
	latest.ResponseBytes = 200
	diagnostics.Record(latest)
	older := base
	older.OccurredAt = latestTime.Add(-time.Minute)
	older.ImageQuotaRemaining = &olderRemaining
	older.AuthIndex = "older"
	older.ResponseBytes = 100
	diagnostics.Record(older)

	record := diagnostics.Snapshot().Records[0]
	if !record.FirstSeen.Equal(older.OccurredAt) || !record.LastSeen.Equal(latest.OccurredAt) ||
		record.LastAuthIndex != "latest" || record.ResponseBytes != 200 ||
		record.LastRemaining == nil || *record.LastRemaining != -1 ||
		record.MinRemaining == nil || *record.MinRemaining != -3 ||
		record.MaxRemaining == nil || *record.MaxRemaining != -1 {
		t.Fatalf("out-of-order record = %+v", record)
	}
}

func TestChatGPTWebAccountInfoDiagnosticsDisableRetainsAndClearResets(t *testing.T) {
	diagnostics := NewChatGPTWebAccountInfoDiagnostics(true)
	event := chatgptwebauth.AccountInfoDiagnosticEvent{
		OccurredAt: time.Now(), Phase: "quota", Stage: "parse", Reason: "invalid_response",
	}
	diagnostics.Record(event)
	diagnostics.SetEnabled(false)
	diagnostics.Record(event)
	snapshot := diagnostics.Snapshot()
	if snapshot.Enabled || snapshot.TotalCount != 1 || snapshot.UniqueCount != 1 {
		t.Fatalf("disabled snapshot = %+v", snapshot)
	}
	cleared := diagnostics.Clear()
	if cleared.Enabled || cleared.TotalCount != 0 || cleared.UniqueCount != 0 ||
		cleared.EvictedCount != 0 || len(cleared.Records) != 0 {
		t.Fatalf("cleared snapshot = %+v", cleared)
	}
}

func TestChatGPTWebAccountInfoDiagnosticsConcurrentRecordReadAndClear(t *testing.T) {
	diagnostics := NewChatGPTWebAccountInfoDiagnostics(true)
	var group sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			for index := 0; index < 100; index++ {
				diagnostics.Record(chatgptwebauth.AccountInfoDiagnosticEvent{
					OccurredAt: time.Now(), Phase: "quota", Stage: "parse",
					Reason: fmt.Sprintf("reason_%d", index%4), Attempt: worker,
				})
				_ = diagnostics.Snapshot()
			}
		}(worker)
	}
	group.Add(1)
	go func() {
		defer group.Done()
		for index := 0; index < 20; index++ {
			_ = diagnostics.Clear()
		}
	}()
	group.Wait()
	snapshot := diagnostics.Snapshot()
	if snapshot.UniqueCount > chatgptwebauth.AccountInfoDiagnosticsCapacity {
		t.Fatalf("snapshot exceeds capacity: %+v", snapshot)
	}
}

package helps

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
)

func TestChatGPTWebAccountInfoRawQuotaResponsesReplaceAndDetach(t *testing.T) {
	responses := NewChatGPTWebAccountInfoRawQuotaResponses(true)
	start := time.Date(2026, time.August, 4, 10, 0, 0, 0, time.UTC)
	body := []byte(`{"limits_progress":[{"feature_name":"image_gen","remaining":17}]}`)
	quota := &chatgptwebauth.ImageQuota{FeaturePresent: true, Present: true, Remaining: 17}
	responses.Record(chatgptwebauth.AccountInfoRawQuotaResponseEvent{
		CapturedAt: start, AuthIndex: "credential-a", Attempt: 1,
		HTTPStatus: 200, ContentType: "application/json", Body: body, ParsedQuota: quota,
	})
	body[0] = 'x'
	quota.Remaining = 99
	responses.Record(chatgptwebauth.AccountInfoRawQuotaResponseEvent{
		CapturedAt: start.Add(time.Minute), AuthIndex: "credential-a", Attempt: 2,
		HTTPStatus: 200, ContentType: "application/json", Body: []byte(`{"remaining":-2}`),
		ParsedQuota: &chatgptwebauth.ImageQuota{FeaturePresent: true, Present: true, Remaining: -2},
	})

	snapshot := responses.Snapshot()
	if !snapshot.Enabled || snapshot.Capacity != chatgptwebauth.AccountInfoRawQuotaResponseCapacity ||
		snapshot.MaxBytes != chatgptwebauth.AccountInfoRawQuotaResponseMaxBytes ||
		len(snapshot.Records) != 1 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	record := snapshot.Records[0]
	if record.AuthIndex != "credential-a" || record.Attempt != 2 || record.Body != `{"remaining":-2}` ||
		record.ParsedQuota == nil || record.ParsedQuota.Remaining != -2 {
		t.Fatalf("record = %+v", record)
	}
	record.ParsedQuota.Remaining = 123
	if got := responses.Snapshot().Records[0].ParsedQuota.Remaining; got != -2 {
		t.Fatalf("snapshot mutation changed store: %d", got)
	}
	encoded, errEncode := json.Marshal(responses.Snapshot().Records[0].ParsedQuota)
	if errEncode != nil {
		t.Fatalf("marshal parsed quota: %v", errEncode)
	}
	if string(encoded) != `{"feature_present":true,"present":true,"remaining":-2}` {
		t.Fatalf("parsed quota JSON = %s", encoded)
	}
}

func TestChatGPTWebAccountInfoRawQuotaResponsesCapacityDisableAndClear(t *testing.T) {
	responses := NewChatGPTWebAccountInfoRawQuotaResponses(false)
	responses.Record(chatgptwebauth.AccountInfoRawQuotaResponseEvent{
		CapturedAt: time.Now(), AuthIndex: "ignored", HTTPStatus: 200, Body: []byte(`{}`),
	})
	if records := responses.Snapshot().Records; len(records) != 0 {
		t.Fatalf("disabled store recorded %d responses", len(records))
	}
	responses.SetEnabled(true)
	start := time.Date(2026, time.August, 4, 10, 0, 0, 0, time.UTC)
	for index := 0; index <= chatgptwebauth.AccountInfoRawQuotaResponseCapacity; index++ {
		responses.Record(chatgptwebauth.AccountInfoRawQuotaResponseEvent{
			CapturedAt: start.Add(time.Duration(index) * time.Second),
			AuthIndex:  fmt.Sprintf("credential-%02d", index),
			HTTPStatus: 200,
			Body:       []byte(fmt.Sprintf(`{"index":%d}`, index)),
		})
	}
	snapshot := responses.Snapshot()
	if len(snapshot.Records) != chatgptwebauth.AccountInfoRawQuotaResponseCapacity || snapshot.EvictedCount != 1 {
		t.Fatalf("bounded snapshot = %+v", snapshot)
	}
	for _, record := range snapshot.Records {
		if record.AuthIndex == "credential-00" {
			t.Fatal("oldest response was not evicted")
		}
	}
	responses.SetEnabled(false)
	cleared := responses.Clear()
	if cleared.Enabled || len(cleared.Records) != 0 || cleared.TotalBytes != 0 || cleared.EvictedCount != 0 {
		t.Fatalf("cleared snapshot = %+v", cleared)
	}
}

func TestChatGPTWebAccountInfoRawQuotaResponsesConcurrentAccess(t *testing.T) {
	responses := NewChatGPTWebAccountInfoRawQuotaResponses(true)
	var group sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			for index := 0; index < 100; index++ {
				responses.Record(chatgptwebauth.AccountInfoRawQuotaResponseEvent{
					CapturedAt: time.Now(), AuthIndex: fmt.Sprintf("credential-%d", (worker+index)%25),
					HTTPStatus: 200, Body: []byte(`{"ok":true}`),
				})
				_ = responses.Snapshot()
			}
		}(worker)
	}
	group.Add(1)
	go func() {
		defer group.Done()
		for index := 0; index < 20; index++ {
			_ = responses.Clear()
		}
	}()
	group.Wait()
	if got := len(responses.Snapshot().Records); got > chatgptwebauth.AccountInfoRawQuotaResponseCapacity {
		t.Fatalf("records = %d, exceeds capacity", got)
	}
}

package logging

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

func TestLiveLogBrokerFiltersAndRedacts(t *testing.T) {
	broker := newLiveLogBroker()
	broker.SetEnabled(true)
	subscription, errSubscribe := broker.Subscribe(LiveLogFilter{
		Levels:   map[string]struct{}{"error": {}},
		Provider: "chatgpt-web",
		Status:   403,
	}, 0)
	if errSubscribe != nil {
		t.Fatalf("subscribe: %v", errSubscribe)
	}
	defer subscription.Close()

	entry := log.NewEntry(log.StandardLogger())
	entry.Time = time.Unix(1700000000, 0)
	entry.Level = log.ErrorLevel
	entry.Message = "request failed authorization=Bearer secret-token email=user@example.com https://user:pass@example.com/path?sig=secret"
	entry.Data = log.Fields{
		"provider":      "chatgpt-web",
		"status":        403,
		"request_id":    "request-safe",
		"stage":         "file_sign",
		"code":          "cloudflare_challenge",
		"retryable":     true,
		"attempts":      3,
		"access_token":  "must-not-appear",
		"response_body": "must-not-appear-either",
	}
	if errFire := broker.Fire(entry); errFire != nil {
		t.Fatalf("fire: %v", errFire)
	}

	select {
	case frame := <-subscription.Frames:
		if frame.Event == nil {
			t.Fatalf("frame = %#v, want event", frame)
		}
		encoded, errMarshal := json.Marshal(frame.Event)
		if errMarshal != nil {
			t.Fatalf("marshal event: %v", errMarshal)
		}
		text := string(encoded)
		for _, secret := range []string{"secret-token", "user@example.com", "user:pass", "sig=secret", "must-not-appear"} {
			if strings.Contains(text, secret) {
				t.Fatalf("event leaked %q: %s", secret, text)
			}
		}
		if frame.Event.Provider != "chatgpt-web" || frame.Event.Status != 403 || frame.Event.Stage != "file_sign" || frame.Event.Attempts != 3 || frame.Event.Retryable == nil || !*frame.Event.Retryable {
			t.Fatalf("event = %#v", frame.Event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestLiveLogBrokerReplayGapAndDisable(t *testing.T) {
	broker := newLiveLogBroker()
	broker.SetEnabled(true)
	for index := 0; index < liveLogSubscriberBuffer+10; index++ {
		broker.publish(LiveLogEvent{Level: "info", Message: fmt.Sprintf("event-%d", index)})
	}
	subscription, errSubscribe := broker.Subscribe(LiveLogFilter{}, 0)
	if errSubscribe != nil {
		t.Fatalf("subscribe: %v", errSubscribe)
	}
	first := <-subscription.Frames
	if first.Gap == nil || first.Gap.Count != 11 {
		t.Fatalf("first frame = %#v, want replay gap of 11", first)
	}
	broker.SetEnabled(false)
	for range subscription.Frames {
	}
	if _, errDisabled := broker.Subscribe(LiveLogFilter{}, 0); errDisabled != ErrLiveLogsDisabled {
		t.Fatalf("disabled subscribe error = %v", errDisabled)
	}
}

func TestLiveLogBrokerReplayReportsOverwrittenCursorRange(t *testing.T) {
	broker := newLiveLogBroker()
	broker.SetEnabled(true)
	for index := 0; index < liveLogCapacity+100; index++ {
		broker.publish(LiveLogEvent{Level: "info", Message: fmt.Sprintf("event-%d", index)})
	}
	subscription, errSubscribe := broker.Subscribe(LiveLogFilter{}, 1)
	if errSubscribe != nil {
		t.Fatalf("subscribe: %v", errSubscribe)
	}
	defer subscription.Close()
	first := <-subscription.Frames
	if first.Gap == nil || first.Gap.From != 2 || first.Gap.To != uint64(liveLogCapacity+100-(liveLogSubscriberBuffer-1)) {
		t.Fatalf("first frame = %#v, want combined retention gap", first)
	}
	if first.Gap.Count != first.Gap.To-first.Gap.From+1 {
		t.Fatalf("gap count = %d, range = %d..%d", first.Gap.Count, first.Gap.From, first.Gap.To)
	}
}

func TestLiveLogBrokerDoesNotBlockSlowSubscriber(t *testing.T) {
	broker := newLiveLogBroker()
	broker.SetEnabled(true)
	subscription, errSubscribe := broker.Subscribe(LiveLogFilter{}, 0)
	if errSubscribe != nil {
		t.Fatalf("subscribe: %v", errSubscribe)
	}
	defer subscription.Close()
	for index := 0; index < liveLogSubscriberBuffer+25; index++ {
		broker.publish(LiveLogEvent{Level: "debug", Message: "fill"})
	}
	for index := 0; index < liveLogSubscriberBuffer; index++ {
		<-subscription.Frames
	}
	broker.publish(LiveLogEvent{Level: "debug", Message: "after-gap"})
	frame := <-subscription.Frames
	if frame.Gap == nil || frame.Gap.Count == 0 {
		t.Fatalf("frame = %#v, want delivery gap", frame)
	}
}

package logging

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/managementdiag"
	log "github.com/sirupsen/logrus"
)

type blockingLiveLogValue struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (value blockingLiveLogValue) String() string {
	value.started <- struct{}{}
	<-value.release
	return "person@example.com upstream detail"
}

func TestLiveLogBrokerFiltersAndPreservesBoundedResponseBody(t *testing.T) {
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
	entry.Message = "request failed authorization=Bearer secret-token email=user@example.com https://user:pass@example.com/path?sig=secret " +
		`{"password":"json-secret","session_token":"opaque-session","privateKey":"opaque-private"}` +
		"\nCookie: first=cookie-one; second=cookie-two\n" +
		"-----BEGIN PRIVATE KEY-----\nprivate-key-material\n-----END PRIVATE KEY-----"
	responseBody := `{"error":{"message":"upstream detail","request_id":"upstream-request"}}`
	entry.Data = log.Fields{
		"provider":      "chatgpt-web",
		"status":        403,
		"request_id":    "request-safe",
		"stage":         "file_sign",
		"code":          "cloudflare_challenge",
		"retryable":     true,
		"attempts":      3,
		"access_token":  "must-not-appear",
		"response_body": responseBody,
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
		for _, secret := range []string{"secret-token", "user@example.com", "user:pass", "sig=secret", "must-not-appear", "json-secret", "opaque-session", "opaque-private", "cookie-one", "cookie-two", "private-key-material"} {
			if strings.Contains(text, secret) {
				t.Fatalf("event leaked %q: %s", secret, text)
			}
		}
		if frame.Event.ResponseBody != responseBody || frame.Event.ResponseBodyTruncated {
			t.Fatalf("response body = %q truncated=%v", frame.Event.ResponseBody, frame.Event.ResponseBodyTruncated)
		}
		if frame.Event.Provider != "chatgpt-web" || frame.Event.Status != 403 || frame.Event.Stage != "file_sign" || frame.Event.Attempts != 3 || frame.Event.Retryable == nil || !*frame.Event.Retryable {
			t.Fatalf("event = %#v", frame.Event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestLiveLogBrokerBoundsRawResponseBodyAndFiltersIt(t *testing.T) {
	broker := newLiveLogBroker()
	broker.SetEnabled(true)
	subscription, errSubscribe := broker.Subscribe(LiveLogFilter{Contains: "diagnostic-tail"}, 0)
	if errSubscribe != nil {
		t.Fatalf("subscribe: %v", errSubscribe)
	}
	defer subscription.Close()

	entry := log.NewEntry(log.StandardLogger())
	entry.Level = log.ErrorLevel
	entry.Message = "upstream request failed"
	entry.Data = log.Fields{
		"status":        502,
		"response_body": "diagnostic-tail" + strings.Repeat("界", liveLogMaxResponseBodyBytes),
	}
	if errFire := broker.Fire(entry); errFire != nil {
		t.Fatalf("fire: %v", errFire)
	}
	frame := <-subscription.Frames
	if frame.Event == nil || !frame.Event.ResponseBodyTruncated || len(frame.Event.ResponseBody) > liveLogMaxResponseBodyBytes || !strings.HasPrefix(frame.Event.ResponseBody, "diagnostic-tail") {
		t.Fatalf("bounded event = %#v", frame.Event)
	}
}

func TestLiveLogBrokerFullDetailKeepsNonCredentialContent(t *testing.T) {
	broker := newLiveLogBroker()
	broker.SetConfiguration(true, "full")
	subscription, errSubscribe := broker.Subscribe(LiveLogFilter{}, 0)
	if errSubscribe != nil {
		t.Fatalf("subscribe: %v", errSubscribe)
	}
	defer subscription.Close()

	entry := log.NewEntry(log.StandardLogger())
	entry.Level = log.ErrorLevel
	entry.Message = "failed for person@example.com https://example.com/path?trace=abc&code=oauth-secret Bearer bearer-secret"
	entry.Data = log.Fields{
		"status": 403,
		"path":   "/backend-api/test?trace=abc&code=oauth-secret",
		"target_path": managementdiag.NewManagementOnlyValueWithFallback(
			"/backend-api/test?trace=abc&code=oauth-secret",
			"/backend-api/test",
		),
		"response_body": managementdiag.NewManagementOnlyValue(`<html>person@example.com upstream detail ` +
			`https://example.com/error?trace=abc Cookie: session=secret</html>`),
	}
	if errFire := broker.Fire(entry); errFire != nil {
		t.Fatalf("fire: %v", errFire)
	}
	frame := <-subscription.Frames
	if frame.Event == nil {
		t.Fatalf("frame = %#v", frame)
	}
	encoded, errMarshal := json.Marshal(frame.Event)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	text := string(encoded)
	for _, want := range []string{"person@example.com", "trace=abc", "upstream detail"} {
		if !strings.Contains(text, want) {
			t.Fatalf("full event missing %q: %s", want, text)
		}
	}
	for _, secret := range []string{"oauth-secret", "bearer-secret", "session=secret"} {
		if strings.Contains(text, secret) {
			t.Fatalf("full event leaked %q: %s", secret, text)
		}
	}
}

func TestLiveLogBrokerFullToSafeClearsRingAndDisconnectsStreams(t *testing.T) {
	broker := newLiveLogBroker()
	broker.SetConfiguration(true, "full")
	subscription, errSubscribe := broker.Subscribe(LiveLogFilter{}, 0)
	if errSubscribe != nil {
		t.Fatal(errSubscribe)
	}
	broker.publish(LiveLogEvent{Level: "error", Message: "person@example.com"})
	<-subscription.Frames

	broker.SetConfiguration(true, "safe")
	if _, open := <-subscription.Frames; open {
		t.Fatal("full-detail subscription remained open after downgrade")
	}
	reconnected, errReconnect := broker.Subscribe(LiveLogFilter{}, 0)
	if errReconnect != nil {
		t.Fatal(errReconnect)
	}
	defer reconnected.Close()
	select {
	case frame := <-reconnected.Frames:
		t.Fatalf("downgrade replayed retained full event: %#v", frame)
	case <-time.After(25 * time.Millisecond):
	}
}

func TestLiveLogBrokerDowngradeSerializesWithInFlightFullEvent(t *testing.T) {
	broker := newLiveLogBroker()
	broker.SetConfiguration(true, "full")
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	entry := log.NewEntry(log.StandardLogger())
	entry.Level = log.ErrorLevel
	entry.Data = log.Fields{
		"status":        http.StatusBadGateway,
		"response_body": blockingLiveLogValue{started: started, release: release},
	}
	fired := make(chan struct{})
	go func() {
		_ = broker.Fire(entry)
		close(fired)
	}()
	<-started

	downgraded := make(chan struct{})
	go func() {
		broker.SetConfiguration(true, "safe")
		close(downgraded)
	}()
	select {
	case <-downgraded:
		// Configuration changes must not wait for diagnostic formatting.
	case <-time.After(25 * time.Millisecond):
		t.Fatal("detail downgrade blocked on an in-flight full event")
	}
	close(release)
	<-fired

	subscription, errSubscribe := broker.Subscribe(LiveLogFilter{}, 0)
	if errSubscribe != nil {
		t.Fatal(errSubscribe)
	}
	defer subscription.Close()
	select {
	case frame := <-subscription.Frames:
		t.Fatalf("downgrade retained an in-flight full event: %#v", frame)
	case <-time.After(25 * time.Millisecond):
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

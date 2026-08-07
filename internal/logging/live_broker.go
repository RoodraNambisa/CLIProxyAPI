package logging

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	log "github.com/sirupsen/logrus"
)

const (
	liveLogCapacity           = 2000
	liveLogSubscriberBuffer   = 256
	liveLogMaxSubscribers     = 8
	liveLogMaxMessageBytes    = 2048
	liveLogMaxFieldValueBytes = 256
)

var (
	globalLiveLogBroker = newLiveLogBroker()
	// ErrLiveLogsDisabled indicates that the in-memory stream is disabled.
	ErrLiveLogsDisabled = errors.New("live logs disabled")
	// ErrLiveLogConnectionLimit indicates that all subscriber slots are occupied.
	ErrLiveLogConnectionLimit = errors.New("live log connection limit reached")

	liveLogJWTPattern        = regexp.MustCompile(`(?i)\beyJ[a-z0-9_-]{8,}\.[a-z0-9_-]{8,}(?:\.[a-z0-9_-]{8,})?\b`)
	liveLogAPIKeyPattern     = regexp.MustCompile(`(?i)\bsk-[a-z0-9_-]{8,}\b`)
	liveLogBearerPattern     = regexp.MustCompile(`(?i)\bbearer\s+[^\s,;]+`)
	liveLogCookieHeader      = regexp.MustCompile(`(?im)\b(set-cookie|cookie)\s*:\s*[^\r\n]*`)
	liveLogEmailPattern      = regexp.MustCompile(`(?i)\b[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}\b`)
	liveLogAssignmentPattern = regexp.MustCompile(`(?i)(["']?(authorization|access[_-]?token|refresh[_-]?token|session[_-]?token|cookie|password|secret|private[_-]?key|assertion|proxy[_-]?url)["']?)(\s*[=:]\s*|\s+)("[^"\r\n]*"|'[^'\r\n]*'|[^\s,;]+)`)
	liveLogPrivateKeyPattern = regexp.MustCompile(`(?is)-----BEGIN [^-\r\n]*PRIVATE KEY-----.*?-----END [^-\r\n]*PRIVATE KEY-----`)
	liveLogURLPattern        = regexp.MustCompile(`https?://[^\s<>"']+`)
	liveLogHostPattern       = regexp.MustCompile(`(?i)^[a-z0-9](?:[a-z0-9.-]{0,253}[a-z0-9])?$`)
)

// LiveLogEvent is a safe, bounded representation of one log entry.
type LiveLogEvent struct {
	Cursor        uint64 `json:"cursor"`
	Timestamp     string `json:"timestamp"`
	Level         string `json:"level"`
	Message       string `json:"message"`
	RequestID     string `json:"request_id,omitempty"`
	Provider      string `json:"provider,omitempty"`
	AuthIndex     string `json:"auth_index,omitempty"`
	Stage         string `json:"stage,omitempty"`
	Code          string `json:"code,omitempty"`
	Status        int    `json:"status,omitempty"`
	Method        string `json:"method,omitempty"`
	Path          string `json:"path,omitempty"`
	Retryable     *bool  `json:"retryable,omitempty"`
	Persona       string `json:"persona,omitempty"`
	UAMajor       string `json:"ua_major,omitempty"`
	Platform      string `json:"platform,omitempty"`
	TargetHost    string `json:"target_host,omitempty"`
	TargetPath    string `json:"target_path,omitempty"`
	ResponseType  string `json:"response_type,omitempty"`
	ContentType   string `json:"content_type,omitempty"`
	CFRay         string `json:"cf_ray,omitempty"`
	ResponseBytes int64  `json:"response_bytes,omitempty"`
	Attempts      int    `json:"attempts,omitempty"`
	Cloudflare    bool   `json:"cloudflare,omitempty"`
}

// LiveLogGap reports events dropped before they could be delivered to a slow subscriber.
type LiveLogGap struct {
	Count uint64 `json:"count"`
	From  uint64 `json:"from"`
	To    uint64 `json:"to"`
}

// LiveLogFrame carries either an event or a delivery gap.
type LiveLogFrame struct {
	Event *LiveLogEvent `json:"event,omitempty"`
	Gap   *LiveLogGap   `json:"gap,omitempty"`
}

// LiveLogFilter is evaluated by the broker before an event enters a subscriber queue.
type LiveLogFilter struct {
	Levels         map[string]struct{}
	Contains       string
	RequestID      string
	Provider       string
	AuthIndex      string
	Stage          string
	Code           string
	Status         int
	Method         string
	Path           string
	HideManagement bool
}

type liveLogSubscription struct {
	id          uint64
	filter      LiveLogFilter
	frames      chan LiveLogFrame
	dropped     uint64
	droppedFrom uint64
	droppedTo   uint64
}

// LiveLogSubscription is an authenticated stream subscription.
type LiveLogSubscription struct {
	Frames <-chan LiveLogFrame
	cancel func()
}

// Close releases the subscription. It is safe to call more than once.
func (subscription *LiveLogSubscription) Close() {
	if subscription == nil || subscription.cancel == nil {
		return
	}
	subscription.cancel()
}

// LiveLogBroker stores recent safe log events and fans them out without blocking writers.
type LiveLogBroker struct {
	enabled atomic.Bool

	mu             sync.Mutex
	ring           [liveLogCapacity]LiveLogEvent
	ringStart      int
	ringCount      int
	nextCursor     uint64
	nextSubscriber uint64
	subscribers    map[uint64]*liveLogSubscription
}

func newLiveLogBroker() *LiveLogBroker {
	return &LiveLogBroker{subscribers: make(map[uint64]*liveLogSubscription)}
}

// ConfigureLiveLogs enables or disables collection and closes active streams when disabled.
func ConfigureLiveLogs(enabled bool) {
	globalLiveLogBroker.SetEnabled(enabled)
}

// GlobalLiveLogBroker returns the process-wide safe live log broker.
func GlobalLiveLogBroker() *LiveLogBroker {
	return globalLiveLogBroker
}

// SetEnabled changes collection state and closes active streams when disabled.
func (broker *LiveLogBroker) SetEnabled(enabled bool) {
	if broker == nil {
		return
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()
	broker.enabled.Store(enabled)
	if enabled {
		return
	}
	for id, subscriber := range broker.subscribers {
		close(subscriber.frames)
		delete(broker.subscribers, id)
	}
}

// Enabled reports whether new events and subscriptions are accepted.
func (broker *LiveLogBroker) Enabled() bool {
	return broker != nil && broker.enabled.Load()
}

// Levels implements logrus.Hook.
func (broker *LiveLogBroker) Levels() []log.Level {
	return log.AllLevels
}

// Fire implements logrus.Hook without modifying the original log entry.
func (broker *LiveLogBroker) Fire(entry *log.Entry) error {
	if broker == nil || entry == nil || !broker.enabled.Load() {
		return nil
	}
	broker.publish(safeLiveLogEvent(entry))
	return nil
}

// Subscribe creates a bounded filtered stream and replays retained events newer than cursor.
func (broker *LiveLogBroker) Subscribe(filter LiveLogFilter, cursor uint64) (*LiveLogSubscription, error) {
	if broker == nil || !broker.enabled.Load() {
		return nil, ErrLiveLogsDisabled
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if !broker.enabled.Load() {
		return nil, ErrLiveLogsDisabled
	}
	if len(broker.subscribers) >= liveLogMaxSubscribers {
		return nil, ErrLiveLogConnectionLimit
	}

	broker.nextSubscriber++
	subscriber := &liveLogSubscription{
		id:     broker.nextSubscriber,
		filter: normalizeLiveLogFilter(filter),
		frames: make(chan LiveLogFrame, liveLogSubscriberBuffer),
	}
	retained := broker.retainedLocked(subscriber.filter, cursor)
	gap := broker.replayGapLocked(cursor)
	retainedCapacity := liveLogSubscriberBuffer
	if gap != nil {
		retainedCapacity--
	}
	if len(retained) > retainedCapacity {
		retainedCapacity = liveLogSubscriberBuffer - 1
		omitted := retained[:len(retained)-retainedCapacity]
		retained = retained[len(retained)-retainedCapacity:]
		if gap == nil {
			gap = &LiveLogGap{From: omitted[0].Cursor}
		}
		gap.Count += uint64(len(omitted))
		gap.To = omitted[len(omitted)-1].Cursor
	}
	if gap != nil {
		subscriber.frames <- LiveLogFrame{Gap: gap}
	}
	for i := range retained {
		event := retained[i]
		subscriber.frames <- LiveLogFrame{Event: &event}
	}
	broker.subscribers[subscriber.id] = subscriber

	var closeOnce sync.Once
	return &LiveLogSubscription{
		Frames: subscriber.frames,
		cancel: func() {
			closeOnce.Do(func() { broker.removeSubscriber(subscriber.id) })
		},
	}, nil
}

func (broker *LiveLogBroker) replayGapLocked(cursor uint64) *LiveLogGap {
	if cursor == 0 || broker.ringCount == 0 {
		return nil
	}
	oldest := broker.ring[broker.ringStart].Cursor
	if oldest <= cursor || oldest-cursor <= 1 {
		return nil
	}
	return &LiveLogGap{
		Count: oldest - cursor - 1,
		From:  cursor + 1,
		To:    oldest - 1,
	}
}

func (broker *LiveLogBroker) removeSubscriber(id uint64) {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	subscriber := broker.subscribers[id]
	if subscriber == nil {
		return
	}
	delete(broker.subscribers, id)
	close(subscriber.frames)
}

func (broker *LiveLogBroker) publish(event LiveLogEvent) {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if !broker.enabled.Load() {
		return
	}
	broker.nextCursor++
	event.Cursor = broker.nextCursor
	broker.appendLocked(event)
	for _, subscriber := range broker.subscribers {
		if !subscriber.filter.matches(event) {
			continue
		}
		broker.deliverLocked(subscriber, event)
	}
}

func (broker *LiveLogBroker) appendLocked(event LiveLogEvent) {
	if broker.ringCount < liveLogCapacity {
		index := (broker.ringStart + broker.ringCount) % liveLogCapacity
		broker.ring[index] = event
		broker.ringCount++
		return
	}
	broker.ring[broker.ringStart] = event
	broker.ringStart = (broker.ringStart + 1) % liveLogCapacity
}

func (broker *LiveLogBroker) retainedLocked(filter LiveLogFilter, cursor uint64) []LiveLogEvent {
	retained := make([]LiveLogEvent, 0, broker.ringCount)
	for offset := 0; offset < broker.ringCount; offset++ {
		event := broker.ring[(broker.ringStart+offset)%liveLogCapacity]
		if event.Cursor > cursor && filter.matches(event) {
			retained = append(retained, event)
		}
	}
	return retained
}

func (broker *LiveLogBroker) deliverLocked(subscriber *liveLogSubscription, event LiveLogEvent) {
	if subscriber.dropped > 0 {
		gap := LiveLogFrame{Gap: &LiveLogGap{
			Count: subscriber.dropped,
			From:  subscriber.droppedFrom,
			To:    subscriber.droppedTo,
		}}
		select {
		case subscriber.frames <- gap:
			subscriber.dropped = 0
			subscriber.droppedFrom = 0
			subscriber.droppedTo = 0
		default:
			subscriber.recordDrop(event.Cursor)
			return
		}
	}
	select {
	case subscriber.frames <- LiveLogFrame{Event: &event}:
	default:
		subscriber.recordDrop(event.Cursor)
	}
}

func (subscriber *liveLogSubscription) recordDrop(cursor uint64) {
	if subscriber.dropped == 0 {
		subscriber.droppedFrom = cursor
	}
	subscriber.dropped++
	subscriber.droppedTo = cursor
}

func normalizeLiveLogFilter(filter LiveLogFilter) LiveLogFilter {
	filter.Contains = strings.ToLower(strings.TrimSpace(filter.Contains))
	filter.RequestID = strings.TrimSpace(filter.RequestID)
	filter.Provider = strings.ToLower(strings.TrimSpace(filter.Provider))
	filter.AuthIndex = strings.TrimSpace(filter.AuthIndex)
	filter.Stage = strings.ToLower(strings.TrimSpace(filter.Stage))
	filter.Code = strings.ToLower(strings.TrimSpace(filter.Code))
	filter.Method = strings.ToUpper(strings.TrimSpace(filter.Method))
	filter.Path = strings.TrimSpace(filter.Path)
	if len(filter.Levels) > 0 {
		normalized := make(map[string]struct{}, len(filter.Levels))
		for level := range filter.Levels {
			normalized[strings.ToLower(strings.TrimSpace(level))] = struct{}{}
		}
		filter.Levels = normalized
	}
	return filter
}

func (filter LiveLogFilter) matches(event LiveLogEvent) bool {
	if len(filter.Levels) > 0 {
		if _, exists := filter.Levels[strings.ToLower(event.Level)]; !exists {
			return false
		}
	}
	if filter.Contains != "" {
		haystack := strings.ToLower(strings.Join([]string{event.Message, event.RequestID, event.Provider, event.AuthIndex, event.Stage, event.Code, event.Method, event.Path}, " "))
		if !strings.Contains(haystack, filter.Contains) {
			return false
		}
	}
	if filter.RequestID != "" && !strings.EqualFold(filter.RequestID, event.RequestID) {
		return false
	}
	if filter.Provider != "" && !strings.EqualFold(filter.Provider, event.Provider) {
		return false
	}
	if filter.AuthIndex != "" && !strings.EqualFold(filter.AuthIndex, event.AuthIndex) {
		return false
	}
	if filter.Stage != "" && !strings.EqualFold(filter.Stage, event.Stage) {
		return false
	}
	if filter.Code != "" && !strings.EqualFold(filter.Code, event.Code) {
		return false
	}
	if filter.Status > 0 && filter.Status != event.Status {
		return false
	}
	if filter.Method != "" && !strings.EqualFold(filter.Method, event.Method) {
		return false
	}
	if filter.Path != "" && !strings.Contains(event.Path, filter.Path) {
		return false
	}
	return !filter.HideManagement || !strings.Contains(event.Path, "/management")
}

func safeLiveLogEvent(entry *log.Entry) LiveLogEvent {
	event := LiveLogEvent{
		Timestamp: entry.Time.UTC().Format(time.RFC3339Nano),
		Level:     entry.Level.String(),
		Message:   sanitizeLiveLogValue(entry.Message, liveLogMaxMessageBytes),
	}
	event.RequestID = safeLiveLogField(entry.Data, liveLogMaxFieldValueBytes, "request_id")
	event.Provider = safeLiveLogField(entry.Data, liveLogMaxFieldValueBytes, "provider")
	event.AuthIndex = safeLiveLogField(entry.Data, liveLogMaxFieldValueBytes, "auth_index", "authIndex")
	event.Stage = safeLiveLogField(entry.Data, liveLogMaxFieldValueBytes, "stage", "failure_stage")
	event.Code = safeLiveLogField(entry.Data, liveLogMaxFieldValueBytes, "code", "error_code")
	event.Status = safeLiveLogStatus(entry.Data)
	event.Method = strings.ToUpper(safeLiveLogField(entry.Data, 16, "method"))
	event.Path = sanitizeLiveLogPath(safeLiveLogField(entry.Data, liveLogMaxFieldValueBytes, "path"))
	event.Persona = safeLiveLogField(entry.Data, 64, "persona")
	event.UAMajor = safeLiveLogField(entry.Data, 16, "ua_major")
	event.Platform = safeLiveLogField(entry.Data, 64, "platform")
	event.TargetHost = sanitizeLiveLogHost(safeLiveLogField(entry.Data, 255, "target_host"))
	event.TargetPath = sanitizeLiveLogPath(safeLiveLogField(entry.Data, liveLogMaxFieldValueBytes, "target_path"))
	event.ResponseType = safeLiveLogField(entry.Data, 32, "response_type")
	event.ContentType = safeLiveLogField(entry.Data, 128, "content_type")
	event.CFRay = safeLiveLogField(entry.Data, 128, "cf_ray")
	event.ResponseBytes = safeLiveLogInt64(entry.Data, "response_bytes")
	event.Attempts = safeLiveLogInt(entry.Data, 100, "attempts", "flow_attempt", "request_attempt")
	if value, ok := safeLiveLogBool(entry.Data, "cloudflare"); ok {
		event.Cloudflare = value
	}
	if value, ok := safeLiveLogBool(entry.Data, "retryable"); ok {
		event.Retryable = &value
	}
	return event
}

func safeLiveLogInt(data log.Fields, maximum int, names ...string) int {
	for _, name := range names {
		value, exists := data[name]
		if !exists {
			continue
		}
		parsed, err := strconv.Atoi(strings.TrimSpace(fmt.Sprint(value)))
		if err == nil && parsed >= 0 && parsed <= maximum {
			return parsed
		}
	}
	return 0
}

func safeLiveLogInt64(data log.Fields, name string) int64 {
	value, exists := data[name]
	if !exists {
		return 0
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(fmt.Sprint(value)), 10, 64)
	if err != nil || parsed < 0 {
		return 0
	}
	return parsed
}

func safeLiveLogField(data log.Fields, maxBytes int, names ...string) string {
	for _, name := range names {
		value, exists := data[name]
		if !exists || value == nil {
			continue
		}
		return sanitizeLiveLogValue(fmt.Sprint(value), maxBytes)
	}
	return ""
}

func safeLiveLogStatus(data log.Fields) int {
	for _, name := range []string{"status", "http_status", "status_code"} {
		value, exists := data[name]
		if !exists {
			continue
		}
		status, err := strconv.Atoi(strings.TrimSpace(fmt.Sprint(value)))
		if err == nil && status >= 100 && status <= 599 {
			return status
		}
	}
	return 0
}

func safeLiveLogBool(data log.Fields, name string) (bool, bool) {
	value, exists := data[name]
	if !exists {
		return false, false
	}
	switch typed := value.(type) {
	case bool:
		return typed, true
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(typed))
		return parsed, err == nil
	default:
		return false, false
	}
}

func sanitizeLiveLogValue(value string, maxBytes int) string {
	value = strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' || !unicode.IsControl(r) {
			return r
		}
		return -1
	}, strings.TrimSpace(value))
	value = liveLogURLPattern.ReplaceAllStringFunc(value, sanitizeLiveLogURL)
	value = liveLogPrivateKeyPattern.ReplaceAllString(value, "<redacted-private-key>")
	value = liveLogCookieHeader.ReplaceAllString(value, `${1}: <redacted>`)
	value = liveLogBearerPattern.ReplaceAllString(value, "Bearer <redacted-token>")
	value = liveLogAssignmentPattern.ReplaceAllString(value, `${1}${3}<redacted>`)
	value = liveLogJWTPattern.ReplaceAllString(value, "<redacted-token>")
	value = liveLogAPIKeyPattern.ReplaceAllString(value, "<redacted-key>")
	value = liveLogEmailPattern.ReplaceAllString(value, "<redacted-email>")
	if maxBytes > 0 && len(value) > maxBytes {
		value = value[:maxBytes] + "…"
	}
	return value
}

func sanitizeLiveLogURL(raw string) string {
	trimmed := strings.TrimRight(raw, ".,);]")
	suffix := raw[len(trimmed):]
	parsed, err := url.Parse(trimmed)
	if err != nil || parsed.Host == "" {
		return "<redacted-url>" + suffix
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String() + suffix
}

func sanitizeLiveLogPath(path string) string {
	if path == "" {
		return ""
	}
	if parsed, err := url.Parse(path); err == nil {
		path = parsed.Path
	}
	if index := strings.IndexByte(path, '?'); index >= 0 {
		path = path[:index]
	}
	return sanitizeLiveLogValue(path, liveLogMaxFieldValueBytes)
}

func sanitizeLiveLogHost(host string) string {
	host = strings.ToLower(strings.TrimSpace(host))
	if parsed, errParse := url.Parse(host); errParse == nil && parsed.IsAbs() {
		host = parsed.Hostname()
	}
	host = strings.TrimSuffix(host, ".")
	if !liveLogHostPattern.MatchString(host) {
		return ""
	}
	return host
}

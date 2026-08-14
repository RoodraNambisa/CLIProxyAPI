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

	"github.com/router-for-me/CLIProxyAPI/v6/internal/managementdiag"
	log "github.com/sirupsen/logrus"
)

const (
	liveLogCapacity             = 2000
	liveLogSubscriberBuffer     = 256
	liveLogMaxSubscribers       = 8
	liveLogMaxMessageBytes      = 2048
	liveLogMaxFieldValueBytes   = 256
	liveLogMaxResponseBodyBytes = 4096
)

var (
	globalLiveLogBroker = newLiveLogBroker()
	// ErrLiveLogsDisabled indicates that the in-memory stream is disabled.
	ErrLiveLogsDisabled = errors.New("live logs disabled")
	// ErrLiveLogConnectionLimit indicates that all subscriber slots are occupied.
	ErrLiveLogConnectionLimit = errors.New("live log connection limit reached")
	liveLogHostPattern        = regexp.MustCompile(`(?i)^[a-z0-9](?:[a-z0-9.-]{0,253}[a-z0-9])?$`)
)

// LiveLogEvent is a bounded representation of one log entry for authenticated management clients.
type LiveLogEvent struct {
	Cursor                uint64 `json:"cursor"`
	Timestamp             string `json:"timestamp"`
	Level                 string `json:"level"`
	Message               string `json:"message"`
	RequestID             string `json:"request_id,omitempty"`
	Provider              string `json:"provider,omitempty"`
	AuthIndex             string `json:"auth_index,omitempty"`
	Stage                 string `json:"stage,omitempty"`
	Code                  string `json:"code,omitempty"`
	Status                int    `json:"status,omitempty"`
	Method                string `json:"method,omitempty"`
	Path                  string `json:"path,omitempty"`
	Retryable             *bool  `json:"retryable,omitempty"`
	Persona               string `json:"persona,omitempty"`
	UAMajor               string `json:"ua_major,omitempty"`
	Platform              string `json:"platform,omitempty"`
	TargetHost            string `json:"target_host,omitempty"`
	TargetPath            string `json:"target_path,omitempty"`
	ResponseType          string `json:"response_type,omitempty"`
	ContentType           string `json:"content_type,omitempty"`
	CFRay                 string `json:"cf_ray,omitempty"`
	ResponseBytes         int64  `json:"response_bytes,omitempty"`
	ResponseBody          string `json:"response_body,omitempty"`
	ResponseBodyTruncated bool   `json:"response_body_truncated,omitempty"`
	Attempts              int    `json:"attempts,omitempty"`
	Cloudflare            bool   `json:"cloudflare,omitempty"`
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
	full    atomic.Bool

	mu             sync.Mutex
	configuration  uint64
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

// ConfigureManagementDiagnostics atomically updates live-log collection and detail policy.
func ConfigureManagementDiagnostics(enabled bool, detailLevel string) {
	globalLiveLogBroker.SetConfiguration(enabled, detailLevel)
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
	if broker.enabled.Load() != enabled {
		broker.configuration++
		broker.enabled.Store(enabled)
	}
	if enabled {
		return
	}
	for id, subscriber := range broker.subscribers {
		close(subscriber.frames)
		delete(broker.subscribers, id)
	}
}

// SetConfiguration updates collection and detail policy. Downgrading to safe
// clears retained full-detail entries and disconnects current subscribers.
func (broker *LiveLogBroker) SetConfiguration(enabled bool, detailLevel string) {
	if broker == nil {
		return
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()
	full := managementdiag.NormalizeDetailLevel(detailLevel) == managementdiag.DetailLevelFull
	downgraded := broker.full.Load() && !full
	if broker.enabled.Load() != enabled || broker.full.Load() != full {
		broker.configuration++
	}
	broker.full.Store(full)
	broker.enabled.Store(enabled)
	if downgraded {
		broker.ringStart = 0
		broker.ringCount = 0
		broker.ring = [liveLogCapacity]LiveLogEvent{}
	}
	if enabled && !downgraded {
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
	broker.mu.Lock()
	if !broker.enabled.Load() {
		broker.mu.Unlock()
		return nil
	}
	detailLevel := managementdiag.DetailLevelSafe
	if broker.full.Load() {
		detailLevel = managementdiag.DetailLevelFull
	}
	configuration := broker.configuration
	broker.mu.Unlock()

	event := managementLiveLogEvent(entry, detailLevel)
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if !broker.enabled.Load() || broker.configuration != configuration {
		return nil
	}
	broker.publishLocked(event)
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
	broker.publishLocked(event)
}

func (broker *LiveLogBroker) publishLocked(event LiveLogEvent) {
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
		haystack := strings.ToLower(strings.Join([]string{event.Message, event.RequestID, event.Provider, event.AuthIndex, event.Stage, event.Code, event.Method, event.Path, event.ResponseBody}, " "))
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

func managementLiveLogEvent(entry *log.Entry, detailLevel string) LiveLogEvent {
	event := LiveLogEvent{
		Timestamp: entry.Time.UTC().Format(time.RFC3339Nano),
		Level:     entry.Level.String(),
		Message:   sanitizeLiveLogValue(entry.Message, detailLevel, liveLogMaxMessageBytes),
	}
	event.RequestID = safeLiveLogField(entry.Data, detailLevel, liveLogMaxFieldValueBytes, "request_id")
	event.Provider = safeLiveLogField(entry.Data, detailLevel, liveLogMaxFieldValueBytes, "provider")
	event.AuthIndex = safeLiveLogField(entry.Data, detailLevel, liveLogMaxFieldValueBytes, "auth_index", "authIndex")
	event.Stage = safeLiveLogField(entry.Data, detailLevel, liveLogMaxFieldValueBytes, "stage", "failure_stage")
	event.Code = safeLiveLogField(entry.Data, detailLevel, liveLogMaxFieldValueBytes, "code", "error_code")
	event.Status = safeLiveLogStatus(entry.Data)
	event.Method = strings.ToUpper(safeLiveLogField(entry.Data, detailLevel, 16, "method"))
	event.Path = sanitizeLiveLogPath(safeLiveLogField(entry.Data, detailLevel, liveLogMaxFieldValueBytes, "path"), detailLevel)
	event.Persona = safeLiveLogField(entry.Data, detailLevel, 64, "persona")
	event.UAMajor = safeLiveLogField(entry.Data, detailLevel, 16, "ua_major")
	event.Platform = safeLiveLogField(entry.Data, detailLevel, 64, "platform")
	event.TargetHost = sanitizeLiveLogHost(safeLiveLogField(entry.Data, detailLevel, 255, "target_host"))
	event.TargetPath = sanitizeLiveLogPath(safeLiveLogField(entry.Data, detailLevel, liveLogMaxFieldValueBytes, "target_path"), detailLevel)
	event.ResponseType = safeLiveLogField(entry.Data, detailLevel, 32, "response_type")
	event.ContentType = safeLiveLogField(entry.Data, detailLevel, 128, "content_type")
	event.CFRay = safeLiveLogField(entry.Data, detailLevel, 128, "cf_ray")
	event.ResponseBytes = safeLiveLogInt64(entry.Data, "response_bytes")
	if event.Status >= 400 || event.Code != "" {
		event.ResponseBody, event.ResponseBodyTruncated = boundedLiveLogResponseBody(entry.Data, detailLevel)
	}
	event.Attempts = safeLiveLogInt(entry.Data, 100, "attempts", "flow_attempt", "request_attempt")
	if value, ok := safeLiveLogBool(entry.Data, "cloudflare"); ok {
		event.Cloudflare = value
	}
	if value, ok := safeLiveLogBool(entry.Data, "retryable"); ok {
		event.Retryable = &value
	}
	return event
}

func boundedLiveLogResponseBody(data log.Fields, detailLevel string) (string, bool) {
	value, exists := data["response_body"]
	if !exists || value == nil {
		return "", false
	}
	var raw string
	switch typed := value.(type) {
	case string:
		raw = typed
	case []byte:
		raw = string(typed)
	case managementdiag.ManagementOnlyValue:
		raw = typed.Value()
	default:
		raw = fmt.Sprint(typed)
	}
	processed, truncated := managementdiag.ProcessResponseBody(raw, detailLevel, liveLogMaxResponseBodyBytes)
	alreadyTruncated, _ := safeLiveLogBool(data, "response_body_truncated")
	return processed, truncated || alreadyTruncated
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

func safeLiveLogField(data log.Fields, detailLevel string, maxBytes int, names ...string) string {
	for _, name := range names {
		value, exists := data[name]
		if !exists || value == nil {
			continue
		}
		raw := fmt.Sprint(value)
		if managementValue, ok := value.(managementdiag.ManagementOnlyValue); ok {
			raw = managementValue.Value()
		}
		return sanitizeLiveLogValue(raw, detailLevel, maxBytes)
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

func sanitizeLiveLogValue(value, detailLevel string, maxBytes int) string {
	processed, truncated := managementdiag.ProcessText(value, detailLevel, maxBytes)
	if truncated {
		return processed + "…"
	}
	return processed
}

func sanitizeLiveLogPath(path, detailLevel string) string {
	if path == "" {
		return ""
	}
	if parsed, errParse := url.Parse(path); errParse == nil && parsed.IsAbs() {
		processed := managementdiag.ProcessURL(path, detailLevel)
		if parsedProcessed, errProcessed := url.Parse(processed); errProcessed == nil {
			path = parsedProcessed.EscapedPath()
			if parsedProcessed.RawQuery != "" {
				path += "?" + parsedProcessed.RawQuery
			}
		}
	} else if strings.HasPrefix(path, "/") {
		processed := managementdiag.ProcessURL("https://management.invalid"+path, detailLevel)
		path = strings.TrimPrefix(processed, "https://management.invalid")
	}
	return sanitizeLiveLogValue(path, detailLevel, liveLogMaxFieldValueBytes)
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

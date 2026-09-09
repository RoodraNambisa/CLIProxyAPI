package helps

import (
	"sync"
	"time"
)

type usageResponseTiming struct {
	mu          sync.RWMutex
	start       time.Time
	firstPacket time.Duration
	firstToken  time.Duration
	packetSet   bool
	tokenSet    bool
}

// StartResponseTTFT starts timing once, immediately before the upstream attempt.
func (r *UsageReporter) StartResponseTTFT() {
	if r == nil {
		return
	}
	r.timing.mu.Lock()
	if r.timing.start.IsZero() {
		r.timing.start = time.Now()
	}
	r.timing.mu.Unlock()
}

func (r *UsageReporter) IsTTFTSet() bool {
	if r == nil {
		return false
	}
	r.timing.mu.RLock()
	defer r.timing.mu.RUnlock()
	return r.timing.tokenSet
}

// MarkFirstResponseByte observes transport data without declaring a model token.
func (r *UsageReporter) MarkFirstResponseByte() { r.ObserveTokenEvent(false) }

func (r *UsageReporter) RecordFirstPacket() { r.ObserveTokenEvent(false) }

// ObserveTokenEvent records a packet independently of the first substantive
// model content. A metadata-only or failed turn retains a zero TTFT.
func (r *UsageReporter) ObserveTokenEvent(isToken bool) {
	if r == nil {
		return
	}
	r.timing.mu.RLock()
	skip := r.timing.start.IsZero() || r.timing.tokenSet || (!isToken && r.timing.packetSet)
	r.timing.mu.RUnlock()
	if skip {
		return
	}
	r.timing.mu.Lock()
	defer r.timing.mu.Unlock()
	if r.timing.tokenSet || (!isToken && r.timing.packetSet) {
		return
	}
	elapsed := max(time.Duration(0), time.Since(r.timing.start))
	if !r.timing.packetSet {
		r.timing.firstPacket = elapsed
		r.timing.packetSet = true
	}
	if isToken {
		r.timing.firstToken = elapsed
		r.timing.tokenSet = true
	}
}

func (r *UsageReporter) responseTimings() (time.Duration, time.Duration) {
	if r == nil {
		return 0, 0
	}
	r.timing.mu.RLock()
	defer r.timing.mu.RUnlock()
	return r.timing.firstToken, r.timing.firstPacket
}

// ObserveResponsesTokenEvent does not retain payloads and stops parsing once
// the first model-content timestamp has been recorded.
func ObserveResponsesTokenEvent(reporter *UsageReporter, payload []byte) {
	if reporter == nil || len(payload) == 0 || reporter.IsTTFTSet() {
		return
	}
	reporter.ObserveTokenEvent(IsResponsesTokenEvent(payload))
}

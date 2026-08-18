package auth

import (
	"strings"
	"sync/atomic"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
)

const (
	ChatGPTWebRequestRefreshOutcomeSameToken         = "same_token"
	ChatGPTWebRequestRefreshOutcomeProbeSucceeded    = "probe_succeeded"
	ChatGPTWebRequestRefreshOutcomeProbeUnauthorized = "probe_unauthorized"
	ChatGPTWebRequestRefreshOutcomeProbeTransient    = "probe_transient"
)

type chatGPTWebRequestRefreshMetrics struct {
	received          atomic.Uint64
	queued            atomic.Int64
	running           atomic.Int64
	deduplicated      atomic.Uint64
	succeeded         atomic.Uint64
	failed            atomic.Uint64
	backpressured     atomic.Uint64
	noStart           atomic.Uint64
	sameToken         atomic.Uint64
	probeSucceeded    atomic.Uint64
	probeUnauthorized atomic.Uint64
	probeTransient    atomic.Uint64
	deadConfirmed     atomic.Uint64
}

func (metrics *chatGPTWebRequestRefreshMetrics) observeOutcome(outcome string) {
	if metrics == nil {
		return
	}
	switch outcome {
	case ChatGPTWebRequestRefreshOutcomeSameToken:
		metrics.sameToken.Add(1)
	case ChatGPTWebRequestRefreshOutcomeProbeSucceeded:
		metrics.probeSucceeded.Add(1)
	case ChatGPTWebRequestRefreshOutcomeProbeUnauthorized:
		metrics.probeUnauthorized.Add(1)
	case ChatGPTWebRequestRefreshOutcomeProbeTransient:
		metrics.probeTransient.Add(1)
	}
}

func (m *Manager) observeChatGPTWebRequestRefreshToken(failedAccessToken string, refreshed *Auth) {
	if m == nil || strings.TrimSpace(failedAccessToken) == "" || authAccessToken(refreshed) != failedAccessToken {
		return
	}
	m.chatGPTWebRequestRefreshMetrics.observeOutcome(ChatGPTWebRequestRefreshOutcomeSameToken)
}

// ChatGPTWebRequestRefreshSnapshot returns aggregate request-triggered refresh
// activity without exposing auth IDs or credential material.
func (m *Manager) ChatGPTWebRequestRefreshSnapshot() chatgptwebauth.RequestRefreshRuntimeSnapshot {
	if m == nil {
		return chatgptwebauth.RequestRefreshRuntimeSnapshot{}
	}
	snapshot := chatgptwebauth.RequestRefreshRuntimeSnapshot{
		Received:          m.chatGPTWebRequestRefreshMetrics.received.Load(),
		Queued:            max(0, m.chatGPTWebRequestRefreshMetrics.queued.Load()),
		Running:           max(0, m.chatGPTWebRequestRefreshMetrics.running.Load()),
		Deduplicated:      m.chatGPTWebRequestRefreshMetrics.deduplicated.Load(),
		Succeeded:         m.chatGPTWebRequestRefreshMetrics.succeeded.Load(),
		Failed:            m.chatGPTWebRequestRefreshMetrics.failed.Load(),
		Backpressured:     m.chatGPTWebRequestRefreshMetrics.backpressured.Load(),
		NoStart:           m.chatGPTWebRequestRefreshMetrics.noStart.Load(),
		SameToken:         m.chatGPTWebRequestRefreshMetrics.sameToken.Load(),
		ProbeSucceeded:    m.chatGPTWebRequestRefreshMetrics.probeSucceeded.Load(),
		ProbeUnauthorized: m.chatGPTWebRequestRefreshMetrics.probeUnauthorized.Load(),
		ProbeTransient:    m.chatGPTWebRequestRefreshMetrics.probeTransient.Load(),
		DeadConfirmed:     m.chatGPTWebRequestRefreshMetrics.deadConfirmed.Load(),
	}
	if m.scheduler != nil {
		m.scheduler.mu.RLock()
		for _, count := range m.scheduler.requestRefreshBlocks {
			if count > 0 {
				snapshot.SchedulerBlocked++
			}
		}
		m.scheduler.mu.RUnlock()
	}
	return snapshot
}

package live

import (
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestMediaRuntimeUsesImmutableConfigurationAndSharedCapacity(t *testing.T) {
	cfg := &config.Config{}
	h := NewHandler(cfg, nil)
	defer h.Close()
	if h.runtime.Load().media != nil || h.runtime.Load().mediaError != nil {
		t.Fatal("default configuration initialized media")
	}
	cfg.Codex.LiveMediaRelay = config.CodexLiveMediaRelayConfig{Enabled: true, MaxSessions: 1, ICEServers: []config.CodexLiveICEServer{{URLs: []string{"stun:fixture.invalid:3478"}}}}
	cfg.ProxyURL = "http://proxy-one.invalid:8080"
	h.UpdateConfig(cfg)
	old := h.runtime.Load()
	if old.media == nil || old.mediaError != nil {
		t.Fatal("media runtime was not configured")
	}
	release, ok := old.media.limiter.acquire(old.media.maxSessions)
	if !ok {
		t.Fatal("media capacity was unavailable")
	}
	defer release()
	cfg.Codex.LiveMediaRelay.ICEServers[0].URLs[0] = "stun:changed.invalid:3478"
	cfg.ProxyURL = "direct"
	h.UpdateConfig(cfg)
	current := h.runtime.Load()
	if old.mediaProxyURL != "http://proxy-one.invalid:8080" || old.media.configuration.ICEServers[0].URLs[0] != "stun:fixture.invalid:3478" {
		t.Fatal("old media runtime changed after publication")
	}
	if current.media == old.media || current.media.limiter != old.media.limiter || current.mediaProxyURL != "direct" {
		t.Fatal("new media runtime lost its snapshot or shared capacity")
	}
	if release, ok := current.media.limiter.acquire(1); ok {
		release()
		t.Fatal("hot update reset occupied capacity")
	}
	cfg.Codex.LiveMediaRelay.MaxSessions = -1
	h.UpdateConfig(cfg)
	if h.runtime.Load().media != nil || h.runtime.Load().mediaError == nil {
		t.Fatal("invalid SDK configuration silently enabled direct passthrough")
	}
	cfg.Codex.LiveMediaRelay.Enabled = false
	h.UpdateConfig(cfg)
	if h.runtime.Load().media != nil || h.runtime.Load().mediaError != nil {
		t.Fatal("disabled media retained an active factory")
	}
	if old.media.maxSessions != 1 {
		t.Fatal("hot disable changed existing media settings")
	}
}

func TestMediaLimiterShutdownWaitsForUnregisteredSessions(t *testing.T) {
	limiter := &mediaSessionLimiter{}
	release, ok := limiter.acquire(1)
	if !ok {
		t.Fatal("fixture capacity unavailable")
	}
	defer release()
	finished := make(chan struct{})
	go func() { limiter.close(); close(finished) }()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		limiter.mu.Lock()
		closed := limiter.closed
		limiter.mu.Unlock()
		if closed {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("limiter did not begin shutdown")
		}
	}
	if release, ok := limiter.acquire(32); ok {
		release()
		t.Fatal("shutdown accepted a new media session")
	}
	select {
	case <-finished:
		t.Fatal("shutdown returned before session release")
	default:
	}
	release()
	receiveMediaValue(t, finished)
	limiter.close()
}

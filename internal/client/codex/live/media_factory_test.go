package live

import (
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pion/webrtc/v4"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestMediaFactoryKeepsConfigurationAndOpusSnapshot(t *testing.T) {
	cfg := config.CodexLiveMediaRelayConfig{Enabled: true, ICEServers: []config.CodexLiveICEServer{{URLs: []string{"turn:fixture.invalid"}, Username: "fixture-user", Credential: "fixture-secret"}}}
	factory, err := newPionMediaRelay(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ICEServers[0].URLs[0] = "stun:changed.invalid"
	if factory.maxSessions != 32 || factory.configuration.ICEServers[0].URLs[0] != "turn:fixture.invalid" || factory.configuration.ICEServers[0].Credential != "fixture-secret" {
		t.Fatal("media factory lost its immutable configuration")
	}
	for _, api := range []*webrtc.API{factory.downstreamAPI, factory.upstreamAPI, factory.proxyUpstreamAPI} {
		peer, errPeer := api.NewPeerConnection(webrtc.Configuration{})
		if errPeer != nil {
			t.Fatal(errPeer)
		}
		t.Cleanup(func() {
			if errClose := peer.Close(); errClose != nil {
				t.Error(errClose)
			}
		})
		if _, errTrack := peer.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio); errTrack != nil {
			t.Fatal(errTrack)
		}
		if _, errChannel := peer.CreateDataChannel(realtimeDataChannelLabel, nil); errChannel != nil {
			t.Fatal(errChannel)
		}
		offer, errOffer := peer.CreateOffer(nil)
		if errOffer != nil {
			t.Fatal(errOffer)
		}
		if !strings.Contains(offer.SDP, "opus/48000/2") || strings.Contains(offer.SDP, "m=video") {
			t.Fatal("native media capabilities changed")
		}
	}
	if _, err := newPionMediaRelay(config.CodexLiveMediaRelayConfig{MaxSessions: -1}, nil); err == nil {
		t.Fatal("invalid media settings accepted")
	}
}

func TestMediaLimiterSharesCapacityAcrossSnapshots(t *testing.T) {
	limiter := &mediaSessionLimiter{}
	old, err := newPionMediaRelay(config.CodexLiveMediaRelayConfig{MaxSessions: 2}, limiter)
	if err != nil {
		t.Fatal(err)
	}
	newer, err := newPionMediaRelay(config.CodexLiveMediaRelayConfig{MaxSessions: 1}, limiter)
	if err != nil {
		t.Fatal(err)
	}
	release, ok := old.limiter.acquire(old.maxSessions)
	if !ok {
		t.Fatal("first media session rejected")
	}
	if _, ok := newer.limiter.acquire(newer.maxSessions); ok {
		t.Fatal("new snapshot ignored active sessions")
	}
	release()
	release()
	release, ok = newer.limiter.acquire(newer.maxSessions)
	if !ok {
		t.Fatal("closed media session leaked a slot")
	}
	release()
	var accepted atomic.Int32
	var workers, attempted sync.WaitGroup
	start, finish := make(chan struct{}), make(chan struct{})
	attempted.Add(64)
	for range 64 {
		workers.Go(func() {
			<-start
			release, ok := limiter.acquire(8)
			if ok {
				accepted.Add(1)
			}
			attempted.Done()
			if ok {
				<-finish
				release()
				release()
			}
		})
	}
	close(start)
	// Wait for every worker to attempt admission before releasing the winners.
	// A separate barrier prevents a fast release from creating another wave.
	attempted.Wait()
	count := accepted.Load()
	close(finish)
	workers.Wait()
	if count != 8 || limiter.active != 0 {
		t.Fatal("media limiter admission or release was not exact")
	}
}

func TestMediaPrivateCandidateFilter(t *testing.T) {
	for _, raw := range []string{"", "0.0.0.0", "127.0.0.1", "10.0.0.1", "172.16.1.2", "192.168.1.1", "169.254.1.1", "224.0.0.1", "::", "::1", "fd00::1", "fe80::1", "ff02::1"} {
		if isPublicMediaRemoteIP(net.ParseIP(raw)) {
			t.Fatal("private or invalid media candidate admitted")
		}
	}
	for _, raw := range []string{"8.8.8.8", "2606:4700:4700::1111"} {
		if !isPublicMediaRemoteIP(net.ParseIP(raw)) {
			t.Fatal("public media candidate rejected")
		}
	}
}

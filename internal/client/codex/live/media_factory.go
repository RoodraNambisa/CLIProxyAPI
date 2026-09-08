package live

import (
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/pion/interceptor"
	"github.com/pion/webrtc/v4"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

const realtimeDataChannelLabel = "oai-events"

var mediaOpusCodec = webrtc.RTPCodecCapability{
	MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2,
	SDPFmtpLine: "minptime=10;useinbandfec=1",
}

// One limiter is shared across configuration revisions. Each new session uses
// its own admission limit, and releases exactly one slot when it closes.
type mediaSessionLimiter struct {
	mu       sync.Mutex
	active   int
	closed   bool
	sessions sync.WaitGroup
}

func (l *mediaSessionLimiter) acquire(limit int) (func(), bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || limit <= 0 || l.active >= limit {
		return nil, false
	}
	l.active++
	l.sessions.Add(1)
	var once sync.Once
	return func() { once.Do(func() { l.mu.Lock(); l.active--; l.mu.Unlock(); l.sessions.Done() }) }, true
}

// Stop acquisition before waiting, including setups not yet in the call store.
// Session owners must cancel their lifetimes before calling close.
func (l *mediaSessionLimiter) close() {
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
	l.sessions.Wait()
}

type pionMediaRelay struct {
	downstreamAPI    *webrtc.API
	upstreamAPI      *webrtc.API
	proxyUpstreamAPI *webrtc.API
	configuration    webrtc.Configuration
	limiter          *mediaSessionLimiter
	maxSessions      int
}

// Construction validates settings and creates no listeners or upstream calls.
// The caller publishes the immutable factory for subsequent session creation.
func newPionMediaRelay(cfg config.CodexLiveMediaRelayConfig, limiter *mediaSessionLimiter) (*pionMediaRelay, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	downstream, errDownstream := newMediaAPI(cfg, cfg.DisablePrivateRemoteIPs, false)
	if errDownstream != nil {
		return nil, errDownstream
	}
	upstream, errUpstream := newMediaAPI(cfg, false, false)
	if errUpstream != nil {
		return nil, errUpstream
	}
	proxied, errProxied := newMediaAPI(cfg, false, true)
	if errProxied != nil {
		return nil, errProxied
	}
	servers := make([]webrtc.ICEServer, 0, len(cfg.ICEServers))
	for _, server := range cfg.ICEServers {
		urls := make([]string, len(server.URLs))
		for i, raw := range server.URLs {
			urls[i] = strings.TrimSpace(raw)
		}
		servers = append(servers, webrtc.ICEServer{URLs: urls, Username: server.Username, Credential: server.Credential, CredentialType: webrtc.ICECredentialTypePassword})
	}
	if limiter == nil {
		limiter = &mediaSessionLimiter{}
	}
	return &pionMediaRelay{downstreamAPI: downstream, upstreamAPI: upstream, proxyUpstreamAPI: proxied,
		configuration: webrtc.Configuration{ICEServers: servers}, limiter: limiter, maxSessions: cfg.EffectiveMaxSessions()}, nil
}

func newMediaAPI(cfg config.CodexLiveMediaRelayConfig, filterPrivate, loopbackOnly bool) (*webrtc.API, error) {
	engine := &webrtc.MediaEngine{}
	if err := engine.RegisterCodec(webrtc.RTPCodecParameters{RTPCodecCapability: mediaOpusCodec, PayloadType: 111}, webrtc.RTPCodecTypeAudio); err != nil {
		return nil, fmt.Errorf("register realtime Opus codec: %w", err)
	}
	interceptors := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(engine, interceptors); err != nil {
		return nil, fmt.Errorf("register realtime interceptors: %w", err)
	}
	settings := webrtc.SettingEngine{}
	if loopbackOnly {
		settings.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4, webrtc.NetworkTypeUDP6, webrtc.NetworkTypeTCP4, webrtc.NetworkTypeTCP6})
		settings.SetIncludeLoopbackCandidate(true)
		settings.SetIPFilter(func(ip net.IP) bool { return ip != nil && ip.IsLoopback() })
	} else {
		if cfg.UDPPortMin != 0 {
			if err := settings.SetEphemeralUDPPortRange(cfg.UDPPortMin, cfg.UDPPortMax); err != nil {
				return nil, fmt.Errorf("configure realtime UDP range: %w", err)
			}
		}
		if ip := strings.TrimSpace(cfg.PublicIP); ip != "" {
			settings.SetNAT1To1IPs([]string{ip}, webrtc.ICECandidateTypeHost)
		}
	}
	if filterPrivate {
		settings.SetRemoteIPFilter(isPublicMediaRemoteIP)
	}
	return webrtc.NewAPI(webrtc.WithMediaEngine(engine), webrtc.WithInterceptorRegistry(interceptors), webrtc.WithSettingEngine(settings)), nil
}

func isPublicMediaRemoteIP(ip net.IP) bool {
	return ip != nil && !ip.IsUnspecified() && !ip.IsLoopback() && !ip.IsPrivate() && !ip.IsLinkLocalUnicast() && !ip.IsLinkLocalMulticast() && !ip.IsMulticast()
}

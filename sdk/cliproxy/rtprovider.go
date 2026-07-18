package cliproxy

import (
	"crypto/sha256"
	"net/http"
	"strings"
	"sync"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
	log "github.com/sirupsen/logrus"
)

// defaultRoundTripperProvider returns a per-auth HTTP RoundTripper based on
// the auth's effective proxy URL. Auth-scoped transports are released when
// the corresponding auth leaves the runtime manager.
type defaultRoundTripperProvider struct {
	mu             sync.RWMutex
	cache          map[string]cachedAuthRoundTripper
	generation     uint64
	buildTransport func(string) (*http.Transport, proxyutil.Mode, error)
}

type cachedAuthRoundTripper struct {
	identity  string
	transport *http.Transport
}

func newDefaultRoundTripperProvider() *defaultRoundTripperProvider {
	return &defaultRoundTripperProvider{
		cache:          make(map[string]cachedAuthRoundTripper),
		buildTransport: proxyutil.BuildHTTPTransport,
	}
}

// RoundTripperFor implements coreauth.RoundTripperProvider.
func (p *defaultRoundTripperProvider) RoundTripperFor(auth *coreauth.Auth) http.RoundTripper {
	if auth == nil {
		return nil
	}
	proxyStr := auth.EffectiveProxyURL()
	if proxyStr == "" {
		return nil
	}
	cacheKey, identity := roundTripperCacheIdentity(auth, proxyStr)
	p.mu.RLock()
	cached, exists := p.cache[cacheKey]
	generation := p.generation
	buildTransport := p.buildTransport
	p.mu.RUnlock()
	if exists && cached.identity == identity && cached.transport != nil {
		return cached.transport
	}
	if buildTransport == nil {
		buildTransport = proxyutil.BuildHTTPTransport
	}
	transport, _, errBuild := buildTransport(proxyStr)
	if errBuild != nil {
		log.Errorf("%v", errBuild)
		return nil
	}
	if transport == nil {
		return nil
	}
	p.mu.Lock()
	if p.generation != generation {
		p.mu.Unlock()
		transport.DisableKeepAlives = true
		transport.CloseIdleConnections()
		return transport
	}
	if current, ok := p.cache[cacheKey]; ok && current.identity == identity && current.transport != nil {
		p.mu.Unlock()
		transport.CloseIdleConnections()
		return current.transport
	}
	if previous, ok := p.cache[cacheKey]; ok && previous.transport != nil {
		previous.transport.CloseIdleConnections()
	}
	p.cache[cacheKey] = cachedAuthRoundTripper{identity: identity, transport: transport}
	p.mu.Unlock()
	return transport
}

// EvictAuth releases the transport cached for one auth entry.
func (p *defaultRoundTripperProvider) EvictAuth(authID string) {
	if p == nil {
		return
	}
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return
	}
	cacheKey := "auth:" + authID
	p.mu.Lock()
	p.generation++
	cached, ok := p.cache[cacheKey]
	if ok {
		delete(p.cache, cacheKey)
	}
	p.mu.Unlock()
	if ok && cached.transport != nil {
		cached.transport.CloseIdleConnections()
	}
}

// CloseIdleConnections releases all transports owned by the provider.
func (p *defaultRoundTripperProvider) CloseIdleConnections() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.generation++
	transports := make([]*http.Transport, 0, len(p.cache))
	for key, cached := range p.cache {
		if cached.transport != nil {
			transports = append(transports, cached.transport)
		}
		delete(p.cache, key)
	}
	p.mu.Unlock()
	for _, transport := range transports {
		transport.CloseIdleConnections()
	}
}

func roundTripperCacheIdentity(auth *coreauth.Auth, proxyURL string) (string, string) {
	digest := sha256.Sum256([]byte(strings.TrimSpace(proxyURL)))
	proxyIdentity := string(digest[:])
	bindingID := ""
	authID := ""
	if auth != nil {
		bindingID = auth.EffectiveProxyBindingID()
		authID = strings.TrimSpace(auth.ID)
	}
	identity := bindingID + "\x00" + proxyIdentity
	if authID != "" && (bindingID != "" || strings.TrimSpace(auth.ProxyURL) != "") {
		return "auth:" + authID, identity
	}
	return "proxy:" + proxyIdentity, identity
}

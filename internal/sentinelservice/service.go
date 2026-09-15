// Package sentinelservice mounts isolated Sentinel computation on the API listener.
package sentinelservice

import (
	"context"
	"errors"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelconfig"
)

type Snapshot struct {
	Running         bool                                     `json:"running"`
	Address         string                                   `json:"address"`
	AccessPath      string                                   `json:"access_path"`
	RestartRequired bool                                     `json:"restart_required"`
	Runtime         chatgptweb.SentinelComputeServerSnapshot `json:"runtime"`
}

type Service struct {
	mu       sync.Mutex
	cfg      sentinelconfig.Server
	path     string
	node     *chatgptweb.SentinelComputeServer
	address  string
	started  bool
	stopping bool
	closed   bool
	stopOnce sync.Once
}

func New(cfg sentinelconfig.Server) (*Service, error) {
	node, err := chatgptweb.NewSentinelComputeServer(cfg)
	if err != nil {
		return nil, err
	}
	return &Service{cfg: cfg, path: cfg.Path(), node: node}, nil
}

// Start records the parent listener; no additional socket or TLS configuration is used.
func (s *Service) Start(address string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("Sentinel service is closed")
	}
	s.address, s.started = address, true
	return nil
}

// Handler dispatches before proxy middleware so challenge bodies, keys and
// private access paths never enter normal proxy request logs or authentication.
func (s *Service) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		prefix, configured, enabled := s.path, s.cfg.Path(), s.started && s.cfg.Enabled && !s.closed
		stopping := s.stopping
		s.mu.Unlock()
		matches := func(value, base string) bool { return value == base || strings.HasPrefix(value, base+"/") }
		if !matches(r.URL.Path, prefix) && !matches(r.URL.EscapedPath(), prefix) && !matches(r.URL.Path, configured) && !matches(r.URL.EscapedPath(), configured) {
			if stopping {
				w.Header().Set("Cache-Control", "no-store")
				http.Error(w, "service is shutting down", http.StatusServiceUnavailable)
				return
			}
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		if !enabled || !matches(r.URL.Path, prefix) || r.URL.RawPath != "" || r.URL.RawQuery != "" || r.URL.ForceQuery || path.Clean(r.URL.Path) != r.URL.Path {
			http.NotFound(w, r)
			return
		}
		forward := r.Clone(r.Context())
		forward.URL.Path = sentinelconfig.DefaultAccessPath + strings.TrimPrefix(r.URL.Path, prefix)
		forward.RequestURI = forward.URL.RequestURI()
		s.node.ServeHTTP(w, forward)
	})
}

func (s *Service) UpdateConfig(cfg sentinelconfig.Server) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("Sentinel service is closed")
	}
	if err := s.node.UpdateConfig(cfg); err != nil {
		return err
	}
	s.cfg = cfg
	return nil
}

func (s *Service) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Snapshot{Running: s.started && s.cfg.Enabled && !s.closed, Address: s.address, AccessPath: s.path, RestartRequired: s.cfg.Path() != s.path, Runtime: s.node.Snapshot()}
}

func (s *Service) Stop(ctx context.Context) error {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		grace := s.cfg.Limits().DrainTimeoutSeconds
		s.stopping = true
		s.mu.Unlock()
		// Keep existing session routes reachable while the compute server drains.
		drainCtx, cancel := context.WithTimeout(ctx, time.Duration(grace)*time.Second)
		defer cancel()
		s.node.Drain(drainCtx)
		s.node.Close()
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
	})
	return nil
}

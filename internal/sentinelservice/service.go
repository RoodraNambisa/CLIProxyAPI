// Package sentinelservice owns the independent Sentinel computation listener.
package sentinelservice

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelconfig"
)

type Snapshot struct {
	Running         bool                                     `json:"running"`
	Address         string                                   `json:"address"`
	RestartRequired bool                                     `json:"restart_required"`
	LastError       string                                   `json:"last_error"`
	Runtime         chatgptweb.SentinelComputeServerSnapshot `json:"runtime"`
}

type Service struct {
	mu        sync.Mutex
	cfg       sentinelconfig.Server
	bound     sentinelconfig.Server
	node      *chatgptweb.SentinelComputeServer
	server    *http.Server
	address   string
	running   bool
	closed    bool
	lastError string
}

func New(cfg sentinelconfig.Server) (*Service, error) {
	node, err := chatgptweb.NewSentinelComputeServer(cfg)
	if err != nil {
		return nil, err
	}
	return &Service{cfg: cfg, node: node}, nil
}
func (s *Service) Start() error { s.mu.Lock(); defer s.mu.Unlock(); return s.startLocked() }
func (s *Service) startLocked() error {
	if s.closed {
		return errors.New("Sentinel service is closed")
	}
	if !s.cfg.Enabled || s.running {
		return nil
	}
	if s.cfg.TLS.Enable {
		if _, err := tls.LoadX509KeyPair(s.cfg.TLS.Cert, s.cfg.TLS.Key); err != nil {
			return errors.New("invalid Sentinel TLS certificate")
		}
	}
	listener, err := net.Listen("tcp", s.cfg.Address())
	if err != nil {
		return errors.New("cannot bind Sentinel solver listener")
	}
	server := &http.Server{Handler: s.node, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 16 << 10}
	s.server = server
	s.address = listener.Addr().String()
	s.running = true
	s.bound = s.cfg
	cfg := s.cfg
	go func() {
		var errServe error
		if cfg.TLS.Enable {
			errServe = server.ServeTLS(listener, cfg.TLS.Cert, cfg.TLS.Key)
		} else {
			errServe = server.Serve(listener)
		}
		s.mu.Lock()
		s.running = false
		if errServe != nil && !errors.Is(errServe, http.ErrServerClosed) {
			s.lastError = "solver listener stopped"
		}
		s.mu.Unlock()
	}()
	return nil
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
	previous := s.cfg
	s.cfg = cfg
	if err := s.startLocked(); err != nil {
		s.cfg = previous
		return err
	}
	if err := s.node.UpdateConfig(cfg); err != nil {
		s.cfg = previous
		return err
	}
	return nil
}
func (s *Service) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Snapshot{Running: s.running, Address: s.address, RestartRequired: s.server != nil && (s.cfg.Address() != s.bound.Address() || s.cfg.TLS != s.bound.TLS), LastError: s.lastError, Runtime: s.node.Snapshot()}
}
func (s *Service) Stop(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	server := s.server
	grace := s.cfg.Limits().DrainTimeoutSeconds
	s.mu.Unlock()
	drainCtx, cancel := context.WithTimeout(ctx, time.Duration(grace)*time.Second)
	defer cancel()
	s.node.Drain(drainCtx)
	s.node.Close()
	if server != nil {
		if err := server.Shutdown(ctx); err != nil {
			_ = server.Close()
			return err
		}
	}
	return nil
}

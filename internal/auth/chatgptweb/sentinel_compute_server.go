package chatgptweb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelcompat"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelconfig"
)

// A retained session reserves two VM heaps plus input/result and SDK headroom.
const computeSessionReservation int64 = 128 << 20

// JSON decoding/encoding may retain several copies of the bounded wire payload.
const computeRPCReservation int64 = 3 * sentinelComputeMaxBody

type computeGeneration struct {
	manager *SentinelRuntimeManager
	policy  *sentinelcompat.Policy
	rules   sentinelcompat.Config
	sdk     bool
	refs    int
	retired bool
}

type computeCachedOperation struct {
	hash     [32]byte
	response sentinelComputeResponse
	status   int
}

type computeServerSession struct {
	id         string
	owner      [32]byte
	lock       chan struct{}
	cancel     context.CancelFunc
	state      *computeState
	generation *computeGeneration
	lastUsed   time.Time
	lease      time.Duration
	cache      map[string]computeCachedOperation
	phase      int
	closing    bool
}

type SentinelComputeServerSnapshot struct {
	Enabled       bool                    `json:"enabled"`
	Draining      bool                    `json:"draining"`
	Instance      string                  `json:"instance"`
	Protocol      int                     `json:"protocol"`
	Active        int                     `json:"active"`
	Queued        int                     `json:"queued"`
	Sessions      int                     `json:"sessions"`
	ReservedBytes int64                   `json:"reserved_bytes"`
	GoSuccess     uint64                  `json:"go_success"`
	SDKSuccess    uint64                  `json:"sdk_success"`
	Failures      uint64                  `json:"failures"`
	RulesHash     string                  `json:"rules_hash"`
	Limits        sentinelconfig.Limits   `json:"limits"`
	SDK           SentinelRuntimeSnapshot `json:"sdk"`
}

// SentinelComputeServer is an isolated, credential-free, local-only RPC handler.
type SentinelComputeServer struct {
	mu                              sync.Mutex
	cfg                             sentinelconfig.Server
	instance                        string
	current                         *computeGeneration
	sessions                        map[string]*computeServerSession
	active, queued                  int
	reserved                        int64
	changed                         chan struct{}
	draining                        bool
	closed                          bool
	goSuccess, sdkSuccess, failures uint64
	cancel                          context.CancelFunc
	handler                         http.Handler
}

func NewSentinelComputeServer(cfg sentinelconfig.Server) (*SentinelComputeServer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	s := &SentinelComputeServer{instance: uuid.NewString(), sessions: map[string]*computeServerSession{}, changed: make(chan struct{})}
	if err := s.UpdateConfig(cfg); err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/sentinel/health", s.health)
	mux.HandleFunc("POST /v1/sentinel/sessions", s.create)
	mux.HandleFunc("POST /v1/sentinel/sessions/{id}/{operation}", s.operation)
	mux.HandleFunc("DELETE /v1/sentinel/sessions/{id}", s.delete)
	s.handler = mux
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	go s.reap(ctx)
	return s, nil
}

func (s *SentinelComputeServer) UpdateConfig(cfg sentinelconfig.Server) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	// Clone pointers/slices so management mutation cannot change a pinned generation.
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(data, &cfg); err != nil {
		return err
	}
	policy, err := sentinelcompat.Compile(cfg.GoVMCompatibility)
	if err != nil {
		return err
	}
	l := cfg.Limits()
	s.mu.Lock()
	if s.current != nil && !s.closed {
		old := s.cfg.Limits()
		if s.current.sdk == cfg.SDKFallbackEnabled && old.SDKWorkers == l.SDKWorkers && old.SDKQueueSize == l.SDKQueueSize && old.SDKCacheVersions == l.SDKCacheVersions && reflect.DeepEqual(s.current.rules, cfg.GoVMCompatibility.Resolved()) {
			s.cfg = cfg
			s.signalLocked()
			s.mu.Unlock()
			return nil
		}
	}
	s.mu.Unlock()
	gen := &computeGeneration{rules: cfg.GoVMCompatibility.Resolved(), policy: policy, sdk: cfg.SDKFallbackEnabled}
	gen.manager = NewSentinelRuntimeManager(SentinelRuntimeConfig{Compatibility: policy, Enabled: cfg.SDKFallbackEnabled, Workers: l.SDKWorkers, QueueSize: l.SDKQueueSize, CacheVersions: l.SDKCacheVersions})
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		gen.manager.Close()
		return errors.New("Sentinel solver is closed")
	}
	old := s.current
	s.current = gen
	s.cfg = cfg
	if old != nil {
		old.retired = true
	}
	closeOld := old != nil && old.refs == 0
	s.signalLocked()
	s.mu.Unlock()
	if closeOld {
		old.manager.Close()
	}
	return nil
}

func (s *SentinelComputeServer) signalLocked() { close(s.changed); s.changed = make(chan struct{}) }

func (s *SentinelComputeServer) Snapshot() SentinelComputeServerSnapshot {
	s.mu.Lock()
	out := SentinelComputeServerSnapshot{Enabled: s.cfg.Enabled, Draining: s.draining, Instance: s.instance, Protocol: SentinelComputeProtocol, Active: s.active, Queued: s.queued, Sessions: len(s.sessions), ReservedBytes: s.reserved, GoSuccess: s.goSuccess, SDKSuccess: s.sdkSuccess, Failures: s.failures, Limits: s.cfg.Limits(), RulesHash: s.current.policy.Version()}
	manager := s.current.manager
	s.mu.Unlock()
	out.SDK = manager.Snapshot()
	return out
}

func (s *SentinelComputeServer) owner(r *http.Request) ([32]byte, bool) {
	var empty [32]byte
	key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if key == r.Header.Get("Authorization") || len(key) > 4096 {
		return empty, false
	}
	digest := sha256.Sum256([]byte(key))
	found := 0
	s.mu.Lock()
	for _, allowed := range s.cfg.APIKeys {
		expected := sha256.Sum256([]byte(allowed))
		found |= subtle.ConstantTimeCompare(digest[:], expected[:])
	}
	closed := s.closed
	s.mu.Unlock()
	return digest, found == 1 && !closed
}

func (s *SentinelComputeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	if _, ok := s.owner(r); !ok {
		s.write(w, http.StatusUnauthorized, sentinelComputeResponse{Error: computeError("unauthorized", true, nil)})
		return
	}
	s.handler.ServeHTTP(w, r)
}

func (s *SentinelComputeServer) health(w http.ResponseWriter, r *http.Request) {
	snapshot := s.Snapshot()
	status := http.StatusOK
	if !snapshot.Enabled || snapshot.Draining {
		status = http.StatusServiceUnavailable
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(snapshot)
}

func (s *SentinelComputeServer) write(w http.ResponseWriter, status int, out sentinelComputeResponse) {
	out.Version = SentinelComputeProtocol
	out.Instance = s.instance
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(out)
}

func (s *SentinelComputeServer) acquire(ctx context.Context, reservation int64) (func(int64), error) {
	s.mu.Lock()
	queued := false
	for {
		l := s.cfg.Limits()
		workers := l.GoWorkers
		if workers == 0 {
			workers = runtime.GOMAXPROCS(0)
		}
		if s.closed {
			if queued {
				s.queued--
			}
			s.mu.Unlock()
			return nil, computeError("unavailable", true, nil)
		}
		if s.active < workers && s.reserved+reservation <= int64(l.MemoryBudgetMiB)<<20 {
			if queued {
				s.queued--
			}
			s.active++
			s.reserved += reservation
			s.mu.Unlock()
			return func(retained int64) {
				s.mu.Lock()
				s.active--
				s.reserved -= reservation - retained
				s.signalLocked()
				s.mu.Unlock()
			}, nil
		}
		if !queued {
			if s.queued >= l.QueueSize {
				s.mu.Unlock()
				return nil, computeError("busy", true, nil)
			}
			s.queued++
			queued = true
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			s.mu.Lock()
			s.queued--
			s.mu.Unlock()
			return nil, ctx.Err()
		case <-changed:
		}
		s.mu.Lock()
	}
}

func decodeComputeRequest(w http.ResponseWriter, r *http.Request) (sentinelComputeRequest, error) {
	var in sentinelComputeRequest
	r.Body = http.MaxBytesReader(w, r.Body, sentinelComputeMaxBody)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&in); err != nil {
		return in, computeError("invalid_input", false, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return in, computeError("invalid_input", false, err)
	}
	if in.Version != SentinelComputeProtocol {
		return in, computeError("unsupported_protocol", true, nil)
	}
	if _, err := uuid.Parse(in.SessionID); err != nil {
		return in, computeError("invalid_input", false, nil)
	}
	if _, err := uuid.Parse(in.OperationID); err != nil {
		return in, computeError("invalid_input", false, nil)
	}
	if in.BudgetMillis < 1 || in.BudgetMillis > 3600_000 {
		return in, computeError("invalid_input", false, nil)
	}
	return in, nil
}

func operationHash(in sentinelComputeRequest, operation string) [32]byte {
	in.BudgetMillis = 0
	body, _ := json.Marshal(in)
	return sha256.Sum256(append([]byte(operation+"\x00"), body...))
}

func (s *SentinelComputeServer) rpc(w http.ResponseWriter, r *http.Request, operation string, create bool) {
	startedAt := time.Now()
	// Reading/admission is bounded independently of caller-supplied JSON.
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(30 * time.Second))
	// This connection may serve a normal proxy request after this RPC.
	defer func() { _ = http.NewResponseController(w).SetReadDeadline(time.Time{}) }()
	readCtx, readCancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer readCancel()
	reservation := computeRPCReservation
	if create {
		reservation += computeSessionReservation
	}
	retained := int64(0)
	release, err := s.acquire(readCtx, reservation)
	if err != nil {
		s.write(w, 503, sentinelComputeResponse{Error: classifyComputeError(err)})
		return
	}
	defer func() { release(retained) }()
	in, err := decodeComputeRequest(w, r)
	if err != nil {
		s.write(w, 400, sentinelComputeResponse{Error: classifyComputeError(err)})
		return
	}
	if !create && r.PathValue("id") != in.SessionID {
		s.write(w, 400, sentinelComputeResponse{Error: computeError("invalid_input", false, nil)})
		return
	}
	remaining := time.Duration(in.BudgetMillis)*time.Millisecond - time.Since(startedAt)
	if remaining <= 0 {
		s.write(w, 503, sentinelComputeResponse{Error: computeError("budget_exhausted", true, nil)})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), remaining)
	defer cancel()
	owner, authorized := s.owner(r)
	if !authorized {
		s.write(w, 401, sentinelComputeResponse{Error: computeError("unauthorized", true, nil)})
		return
	}
	s.mu.Lock()
	session := s.sessions[in.SessionID]
	if session != nil && session.owner != owner {
		s.mu.Unlock()
		s.write(w, 404, sentinelComputeResponse{Error: computeError("session_unavailable", true, nil)})
		return
	}
	if create && session == nil {
		l := s.cfg.Limits()
		if !s.cfg.Enabled || s.draining || s.closed || len(s.sessions) >= l.MaxSessions || s.reserved > int64(l.MemoryBudgetMiB)<<20 {
			s.mu.Unlock()
			s.write(w, 503, sentinelComputeResponse{Error: computeError("busy", true, nil)})
			return
		}
		if in.Input == nil || in.Challenge != nil {
			s.mu.Unlock()
			s.write(w, 400, sentinelComputeResponse{Error: computeError("invalid_input", false, nil)})
			return
		}
		sessionCtx, sessionCancel := context.WithCancel(context.Background())
		state, errState := newComputeState(sessionCtx, *in.Input, s.current.policy, s.current.manager, SentinelComputeHooks{}, s.current.sdk, false)
		if errState != nil {
			sessionCancel()
			s.mu.Unlock()
			s.write(w, 400, sentinelComputeResponse{Error: classifyComputeError(errState)})
			return
		}
		session = &computeServerSession{id: in.SessionID, owner: owner, lock: make(chan struct{}, 1), cancel: sessionCancel, state: state, generation: s.current, lastUsed: time.Now(), lease: time.Duration(l.SessionIdleSeconds) * time.Second, cache: map[string]computeCachedOperation{}}
		s.sessions[in.SessionID] = session
		s.current.refs++
		retained = computeSessionReservation
	}
	if session == nil || session.closing {
		s.mu.Unlock()
		s.write(w, 404, sentinelComputeResponse{Error: computeError("session_unavailable", true, nil)})
		return
	}
	session.lastUsed = time.Now()
	s.mu.Unlock()
	select {
	case session.lock <- struct{}{}:
	case <-ctx.Done():
		s.write(w, 503, sentinelComputeResponse{Error: classifyComputeError(ctx.Err())})
		return
	}
	defer func() { <-session.lock }()
	hash := operationHash(in, operation)
	if cached, ok := session.cache[in.OperationID]; ok {
		if cached.hash != hash {
			s.write(w, 409, sentinelComputeResponse{Error: computeError("operation_conflict", false, nil)})
			return
		}
		s.write(w, cached.status, cached.response)
		return
	}
	if len(session.cache) >= 3 || create && session.phase != 0 || operation == "solve" && session.phase != 1 || operation == "snapshot" && session.phase != 2 {
		s.write(w, 409, sentinelComputeResponse{Error: computeError("operation_conflict", false, nil)})
		return
	}
	stopCancellation := context.AfterFunc(ctx, session.cancel)
	defer stopCancellation()
	var result SentinelComputeResult
	switch operation {
	case "create":
		result, err = session.state.requirements(ctx)
	case "solve":
		if in.Challenge == nil || in.Input != nil {
			err = computeError("invalid_input", false, nil)
		} else {
			result, err = session.state.solve(ctx, *in.Challenge)
		}
	case "snapshot":
		if in.Challenge != nil || in.Input != nil {
			err = computeError("invalid_input", false, nil)
		} else {
			result, err = session.state.snapshot(ctx)
		}
	default:
		err = computeError("invalid_input", false, nil)
	}
	stopCancellation()
	out := sentinelComputeResponse{SessionID: session.id, Result: result, Rules: session.generation.rules, RulesHash: session.generation.policy.Version(), SDKURL: session.state.input.SDKURL, SDKSHA256: session.state.input.SDKSHA256}
	status := 200
	if err != nil {
		out.Error = classifyComputeError(err)
		status = 503
		if !out.Error.Retryable {
			status = 400
		}
	} else {
		session.phase++
	}
	encoded, _ := json.Marshal(out)
	if len(encoded) > sentinelComputeMaxBody {
		out.Result = SentinelComputeResult{}
		out.Error = computeError("safety_limit", false, nil)
		status = 400
	}
	session.cache[in.OperationID] = computeCachedOperation{hash, out, status}
	s.mu.Lock()
	if err != nil {
		s.failures++
	} else if result.Engine == "sdk" {
		s.sdkSuccess++
	} else {
		s.goSuccess++
	}
	s.mu.Unlock()
	s.write(w, status, out)
}

func (s *SentinelComputeServer) create(w http.ResponseWriter, r *http.Request) {
	s.rpc(w, r, "create", true)
}
func (s *SentinelComputeServer) operation(w http.ResponseWriter, r *http.Request) {
	operation := r.PathValue("operation")
	if operation == "keepalive" {
		owner, _ := s.owner(r)
		s.mu.Lock()
		session := s.sessions[r.PathValue("id")]
		ok := session != nil && session.owner == owner && !session.closing
		if ok {
			session.lastUsed = time.Now()
		}
		s.mu.Unlock()
		if !ok {
			s.write(w, 404, sentinelComputeResponse{Error: computeError("session_unavailable", true, nil)})
			return
		}
		s.write(w, 200, sentinelComputeResponse{SessionID: session.id})
		return
	}
	if operation != "solve" && operation != "snapshot" {
		s.write(w, 404, sentinelComputeResponse{Error: computeError("invalid_input", false, nil)})
		return
	}
	s.rpc(w, r, operation, false)
}

func (s *SentinelComputeServer) remove(session *computeServerSession) {
	s.mu.Lock()
	if session.closing {
		s.mu.Unlock()
		return
	}
	session.closing = true
	session.cancel()
	s.mu.Unlock()
	go func() {
		session.lock <- struct{}{}
		session.state.close()
		session.cache = nil
		s.mu.Lock()
		delete(s.sessions, session.id)
		s.reserved -= computeSessionReservation
		session.generation.refs--
		closeGen := session.generation.retired && session.generation.refs == 0
		s.signalLocked()
		s.mu.Unlock()
		<-session.lock
		if closeGen {
			session.generation.manager.Close()
		}
	}()
}
func (s *SentinelComputeServer) delete(w http.ResponseWriter, r *http.Request) {
	owner, _ := s.owner(r)
	s.mu.Lock()
	session := s.sessions[r.PathValue("id")]
	s.mu.Unlock()
	if session != nil && session.owner == owner {
		s.remove(session)
	}
	s.write(w, 200, sentinelComputeResponse{})
}
func (s *SentinelComputeServer) reap(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			expired := []*computeServerSession{}
			for _, session := range s.sessions {
				if time.Since(session.lastUsed) > session.lease {
					expired = append(expired, session)
				}
			}
			s.mu.Unlock()
			for _, session := range expired {
				s.remove(session)
			}
		}
	}
}

// Drain stops new sessions while existing sessions keep their generation until completion.
func (s *SentinelComputeServer) Drain(ctx context.Context) {
	s.mu.Lock()
	s.draining = true
	s.signalLocked()
	s.mu.Unlock()
	for {
		s.mu.Lock()
		empty := len(s.sessions) == 0
		changed := s.changed
		s.mu.Unlock()
		if empty {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-changed:
		}
	}
}
func (s *SentinelComputeServer) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.draining = true
	all := []*computeServerSession{}
	for _, v := range s.sessions {
		all = append(all, v)
	}
	s.current.retired = true
	closeCurrent := s.current.refs == 0
	gen := s.current
	s.signalLocked()
	s.mu.Unlock()
	if s.cancel != nil {
		s.cancel()
	}
	for _, session := range all {
		s.remove(session)
	}
	if closeCurrent {
		gen.manager.Close()
	}
}

func decodeComputeResponse(body []byte, out *sentinelComputeResponse) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	// Ignore additive response metadata within v1 so node-only upgrades remain compatible.
	if err := decoder.Decode(out); err != nil {
		return computeError("invalid_response", true, err)
	}
	if decoder.Decode(&struct{}{}) != io.EOF || out.Version != SentinelComputeProtocol {
		return computeError("unsupported_protocol", true, nil)
	}
	return nil
}

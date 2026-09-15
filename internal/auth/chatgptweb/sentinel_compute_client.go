package chatgptweb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelcompat"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelconfig"
)

type SentinelComputeNodeSnapshot struct {
	RulesHash      string    `json:"rules_hash"`
	SDKSHA256      string    `json:"sdk_sha256"`
	P50LatencyMS   float64   `json:"p50_latency_ms"`
	P95LatencyMS   float64   `json:"p95_latency_ms"`
	Name           string    `json:"name"`
	URL            string    `json:"url"`
	CooldownUntil  time.Time `json:"cooldown_until"`
	Requests       uint64    `json:"requests"`
	Failures       uint64    `json:"failures"`
	GoSuccess      uint64    `json:"go_success"`
	SDKSuccess     uint64    `json:"sdk_success"`
	LocalFallbacks uint64    `json:"local_fallbacks"`
	LastError      string    `json:"last_error"`
	LastLatencyMS  int64     `json:"last_latency_ms"`
}
type computeNode struct {
	latencies []float64
	config    sentinelconfig.Node
	status    SentinelComputeNodeSnapshot
}

type sentinelComputeContextKey struct{}
type sentinelComputeScopeKey struct{}

func WithSentinelComputePool(ctx context.Context, pool *SentinelComputePool) context.Context {
	return context.WithValue(ctx, sentinelComputeContextKey{}, pool)
}
func WithSentinelComputeScope(ctx context.Context, scope string) context.Context {
	return context.WithValue(ctx, sentinelComputeScopeKey{}, scope)
}
func SentinelComputeScope(ctx context.Context) string {
	if ctx != nil {
		if scope, ok := ctx.Value(sentinelComputeScopeKey{}).(string); ok {
			return scope
		}
	}
	return "chat"
}

func computePoolFromContext(ctx context.Context) *SentinelComputePool {
	if ctx == nil {
		return nil
	}
	pool, _ := ctx.Value(sentinelComputeContextKey{}).(*SentinelComputePool)
	return pool
}

// SentinelComputePool snapshots configuration per challenge, not per RPC.
type SentinelComputePool struct {
	mu          sync.Mutex
	mode        string
	config      sentinelconfig.Remote
	localConfig SentinelRuntimeConfig
	nodes       []*computeNode
	next        uint64
	closed      bool
	client      *http.Client
	local       *computeGeneration
}

func NewSentinelComputePool() *SentinelComputePool {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.MaxIdleConns = 64
	transport.MaxIdleConnsPerHost = 16
	dialer := &net.Dialer{KeepAlive: 30 * time.Second}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		// localhost must not escape the loopback interface through resolver configuration.
		if strings.EqualFold(host, "localhost") {
			address = net.JoinHostPort("127.0.0.1", port)
		}
		return dialer.DialContext(ctx, network, address)
	}
	return &SentinelComputePool{client: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}

func (p *SentinelComputePool) UpdateConfig(mode string, cfg sentinelconfig.Remote, local SentinelRuntimeConfig) error {
	if err := cfg.Validate(mode); err != nil {
		return err
	}
	data, _ := json.Marshal(cfg)
	if err := json.Unmarshal(data, &cfg); err != nil {
		return err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errors.New("Sentinel compute pool is closed")
	}
	old := p.local
	gen := old
	if old == nil || p.localConfig.Enabled != local.Enabled || p.localConfig.Workers != local.Workers || p.localConfig.QueueSize != local.QueueSize || p.localConfig.CacheVersions != local.CacheVersions || p.localConfig.Compatibility.Version() != local.Compatibility.Version() {
		gen = &computeGeneration{manager: NewSentinelRuntimeManager(local), policy: local.Compatibility, sdk: local.Enabled}
	}
	p.local = gen
	p.mode = sentinelconfig.Mode(mode)
	p.config = cfg
	p.localConfig = local
	nodes := make([]*computeNode, 0, len(cfg.Nodes))
	for _, node := range cfg.Nodes {
		var reused *computeNode
		for _, existing := range p.nodes {
			if existing.config == node {
				reused = existing
				break
			}
		}
		if reused == nil {
			reused = &computeNode{config: node, status: SentinelComputeNodeSnapshot{Name: node.Name, URL: node.URL}}
		}
		nodes = append(nodes, reused)
	}
	p.nodes = nodes
	if old != nil && old != gen {
		old.retired = true
	}
	closeOld := old != nil && old != gen && old.refs == 0
	p.mu.Unlock()
	if closeOld {
		old.manager.Close()
	}
	return nil
}

func (p *SentinelComputePool) Enabled(scope string) bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.closed && p.config.Enabled(p.mode, scope)
}
func (p *SentinelComputePool) Snapshot() []SentinelComputeNodeSnapshot {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]SentinelComputeNodeSnapshot, 0, len(p.nodes))
	for _, n := range p.nodes {
		out = append(out, n.status)
	}
	return out
}
func (p *SentinelComputePool) Close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	old := p.local
	if old != nil {
		old.retired = true
	}
	closeOld := old != nil && old.refs == 0
	p.mu.Unlock()
	if closeOld {
		old.manager.Close()
	}
	p.client.CloseIdleConnections()
}

// SentinelComputeSession owns one remote session and optional reconstructed local state.
type SentinelComputeSession struct {
	mu         sync.Mutex
	pool       *SentinelComputePool
	node       *computeNode
	instance   string
	id         string
	input      SentinelComputeInput
	policy     *sentinelcompat.Policy
	generation *computeGeneration
	remaining  time.Duration
	result     SentinelComputeResult
	challenge  SentinelComputeChallenge
	hooks      SentinelComputeHooks
	local      *computeState
	ctx        context.Context
	cancel     context.CancelFunc
	stopParent func() bool
	closed     bool
	solved     bool
	operations map[string]string
}

func (p *SentinelComputePool) Begin(ctx context.Context, scope string, input SentinelComputeInput, hooks SentinelComputeHooks) (*SentinelComputeSession, error) {
	var errNormalize error
	input, errNormalize = input.normalized()
	if errNormalize != nil {
		return nil, errNormalize
	}
	if err := input.validate(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	if p.closed || !p.config.Enabled(p.mode, scope) || p.local == nil {
		p.mu.Unlock()
		return nil, computeError("unavailable", true, nil)
	}
	gen := p.local
	gen.refs++
	budget := time.Duration(p.config.Budget()) * time.Second
	nodes := []*computeNode{}
	if len(p.nodes) > 0 {
		start := int(p.next % uint64(len(p.nodes)))
		p.next++
		for i := 0; i < len(p.nodes) && len(nodes) < 2; i++ {
			node := p.nodes[(start+i)%len(p.nodes)]
			if !time.Now().Before(node.status.CooldownUntil) {
				nodes = append(nodes, node)
			}
		}
	}
	p.mu.Unlock()
	sessionCtx, cancel := context.WithCancel(ctx)
	s := &SentinelComputeSession{pool: p, id: uuid.NewString(), input: input, policy: gen.policy, generation: gen, remaining: budget, hooks: hooks, ctx: sessionCtx, cancel: cancel, operations: map[string]string{}}
	err := error(computeError("unavailable", true, nil))
	for i, node := range nodes {
		s.node = node
		limit := s.remaining / time.Duration(len(nodes)-i)
		_, err = s.rpc(ctx, "create", &input, nil, limit)
		if err == nil {
			break
		}
		if ctx.Err() != nil || !classifyComputeError(err).Retryable {
			break
		}
	}
	if err != nil {
		if err = s.fallback(ctx, err); err == nil {
			s.result, err = s.local.requirements(ctx)
		}
	}
	if err != nil {
		s.Close()
		return nil, err
	}
	s.mu.Lock()
	s.stopParent = context.AfterFunc(ctx, s.Close)
	s.mu.Unlock()
	if s.local == nil {
		go s.keepalive()
	}
	return s, nil
}

func (s *SentinelComputeSession) RequirementsToken() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.result.RequirementsToken
}

func (s *SentinelComputeSession) rpc(ctx context.Context, operation string, input *SentinelComputeInput, challenge *SentinelComputeChallenge, limit time.Duration) (SentinelComputeResult, error) {
	if err := ctx.Err(); err != nil {
		return s.result, err
	}
	if s.remaining <= 0 {
		return s.result, computeError("budget_exhausted", true, nil)
	}
	if limit <= 0 || limit > s.remaining {
		limit = s.remaining
	}
	operationID := s.operations[operation]
	if operationID == "" {
		operationID = uuid.NewString()
		s.operations[operation] = operationID
	}
	started := time.Now()
	defer func() { s.remaining -= time.Since(started) }()
	opCtx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	stopSession := context.AfterFunc(s.ctx, cancel)
	defer stopSession()
	var last error
	for attempt := 0; attempt < 2; attempt++ {
		deadline, _ := opCtx.Deadline()
		ms := time.Until(deadline).Milliseconds()
		if ms < 1 {
			return s.result, computeError("budget_exhausted", true, last)
		}
		in := sentinelComputeRequest{Version: SentinelComputeProtocol, SessionID: s.id, OperationID: operationID, BudgetMillis: ms, Input: input, Challenge: challenge}
		body, err := json.Marshal(in)
		if err != nil || len(body) > sentinelComputeMaxBody {
			return s.result, computeError("invalid_input", false, err)
		}
		baseURL, err := sentinelconfig.NodeBaseURL(s.node.config.URL)
		if err != nil {
			return s.result, computeError("invalid_input", false, err)
		}
		path := "/sessions"
		if operation != "create" {
			path += "/" + s.id + "/" + operation
		}
		request, err := http.NewRequestWithContext(opCtx, http.MethodPost, baseURL+path, bytes.NewReader(body))
		if err != nil {
			return s.result, computeError("invalid_input", false, err)
		}
		request.Header.Set("Authorization", "Bearer "+s.node.config.APIKey)
		request.Header.Set("Content-Type", "application/json")
		response, err := s.pool.client.Do(request)
		if err != nil {
			last = computeError("transport", true, err)
			if opCtx.Err() != nil {
				break
			}
			continue
		}
		data, readErr := io.ReadAll(io.LimitReader(response.Body, sentinelComputeMaxBody+1))
		_ = response.Body.Close()
		if readErr != nil {
			last = computeError("transport", true, readErr)
			continue
		}
		var out sentinelComputeResponse
		if len(data) > sentinelComputeMaxBody {
			last = computeError("invalid_response", true, nil)
			break
		}
		if err = decodeComputeResponse(data, &out); err != nil {
			last = err
			break
		}
		if s.instance != "" && s.instance != out.Instance {
			last = computeError("session_unavailable", true, nil)
			break
		}
		if out.Error != nil {
			switch out.Error.Code {
			case "invalid_input", "operation_conflict", "safety_limit":
				out.Error.Retryable = false
			case "unauthorized", "unavailable", "busy", "budget_exhausted", "compatibility", "unsupported_profile", "unsupported_protocol", "session_unavailable", "sdk_unavailable":
				out.Error.Retryable = true
			default:
				out.Error = computeError("invalid_response", true, nil)
			}
			last = out.Error
			s.record(last, time.Since(started), "")
			return s.result, last
		}
		if response.StatusCode != 200 || out.SessionID != s.id || out.Instance == "" {
			last = computeError("invalid_response", true, nil)
			break
		}
		policy, errPolicy := sentinelcompat.Compile(out.Rules)
		if errPolicy != nil || policy.Version() != out.RulesHash {
			last = computeError("invalid_response", true, errPolicy)
			break
		}
		if s.instance != "" && s.policy.Version() != policy.Version() {
			last = computeError("invalid_response", true, nil)
			break
		}
		if out.Result.Engine != "go" && out.Result.Engine != "sdk" {
			last = computeError("invalid_response", true, nil)
			break
		}
		if operation == "create" && out.Result.RequirementsToken == "" || operation == "solve" && challenge.TurnstileRequired && out.Result.TurnstileToken == "" || operation == "snapshot" && s.challenge.ObserverRequired && out.Result.SnapshotToken == "" {
			last = computeError("invalid_response", true, nil)
			break
		}
		if out.SDKURL != s.input.SDKURL {
			last = computeError("invalid_response", true, nil)
			break
		}
		previousHashes, _ := normalizeSentinelHashes(s.input.SDKSHA256)
		responseHashes, hashErr := normalizeSentinelHashes(out.SDKSHA256)
		hashOK := hashErr == nil && (len(previousHashes) == 0 || len(responseHashes) > 0)
		for _, hash := range responseHashes {
			if !sentinelHashAllowed(hash, previousHashes) {
				hashOK = false
			}
		}
		if !hashOK {
			last = computeError("invalid_response", true, nil)
			break
		}
		if s.result.RequirementsToken != "" && out.Result.RequirementsToken != s.result.RequirementsToken {
			last = computeError("invalid_response", true, nil)
			break
		}
		s.instance = out.Instance
		s.policy = policy
		s.input.SDKSHA256 = out.SDKSHA256
		s.result = out.Result
		s.record(nil, time.Since(started), out.Result.Engine)
		return s.result, nil
	}
	if ctx.Err() != nil {
		return s.result, ctx.Err()
	}
	if opCtx.Err() != nil {
		last = computeError("budget_exhausted", true, last)
	}
	if last == nil {
		last = computeError("unavailable", true, nil)
	}
	s.record(last, time.Since(started), "")
	return s.result, last
}

func (s *SentinelComputeSession) record(err error, elapsed time.Duration, engine string) {
	if s.node == nil {
		return
	}
	p := s.pool
	p.mu.Lock()
	defer p.mu.Unlock()
	st := &s.node.status
	st.Requests++
	st.LastLatencyMS = elapsed.Milliseconds()
	st.RulesHash = s.policy.Version()
	st.SDKSHA256 = s.input.SDKSHA256
	s.node.latencies = append(s.node.latencies, float64(elapsed)/float64(time.Millisecond))
	if len(s.node.latencies) > 128 {
		s.node.latencies = s.node.latencies[1:]
	}
	ordered := append([]float64{}, s.node.latencies...)
	sort.Float64s(ordered)
	st.P50LatencyMS = ordered[(len(ordered)-1)/2]
	st.P95LatencyMS = ordered[(len(ordered)-1)*95/100]
	if err != nil {
		e := classifyComputeError(err)
		st.Failures++
		st.LastError = e.Code
		if e.Code == "transport" || e.Code == "budget_exhausted" || e.Code == "unauthorized" || e.Code == "unsupported_protocol" {
			st.CooldownUntil = time.Now().Add(30 * time.Second)
		}
	} else {
		st.LastError = ""
		if engine == "sdk" {
			st.SDKSuccess++
		} else {
			st.GoSuccess++
		}
	}
}

func (s *SentinelComputeSession) fallback(ctx context.Context, cause error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if !s.generation.sdk || !classifyComputeError(cause).Retryable {
		return cause
	}
	if s.local != nil {
		return nil
	}
	state, err := newComputeState(s.ctx, s.input, s.policy, s.generation.manager, s.hooks, true, true)
	if err != nil {
		return err
	}
	state.p = s.result.RequirementsToken
	state.result = s.result
	state.challenge = s.challenge
	s.local = state
	if s.node != nil {
		s.pool.mu.Lock()
		s.node.status.LocalFallbacks++
		s.pool.mu.Unlock()
		s.cleanupRemote()
	}
	return nil
}

func (s *SentinelComputeSession) Solve(ctx context.Context, challenge SentinelComputeChallenge) (SentinelComputeResult, error) {
	if err := ctx.Err(); err != nil {
		return SentinelComputeResult{}, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopSession := context.AfterFunc(s.ctx, cancel)
	defer stopSession()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.result, computeError("session_unavailable", true, nil)
	}
	if err := challenge.validate(); err != nil {
		return s.result, err
	}
	if s.solved {
		return s.result, computeError("operation_conflict", false, nil)
	}
	s.challenge = challenge
	var result SentinelComputeResult
	var err error
	if s.local == nil {
		result, err = s.rpc(ctx, "solve", nil, &challenge, s.remaining)
		if err != nil {
			err = s.fallback(ctx, err)
		}
	}
	if err == nil && s.local != nil {
		result, err = s.local.solve(ctx, challenge)
	}
	if err == nil {
		s.result = result
		s.solved = true
	}
	return result, err
}

func (s *SentinelComputeSession) Snapshot(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopSession := context.AfterFunc(s.ctx, cancel)
	defer stopSession()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || !s.solved {
		return "", computeError("session_unavailable", true, nil)
	}
	var result SentinelComputeResult
	var err error
	wasRemote := s.local == nil
	if wasRemote {
		result, err = s.rpc(ctx, "snapshot", nil, nil, s.remaining)
		if err != nil {
			err = s.fallback(ctx, err)
			if err == nil {
				err = s.local.observer(ctx)
			}
		}
	}
	if err == nil && s.local != nil {
		result, err = s.local.snapshot(ctx)
	}
	if err == nil {
		s.result = result
	}
	return result.SnapshotToken, err
}

func (s *SentinelComputeSession) maintenance(ctx context.Context, method, path string) {
	baseURL, err := sentinelconfig.NodeBaseURL(s.node.config.URL)
	if err != nil {
		return
	}
	request, err := http.NewRequestWithContext(ctx, method, baseURL+"/sessions/"+s.id+path, nil)
	if err != nil {
		return
	}
	request.Header.Set("Authorization", "Bearer "+s.node.config.APIKey)
	response, err := s.pool.client.Do(request)
	if err == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		_ = response.Body.Close()
	}
}
func (s *SentinelComputeSession) cleanupRemote() {
	if s.node == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	go func() { defer cancel(); s.maintenance(ctx, http.MethodDelete, "") }()
}
func (s *SentinelComputeSession) keepalive() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			done := s.closed || s.local != nil
			s.mu.Unlock()
			if done {
				return
			}
			ctx, cancel := context.WithTimeout(s.ctx, 2*time.Second)
			s.maintenance(ctx, http.MethodPost, "/keepalive")
			cancel()
		}
	}
}
func (s *SentinelComputeSession) Close() {
	if s == nil {
		return
	}
	s.cancel()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	if s.stopParent != nil {
		s.stopParent()
	}
	if s.local != nil {
		s.local.close()
	}
	s.cleanupRemote()
	s.mu.Unlock()
	s.pool.mu.Lock()
	s.generation.refs--
	closeGen := s.generation.retired && s.generation.refs == 0
	s.pool.mu.Unlock()
	if closeGen {
		s.generation.manager.Close()
	}
}

// TestSentinelComputeNode checks only the private computation health endpoint.
func TestSentinelComputeNode(ctx context.Context, node sentinelconfig.Node) (SentinelComputeServerSnapshot, error) {
	var out SentinelComputeServerSnapshot
	if _, err := sentinelconfig.ParseNodeURL(node.URL); err != nil {
		return out, err
	}
	p := NewSentinelComputePool()
	defer p.Close()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	baseURL, err := sentinelconfig.NodeBaseURL(node.URL)
	if err != nil {
		return out, err
	}
	target, err := url.Parse(baseURL + "/health")
	if err != nil {
		return out, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return out, err
	}
	req.Header.Set("Authorization", "Bearer "+node.APIKey)
	response, err := p.client.Do(req)
	if err != nil {
		return out, computeError("transport", true, nil)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != 200 {
		return out, computeError("unavailable", true, nil)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 1<<20))
	if err = decoder.Decode(&out); err != nil || out.Protocol != SentinelComputeProtocol {
		return out, computeError("unsupported_protocol", true, nil)
	}
	return out, nil
}

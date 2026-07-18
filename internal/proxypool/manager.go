package proxypool

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
	log "github.com/sirupsen/logrus"
)

const (
	backgroundCheckMaxWait = 15 * time.Second
	maxConcurrentChecks    = 8
)

var errProxyConfigurationChanged = errors.New("proxy configuration changed")

type runtimeEntry struct {
	config internalconfig.ProxyPoolEntryConfig
	ports  proxyutil.PortSet
}

type runtimePool struct {
	config  internalconfig.ProxyPoolConfig
	entries []runtimeEntry
	byID    map[string]runtimeEntry
}

type configSnapshot struct {
	generation uint64
	signature  string
	globalURL  string
	rules      []internalconfig.ProxyRuleConfig
	pools      map[string]runtimePool
}

type bindingLock struct {
	semaphore chan struct{}
}

// Manager owns immutable proxy configuration snapshots, stable credential
// bindings, and runtime-only node health.
type Manager struct {
	config   atomic.Pointer[configSnapshot]
	configMu sync.RWMutex
	probeSeq atomic.Uint64

	mu       sync.RWMutex
	bindings map[string]Binding
	health   map[string]nodeHealth
	auths    AuthSource
	leaseMu  sync.RWMutex
	leases   map[string]int

	persistMu sync.Mutex
	bindLocks sync.Map
	statePath string
	random    io.Reader
	now       func() time.Time
	check     traceChecker

	lifecycleMu sync.Mutex
	cancel      context.CancelFunc
	done        chan struct{}
}

// NewManager creates a proxy-pool runtime and restores stable bindings.
func NewManager(configPath string, cfg *internalconfig.Config) (*Manager, error) {
	m := &Manager{
		bindings:  make(map[string]Binding),
		health:    make(map[string]nodeHealth),
		leases:    make(map[string]int),
		statePath: bindingStatePath(configPath),
		random:    rand.Reader,
		now:       time.Now,
		check:     checkProxyTrace,
	}
	if errLoad := m.loadBindings(); errLoad != nil {
		return nil, errLoad
	}
	if errConfig := m.UpdateConfig(cfg); errConfig != nil {
		return nil, errConfig
	}
	return m, nil
}

// HoldBinding keeps an unregistered credential's binding alive until the
// caller either registers the credential or abandons the acquisition.
func (m *Manager) HoldBinding(authID string) func() {
	if m == nil {
		return func() {}
	}
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return func() {}
	}
	m.leaseMu.Lock()
	if m.leases == nil {
		m.leases = make(map[string]int)
	}
	m.leases[authID]++
	m.leaseMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			m.leaseMu.Lock()
			if count := m.leases[authID]; count > 1 {
				m.leases[authID] = count - 1
			} else {
				delete(m.leases, authID)
			}
			m.leaseMu.Unlock()
		})
	}
}

func (m *Manager) bindingLeaseActive(authID string) bool {
	if m == nil {
		return false
	}
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return false
	}
	m.leaseMu.RLock()
	active := m.leases[authID] > 0
	m.leaseMu.RUnlock()
	return active
}

func bindingStatePath(configPath string) string {
	configPath = strings.TrimSpace(configPath)
	if configPath == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(configPath), ".cli-proxy-api", "proxy-bindings.json")
}

// SetAuthSource sets the live credential source used by management and pruning.
func (m *Manager) SetAuthSource(source AuthSource) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.auths = source
	m.mu.Unlock()
}

// UpdateConfig atomically replaces the normalized routing snapshot.
func (m *Manager) UpdateConfig(cfg *internalconfig.Config) error {
	return m.updateConfig(cfg, "", "")
}

// UpdateConfigWithPoolRename atomically updates routing configuration while
// preserving compatible stable bindings for an explicitly renamed pool.
func (m *Manager) UpdateConfigWithPoolRename(cfg *internalconfig.Config, oldName, newName string) error {
	return m.updateConfig(cfg, strings.TrimSpace(oldName), strings.TrimSpace(newName))
}

func (m *Manager) updateConfig(cfg *internalconfig.Config, oldPoolName, newPoolName string) error {
	if m == nil {
		return nil
	}
	if cfg == nil {
		cfg = &internalconfig.Config{}
	}
	pools, rules, errNormalize := internalconfig.NormalizeProxyConfiguration(cfg.ProxyPools, cfg.ProxyRules)
	if errNormalize != nil {
		return errNormalize
	}
	globalURL := strings.TrimSpace(cfg.ProxyURL)
	signature, errSignature := proxyConfigurationSignature(globalURL, pools, rules)
	if errSignature != nil {
		return errSignature
	}
	m.configMu.Lock()
	defer m.configMu.Unlock()
	previous := m.config.Load()
	renamePool := oldPoolName != "" && newPoolName != "" && !strings.EqualFold(oldPoolName, newPoolName)
	if previous != nil && previous.signature == signature && !renamePool {
		return nil
	}
	generation := uint64(1)
	if previous != nil {
		generation = previous.generation + 1
	}
	snapshot := &configSnapshot{
		generation: generation,
		signature:  signature,
		globalURL:  globalURL,
		rules:      cloneRules(rules),
		pools:      make(map[string]runtimePool, len(pools)),
	}
	for _, poolConfig := range pools {
		pool := runtimePool{
			config: poolConfig,
			byID:   make(map[string]runtimeEntry, len(poolConfig.Entries)),
		}
		pool.config.Entries = append([]internalconfig.ProxyPoolEntryConfig(nil), poolConfig.Entries...)
		for _, entryConfig := range poolConfig.Entries {
			ports, errPorts := proxyutil.ParsePortSet(entryConfig.Ports)
			if errPorts != nil {
				return fmt.Errorf("proxy pool %s entry %s: %w", poolConfig.Name, entryConfig.ID, errPorts)
			}
			entry := runtimeEntry{config: entryConfig, ports: ports}
			pool.entries = append(pool.entries, entry)
			pool.byID[strings.ToLower(entryConfig.ID)] = entry
		}
		snapshot.pools[strings.ToLower(poolConfig.Name)] = pool
	}
	if renamePool {
		if errRename := m.renamePoolBindingsLocked(snapshot, oldPoolName, newPoolName); errRename != nil {
			return errRename
		}
	}
	m.config.Store(snapshot)
	return nil
}

// renamePoolBindingsLocked expects configMu to be write-locked.
func (m *Manager) renamePoolBindingsLocked(snapshot *configSnapshot, oldName, newName string) error {
	m.persistMu.Lock()
	defer m.persistMu.Unlock()

	m.mu.RLock()
	next := cloneBindings(m.bindings)
	m.mu.RUnlock()
	migratedIDs := make([]string, 0)
	for authID, binding := range next {
		if !strings.EqualFold(strings.TrimSpace(binding.Pool), oldName) {
			continue
		}
		candidate := cloneBinding(binding)
		candidate.Pool = newName
		if _, valid := m.bindingURL(snapshot, candidate); !valid {
			continue
		}
		next[authID] = candidate
		migratedIDs = append(migratedIDs, candidate.ID)
	}
	if len(migratedIDs) == 0 {
		return nil
	}
	if errPersist := m.persistBindings(next); errPersist != nil {
		return errPersist
	}
	m.mu.Lock()
	m.bindings = next
	for _, bindingID := range migratedIDs {
		delete(m.health, bindingID)
	}
	m.mu.Unlock()
	return nil
}

func proxyConfigurationSignature(globalURL string, pools []internalconfig.ProxyPoolConfig, rules []internalconfig.ProxyRuleConfig) (string, error) {
	payload, errMarshal := json.Marshal(struct {
		GlobalURL string                           `json:"global_url"`
		Pools     []internalconfig.ProxyPoolConfig `json:"pools"`
		Rules     []internalconfig.ProxyRuleConfig `json:"rules"`
	}{
		GlobalURL: globalURL,
		Pools:     pools,
		Rules:     rules,
	})
	if errMarshal != nil {
		return "", fmt.Errorf("encode proxy configuration signature: %w", errMarshal)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func cloneRules(rules []internalconfig.ProxyRuleConfig) []internalconfig.ProxyRuleConfig {
	out := make([]internalconfig.ProxyRuleConfig, len(rules))
	for index := range rules {
		out[index] = rules[index]
		out[index].Providers = append([]string(nil), rules[index].Providers...)
		out[index].Priorities = append([]int(nil), rules[index].Priorities...)
	}
	return out
}

func (m *Manager) snapshot() *configSnapshot {
	if m == nil {
		return nil
	}
	return m.config.Load()
}

func authPriority(auth *coreauth.Auth) int {
	if auth == nil || auth.Attributes == nil {
		return 0
	}
	priority, errParse := strconv.Atoi(strings.TrimSpace(auth.Attributes["priority"]))
	if errParse != nil {
		return 0
	}
	return priority
}

// Resolve implements coreauth.ProxyResolver.
func (m *Manager) Resolve(ctx context.Context, auth *coreauth.Auth) (coreauth.ResolvedProxy, error) {
	if auth == nil {
		return coreauth.ResolvedProxy{}, nil
	}
	if strings.EqualFold(strings.TrimSpace(auth.Provider), "aistudio") {
		return m.resolveAIStudioRelayProxy(auth)
	}
	if explicit := strings.TrimSpace(auth.ProxyURL); explicit != "" {
		return coreauth.ResolvedProxy{URL: explicit, Source: "auth"}, nil
	}
	lock, errLock := m.lockBinding(ctx, auth.ID)
	if errLock != nil {
		return coreauth.ResolvedProxy{}, errLock
	}
	defer lock()
	for {
		if ctx != nil && ctx.Err() != nil {
			return coreauth.ResolvedProxy{}, ctx.Err()
		}
		snapshot := m.snapshot()
		if snapshot == nil {
			return coreauth.ResolvedProxy{}, nil
		}
		poolName, matched := internalconfig.MatchProxyRule(snapshot.rules, auth.Provider, authPriority(auth))
		if !matched {
			if snapshot.globalURL != "" {
				return coreauth.ResolvedProxy{URL: snapshot.globalURL, Source: "global"}, nil
			}
			return coreauth.ResolvedProxy{Source: "inherit"}, nil
		}
		resolved, errResolve := m.resolvePoolBinding(ctx, snapshot, auth.ID, poolName, false)
		if errors.Is(errResolve, errProxyConfigurationChanged) {
			continue
		}
		return resolved, errResolve
	}
}

func (m *Manager) resolveAIStudioRelayProxy(auth *coreauth.Auth) (coreauth.ResolvedProxy, error) {
	if auth == nil {
		return coreauth.ResolvedProxy{}, nil
	}
	if explicit := strings.TrimSpace(auth.ProxyURL); explicit != "" {
		setting, errParse := proxyutil.Parse(explicit)
		if errParse != nil || setting.Mode == proxyutil.ModeProxy {
			return coreauth.ResolvedProxy{}, &UnavailableError{Cause: errParse}
		}
		return coreauth.ResolvedProxy{URL: explicit, Source: "auth"}, nil
	}
	snapshot := m.snapshot()
	if snapshot == nil {
		return coreauth.ResolvedProxy{}, nil
	}
	poolName, matched := internalconfig.MatchProxyRule(snapshot.rules, auth.Provider, authPriority(auth))
	if matched {
		return coreauth.ResolvedProxy{}, &UnavailableError{Pool: poolName}
	}
	if snapshot.globalURL != "" {
		setting, errParse := proxyutil.Parse(snapshot.globalURL)
		if errParse != nil || setting.Mode == proxyutil.ModeProxy {
			return coreauth.ResolvedProxy{}, &UnavailableError{Cause: errParse}
		}
		return coreauth.ResolvedProxy{URL: snapshot.globalURL, Source: "global"}, nil
	}
	return coreauth.ResolvedProxy{Source: "inherit"}, nil
}

func (m *Manager) lockBinding(ctx context.Context, authID string) (func(), error) {
	key := strings.TrimSpace(authID)
	if key == "" {
		key = "__anonymous__"
	}
	created := &bindingLock{semaphore: make(chan struct{}, 1)}
	created.semaphore <- struct{}{}
	raw, _ := m.bindLocks.LoadOrStore(key, created)
	lock := raw.(*bindingLock)
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-lock.semaphore:
		return func() { lock.semaphore <- struct{}{} }, nil
	}
}

func (m *Manager) resolvePoolBinding(ctx context.Context, snapshot *configSnapshot, authID, poolName string, force bool) (coreauth.ResolvedProxy, error) {
	pool, exists := snapshot.pools[strings.ToLower(strings.TrimSpace(poolName))]
	if !exists {
		return coreauth.ResolvedProxy{}, &UnavailableError{Pool: poolName}
	}

	m.mu.RLock()
	current, hasCurrent := m.bindings[authID]
	m.mu.RUnlock()
	currentURL := ""
	if hasCurrent && strings.EqualFold(current.Pool, pool.config.Name) {
		if resolvedURL, valid := resolveBindingURL(pool, current); valid {
			currentURL = resolvedURL
			if !force {
				usable, errUsable := m.bindingUsable(ctx, snapshot, current, resolvedURL)
				if errUsable != nil {
					return coreauth.ResolvedProxy{}, errUsable
				}
				if usable {
					if m.snapshot() != snapshot {
						return coreauth.ResolvedProxy{}, errProxyConfigurationChanged
					}
					return resolvedProxy(current, resolvedURL), nil
				}
			}
		}
	}

	binding, resolvedURL, retryAt, errAllocate := m.allocateBinding(ctx, snapshot, pool, authID, currentURL)
	if errAllocate != nil {
		if ctx != nil && ctx.Err() != nil {
			return coreauth.ResolvedProxy{}, ctx.Err()
		}
		return coreauth.ResolvedProxy{}, &UnavailableError{Pool: pool.config.Name, RetryTime: retryAt, Cause: errAllocate}
	}
	if errSave := m.saveBindingForSnapshot(snapshot, binding); errSave != nil {
		m.deleteHealth(binding.ID)
		if errors.Is(errSave, errProxyConfigurationChanged) {
			return coreauth.ResolvedProxy{}, errSave
		}
		return coreauth.ResolvedProxy{}, &UnavailableError{Pool: pool.config.Name, Cause: errSave}
	}
	return resolvedProxy(binding, resolvedURL), nil
}

func resolvedProxy(binding Binding, resolvedURL string) coreauth.ResolvedProxy {
	return coreauth.ResolvedProxy{URL: resolvedURL, Source: "pool", BindingID: binding.ID}
}

func resolveBindingURL(pool runtimePool, binding Binding) (string, bool) {
	entry, exists := pool.byID[strings.ToLower(strings.TrimSpace(binding.Entry))]
	if !exists {
		return "", false
	}
	resolvedURL, errExpand := proxyutil.ExpandURLTemplateValues(entry.config.URLTemplate, binding.PlaceholderValues)
	if errExpand != nil {
		return "", false
	}
	if entry.ports.Count() > 0 {
		if !entry.ports.Contains(binding.Port) {
			return "", false
		}
		var errPort error
		resolvedURL, errPort = proxyutil.WithPort(resolvedURL, binding.Port)
		if errPort != nil {
			return "", false
		}
	} else if binding.Port != 0 {
		return "", false
	}
	setting, errParse := proxyutil.Parse(resolvedURL)
	return resolvedURL, errParse == nil && setting.Mode == proxyutil.ModeProxy
}

func (m *Manager) bindingUsable(ctx context.Context, snapshot *configSnapshot, binding Binding, resolvedURL string) (bool, error) {
	if ctx != nil && ctx.Err() != nil {
		return false, ctx.Err()
	}
	m.mu.RLock()
	health, known := m.health[binding.ID]
	m.mu.RUnlock()
	now := m.now()
	if known && health.Generation == snapshot.generation && !health.OK && now.Before(health.RetryAfter) {
		return false, nil
	}
	if known && health.Generation == snapshot.generation && health.OK && now.Before(health.RetryAfter) {
		return true, nil
	}
	probeEpoch := m.nextProbeEpoch()
	result := m.check(ctx, resolvedURL)
	if ctx != nil && ctx.Err() != nil {
		return false, ctx.Err()
	}
	m.storeBoundHealth(snapshot, binding, resolvedURL, result, probeEpoch)
	return result.OK, nil
}

func (m *Manager) allocateBinding(ctx context.Context, snapshot *configSnapshot, pool runtimePool, authID, excludedURL string) (Binding, string, time.Time, error) {
	attempts := pool.config.BindAttempts
	if attempts < 1 {
		attempts = internalconfig.DefaultProxyPoolBindAttempts
	}
	remaining := poolCandidateCount(pool)
	if remaining == 0 {
		return Binding{}, "", time.Time{}, errors.New("proxy pool has no entries")
	}
	maxDraws := attempts*10 + len(pool.entries)
	if maxDraws > remaining {
		maxDraws = remaining
	}
	swaps := make(map[int]int, maxDraws)
	seen := make(map[string]struct{}, attempts)
	var (
		earliestRetry time.Time
		lastErr       error
	)
	for probes, draws := 0, 0; probes < attempts && draws < maxDraws && remaining > 0; draws++ {
		if ctx != nil && ctx.Err() != nil {
			return Binding{}, "", time.Time{}, ctx.Err()
		}
		pick, errRandom := randomInt(m.random, remaining)
		if errRandom != nil {
			return Binding{}, "", time.Time{}, errRandom
		}
		ordinal := pick
		if mapped, ok := swaps[pick]; ok {
			ordinal = mapped
		}
		last := remaining - 1
		lastOrdinal := last
		if mapped, ok := swaps[last]; ok {
			lastOrdinal = mapped
		}
		delete(swaps, last)
		if pick != last {
			swaps[pick] = lastOrdinal
		}
		remaining--

		binding, resolvedURL, errCandidate := m.bindingAtOrdinal(pool, authID, ordinal)
		if errCandidate != nil {
			lastErr = errCandidate
			continue
		}
		fingerprint := proxyURLFingerprint(resolvedURL)
		if resolvedURL == excludedURL {
			continue
		}
		if _, duplicate := seen[fingerprint]; duplicate {
			continue
		}
		seen[fingerprint] = struct{}{}
		probes++
		probeEpoch := m.nextProbeEpoch()
		result := m.check(ctx, resolvedURL)
		if ctx != nil && ctx.Err() != nil {
			return Binding{}, "", time.Time{}, ctx.Err()
		}
		health := m.storeHealth(snapshot, binding, resolvedURL, result, probeEpoch)
		if result.OK {
			return binding, resolvedURL, time.Time{}, nil
		}
		m.deleteHealth(binding.ID)
		lastErr = errors.New("proxy health check failed")
		if earliestRetry.IsZero() || health.RetryAfter.Before(earliestRetry) {
			earliestRetry = health.RetryAfter
		}
	}
	if lastErr == nil {
		lastErr = errors.New("proxy pool has no distinct candidate")
	}
	return Binding{}, "", earliestRetry, lastErr
}

func (m *Manager) randomBinding(pool runtimePool, authID string) (Binding, string, error) {
	total := poolCandidateCount(pool)
	if total == 0 {
		return Binding{}, "", errors.New("proxy pool has no entries")
	}
	ordinal, errRandom := randomInt(m.random, total)
	if errRandom != nil {
		return Binding{}, "", errRandom
	}
	return m.bindingAtOrdinal(pool, authID, ordinal)
}

func poolCandidateCount(pool runtimePool) int {
	total := 0
	for _, entry := range pool.entries {
		weight := entry.ports.Count()
		if weight == 0 {
			weight = 1
		}
		total += weight
	}
	return total
}

func (m *Manager) bindingAtOrdinal(pool runtimePool, authID string, ordinal int) (Binding, string, error) {
	if ordinal < 0 || ordinal >= poolCandidateCount(pool) {
		return Binding{}, "", errors.New("proxy candidate ordinal is out of range")
	}
	var selected runtimeEntry
	port := 0
	for _, entry := range pool.entries {
		weight := entry.ports.Count()
		if weight == 0 {
			weight = 1
		}
		if ordinal >= weight {
			ordinal -= weight
			continue
		}
		selected = entry
		if entry.ports.Count() > 0 {
			port, _ = entry.ports.PortAt(ordinal)
		}
		break
	}
	resolvedURL, values, errExpand := proxyutil.ExpandURLTemplate(selected.config.URLTemplate, pool.config.PlaceholderCharset, m.random)
	if errExpand != nil {
		return Binding{}, "", errExpand
	}
	if port > 0 {
		var errPort error
		resolvedURL, errPort = proxyutil.WithPort(resolvedURL, port)
		if errPort != nil {
			return Binding{}, "", errPort
		}
	}
	bindingID, errID := randomBindingID(m.random)
	if errID != nil {
		return Binding{}, "", errID
	}
	binding := Binding{
		ID:                bindingID,
		AuthID:            authID,
		Pool:              pool.config.Name,
		Entry:             selected.config.ID,
		Port:              port,
		PlaceholderValues: append([]string(nil), values...),
		BoundAt:           m.now().UTC(),
	}
	return binding, resolvedURL, nil
}

func randomInt(source io.Reader, max int) (int, error) {
	if max <= 0 {
		return 0, errors.New("random upper bound must be positive")
	}
	value, errRandom := rand.Int(source, big.NewInt(int64(max)))
	if errRandom != nil {
		return 0, errRandom
	}
	return int(value.Int64()), nil
}

func randomBindingID(source io.Reader) (string, error) {
	value := make([]byte, 16)
	if _, errRead := io.ReadFull(source, value); errRead != nil {
		return "", errRead
	}
	return hex.EncodeToString(value), nil
}

func proxyURLFingerprint(raw string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(raw)))
	return hex.EncodeToString(sum[:16])
}

func (m *Manager) nextProbeEpoch() uint64 {
	if m == nil {
		return 0
	}
	return m.probeSeq.Add(1)
}

func (m *Manager) storeHealth(snapshot *configSnapshot, binding Binding, resolvedURL string, result TraceResult, probeEpoch uint64) nodeHealth {
	health := m.newHealth(snapshot, binding, resolvedURL, result, probeEpoch)
	m.mu.Lock()
	previous := m.health[binding.ID]
	if current := m.config.Load(); current != nil && current.generation == snapshot.generation && previous.ProbeEpoch <= probeEpoch {
		m.health[binding.ID] = health
	}
	m.mu.Unlock()
	return health
}

func (m *Manager) storeBoundHealth(snapshot *configSnapshot, binding Binding, resolvedURL string, result TraceResult, probeEpoch uint64) (nodeHealth, bool) {
	health := m.newHealth(snapshot, binding, resolvedURL, result, probeEpoch)
	m.mu.Lock()
	currentSnapshot := m.config.Load()
	currentBinding, bound := m.bindings[binding.AuthID]
	previous := m.health[binding.ID]
	if currentSnapshot != nil && currentSnapshot.generation == snapshot.generation && bound && currentBinding.ID == binding.ID && previous.ProbeEpoch <= probeEpoch {
		m.health[binding.ID] = health
		m.mu.Unlock()
		return health, true
	}
	m.mu.Unlock()
	return health, false
}

func (m *Manager) newHealth(snapshot *configSnapshot, binding Binding, resolvedURL string, result TraceResult, probeEpoch uint64) nodeHealth {
	interval := time.Duration(internalconfig.DefaultProxyPoolCheckIntervalSeconds) * time.Second
	if pool, exists := snapshot.pools[strings.ToLower(binding.Pool)]; exists && pool.config.CheckIntervalSeconds > 0 {
		interval = time.Duration(pool.config.CheckIntervalSeconds) * time.Second
	}
	health := nodeHealth{
		TraceResult: result,
		Pool:        binding.Pool,
		Entry:       binding.Entry,
		BindingID:   binding.ID,
		MaskedURL:   proxyutil.MaskProxyURL(resolvedURL),
		RetryAfter:  m.now().Add(interval),
		Generation:  snapshot.generation,
		ProbeEpoch:  probeEpoch,
	}
	if health.CheckedAt.IsZero() {
		health.CheckedAt = m.now().UTC()
	}
	return health
}

func (m *Manager) deleteHealth(bindingID string) {
	if m == nil || bindingID == "" {
		return
	}
	m.mu.Lock()
	delete(m.health, bindingID)
	m.mu.Unlock()
}

// ReportFailure implements coreauth.ProxyResolver. Only proxy infrastructure
// failures are converted; provider and request errors remain unchanged.
func (m *Manager) ReportFailure(ctx context.Context, auth *coreauth.Auth, err error) error {
	if err == nil || auth == nil || auth.EffectiveProxyBindingID() == "" {
		return err
	}
	var unavailable *UnavailableError
	if errors.As(err, &unavailable) {
		return err
	}
	if ctx != nil && ctx.Err() != nil {
		return err
	}
	definitive := isProxyInfrastructureError(err)
	ambiguous := isAmbiguousProxyInfrastructureError(err)
	if !definitive && !ambiguous {
		return err
	}
	bindingID := auth.EffectiveProxyBindingID()
	m.mu.RLock()
	binding, exists := m.bindings[auth.ID]
	m.mu.RUnlock()
	if !exists || binding.ID != bindingID {
		return err
	}
	snapshot := m.snapshot()
	if snapshot == nil {
		return err
	}
	resolvedURL, valid := m.bindingURL(snapshot, binding)
	if !valid {
		resolvedURL = auth.EffectiveProxyURL()
	}
	if ambiguous && !definitive {
		probeEpoch := m.nextProbeEpoch()
		result := m.check(ctx, resolvedURL)
		if ctx != nil && ctx.Err() != nil {
			return err
		}
		health, stored := m.storeBoundHealth(snapshot, binding, resolvedURL, result, probeEpoch)
		if !stored {
			return err
		}
		if result.OK {
			return err
		}
		return &UnavailableError{Pool: binding.Pool, RetryTime: health.RetryAfter, Cause: err}
	}
	probeEpoch := m.nextProbeEpoch()
	result := TraceResult{CheckedAt: m.now().UTC(), Error: "request_failed", Message: "proxy request failed"}
	health, stored := m.storeBoundHealth(snapshot, binding, resolvedURL, result, probeEpoch)
	if !stored {
		return err
	}
	log.WithFields(log.Fields{
		"auth_id": auth.ID,
		"pool":    binding.Pool,
		"entry":   binding.Entry,
		"proxy":   proxyutil.MaskProxyURL(resolvedURL),
	}).Debug("marked proxy binding unhealthy after request failure")
	return &UnavailableError{Pool: binding.Pool, RetryTime: health.RetryAfter, Cause: err}
}

func isProxyInfrastructureError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	type statusError interface{ StatusCode() int }
	var status statusError
	if errors.As(err, &status) {
		if status.StatusCode() == http.StatusProxyAuthRequired {
			return true
		}
	}
	type proxyInfrastructureError interface {
		ProxyInfrastructureError() bool
	}
	var proxyFailure proxyInfrastructureError
	if errors.As(err, &proxyFailure) && proxyFailure.ProxyInfrastructureError() {
		return true
	}
	message := strings.ToLower(err.Error())
	for _, marker := range []string{
		"proxyconnect", "proxy authentication", "proxy tunnel", "socks connect",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func isAmbiguousProxyInfrastructureError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	for _, marker := range []string{
		"connection refused", "connection reset", "no such host", "network is unreachable",
		"tls handshake", "use of closed network connection", "unexpected eof", "bad gateway", "forbidden",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func (m *Manager) bindingURL(snapshot *configSnapshot, binding Binding) (string, bool) {
	pool, exists := snapshot.pools[strings.ToLower(binding.Pool)]
	if !exists {
		return "", false
	}
	return resolveBindingURL(pool, binding)
}

func (m *Manager) saveBinding(binding Binding) error {
	if strings.TrimSpace(binding.AuthID) == "" || strings.TrimSpace(binding.ID) == "" {
		return errors.New("proxy binding is incomplete")
	}
	m.persistMu.Lock()
	defer m.persistMu.Unlock()
	m.mu.Lock()
	previous, hadPrevious := m.bindings[binding.AuthID]
	m.bindings[binding.AuthID] = cloneBinding(binding)
	snapshot := cloneBindings(m.bindings)
	m.mu.Unlock()
	if errPersist := m.persistBindings(snapshot); errPersist != nil {
		m.mu.Lock()
		if current, exists := m.bindings[binding.AuthID]; exists && current.ID == binding.ID {
			if hadPrevious {
				m.bindings[binding.AuthID] = previous
			} else {
				delete(m.bindings, binding.AuthID)
			}
		}
		m.mu.Unlock()
		return errPersist
	}
	if hadPrevious && previous.ID != binding.ID {
		m.deleteHealth(previous.ID)
	}
	return nil
}

func (m *Manager) saveBindingForSnapshot(snapshot *configSnapshot, binding Binding) error {
	if snapshot == nil {
		return errProxyConfigurationChanged
	}
	m.configMu.RLock()
	defer m.configMu.RUnlock()
	if m.config.Load() != snapshot {
		return errProxyConfigurationChanged
	}
	return m.saveBinding(binding)
}

func cloneBinding(binding Binding) Binding {
	binding.PlaceholderValues = append([]string(nil), binding.PlaceholderValues...)
	return binding
}

func cloneBindings(input map[string]Binding) map[string]Binding {
	out := make(map[string]Binding, len(input))
	for authID, binding := range input {
		out[authID] = cloneBinding(binding)
	}
	return out
}

func (m *Manager) loadBindings() error {
	if m == nil || m.statePath == "" {
		return nil
	}
	data, errRead := os.ReadFile(m.statePath)
	if errors.Is(errRead, os.ErrNotExist) {
		return nil
	}
	if errRead != nil {
		return fmt.Errorf("read proxy bindings: %w", errRead)
	}
	var state bindingStateFile
	if errDecode := json.Unmarshal(data, &state); errDecode != nil {
		return fmt.Errorf("decode proxy bindings: %w", errDecode)
	}
	if state.Version != bindingStateVersion {
		return fmt.Errorf("unsupported proxy binding state version %d", state.Version)
	}
	for authID, binding := range state.Bindings {
		binding.AuthID = strings.TrimSpace(binding.AuthID)
		if binding.AuthID == "" {
			binding.AuthID = strings.TrimSpace(authID)
		}
		if binding.AuthID == "" || binding.ID == "" || binding.Pool == "" || binding.Entry == "" {
			continue
		}
		m.bindings[binding.AuthID] = cloneBinding(binding)
	}
	return nil
}

func (m *Manager) persistBindings(bindings map[string]Binding) error {
	if m == nil || m.statePath == "" {
		return nil
	}
	state := bindingStateFile{Version: bindingStateVersion, Bindings: bindings}
	data, errMarshal := json.MarshalIndent(state, "", "  ")
	if errMarshal != nil {
		return errMarshal
	}
	directory := filepath.Dir(m.statePath)
	if errMkdir := os.MkdirAll(directory, 0o700); errMkdir != nil {
		return fmt.Errorf("create proxy binding directory: %w", errMkdir)
	}
	if errChmod := os.Chmod(directory, 0o700); errChmod != nil {
		return fmt.Errorf("secure proxy binding directory: %w", errChmod)
	}
	temp, errCreate := os.CreateTemp(directory, ".proxy-bindings-*")
	if errCreate != nil {
		return fmt.Errorf("create proxy binding temporary file: %w", errCreate)
	}
	tempName := temp.Name()
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = os.Remove(tempName)
		}
	}()
	if errChmod := temp.Chmod(0o600); errChmod != nil {
		_ = temp.Close()
		return fmt.Errorf("secure proxy binding temporary file: %w", errChmod)
	}
	if _, errWrite := temp.Write(data); errWrite != nil {
		_ = temp.Close()
		return fmt.Errorf("write proxy bindings: %w", errWrite)
	}
	if errSync := temp.Sync(); errSync != nil {
		_ = temp.Close()
		return fmt.Errorf("sync proxy bindings: %w", errSync)
	}
	if errClose := temp.Close(); errClose != nil {
		return fmt.Errorf("close proxy bindings: %w", errClose)
	}
	if errRename := os.Rename(tempName, m.statePath); errRename != nil {
		return fmt.Errorf("replace proxy bindings: %w", errRename)
	}
	removeTemp = false
	directoryHandle, errOpenDirectory := os.Open(directory)
	if errOpenDirectory != nil {
		log.WithError(errOpenDirectory).Warn("failed to open proxy binding directory for sync")
		return nil
	}
	if errSyncDirectory := directoryHandle.Sync(); errSyncDirectory != nil {
		log.WithError(errSyncDirectory).Warn("failed to sync proxy binding directory")
	}
	if errCloseDirectory := directoryHandle.Close(); errCloseDirectory != nil {
		log.WithError(errCloseDirectory).Debug("failed to close proxy binding directory")
	}
	return nil
}

// SortedBindings returns a stable internal snapshot for health and tests.
func (m *Manager) SortedBindings() []Binding {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	bindings := make([]Binding, 0, len(m.bindings))
	for _, binding := range m.bindings {
		bindings = append(bindings, cloneBinding(binding))
	}
	m.mu.RUnlock()
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].AuthID < bindings[j].AuthID })
	return bindings
}

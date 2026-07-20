package proxypool

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
	log "github.com/sirupsen/logrus"
)

func checkProxyTrace(ctx context.Context, proxyURL string) TraceResult {
	result := proxyutil.CheckTrace(ctx, proxyURL)
	return TraceResult{
		OK:        result.OK,
		IP:        result.IP,
		Location:  result.Location,
		HTTP:      result.HTTP,
		TLS:       result.TLS,
		Colo:      result.Colo,
		ElapsedMS: result.Elapsed.Milliseconds(),
		CheckedAt: time.Now().UTC(),
		Error:     result.Error,
		Message:   result.Message,
	}
}

// Start launches health checks for currently bound nodes only.
func (m *Manager) Start(parent context.Context) {
	if m == nil {
		return
	}
	if parent == nil {
		parent = context.Background()
	}
	m.lifecycleMu.Lock()
	if m.cancel != nil {
		m.lifecycleMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	m.cancel = cancel
	m.done = done
	m.lifecycleMu.Unlock()
	go func() {
		defer close(done)
		timer := time.NewTimer(0)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				m.checkBoundNodes(ctx)
				timer.Reset(m.nextBoundCheckDelay())
			}
		}
	}()
}

// Stop cancels and waits for background health checks.
func (m *Manager) Stop() {
	if m == nil {
		return
	}
	m.lifecycleMu.Lock()
	cancel := m.cancel
	done := m.done
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	m.cancel = nil
	m.done = nil
	m.lifecycleMu.Unlock()
}

func (m *Manager) checkBoundNodes(ctx context.Context) {
	snapshot := m.snapshot()
	if snapshot == nil {
		return
	}
	bindings := m.SortedBindings()
	valid, remove := m.validBindings(snapshot, bindings)
	if len(remove) > 0 {
		if errRemove := m.removeBindings(remove); errRemove != nil {
			log.WithError(errRemove).Warn("failed to prune stale proxy bindings")
		}
	}
	now := m.now()
	type checkItem struct {
		binding Binding
		url     string
	}
	items := make([]checkItem, 0, len(valid))
	for _, binding := range valid {
		resolvedURL, ok := m.bindingURL(snapshot, binding)
		if !ok {
			continue
		}
		m.mu.RLock()
		health, known := m.health[binding.ID]
		m.mu.RUnlock()
		if known && health.Generation == snapshot.generation && now.Before(health.RetryAfter) {
			continue
		}
		items = append(items, checkItem{binding: binding, url: resolvedURL})
	}
	m.runChecks(ctx, len(items), func(index int) {
		item := items[index]
		probeEpoch := m.nextProbeEpoch()
		result := m.check(ctx, item.url)
		if ctx != nil && ctx.Err() != nil {
			return
		}
		m.storeBoundHealth(snapshot, item.binding, item.url, result, probeEpoch)
	})
}

func (m *Manager) nextBoundCheckDelay() time.Duration {
	snapshot := m.snapshot()
	if snapshot == nil {
		return backgroundCheckMaxWait
	}
	now := m.now()
	delay := backgroundCheckMaxWait
	for _, binding := range m.SortedBindings() {
		if _, valid := m.bindingURL(snapshot, binding); !valid {
			continue
		}
		m.mu.RLock()
		health, known := m.health[binding.ID]
		m.mu.RUnlock()
		if !known || health.Generation != snapshot.generation || !health.RetryAfter.After(now) {
			return time.Millisecond
		}
		if candidate := health.RetryAfter.Sub(now); candidate < delay {
			delay = candidate
		}
	}
	if delay < time.Millisecond {
		return time.Millisecond
	}
	return delay
}

type bindingRemovalCandidate struct {
	AuthID    string
	BindingID string
}

type bindingAuthSnapshot struct {
	auths            map[string]*coreauth.Auth
	linkedSourceUIDs map[string]map[string]struct{}
}

func buildBindingAuthSnapshot(source AuthSource) bindingAuthSnapshot {
	snapshot := bindingAuthSnapshot{
		auths:            make(map[string]*coreauth.Auth),
		linkedSourceUIDs: make(map[string]map[string]struct{}),
	}
	if source == nil {
		return snapshot
	}
	auths := source.List()
	for _, auth := range auths {
		if auth != nil {
			snapshot.auths[auth.ID] = auth
		}
	}
	for _, auth := range auths {
		if auth == nil {
			continue
		}
		sourceID := coreauth.ChatGPTWebLinkedSourceID(auth)
		sourceUID := coreauth.ChatGPTWebLinkedSourceUID(auth)
		if sourceID == "" || sourceUID == "" {
			continue
		}
		if snapshot.linkedSourceUIDs[sourceID] == nil {
			snapshot.linkedSourceUIDs[sourceID] = make(map[string]struct{})
		}
		snapshot.linkedSourceUIDs[sourceID][sourceUID] = struct{}{}
		if existingSource := snapshot.auths[sourceID]; existingSource != nil &&
			coreauth.ChatGPTWebCredentialUID(existingSource) == sourceUID &&
			coreauth.ChatGPTWebAuthRetainedForDependents(existingSource) {
			retainedSource := existingSource.Clone()
			retainedSource.Disabled = false
			retainedSource.Status = coreauth.StatusActive
			snapshot.auths[sourceID] = retainedSource
		}
	}
	return snapshot
}

func (m *Manager) validBindings(snapshot *configSnapshot, bindings []Binding) ([]Binding, []bindingRemovalCandidate) {
	m.mu.RLock()
	source := m.auths
	m.mu.RUnlock()
	if source == nil {
		return bindings, nil
	}
	authSnapshot := buildBindingAuthSnapshot(source)
	valid := make([]Binding, 0, len(bindings))
	remove := make([]bindingRemovalCandidate, 0)
	for _, binding := range bindings {
		if !m.bindingValidInAuthSnapshot(snapshot, binding, authSnapshot, m.bindingLeaseActive(binding.AuthID)) {
			remove = append(remove, bindingRemovalCandidate{AuthID: binding.AuthID, BindingID: binding.ID})
			continue
		}
		valid = append(valid, binding)
	}
	return valid, remove
}

func (m *Manager) bindingValidInAuthSnapshot(snapshot *configSnapshot, binding Binding, authSnapshot bindingAuthSnapshot, leaseActive bool) bool {
	auth := authSnapshot.auths[binding.AuthID]
	if auth == nil {
		if leaseActive {
			_, validURL := m.bindingURL(snapshot, binding)
			return validURL
		}
		credentialUID := strings.TrimSpace(binding.CredentialUID)
		_, linked := authSnapshot.linkedSourceUIDs[binding.AuthID][credentialUID]
		_, validURL := m.bindingURL(snapshot, binding)
		return credentialUID != "" && linked && validURL
	}
	return m.bindingValidWithLease(snapshot, binding, auth, leaseActive)
}

func (m *Manager) bindingValid(snapshot *configSnapshot, binding Binding, auth *coreauth.Auth) bool {
	return m.bindingValidWithLease(snapshot, binding, auth, m.bindingLeaseActive(binding.AuthID))
}

func (m *Manager) bindingValidWithLease(snapshot *configSnapshot, binding Binding, auth *coreauth.Auth, leaseActive bool) bool {
	if snapshot == nil {
		return false
	}
	if auth != nil {
		credentialUID := coreauth.ChatGPTWebCredentialUID(auth)
		if !bindingCredentialGenerationMatches(binding.CredentialUID, credentialUID) {
			return false
		}
	}
	if leaseActive {
		_, valid := m.bindingURL(snapshot, binding)
		return valid
	}
	if auth == nil {
		return false
	}
	if auth.Disabled || auth.Status == coreauth.StatusDisabled || strings.TrimSpace(auth.ProxyURL) != "" {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(auth.Provider), "aistudio") {
		return false
	}
	poolName, matched := internalconfig.MatchProxyRule(snapshot.rules, auth.Provider, authPriority(auth))
	if !matched || !strings.EqualFold(poolName, binding.Pool) {
		return false
	}
	_, valid := m.bindingURL(snapshot, binding)
	return valid
}

func bindingCredentialGenerationMatches(bindingUID, credentialUID string) bool {
	bindingUID = strings.TrimSpace(bindingUID)
	credentialUID = strings.TrimSpace(credentialUID)
	if credentialUID == "" {
		return bindingUID == ""
	}
	return bindingUID == "" || bindingUID == credentialUID
}

func (m *Manager) removeBindings(candidates []bindingRemovalCandidate) error {
	if len(candidates) == 0 {
		return nil
	}
	m.mu.RLock()
	source := m.auths
	m.mu.RUnlock()
	if source == nil {
		return nil
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].AuthID < candidates[j].AuthID })
	unique := candidates[:0]
	for _, candidate := range candidates {
		if candidate.AuthID == "" || candidate.BindingID == "" || len(unique) > 0 && unique[len(unique)-1].AuthID == candidate.AuthID {
			continue
		}
		unique = append(unique, candidate)
	}
	if len(unique) == 0 {
		return nil
	}
	unlocks := make([]func(), 0, len(unique))
	for _, candidate := range unique {
		unlock, errLock := m.lockBinding(context.Background(), candidate.AuthID)
		if errLock != nil {
			for index := len(unlocks) - 1; index >= 0; index-- {
				unlocks[index]()
			}
			return errLock
		}
		unlocks = append(unlocks, unlock)
	}
	defer func() {
		for index := len(unlocks) - 1; index >= 0; index-- {
			unlocks[index]()
		}
	}()
	authSnapshot := buildBindingAuthSnapshot(source)

	m.configMu.RLock()
	defer m.configMu.RUnlock()
	snapshot := m.snapshot()
	m.persistMu.Lock()
	defer m.persistMu.Unlock()
	m.leaseMu.RLock()
	m.mu.Lock()
	previous := cloneBindings(m.bindings)
	removedBindingIDs := make([]string, 0, len(unique))
	for _, candidate := range unique {
		binding, exists := m.bindings[candidate.AuthID]
		leaseActive := m.leases[strings.TrimSpace(candidate.AuthID)] > 0
		if !exists || binding.ID != candidate.BindingID || m.bindingValidInAuthSnapshot(snapshot, binding, authSnapshot, leaseActive) {
			continue
		}
		removedBindingIDs = append(removedBindingIDs, binding.ID)
		delete(m.bindings, candidate.AuthID)
	}
	if len(removedBindingIDs) == 0 {
		m.mu.Unlock()
		m.leaseMu.RUnlock()
		return nil
	}
	next := cloneBindings(m.bindings)
	m.mu.Unlock()
	m.leaseMu.RUnlock()
	if errPersist := m.persistBindings(next); errPersist != nil {
		m.mu.Lock()
		m.bindings = previous
		m.mu.Unlock()
		return errPersist
	}
	m.mu.Lock()
	for _, bindingID := range removedBindingIDs {
		delete(m.health, bindingID)
	}
	m.mu.Unlock()
	return nil
}

func (m *Manager) runChecks(ctx context.Context, count int, run func(int)) {
	if count <= 0 || run == nil {
		return
	}
	semaphore := make(chan struct{}, maxConcurrentChecks)
	var wait sync.WaitGroup
	for index := 0; index < count; index++ {
		if ctx == nil {
			semaphore <- struct{}{}
		} else {
			select {
			case <-ctx.Done():
				wait.Wait()
				return
			case semaphore <- struct{}{}:
			}
		}
		wait.Add(1)
		go func(checkIndex int) {
			defer wait.Done()
			defer func() { <-semaphore }()
			run(checkIndex)
		}(index)
	}
	wait.Wait()
}

// PoolStatuses returns summaries for all configured pools.
func (m *Manager) PoolStatuses() []PoolStatus {
	snapshot := m.snapshot()
	if snapshot == nil {
		return nil
	}
	statuses := make(map[string]*PoolStatus, len(snapshot.pools))
	for _, pool := range snapshot.pools {
		status := &PoolStatus{Name: pool.config.Name}
		statuses[strings.ToLower(pool.config.Name)] = status
	}
	m.mu.RLock()
	source := m.auths
	m.mu.RUnlock()
	for _, binding := range m.SortedBindings() {
		status := statuses[strings.ToLower(binding.Pool)]
		if status == nil || !bindingMatchesCurrentAuthGeneration(source, binding) {
			continue
		}
		status.BindingCount++
		resolvedURL, valid := m.bindingURL(snapshot, binding)
		if !valid {
			status.UnknownCount++
			continue
		}
		bindingStatus := m.bindingStatus(snapshot, binding, resolvedURL, source)
		status.Bindings = append(status.Bindings, bindingStatus)
		if bindingStatus.Healthy == nil {
			status.UnknownCount++
			continue
		}
		if *bindingStatus.Healthy {
			status.HealthyCount++
		} else {
			status.UnhealthyCount++
		}
		if bindingStatus.LastCheckAt != nil && (status.LastCheckAt == nil || bindingStatus.LastCheckAt.After(*status.LastCheckAt)) {
			checkedAt := *bindingStatus.LastCheckAt
			status.LastCheckAt = &checkedAt
		}
	}
	out := make([]PoolStatus, 0, len(statuses))
	for _, status := range statuses {
		out = append(out, *status)
	}
	sort.Slice(out, func(i, j int) bool { return strings.ToLower(out[i].Name) < strings.ToLower(out[j].Name) })
	return out
}

// BindingStatuses returns masked binding details for current credentials.
func (m *Manager) BindingStatuses() []BindingStatus {
	snapshot := m.snapshot()
	if snapshot == nil {
		return nil
	}
	m.mu.RLock()
	source := m.auths
	m.mu.RUnlock()
	statuses := make([]BindingStatus, 0)
	for _, binding := range m.SortedBindings() {
		resolvedURL, valid := m.bindingURL(snapshot, binding)
		if !valid {
			continue
		}
		if !bindingMatchesCurrentAuthGeneration(source, binding) {
			continue
		}
		status := m.bindingStatus(snapshot, binding, resolvedURL, source)
		statuses = append(statuses, status)
	}
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].AuthID < statuses[j].AuthID })
	return statuses
}

func bindingMatchesCurrentAuthGeneration(source AuthSource, binding Binding) bool {
	if source == nil {
		return true
	}
	auth, exists := source.GetByID(binding.AuthID)
	if !exists || auth == nil {
		return true
	}
	return bindingCredentialGenerationMatches(binding.CredentialUID, coreauth.ChatGPTWebCredentialUID(auth))
}

func (m *Manager) bindingStatus(snapshot *configSnapshot, binding Binding, resolvedURL string, source AuthSource) BindingStatus {
	status := BindingStatus{
		CredentialUID: binding.CredentialUID,
		AuthID:        binding.AuthID,
		Pool:          binding.Pool,
		Entry:         binding.Entry,
		Port:          binding.Port,
		BindingID:     binding.ID,
		ProxyURL:      proxyutil.MaskProxyURL(resolvedURL),
		BoundAt:       binding.BoundAt,
	}
	if source != nil {
		if auth, ok := source.GetByID(binding.AuthID); ok && auth != nil {
			status.AuthIndex = auth.EnsureIndex()
			status.Provider = auth.Provider
		}
	}
	m.mu.RLock()
	health, known := m.health[binding.ID]
	m.mu.RUnlock()
	if !known || health.Generation != snapshot.generation {
		return status
	}
	healthy := health.OK
	status.Healthy = &healthy
	checkedAt := health.CheckedAt
	status.LastCheckAt = &checkedAt
	nextCheck := health.RetryAfter
	status.NextCheckAt = &nextCheck
	status.IP = health.IP
	status.Location = health.Location
	status.ElapsedMS = health.ElapsedMS
	status.Error = health.Error
	status.ErrorMessage = health.Message
	return status
}

// CheckPool checks bound nodes and up to sample unbound candidates.
func (m *Manager) CheckPool(ctx context.Context, poolName string, sample int) ([]CheckResult, error) {
	snapshot := m.snapshot()
	if snapshot == nil {
		return nil, errors.New("proxy configuration unavailable")
	}
	pool, exists := snapshot.pools[strings.ToLower(strings.TrimSpace(poolName))]
	if !exists {
		return nil, errors.New("proxy pool not found")
	}
	m.mu.RLock()
	source := m.auths
	m.mu.RUnlock()
	type checkItem struct {
		binding Binding
		url     string
		bound   bool
	}
	items := make([]checkItem, 0, sample+4)
	seen := make(map[string]struct{})
	for _, binding := range m.SortedBindings() {
		if !strings.EqualFold(binding.Pool, pool.config.Name) {
			continue
		}
		if !bindingMatchesCurrentAuthGeneration(source, binding) {
			continue
		}
		resolvedURL, valid := resolveBindingURL(pool, binding)
		if !valid {
			continue
		}
		seen[proxyURLFingerprint(resolvedURL)] = struct{}{}
		items = append(items, checkItem{binding: binding, url: resolvedURL, bound: true})
	}
	targetCount := len(items) + sample
	for attempts := 0; len(items) < targetCount && attempts < sample*10+10; attempts++ {
		binding, resolvedURL, errCandidate := m.randomBinding(pool, "")
		if errCandidate != nil {
			return nil, errCandidate
		}
		fingerprint := proxyURLFingerprint(resolvedURL)
		if _, duplicate := seen[fingerprint]; duplicate {
			continue
		}
		seen[fingerprint] = struct{}{}
		items = append(items, checkItem{binding: binding, url: resolvedURL})
	}
	results := make([]CheckResult, len(items))
	m.runChecks(ctx, len(items), func(index int) {
		item := items[index]
		probeEpoch := m.nextProbeEpoch()
		trace := m.check(ctx, item.url)
		if ctx != nil && ctx.Err() != nil {
			return
		}
		if item.bound {
			m.storeBoundHealth(snapshot, item.binding, item.url, trace, probeEpoch)
		}
		results[index] = CheckResult{
			Pool:      pool.config.Name,
			Entry:     item.binding.Entry,
			Port:      item.binding.Port,
			BindingID: item.binding.ID,
			Bound:     item.bound,
			ProxyURL:  proxyutil.MaskProxyURL(item.url),
			OK:        trace.OK,
			IP:        trace.IP,
			Location:  trace.Location,
			HTTP:      trace.HTTP,
			TLS:       trace.TLS,
			Colo:      trace.Colo,
			ElapsedMS: trace.ElapsedMS,
			CheckedAt: trace.CheckedAt,
			Error:     trace.Error,
			Message:   trace.Message,
		}
	})
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Bound != results[j].Bound {
			return results[i].Bound
		}
		if results[i].Entry != results[j].Entry {
			return results[i].Entry < results[j].Entry
		}
		return results[i].Port < results[j].Port
	})
	return results, nil
}

// Rebind replaces bindings only after a healthy alternative has been found.
func (m *Manager) Rebind(ctx context.Context, authIDs []string) []RebindResult {
	m.mu.RLock()
	source := m.auths
	m.mu.RUnlock()
	results := make([]RebindResult, 0, len(authIDs))
	seen := make(map[string]struct{}, len(authIDs))
	for _, rawID := range authIDs {
		authID := strings.TrimSpace(rawID)
		if authID == "" {
			continue
		}
		if _, duplicate := seen[authID]; duplicate {
			continue
		}
		seen[authID] = struct{}{}
		result := RebindResult{AuthID: authID}
		if source == nil {
			result.Error = "auth source unavailable"
			result.HTTPStatus = http.StatusServiceUnavailable
			results = append(results, result)
			continue
		}
		auth, exists := source.GetByID(authID)
		if !exists || auth == nil {
			result.Error = "auth not found"
			result.HTTPStatus = http.StatusNotFound
			results = append(results, result)
			continue
		}
		if strings.TrimSpace(auth.ProxyURL) != "" {
			result.Error = "auth has an explicit proxy"
			result.HTTPStatus = http.StatusConflict
			results = append(results, result)
			continue
		}
		if strings.EqualFold(strings.TrimSpace(auth.Provider), "aistudio") {
			result.Error = "AIStudio relay cannot use server-side proxy pools"
			result.HTTPStatus = http.StatusServiceUnavailable
			results = append(results, result)
			continue
		}
		snapshot := m.snapshot()
		poolName, matched := internalconfig.MatchProxyRule(snapshot.rules, auth.Provider, authPriority(auth))
		if !matched {
			result.Error = "auth does not match a proxy rule"
			result.HTTPStatus = http.StatusConflict
			results = append(results, result)
			continue
		}
		unlock, errLock := m.lockBinding(ctx, authID)
		if errLock != nil {
			result.Error = errLock.Error()
			result.HTTPStatus = http.StatusRequestTimeout
			results = append(results, result)
			continue
		}
		resolved, errResolve := m.resolvePoolBinding(ctx, snapshot, authID, coreauth.ChatGPTWebCredentialUID(auth), poolName, true)
		unlock()
		if errResolve != nil {
			var unavailable *UnavailableError
			if errors.As(errResolve, &unavailable) {
				result.Error = unavailable.Message()
			} else {
				result.Error = errResolve.Error()
			}
			result.HTTPStatus = http.StatusServiceUnavailable
			results = append(results, result)
			continue
		}
		result.Updated = true
		for _, status := range m.BindingStatuses() {
			if status.AuthID == authID && status.BindingID == resolved.BindingID {
				copyStatus := status
				result.Binding = &copyStatus
				break
			}
		}
		results = append(results, result)
	}
	return results
}

// CheckNow is used by tests and operators that need an immediate bound-node pass.
func (m *Manager) CheckNow(ctx context.Context) { m.checkBoundNodes(ctx) }

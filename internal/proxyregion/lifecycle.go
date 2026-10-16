package proxyregion

import (
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func (j *job) complete() { j.finish.Do(func() { close(j.done); j.cancel() }) }
func (s *Service) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
func (s *Service) startWorkersLocked() {
	for s.workers < s.settings.LookupConcurrency {
		index := s.workers
		s.workers++
		s.wg.Add(1)
		go s.worker(index)
	}
}
func (s *Service) requestObservation(r Route) {
	s.mu.Lock()
	d := s.demandObservation
	s.mu.Unlock()
	if r.valid() && d != nil {
		d(r)
	}
}
func (s *Service) dispatchLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		case <-s.wake:
		}
		s.dispatch()
	}
}
func (s *Service) dispatch() {
	s.mu.Lock()
	limit := min(256, max(1, s.settings.LookupConcurrency*2))
	urgent, normal := []*job{}, []*job{}
	for e := s.jobs.Front(); e != nil; e = e.Next() {
		j := e.Value.(*job)
		if j.queued || j.running {
			continue
		}
		if j.urgent {
			if len(urgent) < limit {
				urgent = append(urgent, j)
			}
		} else if len(normal) < limit {
			normal = append(normal, j)
		}
		if len(urgent) == limit && len(normal) == limit {
			break
		}
	}
	batch := make([]*job, 0, limit)
	u, n := 0, 0
	for i := 0; i < limit && (u < len(urgent) || n < len(normal)); i++ {
		var j *job
		if u < len(urgent) && (i%4 != 3 || n == len(normal)) {
			j = urgent[u]
			u++
		} else {
			j = normal[n]
			n++
		}
		batch = append(batch, j)
		s.jobs.MoveToBack(j.element)
		j.queued = true
	}

	s.mu.Unlock()
	for _, j := range batch {
		s.mu.Lock()
		j.queued = false
		s.mu.Unlock()
		if j.ctx.Err() != nil {
			continue
		}
		observation := s.readObservation(j.route)
		if observation.IP == "" {
			s.mu.Lock()
			hasDetector := s.demandObservation != nil
			s.mu.Unlock()
			if !hasDetector {
				s.finishJob(j, lookupResult{Reason: "exit_unobserved"})
				continue
			}
			s.requestObservation(j.route)
			if observation.Reason != "" && s.now().Before(observation.RetryAt) {
				s.finishJob(j, lookupResult{Reason: observation.Reason, Retry: max(0, observation.RetryAt.Sub(s.now()))})
			}
			continue
		}
		s.mu.Lock()
		if s.pending[j.key] == j && !j.queued && !j.running && j.ctx.Err() == nil && len(s.queue) < s.settings.LookupConcurrency {
			s.pinLocked(j.route, observation.IP)
			select {
			case s.queue <- j:
				j.queued = true
			default:
			}
		}
		s.mu.Unlock()
	}
}
func (s *Service) pinLocked(r Route, ip string) {
	// A missing temporary observation does not prove the active exit changed.
	if ip == "" {
		return
	}
	el := s.subscriptions[r.Key()]
	if el == nil {
		return
	}
	v := el.Value.(*subscription)
	if _, anonymous := v.Owners[""]; anonymous && len(v.Owners) == 1 {
		return
	}
	if v.IPRef == ip {
		return
	}
	if v.IPRef != "" {
		s.ips.unpin(v.IPRef)
	}
	v.IPRef = ip
	if ip != "" {
		s.ips.pin(ip)
	}
	s.ips.resize(s.settings.IPCacheSize)
}

// Register records an existing route without allocating a binding or probing it.
func (s *Service) Register(r Route) {
	if s == nil || !r.valid() {
		return
	}
	o := s.readObservation(r)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.subscribeLocked(r)
	s.pinLocked(r, o.IP)
}
func (s *Service) ForgetOwner(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, el := range s.subscriptions {
		v := el.Value.(*subscription)
		delete(v.Owners, id)
		if len(v.Owners) == 0 {
			s.removeSubscriptionLocked(key)
		}
	}
	for key := range s.owners {
		if strings.HasPrefix(key, id+"\x00") {
			delete(s.owners, key)
		}
	}
	s.ips.resize(s.settings.IPCacheSize)
}
func (s *Service) SetReconciler(f func()) { s.mu.Lock(); s.reconcile = f; s.mu.Unlock() }
func (s *Service) SetManualSource(f func(Route) (config.ProxyRegionConfig, string)) {
	s.mu.Lock()
	s.manual = f
	s.mu.Unlock()
}
func (s *Service) Manual(r Route) (config.ProxyRegionConfig, string) {
	if s == nil || !r.valid() {
		return config.ProxyRegionConfig{}, ""
	}
	s.mu.Lock()
	f := s.manual
	s.mu.Unlock()
	if f == nil {
		return config.ProxyRegionConfig{}, ""
	}
	return f(r)
}
func (s *Service) Counts() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	active, idle, running := 0, 0, 0
	for ip := range s.ips.items {
		if s.ips.pins[ip] > 0 {
			active++
		} else {
			idle++
		}
	}
	for _, j := range s.pending {
		if j.running {
			running++
		}
	}
	return map[string]int{"active_ips": active, "idle_ips": idle, "pending": len(s.pending) - running, "running": running}
}

func (s *Service) PruneOwners(active func(string) bool) {
	s.mu.Lock()
	ids := map[string]bool{}
	for _, el := range s.subscriptions {
		for id := range el.Value.(*subscription).Owners {
			if id != "" {
				ids[id] = true
			}
		}
	}
	s.mu.Unlock()
	for id := range ids {
		if !active(id) {
			s.ForgetOwner(id)
		}
	}
}

func (s *Service) NotifyConsumers() {
	if s == nil {
		return
	}
	select {
	case s.consumerWake <- struct{}{}:
	default:
	}
}

func quotaKey(r Route) string { return (Route{ProxyURL: r.ProxyURL}).Key() }

// Observed updates references without reading back through the detector.
func (s *Service) Observed(r Route, ip string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.pinLocked(r, ip)
		s.signal()
	}
}

func (s *Service) SetManualSources(f func(Route) (string, string)) {
	s.mu.Lock()
	s.manualSources = f
	s.mu.Unlock()
}
func (s *Service) ManualSources(r Route) (string, string) {
	if s == nil {
		return "", ""
	}
	s.mu.Lock()
	f := s.manualSources
	s.mu.Unlock()
	if f != nil {
		return f(r)
	}
	_, source := s.Manual(r)
	return source, source
}

func ownerScopeKey(id, scope string) string {
	if scope == "" {
		scope = ChatGPTScope
	}
	return id + "\x00" + scope
}

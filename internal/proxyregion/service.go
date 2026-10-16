// Package proxyregion enriches proxy-manager exit observations with IP metadata.
package proxyregion

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
	"golang.org/x/sync/singleflight"
)

const ChatGPTScope = "https://chatgpt.com/cdn-cgi/trace"
const maxBody = 64 << 10

// Route carries the actual selected connection, not a second proxy binding.
type Route struct {
	ProxyURL  string
	BindingID string
	Scope     string
	OwnerID   string `json:"-"`
}

func (r Route) normalized() Route {
	r.ProxyURL = strings.TrimSpace(r.ProxyURL)
	if r.Scope == "" {
		r.Scope = ChatGPTScope
	}
	return r
}

// ScopeForTarget prevents an exit observed for another origin from being reused.
func ScopeForTarget(target string) string {
	u, err := url.Parse(target)
	if err != nil || u.Host == "" {
		return ChatGPTScope
	}
	switch u.Scheme {
	case "wss":
		u.Scheme = "https"
	case "ws":
		u.Scheme = "http"
	}
	return u.Scheme + "://" + u.Host + "/cdn-cgi/trace"
}
func (r Route) Key() string {
	b, _ := json.Marshal(r.normalized())
	v := sha256.Sum256(b)
	return hex.EncodeToString(v[:])
}
func (r Route) valid() bool {
	v, e := proxyutil.Parse(r.ProxyURL)
	return e == nil && v.Mode == proxyutil.ModeProxy
}

type Observation struct {
	Reason   string
	RetryAt  time.Time
	FailedAt time.Time
	IP       string
	Source   string
	At       time.Time
}
type Geo struct {
	IP       string    `json:"ip"`
	Country  string    `json:"country"`
	Region   string    `json:"region"`
	Timezone string    `json:"timezone"`
	Source   string    `json:"observation_source"`
	At       time.Time `json:"detected_at"`
}
type Snapshot struct {
	Geo
	RefreshMode       string    `json:"refresh_mode,omitempty"`
	ColdCacheMode     string    `json:"cold_cache_mode,omitempty"`
	RouteID           string    `json:"route_id"`
	SuggestedLanguage string    `json:"suggested_language,omitempty"`
	CacheState        string    `json:"cache_state"`
	CacheAgeSeconds   int64     `json:"cache_age_seconds"`
	RefreshState      string    `json:"refresh_state"`
	Reason            string    `json:"reason,omitempty"`
	RetryAfterSeconds int64     `json:"retry_after_seconds,omitempty"`
	ObservedAt        time.Time `json:"observed_at,omitempty"`
	LastFailure       time.Time `json:"last_failure,omitempty"`
	NextRetry         time.Time `json:"next_retry,omitempty"`
	TimezoneSource    string    `json:"timezone_source,omitempty"`
	LanguageSource    string    `json:"language_source,omitempty"`
}

// subscription retains consumers and cooldowns, not exit data. Exit observations
// are always read from the proxy manager; their lifetime follows its bindings.
type cooldown struct {
	until  time.Time
	reason string
}
type subscription struct {
	Route    Route
	Owners   map[string]struct{}
	RetryAt  time.Time
	Reason   string
	Failures int
	IPRef    string
	FailedAt time.Time
	revision uint64
}
type job struct {
	done       chan struct{}
	route      Route
	key        string
	generation uint64
	ctx        context.Context
	cancel     context.CancelFunc
	settings   config.ResolvedProxyIP
	running    bool
	queued     bool
	urgent     bool
	element    *list.Element
	finish     sync.Once
}
type regionLRUItem[V any] struct {
	key   string
	value V
}
type regionLRU[V any] struct {
	items     map[string]*list.Element
	order     *list.List
	pins      map[string]int
	idle      *list.List
	idleNodes map[string]*list.Element
}

func newRegionLRU[V any]() *regionLRU[V] {
	return &regionLRU[V]{items: make(map[string]*list.Element), order: list.New(), pins: map[string]int{}, idle: list.New(), idleNodes: map[string]*list.Element{}}
}
func (l *regionLRU[V]) get(key string, touch bool) (V, bool) {
	if e := l.items[key]; e != nil {
		if touch {
			l.order.MoveToFront(e)
			if idle := l.idleNodes[key]; idle != nil {
				l.idle.MoveToFront(idle)
			}
		}
		return e.Value.(regionLRUItem[V]).value, true
	}
	var zero V
	return zero, false
}
func (l *regionLRU[V]) put(key string, value V, capacity int) {
	if e := l.items[key]; e != nil {
		e.Value = regionLRUItem[V]{key, value}
		l.order.MoveToFront(e)
	} else {
		l.items[key] = l.order.PushFront(regionLRUItem[V]{key, value})
	}
	if l.pins[key] == 0 {
		if e := l.idleNodes[key]; e != nil {
			l.idle.MoveToFront(e)
		} else {
			l.idleNodes[key] = l.idle.PushFront(key)
		}
	}
	l.resize(capacity)
}
func (l *regionLRU[V]) pin(key string) {
	l.pins[key]++
	if e := l.idleNodes[key]; e != nil {
		l.idle.Remove(e)
		delete(l.idleNodes, key)
	}
}
func (l *regionLRU[V]) unpin(key string) {
	if key == "" {
		return
	}
	if l.pins[key] > 1 {
		l.pins[key]--
		return
	}
	delete(l.pins, key)
	if _, ok := l.items[key]; ok && l.idleNodes[key] == nil {
		l.idleNodes[key] = l.idle.PushFront(key)
	}
}
func (l *regionLRU[V]) resize(capacity int) {
	for l.idle.Len() > capacity {
		e := l.idle.Back()
		key := e.Value.(string)
		l.idle.Remove(e)
		delete(l.idleNodes, key)
		l.order.Remove(l.items[key])
		delete(l.items, key)
	}
}

// Service is shared by providers and owns only metadata caching and scheduling.
type Service struct {
	mu                                sync.Mutex
	settings                          config.ResolvedProxyIP
	ips                               *regionLRU[Geo]
	cooldowns                         *regionLRU[cooldown]
	subscriptions                     map[string]*list.Element
	order                             *list.List
	owners                            map[string]string
	pending                           map[string]*job
	queue                             chan *job
	jobs                              *list.List
	wake                              chan struct{}
	consumerWake                      chan struct{}
	workers                           int
	reconcile                         func()
	manual                            func(Route) (config.ProxyRegionConfig, string)
	manualSources                     func(Route) (string, string)
	ctx                               context.Context
	cancel                            context.CancelFunc
	generation                        uint64
	closed, started, schedulerStarted bool
	cursor                            string
	wg                                sync.WaitGroup
	group                             singleflight.Group
	now                               func() time.Time
	probe                             func(context.Context, string, string) ([]byte, int, string, error)
	observation                       func(Route) Observation
	demandObservation                 func(Route)
	ensureObservation                 func(context.Context, Route) Observation
	ownerActive                       func(string) bool
}

func New(parent context.Context, cfg config.ProxyIPConfig) *Service {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	return &Service{settings: cfg.Resolved(), ips: newRegionLRU[Geo](), cooldowns: newRegionLRU[cooldown](), subscriptions: map[string]*list.Element{}, order: list.New(), owners: map[string]string{}, pending: map[string]*job{}, queue: make(chan *job, 256), jobs: list.New(), wake: make(chan struct{}, 1), consumerWake: make(chan struct{}, 1), ctx: ctx, cancel: cancel, now: time.Now, probe: probeRegion}
}
func (s *Service) SetObservationSource(read func(Route) Observation, ensure func(context.Context, Route) Observation) {
	s.mu.Lock()
	s.observation = read
	s.ensureObservation = ensure
	s.mu.Unlock()
}
func (s *Service) SetObservationDemand(f func(Route)) {
	s.mu.Lock()
	s.demandObservation = f
	s.mu.Unlock()
}
func (s *Service) SetOwnerValidator(check func(string) bool) {
	s.mu.Lock()
	s.ownerActive = check
	s.mu.Unlock()
}
func (s *Service) UpdateConfig(cfg config.ProxyIPConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	next := cfg.Resolved()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if next.LookupURL != s.settings.LookupURL || next.LookupTimeoutSeconds != s.settings.LookupTimeoutSeconds {
		s.generation++
		for _, j := range s.pending {
			j.complete()
		}
		clear(s.pending)
		s.jobs.Init()
		if next.LookupURL != s.settings.LookupURL {
			s.ips = newRegionLRU[Geo]()
			for e := s.order.Front(); e != nil; e = e.Next() {
				e.Value.(*subscription).IPRef = ""
			}
			s.cooldowns = newRegionLRU[cooldown]()
			for e := s.order.Front(); e != nil; e = e.Next() {
				v := e.Value.(*subscription)
				v.RetryAt = time.Time{}
				v.Reason = ""
				v.Failures = 0
			}
		}
	}
	s.settings = next
	if s.started {
		s.startWorkersLocked()
	}
	s.ips.resize(next.IPCacheSize)
	s.cooldowns.resize(next.IPCacheSize)
	s.startSchedulerLocked()
	s.NotifyConsumers()
	return nil
}
func (s *Service) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.closed = true
	s.cancel()
	for _, j := range s.pending {
		j.cancel()
	}
	s.mu.Unlock()
	s.wg.Wait()
	s.mu.Lock()
	clear(s.pending)
	clear(s.subscriptions)
	clear(s.owners)
	s.order.Init()
	s.ips = newRegionLRU[Geo]()
	s.cooldowns = newRegionLRU[cooldown]()
	s.jobs.Init()
	s.mu.Unlock()
}
func (s *Service) readObservation(route Route) Observation {
	if s == nil {
		return Observation{}
	}
	s.mu.Lock()
	read := s.observation
	s.mu.Unlock()
	if read == nil {
		return Observation{}
	}
	v := read(route)
	v.IP = regionPublicIP(v.IP)
	return v
}
func (s *Service) Resolve(r Route) Snapshot { return s.snapshot(r, true) }
func (s *Service) Preview(r Route) Snapshot { return s.snapshot(r, false) }
func (s *Service) snapshot(route Route, enqueue bool) Snapshot {
	result := Snapshot{RouteID: route.Key(), CacheState: "missing", RefreshState: "idle"}
	if s == nil {
		result.Reason = "unavailable"
		return result
	}
	if !route.valid() {
		result.Reason = "proxy_required"
		return result
	}
	if enqueue {
		s.mu.Lock()
		demand := s.demandObservation
		s.mu.Unlock()
		if demand != nil {
			demand(route)
		}
	}
	observation := s.readObservation(route)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		result.Reason = "closed"
		return result
	}
	now := s.now()
	result.RefreshMode = s.settings.RefreshMode
	result.ColdCacheMode = s.settings.ColdCacheMode
	result.LastFailure = observation.FailedAt
	result.NextRetry = observation.RetryAt
	result.ObservedAt = observation.At
	result.Source = observation.Source
	result.Reason = observation.Reason
	result.IP = observation.IP
	if geo, ok := s.ips.get(observation.IP, enqueue); ok && observation.IP != "" {
		result.Geo = geo
		result.Source = observation.Source
		result.SuggestedLanguage = SuggestedLanguage(geo.Country)
		result.TimezoneSource = "ip_cache"
		result.LanguageSource = "country_mapping"
		result.CacheState = "fresh"
		result.CacheAgeSeconds = max(0, int64(now.Sub(geo.At).Seconds()))
		if now.Sub(geo.At) >= time.Duration(s.settings.IPCacheTTLSeconds)*time.Second {
			result.CacheState = "stale"
		}
	}
	if el := s.subscriptions[route.Key()]; el != nil {
		v := el.Value.(*subscription)
		if !v.FailedAt.IsZero() {
			result.LastFailure = v.FailedAt
		}
		if !v.RetryAt.IsZero() {
			result.NextRetry = v.RetryAt
		}
		if v.Reason != "" {
			result.Reason = v.Reason
		}
		if now.Before(v.RetryAt) {
			result.RetryAfterSeconds = max(1, int64(v.RetryAt.Sub(now).Seconds()))
		}
		if (result.Reason != "" || observation.Reason != "") && result.CacheState == "fresh" {
			result.CacheState = "stale"
		}
	}
	if pause, ok := s.cooldowns.get(quotaKey(route), false); ok && now.Before(pause.until) {
		result.Reason = pause.reason
		result.RetryAfterSeconds = max(1, int64(pause.until.Sub(now).Seconds()))
		if result.CacheState == "fresh" {
			result.CacheState = "stale"
		}
	}
	if enqueue {
		s.subscribeLocked(route)
		s.pinLocked(route, observation.IP)
		if result.CacheState != "fresh" {
			if reason := s.enqueueLocked(route, now); reason != "" {
				result.Reason = reason
			}
		}
	}
	if j := s.pending[route.Key()]; j != nil {
		result.RefreshState = "queued"
		if j.running {
			result.RefreshState = "running"
		} else if !j.queued {
			result.RefreshState = "pending"
		}
	}
	if result.CacheState == "missing" && result.Reason == "" {
		result.Reason = "cache_miss"
		if observation.IP == "" {
			result.Reason = "exit_unobserved"
		}
	}
	return result
}
func (s *Service) subscribeLocked(route Route) {
	key := route.Key()
	el := s.subscriptions[key]
	if el == nil {
		el = s.order.PushBack(&subscription{Route: route, Owners: map[string]struct{}{}})
		s.subscriptions[key] = el
	}
	if previous := s.owners[ownerScopeKey(route.OwnerID, route.Scope)]; route.OwnerID != "" && previous != "" && previous != key {
		if old := s.subscriptions[previous]; old != nil {
			delete(old.Value.(*subscription).Owners, route.OwnerID)
			if len(old.Value.(*subscription).Owners) == 0 {
				s.removeSubscriptionLocked(previous)
			}
		}
	}
	el.Value.(*subscription).Owners[route.OwnerID] = struct{}{}
	el.Value.(*subscription).revision++
	if route.OwnerID != "" {
		s.owners[ownerScopeKey(route.OwnerID, route.Scope)] = key
	}
	s.startSchedulerLocked()
}
func (s *Service) removeSubscriptionLocked(key string) {
	if el := s.subscriptions[key]; el != nil {
		v := el.Value.(*subscription)
		if v.IPRef != "" {
			s.ips.unpin(v.IPRef)
		}
		s.order.Remove(el)
		delete(s.subscriptions, key)
	}
	if j := s.pending[key]; j != nil {
		j.complete()
		if j.element != nil {
			s.jobs.Remove(j.element)
			j.element = nil
		}
		delete(s.pending, key)
	}
}
func (s *Service) Refresh(route Route) string {
	if s == nil {
		return "unavailable"
	}
	s.requestObservation(route)
	s.mu.Lock()
	defer s.mu.Unlock()
	if !route.valid() {
		return "proxy_required"
	}
	s.subscribeLocked(route)
	return s.enqueueLocked(route, s.now())
}
func (s *Service) enqueueLocked(route Route, now time.Time) string {
	if s.closed || s.ctx.Err() != nil {
		return "closed"
	}
	key := route.Key()
	if s.pending[key] != nil {
		return ""
	}
	if el := s.subscriptions[key]; el != nil && now.Before(el.Value.(*subscription).RetryAt) {
		return "refresh_cooldown"
	}
	if pause, ok := s.cooldowns.get(quotaKey(route), false); ok && now.Before(pause.until) {
		return "refresh_cooldown"
	}
	if !s.started {
		s.started = true
		s.startWorkersLocked()
		s.wg.Add(1)
		go s.dispatchLoop()
	}
	ctx, cancel := context.WithCancel(s.ctx)
	j := &job{done: make(chan struct{}), route: route, key: key, generation: s.generation, ctx: ctx, cancel: cancel, settings: s.settings}
	s.pending[key] = j
	j.element = s.jobs.PushBack(j)
	s.signal()
	return ""
}

func (s *Service) ForgetBinding(binding string) {
	if s == nil || binding == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, el := range s.subscriptions {
		if el.Value.(*subscription).Route.BindingID == binding {
			for owner := range el.Value.(*subscription).Owners {
				delete(s.owners, ownerScopeKey(owner, el.Value.(*subscription).Route.Scope))
			}
			s.removeSubscriptionLocked(key)
		}
	}
}
func (s *Service) Status(offset, limit int) ([]Snapshot, int, int) {
	s.mu.Lock()
	count, ips := len(s.subscriptions), len(s.ips.items)
	routes := []Route{}
	i := 0
	for e := s.order.Front(); e != nil && len(routes) < min(200, max(1, limit)); e = e.Next() {
		if i >= max(0, offset) {
			routes = append(routes, e.Value.(*subscription).Route)
		}
		i++
	}
	s.mu.Unlock()
	out := []Snapshot{}
	for _, r := range routes {
		out = append(out, s.Preview(r))
	}
	return out, count, ips
}
func (s *Service) active(route Route) bool {
	s.mu.Lock()
	check := s.ownerActive
	owners := []string{}
	var revision uint64
	original := s.subscriptions[route.Key()]
	if el := s.subscriptions[route.Key()]; el != nil {
		revision = el.Value.(*subscription).revision
		for owner := range el.Value.(*subscription).Owners {
			owners = append(owners, owner)
		}
	}
	s.mu.Unlock()
	if check == nil || len(owners) == 0 {
		return true
	}
	for _, owner := range owners {
		if check(owner) {
			return true
		}
	}
	s.mu.Lock()
	current := s.subscriptions[route.Key()]
	if current != original || current != nil && current.Value.(*subscription).revision != revision {
		s.mu.Unlock()
		return true
	}
	s.removeSubscriptionLocked(route.Key())
	for _, owner := range owners {
		if s.owners[ownerScopeKey(owner, route.Scope)] == route.Key() {
			delete(s.owners, ownerScopeKey(owner, route.Scope))
		}
	}
	s.mu.Unlock()
	return false
}
func (s *Service) worker(index int) {
	defer s.wg.Done()
	for {
		s.mu.Lock()
		enabled := index < s.settings.LookupConcurrency
		s.mu.Unlock()
		if !enabled {
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
		select {
		case <-s.ctx.Done():
			return
		case j := <-s.queue:
			s.mu.Lock()
			valid := !s.closed && j.generation == s.generation && s.pending[j.key] == j && j.ctx.Err() == nil
			if valid {
				j.running = true
			}
			s.mu.Unlock()
			if !valid || !s.active(j.route) {
				s.finishJob(j, lookupResult{Reason: "lookup_cancelled"})
				continue
			}
			ctx, cancel := context.WithTimeout(j.ctx, time.Duration(j.settings.LookupTimeoutSeconds)*time.Second)
			result := s.lookup(ctx, j)
			cancel()
			s.finishJob(j, result)
		}
	}
}
func (s *Service) finishJob(j *job, result lookupResult) {
	s.mu.Lock()
	if !s.closed && j.generation == s.generation && s.pending[j.key] == j {
		if el := s.subscriptions[j.key]; el != nil {
			v := el.Value.(*subscription)
			v.Reason = result.Reason
			if result.Reason == "" {
				v.Failures = 0
				v.RetryAt = time.Time{}
			} else if j.ctx.Err() == nil {
				v.Failures++
				v.FailedAt = s.now()
				v.RetryAt = s.now().Add(max(result.Retry, RetryDelay(v.Failures)))
				s.cooldowns.put(quotaKey(j.route), cooldown{until: v.RetryAt, reason: v.Reason}, s.settings.IPCacheSize)
			}
		}
		delete(s.pending, j.key)
		if j.element != nil {
			s.jobs.Remove(j.element)
			j.element = nil
		}
		if el := s.subscriptions[j.key]; el != nil {
			v := el.Value.(*subscription)
			if len(v.Owners) == 1 {
				if _, ok := v.Owners[""]; ok {
					s.removeSubscriptionLocked(j.key)
				}
			}
		}
	}
	s.mu.Unlock()
	j.complete()
	s.signal()
}

type lookupResult struct {
	Geo           Geo
	Reason, Owner string
	Retry         time.Duration
}

func (s *Service) lookup(ctx context.Context, j *job) lookupResult {
	observation := s.readObservation(j.route)
	if observation.IP == "" {
		return lookupResult{Reason: "exit_unobserved"}
	}
	fetch := func() (any, error) {
		s.mu.Lock()
		geo, ok := s.ips.get(observation.IP, true)
		now := s.now()
		s.mu.Unlock()
		if ok && now.Sub(geo.At) < time.Duration(j.settings.IPCacheTTLSeconds)*time.Second {
			return lookupResult{Geo: geo}, nil
		}
		target := strings.ReplaceAll(j.settings.LookupURL, "{ip}", observation.IP)
		body, status, retry, err := s.probe(ctx, j.route.ProxyURL, target)
		result := lookupResult{Owner: j.key}
		if status == 429 {
			result.Reason = "lookup_rate_limited"
			result.Retry = regionRetryAfter(retry, s.now())
			if result.Retry == 0 {
				result.Retry = 24 * time.Hour
			}
		} else if err != nil {
			result.Reason = "lookup_network_error"
		} else if status != 200 {
			result.Reason = "lookup_http_error"
		} else {
			result.Geo, result.Reason = regionResponse(body, observation.IP, observation.Source, s.now())
		}
		if result.Geo.IP != "" {
			latest := s.readObservation(j.route)
			s.mu.Lock()
			if !s.closed && j.generation == s.generation && j.ctx.Err() == nil {
				s.pinLocked(j.route, latest.IP)
				s.ips.put(result.Geo.IP, result.Geo, s.settings.IPCacheSize)
			}
			s.mu.Unlock()
		}
		return result, nil
	}
	ch := s.group.DoChan(fmt.Sprintf("%d:%s", j.generation, observation.IP), fetch)
	select {
	case <-ctx.Done():
		return lookupResult{Reason: "lookup_cancelled"}
	case v := <-ch:
		result := v.Val.(lookupResult)
		if result.Reason != "" && result.Owner != "" && result.Owner != j.key {
			v, _ := fetch()
			return v.(lookupResult)
		}
		return result
	}
}
func RetryDelay(n int) time.Duration {
	return time.Duration(float64(min(30*time.Minute, 30*time.Second*time.Duration(1<<min(max(n-1, 0), 6)))) * (0.8 + rand.Float64()*0.4))
}
func (s *Service) ResolveForRequest(ctx context.Context, route Route) Snapshot {
	result := s.Resolve(route)
	if s == nil || result.Timezone != "" {
		return result
	}
	s.mu.Lock()
	settings := s.settings
	j := s.pending[route.Key()]
	if j != nil {
		j.urgent = true
		s.signal()
	}
	s.mu.Unlock()
	if settings.ColdCacheMode != "wait" || j == nil {
		if j == nil && settings.ColdCacheMode == "wait" {
			return s.Preview(route)
		}
		return result
	}
	wait, cancel := context.WithTimeout(ctx, time.Duration(settings.LookupTimeoutSeconds)*time.Second)
	defer cancel()
	select {
	case <-j.done:
		return s.Preview(route)
	case <-s.ctx.Done():
		result.Reason = "closed"
	case <-j.ctx.Done():
		result = s.Preview(route)
		if result.Timezone == "" && result.Reason == "" {
			result.Reason = "lookup_cancelled"
		}
	case <-wait.Done():
		result = s.Preview(route)
		result.Reason = "lookup_wait_timeout"
		if ctx.Err() != nil {
			result.Reason = "lookup_wait_cancelled"
		}
	}
	return result
}
func (s *Service) startSchedulerLocked() {
	if s.closed || s.schedulerStarted {
		return
	}
	s.schedulerStarted = true
	s.wg.Add(1)
	go s.schedule()
}
func (s *Service) schedule() {
	defer s.wg.Done()
	s.mu.Lock()
	delay := time.Duration(s.settings.RefreshIntervalSeconds) * time.Second
	s.mu.Unlock()
	timer := time.NewTimer(delay)
	defer timer.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.consumerWake:
			s.mu.Lock()
			reconcile := s.reconcile
			nextDelay := time.Duration(s.settings.RefreshIntervalSeconds) * time.Second
			s.mu.Unlock()
			if nextDelay != delay {
				delay = nextDelay
				timer.Reset(delay)
			}
			if reconcile != nil {
				reconcile()
			}
		case <-timer.C:
			s.RefreshBatch()
			s.mu.Lock()
			delay = time.Duration(s.settings.RefreshIntervalSeconds) * time.Second
			s.mu.Unlock()
			timer.Reset(delay)
		}
	}
}

// RefreshBatch scans a bounded number of consumers and never floods the queue.
func (s *Service) RefreshBatch() int {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0
	}
	scheduled := s.settings.RefreshMode == "scheduled"
	prepare := s.settings.ColdCacheMode == "wait"
	batch := s.settings.RefreshBatchSize
	routes := []Route{}
	current := s.subscriptions[s.cursor]
	if current == nil {
		current = s.order.Front()
	}
	for i := 0; current != nil && i < min(len(s.subscriptions), batch); i++ {
		routes = append(routes, current.Value.(*subscription).Route)
		current = current.Next()
		if current == nil {
			current = s.order.Front()
		}
		s.cursor = current.Value.(*subscription).Route.Key()
	}
	s.mu.Unlock()
	count := 0
	for _, route := range routes {
		if !s.active(route) {
			continue
		}
		snapshot := s.Preview(route)
		s.mu.Lock()
		s.pinLocked(route, snapshot.IP)
		s.mu.Unlock()
		if !scheduled && !(prepare && snapshot.CacheState == "missing") {
			continue
		}
		if snapshot.CacheState != "fresh" && snapshot.RefreshState == "idle" && snapshot.RetryAfterSeconds == 0 {
			if s.Refresh(route) == "" {
				count++
			}
		}
	}
	return count
}

// Resume is called only while the owning manager holds its lifecycle lock.
func (s *Service) Resume(parent context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		return
	}
	s.ctx, s.cancel = context.WithCancel(parent)
	s.closed = false
	s.started = false
	s.workers = 0
	s.schedulerStarted = false
	s.queue = make(chan *job, 256)
	s.startSchedulerLocked()
}

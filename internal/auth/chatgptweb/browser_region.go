package chatgptweb

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"golang.org/x/sync/singleflight"
	"golang.org/x/text/language"
)

// BrowserRegionSnapshot contains no transport identity or account material.
type BrowserRegionSnapshot struct {
	Timezone          string `json:"timezone"`
	OffsetMinutes     int    `json:"timezone_offset_minutes"`
	Language          string `json:"language"`
	TimezoneSource    string `json:"timezone_source"`
	LanguageSource    string `json:"language_source"`
	ObservationSource string `json:"observation_source,omitempty"`
	Country           string `json:"country,omitempty"`
	Region            string `json:"region,omitempty"`
	CacheState        string `json:"cache_state"`
	CacheAgeSeconds   int64  `json:"cache_age_seconds"`
	RefreshState      string `json:"refresh_state"`
	Reason            string `json:"reason,omitempty"`
	RetryAfterSeconds int64  `json:"retry_after_seconds,omitempty"`
	OverrideLanguage  bool   `json:"-"`
}

type browserRegionGeo struct {
	IP, Country, Region, Timezone, Source string
	At                                    time.Time
}
type browserRegionEntry struct {
	Geo                browserRegionGeo
	ObservedIP         string
	ObservedAt         time.Time
	RefreshAt, RetryAt time.Time
	Failures           int
	Reason             string
}
type regionLRUItem[V any] struct {
	key   string
	value V
}
type regionLRU[V any] struct {
	items map[string]*list.Element
	order *list.List
}

func newRegionLRU[V any]() *regionLRU[V] {
	return &regionLRU[V]{items: make(map[string]*list.Element), order: list.New()}
}
func (l *regionLRU[V]) get(key string, touch bool) (V, bool) {
	if e := l.items[key]; e != nil {
		if touch {
			l.order.MoveToFront(e)
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
	l.resize(capacity)
}
func (l *regionLRU[V]) resize(capacity int) {
	for len(l.items) > capacity {
		e := l.order.Back()
		delete(l.items, e.Value.(regionLRUItem[V]).key)
		l.order.Remove(e)
	}
}

type browserRegionJob struct {
	key, proxy string
	generation uint64
	ctx        context.Context
	settings   config.ResolvedChatGPTWebBrowserRegion
	running    bool
}
type browserRegionPolicy struct {
	settings  config.ResolvedChatGPTWebBrowserRegion
	timezone  string
	offset    int
	hasOffset bool
}

// BrowserRegionResolver performs only lazy, bounded, provider-owned metadata work.
type BrowserRegionResolver struct {
	mu               sync.Mutex
	policy           browserRegionPolicy
	routes           *regionLRU[browserRegionEntry]
	ips              *regionLRU[browserRegionEntry]
	pending          map[string]*browserRegionJob
	queue            chan *browserRegionJob
	ctx              context.Context
	cancel           context.CancelFunc
	generationCtx    context.Context
	generationCancel context.CancelFunc
	generation       uint64
	closed           bool
	started          bool
	wg               sync.WaitGroup
	group            singleflight.Group
	now              func() time.Time
	probe            func(context.Context, string, string) ([]byte, int, string, error)
}

func NewBrowserRegionResolver(parent context.Context) *BrowserRegionResolver {
	ctx, cancel := context.WithCancel(parent)
	r := &BrowserRegionResolver{ctx: ctx, cancel: cancel, routes: newRegionLRU[browserRegionEntry](), ips: newRegionLRU[browserRegionEntry](), pending: make(map[string]*browserRegionJob), queue: make(chan *browserRegionJob, 256), now: time.Now, probe: probeBrowserRegion}
	r.generationCtx, r.generationCancel = context.WithCancel(ctx)
	r.UpdateConfig(config.ChatGPTWebConfig{})
	return r
}
func (r *BrowserRegionResolver) Close() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.closed = true
	r.cancel()
	r.generationCancel()
	r.mu.Unlock()
	r.wg.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()
	clear(r.pending)
	r.routes = newRegionLRU[browserRegionEntry]()
	r.ips = newRegionLRU[browserRegionEntry]()
	for {
		select {
		case <-r.queue:
		default:
			return
		}
	}
}
func (r *BrowserRegionResolver) UpdateConfig(cfg config.ChatGPTWebConfig) {
	if cfg.BrowserRegion.Validate() != nil {
		return
	}
	p := browserRegionPolicy{settings: cfg.BrowserRegion.Resolved(), timezone: cfg.Timezone}
	if cfg.TimezoneOffsetMinutes != nil {
		p.offset = *cfg.TimezoneOffsetMinutes
		p.hasOffset = true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || p == r.policy {
		return
	}
	r.generationCancel()
	r.generationCtx, r.generationCancel = context.WithCancel(r.ctx)
	r.generation++
	if p.settings.LookupURL != r.policy.settings.LookupURL {
		r.routes = newRegionLRU[browserRegionEntry]()
		r.ips = newRegionLRU[browserRegionEntry]()
	} else if p.settings.CacheTTLSeconds != r.policy.settings.CacheTTLSeconds {
		// Recompute eligibility only. Changing the TTL never schedules a sweep.
		for _, element := range r.routes.items {
			item := element.Value.(regionLRUItem[browserRegionEntry])
			if item.value.Geo.IP != "" && item.value.Reason == "" {
				at := item.value.ObservedAt
				if at.IsZero() {
					at = item.value.Geo.At
				}
				item.value.RefreshAt = at.Add(regionJitter(time.Duration(p.settings.CacheTTLSeconds) * time.Second))
				element.Value = item
			}
		}
	}
	r.policy = p
	r.pending = make(map[string]*browserRegionJob)
	r.routes.resize(p.settings.RouteCacheSize)
	r.ips.resize(p.settings.IPCacheSize)
}

func browserRegionRouteKey(proxy string) string {
	proxy = strings.TrimSpace(proxy)
	if proxy == "" {
		proxy = "direct"
	}
	sum := sha256.Sum256([]byte(proxy))
	return hex.EncodeToString(sum[:])
}

// Resolve never waits for network work. Preview uses the same calculation without enqueueing.
func (r *BrowserRegionResolver) Resolve(proxy string, persona Persona) BrowserRegionSnapshot {
	return r.snapshot(proxy, persona, true)
}
func (r *BrowserRegionResolver) Preview(proxy string, persona Persona) BrowserRegionSnapshot {
	return r.snapshot(proxy, persona, false)
}
func (r *BrowserRegionResolver) snapshot(proxy string, persona Persona, enqueue bool) BrowserRegionSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	p := r.policy
	key := browserRegionRouteKey(proxy)
	manual := config.ChatGPTWebConfig{Timezone: p.timezone}
	if p.hasOffset {
		value := p.offset
		manual.TimezoneOffsetMinutes = &value
	}
	zone := manual.ResolvedTimezone(now)
	s := BrowserRegionSnapshot{Timezone: zone.Timezone, OffsetMinutes: zone.OffsetMinutes, Language: persona.Language, TimezoneSource: "configured", LanguageSource: "credential", CacheState: "missing", RefreshState: "idle"}
	if p.settings.Language != "" {
		s.Language = p.settings.Language
		s.LanguageSource = "configured"
		s.OverrideLanguage = true
	}
	entry, ok := r.routes.get(key, enqueue)
	due := !ok || !now.Before(entry.RefreshAt) || (entry.Geo.IP != "" && now.Sub(entry.Geo.At) >= 24*time.Hour)
	if ok {
		s.Reason = entry.Reason
		if now.Before(entry.RetryAt) {
			s.RetryAfterSeconds = max(1, int64(entry.RetryAt.Sub(now).Seconds()))
		}
		if entry.Geo.IP != "" && entry.Geo.IP == entry.ObservedIP {
			s.CacheState = "fresh"
			if due || entry.Reason != "" {
				s.CacheState = "stale"
			}
			s.CacheAgeSeconds = max(0, int64(now.Sub(entry.Geo.At).Seconds()))
			s.Country = entry.Geo.Country
			s.Region = entry.Geo.Region
			s.ObservationSource = entry.Geo.Source
			if p.settings.AutoTimezone {
				s.Timezone = entry.Geo.Timezone
				s.OffsetMinutes = (config.ChatGPTWebConfig{Timezone: s.Timezone}).ResolvedTimezone(now).OffsetMinutes
				s.TimezoneSource = s.CacheState + "_cache"
			}
			if p.settings.AutoLanguage {
				if tag := regionDefaultLanguage(entry.Geo.Country); tag != "" {
					s.Language = tag
					s.LanguageSource = s.CacheState + "_cache"
					s.OverrideLanguage = true
				}
			}
		}
	}
	if enqueue && (p.settings.AutoTimezone || p.settings.AutoLanguage) && due {
		if reason := r.enqueueLocked(key, proxy, now); reason != "" {
			s.Reason = reason
		}
	}
	if pending := r.pending[key]; pending != nil {
		s.RefreshState = "queued"
		if pending.running {
			s.RefreshState = "running"
		}
	}
	if s.CacheState == "missing" && s.Reason == "" && (p.settings.AutoTimezone || p.settings.AutoLanguage) {
		s.Reason = "cache_miss"
	}
	return s
}

// Refresh coalesces explicit refreshes and respects failure cooldowns.
func (r *BrowserRegionResolver) Refresh(proxy string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.enqueueLocked(browserRegionRouteKey(proxy), proxy, r.now())
}
func (r *BrowserRegionResolver) enqueueLocked(key, proxy string, now time.Time) string {
	if r.closed || r.ctx.Err() != nil {
		return "closed"
	}
	if r.pending[key] != nil {
		return ""
	}
	if e, ok := r.routes.get(key, false); ok && now.Before(e.RetryAt) {
		return "refresh_cooldown"
	}
	if !r.started {
		r.started = true
		for range 2 {
			r.wg.Add(1)
			go r.worker()
		}
	}
	job := &browserRegionJob{key: key, proxy: proxy, generation: r.generation, ctx: r.generationCtx, settings: r.policy.settings}
	select {
	case r.queue <- job:
		r.pending[key] = job
		return ""
	default:
		return "queue_full"
	}
}
func (r *BrowserRegionResolver) worker() {
	defer r.wg.Done()
	for {
		select {
		case <-r.ctx.Done():
			return
		case job := <-r.queue:
			r.mu.Lock()
			valid := !r.closed && r.generation == job.generation && r.pending[job.key] == job
			if valid {
				job.running = true
			}
			r.mu.Unlock()
			if !valid {
				continue
			}
			ctx, cancel := context.WithTimeout(job.ctx, time.Duration(job.settings.LookupTimeoutSeconds)*time.Second)
			geo, observed, reason, retry := r.lookup(ctx, job)
			cancel()
			r.mu.Lock()
			if r.generation == job.generation && !r.closed {
				entry, _ := r.routes.get(job.key, false)
				now := r.now()
				if observed != "" {
					entry.ObservedIP = observed
					if entry.Geo.IP != observed {
						entry.Geo = browserRegionGeo{}
					}
				}
				if geo.IP != "" && geo.IP == entry.ObservedIP {
					entry.Geo = geo
				}
				if reason == "" {
					entry = browserRegionEntry{Geo: geo, ObservedIP: geo.IP, ObservedAt: now, RefreshAt: now.Add(regionJitter(time.Duration(job.settings.CacheTTLSeconds) * time.Second))}
				} else {
					entry.Failures++
					entry.Reason = reason
					delay := regionRetryDelay(entry.Failures)
					if retry > delay {
						delay = retry
					}
					entry.RetryAt = now.Add(delay)
					entry.RefreshAt = now
				}
				r.routes.put(job.key, entry, r.policy.settings.RouteCacheSize)
				if r.pending[job.key] == job {
					delete(r.pending, job.key)
				}
			}
			r.mu.Unlock()
		}
	}
}
func regionJitter(delay time.Duration) time.Duration {
	return time.Duration(float64(delay) * (0.8 + rand.Float64()*0.4))
}
func regionRetryDelay(failures int) time.Duration {
	return min(30*time.Minute, regionJitter(min(30*time.Minute, 30*time.Second*time.Duration(1<<min(max(failures-1, 0), 6)))))
}
func regionDefaultLanguage(country string) string {
	region, err := language.ParseRegion(country)
	if err != nil || country == "" || !region.IsCountry() {
		return ""
	}
	tag, err := language.Parse("und-" + region.String())
	if err != nil {
		return ""
	}
	base, confidence := tag.Base()
	if confidence == language.No || base.String() == "und" {
		return ""
	}
	return language.Make(base.String() + "-" + region.String()).String()
}

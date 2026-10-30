package admission

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

type keyState struct {
	mu      sync.Mutex
	id      string
	policy  config.APIKeyGroup
	deleted bool
	minute  int64
	used    int
}

type Controller struct {
	now               func() time.Time
	keyIDs            atomic.Pointer[map[string]*keyState]
	Traffic           *Traffic
	Queue             *Queue
	keys              atomic.Pointer[map[string]*keyState]
	maintenance       atomic.Int64
	sweeping          atomic.Bool
	stopped           atomic.Bool
	RouteQueueEnabled atomic.Bool
}

func NewController() *Controller {
	c := &Controller{Traffic: NewTraffic(), Queue: NewQueue()}
	empty := make(map[string]*keyState)
	c.keys.Store(&empty)
	c.keyIDs.Store(&empty)
	return c
}
func (c *Controller) Configure(cfg *config.Config) {
	if cfg == nil {
		cfg = &config.Config{}
	}
	routeQueue := cfg.Routing.Queue.Enabled
	for _, rule := range cfg.Routing.PriorityOverrides {
		if rule.Queue != nil && rule.Queue.Enabled {
			routeQueue = true
		}
	}
	c.RouteQueueEnabled.Store(routeQueue)
	old := c.keys.Load()
	next := make(map[string]*keyState, len(cfg.APIKeys))
	policies := make(map[string]config.APIKeyGroup, len(cfg.APIKeyGroups))
	for _, p := range cfg.APIKeyGroups {
		policies[p.APIKey] = p
	}
	for _, key := range cfg.APIKeys {
		s := (*old)[key]
		if s == nil {
			s = &keyState{id: KeyID(key)}
		}
		s.mu.Lock()
		s.policy = policies[key]
		s.deleted = false
		s.mu.Unlock()
		next[key] = s
	}
	for key, s := range *old {
		if next[key] == nil {
			s.mu.Lock()
			s.deleted = true
			c.Traffic.Retire("key", s.id)
			s.mu.Unlock()
		}
	}
	byID := make(map[string]*keyState, len(next))
	for _, state := range next {
		byID[state.id] = state
	}
	c.keyIDs.Store(&byID)
	c.keys.Store(&next)
	c.Queue.Configure(cfg.Admission)
}

type Lease struct {
	entity *Entity
	once   sync.Once
	end    func()
}

func (l *Lease) Close() {
	if l != nil {
		l.once.Do(l.end)
	}
}

func (c *Controller) Acquire(ctx context.Context, key string, bytes int64, spent *time.Duration) (*Lease, error) {
	id := "anonymous"
	if key != "" {
		id = KeyID(key)
	}
	return c.AcquireIdentity(ctx, id, key != "", bytes, spent)
}

// AcquireIdentity accepts only a server-authenticated identity. Temporary grants
// carry their issuer ID; custom access providers may have no local policy.
func (c *Controller) AcquireIdentity(ctx context.Context, id string, required bool, bytes int64, spent *time.Duration) (*Lease, error) {
	if c.stopped.Load() {
		return nil, context.Canceled
	}
	c.startSweep()
	var entity *Entity
	state := (*c.keyIDs.Load())[id]
	if state == nil && required {
		return nil, &revokedKeyError{}
	}
	policy := config.APIKeyGroup{}
	if state != nil {
		state.mu.Lock()
		id = state.id
		policy = state.policy
		state.mu.Unlock()
	}
	group := "key:" + id
	err := c.Queue.Do(ctx, group, group, policy.Queue, bytes, spent, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		now := time.Now()
		if c.now != nil {
			now = c.now()
		}
		if state != nil {
			state.mu.Lock()
			defer state.mu.Unlock()
			if state.deleted {
				return &revokedKeyError{}
			}
			policy = state.policy
			if state.minute != now.Unix()/60 {
				state.minute = now.Unix() / 60
				state.used = 0
			}
			if policy.RPMLimit > 0 && state.used >= policy.RPMLimit {
				return &Busy{Reason: "API key request rate limit exceeded", ReadyAt: now.Truncate(time.Minute).Add(time.Minute)}
			}
		}
		var acquired bool
		entity, acquired = c.Traffic.Acquire("key", id, policy.ConcurrencyLimit)
		if !acquired {
			return &Busy{Reason: "API key concurrent request limit exceeded"}
		}
		if state != nil {
			state.used++
		}
		entity.Start(now)
		c.Traffic.ObserveLogical(true, now)
		return nil
	})
	if err != nil {
		return nil, err
	}
	lease := &Lease{entity: entity, end: func() {
		now := time.Now()
		entity.Release()
		entity.Complete(now)
		c.Traffic.ObserveLogical(false, now)
		c.Queue.Notify(group)
	}}
	if c.stopped.Load() {
		lease.Close()
		return nil, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		lease.Close()
		return nil, err
	}
	return lease, nil
}

type revokedKeyError struct{}

func (*revokedKeyError) Error() string   { return "API key is no longer available" }
func (*revokedKeyError) StatusCode() int { return 401 }

func (c *Controller) KeySnapshots(ids []string, now time.Time) []Snapshot {
	byID := *c.keyIDs.Load()
	items := make([]Snapshot, 0, len(ids))
	for _, id := range ids {
		s := byID[id]
		if s == nil {
			continue
		}
		v := c.Traffic.Lookup("key", id).Snapshot(now)
		v.ID = id
		s.mu.Lock()
		v.Limit = s.policy.ConcurrencyLimit
		v.RPMLimit = s.policy.RPMLimit
		v.WindowMinutes = 1
		if s.minute == now.Unix()/60 {
			v.WindowUsed = s.used
		}
		s.mu.Unlock()
		v.Queued, _ = c.Queue.Counts("key:" + id)
		items = append(items, v)
	}
	return items
}
func (c *Controller) Overview(now time.Time, seconds int, current, completed bool) Overview {
	r := c.Traffic.Overview(now, seconds, current, completed)
	r.Queued, r.QueuedBytes = c.Queue.Counts("")

	return r
}

func (c *Controller) startSweep() {
	if c.stopped.Load() || !c.sweeping.CompareAndSwap(false, true) {
		return
	}
	var sweep func()
	sweep = func() {
		if c.stopped.Load() {
			c.sweeping.Store(false)
			return
		}
		shard := c.maintenance.Add(1)
		c.Traffic.PruneShard(int(shard%32), time.Now())
		time.AfterFunc(3*time.Second, sweep)
	}
	time.AfterFunc(3*time.Second, sweep)
}
func (c *Controller) Close() { c.stopped.Store(true); c.Queue.Close() }

func (l *Lease) WaitCredential(delta int) {
	if l != nil {
		l.entity.WaitCredential(delta)
	}
}

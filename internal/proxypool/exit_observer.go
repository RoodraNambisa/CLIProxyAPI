package proxypool

import (
	"container/list"
	"context"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxyregion"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
)

type exitRecord struct {
	route      proxyregion.Route
	owner      string
	value      TraceResult
	attempted  time.Time
	generation uint64
	reason     string
}
type exitTask struct {
	key      string
	route    proxyregion.Route
	owner    string
	snapshot *configSnapshot
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
}

// exitObserver extends proxy management to destination-scoped observations.
// Bound observations follow bindings; only unbound records use bounded LRU.
type exitObserver struct {
	manager         *Manager
	bindingOwners   sync.Map
	mu              sync.Mutex
	bound           map[string]map[string]*exitRecord
	temporary       map[string]*list.Element
	order           *list.List
	pending         map[string]*exitTask
	queue           chan *exitTask
	ctx             context.Context
	cancel          context.CancelFunc
	started, closed bool
	wg              sync.WaitGroup
}

func newExitObserver(m *Manager) *exitObserver {
	ctx, cancel := context.WithCancel(context.Background())
	return &exitObserver{manager: m, bound: map[string]map[string]*exitRecord{}, temporary: map[string]*list.Element{}, order: list.New(), pending: map[string]*exitTask{}, queue: make(chan *exitTask, 256), ctx: ctx, cancel: cancel}
}
func normalizeExitRoute(r proxyregion.Route) proxyregion.Route {
	if r.Scope == "" {
		r.Scope = proxyregion.ChatGPTScope
	}
	return r
}
func (o *exitObserver) binding(r proxyregion.Route, snapshot *configSnapshot) (string, time.Duration, bool) {
	if snapshot == nil {
		return "", 0, false
	}
	p, e := proxyutil.Parse(r.ProxyURL)
	if e != nil || p.Mode != proxyutil.ModeProxy {
		return "", 0, false
	}
	if r.BindingID == "" {
		return "", time.Duration(*snapshot.health.TemporaryRouteCheckIntervalSeconds) * time.Second, true
	}
	m := o.manager
	m.mu.RLock()
	defer m.mu.RUnlock()
	check := func(b Binding) (string, time.Duration, bool) {
		url, valid := m.bindingURL(snapshot, b)
		if !valid || url != r.ProxyURL {
			return "", 0, false
		}
		pool := snapshot.pools[strings.ToLower(b.Pool)]
		return b.AuthID, time.Duration(pool.config.CheckIntervalSeconds) * time.Second, true
	}
	if b, ok := m.bindings[r.OwnerID]; ok && b.ID == r.BindingID {
		return check(b)
	}
	if owner, ok := o.bindingOwners.Load(r.BindingID); ok {
		if b, found := m.bindings[owner.(string)]; found && b.ID == r.BindingID {
			return check(b)
		}
	}
	for _, b := range m.bindings {
		if b.ID == r.BindingID {
			o.bindingOwners.Store(b.ID, b.AuthID)
			return check(b)
		}
	}
	return "", 0, false
}
func (o *exitObserver) getLocked(r proxyregion.Route) *exitRecord {
	if r.BindingID != "" {
		return o.bound[r.BindingID][r.Key()]
	}
	if el := o.temporary[r.Key()]; el != nil {
		o.order.MoveToFront(el)
		return el.Value.(*exitRecord)
	}
	return nil
}
func (o *exitObserver) putLocked(v *exitRecord, snapshot *configSnapshot) {
	r := v.route
	if r.BindingID != "" {
		if o.bound[r.BindingID] == nil {
			o.bound[r.BindingID] = map[string]*exitRecord{}
		}
		o.bound[r.BindingID][r.Key()] = v
		return
	}
	if el := o.temporary[r.Key()]; el != nil {
		el.Value = v
		o.order.MoveToFront(el)
	} else {
		o.temporary[r.Key()] = o.order.PushFront(v)
	}
	for len(o.temporary) > *snapshot.health.TemporaryRouteCacheSize {
		last := o.order.Back()
		delete(o.temporary, last.Value.(*exitRecord).route.Key())
		o.order.Remove(last)
	}
}
func (o *exitObserver) snapshot(r proxyregion.Route) proxyregion.Observation {
	r = normalizeExitRoute(r)
	snapshot := o.manager.snapshot()
	owner, _, valid := o.binding(r, snapshot)
	if !valid {
		return proxyregion.Observation{}
	}
	// Reuse a pool's existing successful check if it observed this exact target.
	if r.BindingID != "" {
		o.manager.mu.RLock()
		health := o.manager.health[r.BindingID]
		b := o.manager.bindings[owner]
		o.manager.mu.RUnlock()
		if health.Generation == snapshot.generation && health.OK && health.IP != "" {
			scope := health.Endpoint
			for _, ep := range snapshot.health.Endpoints {
				if ep.Name == scope {
					scope = ep.URL
					break
				}
			}
			if scope == r.Scope {
				o.importHealth(snapshot, b, r.ProxyURL, health.TraceResult)
			}
		}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	v := o.getLocked(r)
	if v == nil || v.generation != snapshot.generation || !v.value.OK {
		return proxyregion.Observation{}
	}
	return proxyregion.Observation{IP: v.value.IP, At: v.value.CheckedAt, Source: r.Scope, Reason: v.reason}
}
func (o *exitObserver) request(r proxyregion.Route) *exitTask {
	r = normalizeExitRoute(r)
	_ = o.snapshot(r)
	snapshot := o.manager.snapshot()
	owner, interval, valid := o.binding(r, snapshot)
	if !valid {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return nil
	}
	key := r.Key()
	if pending := o.pending[key]; pending != nil {
		return pending
	}
	if v := o.getLocked(r); v != nil && v.generation == snapshot.generation && o.manager.now().Sub(v.attempted) < interval {
		return nil
	}
	if !o.started {
		o.started = true
		for range 2 {
			o.wg.Add(1)
			go o.worker()
		}
	}
	ctx, cancel := context.WithCancel(o.ctx)
	task := &exitTask{key: key, route: r, owner: owner, snapshot: snapshot, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	select {
	case o.queue <- task:
		o.pending[key] = task
		return task
	default:
		cancel()
		return nil
	}
}
func (o *exitObserver) ensure(ctx context.Context, r proxyregion.Route) proxyregion.Observation {
	task := o.request(r)
	if task != nil {
		select {
		case <-task.done:
		case <-ctx.Done():
		case <-o.ctx.Done():
		}
	}
	return o.snapshot(r)
}
func (o *exitObserver) worker() {
	defer o.wg.Done()
	for {
		select {
		case <-o.ctx.Done():
			return
		case task := <-o.queue:
			release, err := o.manager.admission.acquire(task.ctx)
			if err == nil {
				_, interval, valid := o.binding(task.route, task.snapshot)
				o.mu.Lock()
				record := o.getLocked(task.route)
				fresh := record != nil && record.generation == task.snapshot.generation && o.manager.now().Sub(record.attempted) < interval
				o.mu.Unlock()
				if valid && !fresh {
					result := o.manager.checkProxyTarget(task.ctx, task.route.ProxyURL, config.ProxyHealthCheckEndpointConfig{Name: task.route.Scope, URL: task.route.Scope, Mode: config.ProxyHealthCheckModeCloudflareTrace}, task.snapshot)
					o.publish(task, result)
				}
				release()
			}
			o.mu.Lock()
			if o.pending[task.key] == task {
				delete(o.pending, task.key)
			}
			o.mu.Unlock()
			close(task.done)
			task.cancel()
		}
	}
}
func (o *exitObserver) publish(task *exitTask, result TraceResult) {
	if task.ctx.Err() != nil || o.manager.snapshot() != task.snapshot {
		return
	}
	_, _, valid := o.binding(task.route, task.snapshot)
	if !valid {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return
	}
	old := o.getLocked(task.route)
	if old != nil && old.attempted.After(result.CheckedAt) {
		return
	}
	v := &exitRecord{route: task.route, owner: task.owner, value: result, attempted: result.CheckedAt, generation: task.snapshot.generation}
	if !result.OK {
		v.reason = "exit_refresh_failed"
		if old != nil {
			v.value = old.value
		}
	}
	o.putLocked(v, task.snapshot)
}
func (o *exitObserver) importHealth(snapshot *configSnapshot, binding Binding, proxy string, result TraceResult) {
	if !result.OK || result.IP == "" {
		return
	}
	scope := result.Endpoint
	for _, e := range snapshot.health.Endpoints {
		if e.Name == scope {
			scope = e.URL
			break
		}
	}
	r := proxyregion.Route{ProxyURL: proxy, BindingID: binding.ID, Scope: scope, OwnerID: binding.AuthID}
	o.publish(&exitTask{route: r, owner: binding.AuthID, snapshot: snapshot, ctx: o.ctx}, result)
}

// refreshBound is called inside the existing pool round and admission slot.
func (o *exitObserver) refreshBound(ctx context.Context, snapshot *configSnapshot, binding Binding, proxy string) {
	o.mu.Lock()
	routes := []proxyregion.Route{}
	for _, v := range o.bound[binding.ID] {
		if o.manager.now().Sub(v.attempted) >= time.Duration(snapshot.pools[strings.ToLower(binding.Pool)].config.CheckIntervalSeconds)*time.Second {
			routes = append(routes, v.route)
		}
	}
	for _, task := range o.pending {
		if task.route.BindingID == binding.ID {
			routes = append(routes, task.route)
		}
	}
	o.mu.Unlock()
	for _, r := range routes {
		if ctx.Err() != nil {
			return
		}
		o.mu.Lock()
		current := o.getLocked(r)
		fresh := current != nil && current.generation == snapshot.generation && o.manager.now().Sub(current.attempted) < time.Duration(snapshot.pools[strings.ToLower(binding.Pool)].config.CheckIntervalSeconds)*time.Second
		o.mu.Unlock()
		if fresh {
			continue
		}
		result := o.manager.checkProxyTarget(ctx, proxy, config.ProxyHealthCheckEndpointConfig{Name: r.Scope, URL: r.Scope, Mode: config.ProxyHealthCheckModeCloudflareTrace}, snapshot)
		o.publish(&exitTask{route: r, owner: binding.AuthID, snapshot: snapshot, ctx: ctx}, result)
	}
}
func (o *exitObserver) configure(snapshot *configSnapshot) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, task := range o.pending {
		if task.snapshot.generation != snapshot.generation {
			task.cancel()
		}
	}
	for len(o.temporary) > *snapshot.health.TemporaryRouteCacheSize {
		last := o.order.Back()
		delete(o.temporary, last.Value.(*exitRecord).route.Key())
		o.order.Remove(last)
	}
}
func (o *exitObserver) forget(binding string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.bound, binding)
	o.bindingOwners.Delete(binding)
	for _, task := range o.pending {
		if task.route.BindingID == binding {
			task.cancel()
		}
	}
}
func (o *exitObserver) close() {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.closed = true
	o.cancel()
	o.mu.Unlock()
	o.wg.Wait()
}

func (o *exitObserver) resume(parent context.Context) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.closed {
		return
	}
	o.ctx, o.cancel = context.WithCancel(parent)
	o.closed = false
	o.started = false
	clear(o.pending)
	o.queue = make(chan *exitTask, 256)
}

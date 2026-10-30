// Package admission owns process-local traffic observations and admission queues.
package admission

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"
)

const historySeconds = 86400

type bucket struct {
	second int64
	count  int64
}
type recent struct{ buckets [64]bucket }

func (r *recent) add(now time.Time) {
	s := now.Unix()
	b := &r.buckets[uint64(s)%64]
	if b.second != s {
		*b = bucket{second: s}
	}
	b.count++
}
func (r *recent) counts(now time.Time) (int64, int64) {
	var rolling, minute int64
	s := now.Unix()
	start := s - s%60
	for _, b := range r.buckets {
		if b.second > s-60 && b.second <= s {
			rolling += b.count
			if b.second >= start {
				minute += b.count
			}
		}
	}
	return rolling, minute
}

// Entity remains alive while held by an execution, even after configuration deletion.
type Entity struct {
	mu        sync.Mutex
	id        string
	active    int
	waiting   int
	limit     int
	started   recent
	completed recent
	last      time.Time
}

type Snapshot struct {
	WaitingCredentials int       `json:"waiting_credentials"`
	ConcurrencySource  string    `json:"concurrency_source,omitempty"`
	QueueScope         string    `json:"queue_scope,omitempty"`
	ID                 string    `json:"id"`
	Active             int       `json:"active"`
	Limit              int       `json:"concurrency_limit"`
	RPM60              int64     `json:"rpm_60s"`
	CurrentMinute      int64     `json:"current_minute"`
	CompletedRPM60     int64     `json:"completed_rpm_60s"`
	Queued             int       `json:"queued"`
	RPMLimit           int       `json:"rpm_limit"`
	WindowUsed         int       `json:"window_used"`
	WindowMinutes      int       `json:"window_minutes"`
	Source             string    `json:"source,omitempty"`
	AsOf               time.Time `json:"as_of"`
}

type entityShard struct {
	sync.Mutex
	entities map[string]*Entity
}
type historyBucket struct {
	second             int64
	started, completed int64
}
type Traffic struct {
	shards    [32]entityShard
	historyMu sync.Mutex
	history   []historyBucket
	active    atomic.Int64
	StartedAt time.Time
}

func NewTraffic() *Traffic    { return &Traffic{StartedAt: time.Now().UTC()} }
func KeyID(key string) string { h := sha256.Sum256([]byte(key)); return hex.EncodeToString(h[:16]) }
func shardFor(id string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(id); i++ {
		h = (h ^ uint32(id[i])) * 16777619
	}
	return h % 32
}

func (t *Traffic) Lookup(kind, id string) *Entity {
	key := kind + ":" + id
	shard := &t.shards[shardFor(key)]
	shard.Lock()
	e := shard.entities[key]
	shard.Unlock()
	return e
}

func (t *Traffic) Acquire(kind, id string, limit int) (*Entity, bool) {
	key := kind + ":" + id
	shard := &t.shards[shardFor(key)]
	shard.Lock()
	defer shard.Unlock()
	if shard.entities == nil {
		shard.entities = make(map[string]*Entity)
	}
	e := shard.entities[key]
	if e == nil {
		e = &Entity{id: id}
		shard.entities[key] = e
	}
	return e, e.TryAcquire(limit)
}

func (e *Entity) TryAcquire(limit int) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.limit = limit
	if limit > 0 && e.active >= limit {
		return false
	}
	e.active++
	e.last = time.Now()
	return true
}
func (e *Entity) Busy(limit int) bool {
	if e == nil || limit <= 0 {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.limit = limit
	return e.active >= limit
}
func (e *Entity) Release() bool {
	if e == nil {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	freed := e.limit > 0 && e.active == e.limit
	if e.active > 0 {
		e.active--
	}
	e.last = time.Now()
	return freed
}
func (e *Entity) Start(now time.Time) {
	if e == nil {
		return
	}
	e.mu.Lock()
	e.started.add(now)
	e.last = now
	e.mu.Unlock()
}
func (e *Entity) Complete(now time.Time) {
	if e == nil {
		return
	}
	e.mu.Lock()
	e.completed.add(now)
	e.last = now
	e.mu.Unlock()
}
func (e *Entity) Snapshot(now time.Time) Snapshot {
	if e == nil {
		return Snapshot{AsOf: now}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	r, m := e.started.counts(now)
	c, _ := e.completed.counts(now)
	return Snapshot{ID: e.id, Active: e.active, WaitingCredentials: e.waiting, Limit: e.limit, RPM60: r, CurrentMinute: m, CompletedRPM60: c, AsOf: now}
}

// ObserveLogical is called once at admission and once at final completion.
func (t *Traffic) ObserveLogical(start bool, now time.Time) {
	if start {
		t.active.Add(1)
	} else {
		t.active.Add(-1)
	}
	t.historyMu.Lock()
	defer t.historyMu.Unlock()
	if t.history == nil {
		t.history = make([]historyBucket, historySeconds)
	}
	s := now.Unix()
	b := &t.history[uint64(s)%historySeconds]
	if b.second != s {
		*b = historyBucket{second: s}
	}
	if start {
		b.started++
	} else {
		b.completed++
	}
}

type Point struct {
	From     time.Time `json:"from"`
	To       time.Time `json:"to"`
	Requests int64     `json:"requests"`
	RPM      float64   `json:"rpm"`
}
type Overview struct {
	AsOf          time.Time `json:"as_of"`
	StartedAt     time.Time `json:"started_at"`
	Active        int64     `json:"active"`
	Queued        int       `json:"queued"`
	QueuedBytes   int64     `json:"queued_bytes"`
	Requests      int64     `json:"requests"`
	RPM           float64   `json:"rpm"`
	WindowSeconds int       `json:"window_seconds"`
	BucketSeconds int       `json:"bucket_seconds"`
	Items         []Point   `json:"items"`
}

func (t *Traffic) Overview(now time.Time, seconds int, currentMinute, completed bool) Overview {
	seconds = max(60, min(historySeconds, seconds))
	end := now.Unix() + 1
	from := end - int64(seconds)
	if currentMinute {
		from = now.Unix() / 60 * 60
		seconds = int(end - from)
	}
	step := (seconds + 1199) / 1200
	result := Overview{AsOf: now, StartedAt: t.StartedAt, Active: max(0, t.active.Load()), WindowSeconds: seconds, BucketSeconds: step, Items: make([]Point, 0, (seconds+step-1)/step)}
	counts := make([]int64, seconds)
	t.historyMu.Lock()
	if t.history != nil {
		for sec := from; sec < end; sec++ {
			b := t.history[uint64(sec)%historySeconds]
			if b.second == sec {
				if completed {
					counts[sec-from] = b.completed
				} else {
					counts[sec-from] = b.started
				}
			}
		}
	}
	t.historyMu.Unlock()
	for s := from; s < end; s += int64(step) {
		to := min(end, s+int64(step))
		point := Point{From: time.Unix(s, 0).UTC(), To: time.Unix(to, 0).UTC()}
		for sec := s; sec < to; sec++ {
			point.Requests += counts[sec-from]
		}
		point.RPM = float64(point.Requests) * 60 / float64(to-s)
		result.Requests += point.Requests
		result.Items = append(result.Items, point)
	}
	result.RPM = float64(result.Requests) * 60 / float64(seconds)
	if currentMinute {
		result.RPM = float64(result.Requests)
	}
	return result
}

// PruneShard bounds maintenance work and never removes an active entry.
func (t *Traffic) PruneShard(index int, now time.Time) {
	s := &t.shards[index%len(t.shards)]
	s.Lock()
	defer s.Unlock()
	for id, e := range s.entities {
		e.mu.Lock()
		idle := e.active == 0 && now.Sub(e.last) > 2*time.Minute
		e.mu.Unlock()
		if idle {
			delete(s.entities, id)
		}
	}
}

func (t *Traffic) Retire(kind, id string) {
	key := kind + ":" + id
	shard := &t.shards[shardFor(key)]
	shard.Lock()
	delete(shard.entities, key)
	shard.Unlock()
}

func (e *Entity) WaitCredential(delta int) {
	if e != nil {
		e.mu.Lock()
		e.waiting += delta
		e.mu.Unlock()
	}
}

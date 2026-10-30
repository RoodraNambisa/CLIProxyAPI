package admission

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

// Busy is a local admission failure. A zero ReadyAt means event-driven waiting.
type Busy struct {
	Reason  string
	ReadyAt time.Time
}

func (e *Busy) Error() string             { return e.Reason }
func (*Busy) StatusCode() int             { return http.StatusTooManyRequests }
func (*Busy) SkipAuthResult() bool        { return true }
func (*Busy) PreserveErrorResponse() bool { return true }
func (e *Busy) Headers() http.Header {
	h := make(http.Header)
	if !e.ReadyAt.IsZero() {
		h.Set("Retry-After", strconv.Itoa(max(1, int(time.Until(e.ReadyAt).Seconds()+0.999))))
	}
	return h
}

type waiter struct {
	scope, group   string
	element        *list.Element
	ctx            context.Context
	try            func() error
	bytes          int64
	deadline, next time.Time
	done           chan error
	executing      bool
}

// Queue has one coordinator only while work waits. Callbacks never run under its lock.
type Queue struct {
	mu       sync.Mutex
	waiters  map[string]*list.List
	groups   map[string]int
	count    int
	bytes    int64
	maxCount int
	maxBytes int64
	notify   chan struct{}
	dirty    map[string]bool
	claims   map[string]bool
	running  bool
	closed   bool
}

func NewQueue() *Queue {
	return &Queue{claims: make(map[string]bool), waiters: make(map[string]*list.List), groups: make(map[string]int), dirty: make(map[string]bool), notify: make(chan struct{}, 1), maxCount: 1024, maxBytes: 256 << 20}
}
func (q *Queue) Configure(c config.AdmissionConfig) {
	q.mu.Lock()
	q.maxCount, q.maxBytes = c.Limits()
	q.signalLocked("*")
	q.mu.Unlock()
}
func (q *Queue) signalLocked(group string) {
	if group != "*" && q.groups[group] == 0 {
		return
	}
	q.dirty[group] = true
	select {
	case q.notify <- struct{}{}:
	default:
	}
}
func (q *Queue) Notify(group string) {
	q.mu.Lock()
	if group == "*" || q.groups[group] > 0 {
		q.signalLocked(group)
	}
	q.mu.Unlock()
}
func (q *Queue) Group(scope string) string {
	q.mu.Lock()
	defer q.mu.Unlock()
	if l := q.waiters[scope]; l != nil && l.Front() != nil {
		return l.Front().Value.(*waiter).group
	}
	return ""
}
func (q *Queue) Counts(group string) (int, int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if group != "" {
		return q.groups[group], 0
	}
	return q.count, q.bytes
}

// Do serializes a scope's waiters. Spent is request-owned and survives retries.
func (q *Queue) Do(ctx context.Context, scope, group string, policy config.AdmissionQueue, bytes int64, spent *time.Duration, try func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !policy.Enabled {
		return try()
	}
	q.mu.Lock()
	hasWaiting := q.claims[scope] || q.waiters[scope] != nil && q.waiters[scope].Len() > 0
	if !hasWaiting {
		q.claims[scope] = true
	}
	q.mu.Unlock()
	var err error
	if !hasWaiting {
		err = try()
		var busy *Busy
		if !errors.As(err, &busy) {
			q.mu.Lock()
			delete(q.claims, scope)
			q.signalLocked(group)
			q.mu.Unlock()
			return err
		}
	}
	if !hasWaiting {
		defer func() {
			if !hasWaiting {
				q.mu.Lock()
				delete(q.claims, scope)
				q.signalLocked(group)
				q.mu.Unlock()
			}
		}()
	}
	start := time.Now()
	w := &waiter{scope: scope, group: group, ctx: ctx, try: try, bytes: max(0, bytes), done: make(chan error, 1)}
	if policy.WaitTimeoutSeconds > 0 {
		remaining := time.Duration(policy.WaitTimeoutSeconds) * time.Second
		if spent != nil {
			remaining -= *spent
		}
		if remaining <= 0 {
			return &Busy{Reason: "Admission queue wait timed out"}
		}
		w.deadline = start.Add(remaining)
	}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return context.Canceled
	}
	if q.count >= q.maxCount || w.bytes > q.maxBytes-q.bytes || policy.MaxWaiting > 0 && q.groups[group] >= policy.MaxWaiting {
		q.mu.Unlock()
		return &Busy{Reason: "Admission waiting queue is full"}
	}
	l := q.waiters[scope]
	if l == nil {
		l = list.New()
		q.waiters[scope] = l
	}
	if !hasWaiting {
		w.element = l.PushFront(w)
		delete(q.claims, scope)
		hasWaiting = true
	} else {
		w.element = l.PushBack(w)
	}
	q.count++
	q.groups[group]++
	q.bytes += w.bytes
	q.signalLocked(group)
	if !q.running {
		q.running = true
		go q.run()
	}
	q.mu.Unlock()
	select {
	case err = <-w.done:
	case <-ctx.Done():
		q.mu.Lock()
		executing := w.executing || w.element == nil
		if !executing {
			q.removeLocked(w)
			q.signalLocked(group)
		}
		q.mu.Unlock()
		if executing {
			err = <-w.done
		} else {
			err = ctx.Err()
		}
	}
	if spent != nil {
		*spent += time.Since(start)
	}
	return err
}
func (q *Queue) removeLocked(w *waiter) {
	if w.element == nil {
		return
	}
	l := q.waiters[w.scope]
	l.Remove(w.element)
	w.element = nil
	w.try = nil
	w.ctx = nil
	q.count--
	q.bytes -= w.bytes
	q.groups[w.group]--
	if q.groups[w.group] == 0 {
		delete(q.groups, w.group)
	}
	if l.Len() == 0 {
		delete(q.waiters, w.scope)
	}
}
func (q *Queue) run() {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		q.mu.Lock()
		if q.count == 0 {
			q.running = false
			q.mu.Unlock()
			return
		}
		now := time.Now()
		var ready []*waiter
		next := now.Add(time.Hour)
		for scope, l := range q.waiters {
			if q.claims[scope] {
				continue
			}
			for el := l.Front(); el != nil; {
				w := el.Value.(*waiter)
				el = el.Next()
				if w.ctx.Err() != nil || !w.deadline.IsZero() && !w.deadline.After(now) {
					err := w.ctx.Err()
					q.removeLocked(w)
					if err == nil {
						err = &Busy{Reason: "Admission queue wait timed out"}
					}
					w.done <- err
					continue
				}
				if !w.deadline.IsZero() && w.deadline.Before(next) {
					next = w.deadline
				}
			}
			if front := l.Front(); front != nil {
				w := front.Value.(*waiter)
				if q.dirty["*"] || q.dirty[w.group] || !w.next.IsZero() && !w.next.After(now) {
					ready = append(ready, w)
				}
				if !w.next.IsZero() && w.next.Before(next) {
					next = w.next
				}
			}
		}
		clear(q.dirty)
		q.mu.Unlock()
		for _, w := range ready {
			q.mu.Lock()
			if w.element == nil {
				q.mu.Unlock()
				continue
			}
			w.executing = true
			q.mu.Unlock()
			err := w.ctx.Err()
			if err == nil {
				err = w.try()
			}
			q.mu.Lock()
			w.executing = false
			if err != nil && (q.closed || w.ctx.Err() != nil) {
				err = context.Canceled
			}
			if w.element != nil {
				var busy *Busy
				if errors.As(err, &busy) && w.ctx.Err() == nil {
					w.next = busy.ReadyAt
					if !w.next.IsZero() && w.next.Before(next) {
						next = w.next
					}
				} else {
					q.removeLocked(w)
					w.done <- err
					q.signalLocked(w.group)
				}
			}
			q.mu.Unlock()
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(max(time.Millisecond, time.Until(next)))
		select {
		case <-q.notify:
		case <-timer.C:
		}
	}
}
func (q *Queue) Close() {
	q.mu.Lock()
	q.closed = true
	for _, l := range q.waiters {
		for el := l.Front(); el != nil; {
			w := el.Value.(*waiter)
			el = el.Next()
			if w.executing {
				continue
			}
			q.removeLocked(w)
			w.done <- fmt.Errorf("admission stopped: %w", context.Canceled)
		}
	}
	q.signalLocked("*")
	q.mu.Unlock()
}

// A freed credential can satisfy a request that had also considered other
// priorities. Notify routing groups only; Key RPM queues are independent.
func (q *Queue) NotifyRoutes() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for group := range q.groups {
		if strings.HasPrefix(group, "route:") {
			q.signalLocked(group)
		}
	}
}

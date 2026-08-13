package executor

import (
	"container/heap"
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type chatGPTWebReloginQueueTask struct {
	authID        string
	instanceID    string
	generationKey string
	attempt       int
	dueAt         time.Time
	sequence      uint64
	heapIndex     int
}

type chatGPTWebReloginQueueActive struct {
	task   *chatGPTWebReloginQueueTask
	cancel context.CancelFunc
}

type chatGPTWebReloginTaskHeap []*chatGPTWebReloginQueueTask

func (h chatGPTWebReloginTaskHeap) Len() int { return len(h) }

func (h chatGPTWebReloginTaskHeap) Less(i, j int) bool {
	if !h[i].dueAt.Equal(h[j].dueAt) {
		return h[i].dueAt.Before(h[j].dueAt)
	}
	return h[i].sequence < h[j].sequence
}

func (h chatGPTWebReloginTaskHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}

func (h *chatGPTWebReloginTaskHeap) Push(value any) {
	task := value.(*chatGPTWebReloginQueueTask)
	task.heapIndex = len(*h)
	*h = append(*h, task)
}

func (h *chatGPTWebReloginTaskHeap) Pop() any {
	old := *h
	last := len(old) - 1
	task := old[last]
	old[last] = nil
	task.heapIndex = -1
	*h = old[:last]
	return task
}

type chatGPTWebReloginQueue struct {
	executor *ChatGPTWebExecutor
	ctx      context.Context
	cancel   context.CancelFunc
	wake     chan struct{}
	work     chan *chatGPTWebReloginQueueTask

	mu       sync.Mutex
	enabled  bool
	closed   bool
	sequence uint64
	tasks    map[string]*chatGPTWebReloginQueueTask
	active   map[string]*chatGPTWebReloginQueueActive
	delayed  chatGPTWebReloginTaskHeap

	promoted     atomic.Uint64
	deduplicated atomic.Uint64
	canceled     atomic.Uint64
	wg           sync.WaitGroup
}

func newChatGPTWebReloginQueue(executor *ChatGPTWebExecutor, parent context.Context, workers int) *chatGPTWebReloginQueue {
	if parent == nil {
		parent = context.Background()
	}
	if workers <= 0 {
		workers = chatGPTWebBackgroundReloginConcurrency
	}
	ctx, cancel := context.WithCancel(parent)
	queue := &chatGPTWebReloginQueue{
		executor: executor,
		ctx:      ctx,
		cancel:   cancel,
		wake:     make(chan struct{}, 1),
		work:     make(chan *chatGPTWebReloginQueueTask),
		tasks:    make(map[string]*chatGPTWebReloginQueueTask),
		active:   make(map[string]*chatGPTWebReloginQueueActive),
	}
	heap.Init(&queue.delayed)
	queue.wg.Add(1 + workers)
	go queue.schedule()
	for range workers {
		go queue.worker()
	}
	return queue
}

func (q *chatGPTWebReloginQueue) close() {
	if q == nil {
		return
	}
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		q.enabled = false
		q.clearLocked(true)
		q.cancel()
	}
	q.mu.Unlock()
	q.notify()
	q.wg.Wait()
}

func (q *chatGPTWebReloginQueue) setEnabled(enabled bool) {
	if q == nil {
		return
	}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	changed := q.enabled != enabled
	q.enabled = enabled
	if changed && !enabled {
		q.clearLocked(true)
	}
	q.mu.Unlock()
	if changed {
		q.notify()
	}
}

func (q *chatGPTWebReloginQueue) enqueueAuth(auth *cliproxyauth.Auth) bool {
	if q == nil || auth == nil {
		return false
	}
	task := &chatGPTWebReloginQueueTask{
		authID:        strings.TrimSpace(auth.ID),
		instanceID:    strings.TrimSpace(auth.RuntimeInstanceID()),
		generationKey: chatGPTWebReloginGenerationKey(auth),
		attempt:       1,
		dueAt:         q.executor.currentTime(),
		heapIndex:     -1,
	}
	if task.authID == "" || task.generationKey == "" {
		return false
	}
	q.mu.Lock()
	if q.closed || !q.enabled {
		q.mu.Unlock()
		return false
	}
	if _, exists := q.tasks[task.generationKey]; exists {
		q.deduplicated.Add(1)
		q.mu.Unlock()
		return false
	}
	if _, exists := q.active[task.generationKey]; exists {
		q.deduplicated.Add(1)
		q.mu.Unlock()
		return false
	}
	q.sequence++
	task.sequence = q.sequence
	q.tasks[task.generationKey] = task
	heap.Push(&q.delayed, task)
	q.mu.Unlock()
	q.notify()
	return true
}

func (q *chatGPTWebReloginQueue) promote(auth *cliproxyauth.Auth) *chatGPTWebReloginQueueTask {
	if q == nil || auth == nil {
		return nil
	}
	key := chatGPTWebReloginGenerationKey(auth)
	q.mu.Lock()
	task := q.tasks[key]
	if task != nil {
		delete(q.tasks, key)
		if task.heapIndex >= 0 {
			heap.Remove(&q.delayed, task.heapIndex)
		}
		q.promoted.Add(1)
	}
	q.mu.Unlock()
	if task != nil {
		q.notify()
	}
	return task
}

func (q *chatGPTWebReloginQueue) restore(task *chatGPTWebReloginQueueTask) {
	if q == nil || task == nil || !q.executor.backgroundReloginTaskPending(task) {
		return
	}
	task.dueAt = q.executor.currentTime()
	task.heapIndex = -1
	q.mu.Lock()
	if q.closed || !q.enabled || q.tasks[task.generationKey] != nil || q.active[task.generationKey] != nil {
		q.mu.Unlock()
		return
	}
	q.sequence++
	task.sequence = q.sequence
	q.tasks[task.generationKey] = task
	heap.Push(&q.delayed, task)
	q.mu.Unlock()
	q.notify()
}

func (q *chatGPTWebReloginQueue) removeAuthInstance(authID string, instanceID string) {
	if q == nil || strings.TrimSpace(authID) == "" {
		return
	}
	authID = strings.TrimSpace(authID)
	instanceID = strings.TrimSpace(instanceID)
	q.mu.Lock()
	removed := uint64(0)
	for key, task := range q.tasks {
		if task.authID != authID || (instanceID != "" && task.instanceID != instanceID) {
			continue
		}
		delete(q.tasks, key)
		if task.heapIndex >= 0 {
			heap.Remove(&q.delayed, task.heapIndex)
		}
		removed++
	}
	for key, active := range q.active {
		if active == nil || active.task == nil || active.task.authID != authID ||
			(instanceID != "" && active.task.instanceID != instanceID) {
			continue
		}
		delete(q.active, key)
		if active.cancel != nil {
			active.cancel()
		}
		removed++
	}
	q.mu.Unlock()
	if removed > 0 {
		q.canceled.Add(removed)
		q.notify()
	}
}

func (q *chatGPTWebReloginQueue) snapshot() chatgptwebauth.BackgroundReloginRuntimeSnapshot {
	if q == nil {
		return chatgptwebauth.BackgroundReloginRuntimeSnapshot{}
	}
	now := q.executor.currentTime()
	snapshot := chatgptwebauth.BackgroundReloginRuntimeSnapshot{
		Promoted:     q.promoted.Load(),
		Deduplicated: q.deduplicated.Load(),
		Canceled:     q.canceled.Load(),
	}
	q.mu.Lock()
	for _, task := range q.tasks {
		if task.dueAt.After(now) {
			snapshot.Delayed++
		} else {
			snapshot.Queued++
		}
	}
	for _, active := range q.active {
		if active != nil && active.cancel != nil {
			snapshot.Running++
		} else {
			snapshot.Queued++
		}
	}
	q.mu.Unlock()
	return snapshot
}

func (q *chatGPTWebReloginQueue) schedule() {
	defer q.wg.Done()
	var timer *time.Timer
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			stopChatGPTWebReloginTimer(timer)
			return
		}
		if !q.enabled || len(q.delayed) == 0 {
			q.mu.Unlock()
			stopChatGPTWebReloginTimer(timer)
			timer = nil
			select {
			case <-q.ctx.Done():
				return
			case <-q.wake:
			}
			continue
		}
		task := q.delayed[0]
		delay := task.dueAt.Sub(q.executor.currentTime())
		q.mu.Unlock()
		if delay > 0 {
			if timer == nil {
				timer = time.NewTimer(delay)
			} else {
				stopChatGPTWebReloginTimer(timer)
				timer.Reset(delay)
			}
			select {
			case <-q.ctx.Done():
				stopChatGPTWebReloginTimer(timer)
				return
			case <-q.wake:
				continue
			case <-timer.C:
			}
		}

		q.mu.Lock()
		if q.closed || !q.enabled || len(q.delayed) == 0 || q.delayed[0] != task || task.dueAt.After(q.executor.currentTime()) {
			q.mu.Unlock()
			continue
		}
		heap.Pop(&q.delayed)
		delete(q.tasks, task.generationKey)
		q.active[task.generationKey] = &chatGPTWebReloginQueueActive{task: task}
		q.mu.Unlock()

		select {
		case <-q.ctx.Done():
			q.finish(task, false)
			return
		case <-q.wake:
			q.mu.Lock()
			_, active := q.active[task.generationKey]
			q.mu.Unlock()
			if !active {
				continue
			}
			select {
			case <-q.ctx.Done():
				q.finish(task, false)
				return
			case q.work <- task:
			}
		case q.work <- task:
		}
	}
}

func (q *chatGPTWebReloginQueue) worker() {
	defer q.wg.Done()
	for {
		select {
		case <-q.ctx.Done():
			return
		case task := <-q.work:
			if task == nil {
				continue
			}
			ctx, cancel := context.WithCancel(q.ctx)
			q.mu.Lock()
			active := q.active[task.generationKey]
			if active != nil && active.task == task && q.enabled && !q.closed {
				active.cancel = cancel
			} else {
				active = nil
			}
			q.mu.Unlock()
			if active == nil {
				cancel()
				continue
			}
			retry := q.executor.executeBackgroundReloginTask(ctx, task)
			cancel()
			q.finish(task, retry)
		}
	}
}

func (q *chatGPTWebReloginQueue) finish(task *chatGPTWebReloginQueueTask, retry bool) {
	if q == nil || task == nil {
		return
	}
	q.mu.Lock()
	active := q.active[task.generationKey]
	if active == nil || active.task != task {
		q.mu.Unlock()
		return
	}
	delete(q.active, task.generationKey)
	if retry && q.enabled && !q.closed && q.tasks[task.generationKey] == nil {
		q.sequence++
		task.sequence = q.sequence
		task.heapIndex = -1
		q.tasks[task.generationKey] = task
		heap.Push(&q.delayed, task)
	}
	q.mu.Unlock()
	q.notify()
}

func (q *chatGPTWebReloginQueue) clearLocked(cancelActive bool) {
	removed := uint64(len(q.tasks))
	q.tasks = make(map[string]*chatGPTWebReloginQueueTask)
	q.delayed = nil
	heap.Init(&q.delayed)
	if cancelActive {
		removed += uint64(len(q.active))
		for _, active := range q.active {
			if active != nil && active.cancel != nil {
				active.cancel()
			}
		}
		q.active = make(map[string]*chatGPTWebReloginQueueActive)
	}
	if removed > 0 {
		q.canceled.Add(removed)
	}
}

func (q *chatGPTWebReloginQueue) notify() {
	if q == nil {
		return
	}
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func stopChatGPTWebReloginTimer(timer *time.Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

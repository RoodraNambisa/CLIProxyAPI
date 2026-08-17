package executor

import (
	"container/heap"
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
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
	task    *chatGPTWebReloginQueueTask
	cancel  context.CancelFunc
	running bool
}

type chatGPTWebReloginExecutionGate struct {
	mu      sync.Mutex
	limit   int
	active  int
	changed chan struct{}
}

var sharedChatGPTWebReloginExecutionGate = &chatGPTWebReloginExecutionGate{
	limit:   config.DefaultChatGPTWebAutoReloginWorkers,
	changed: make(chan struct{}),
}

func (gate *chatGPTWebReloginExecutionGate) resize(limit int) {
	if gate == nil {
		return
	}
	if limit < 1 {
		limit = config.DefaultChatGPTWebAutoReloginWorkers
	}
	gate.mu.Lock()
	if gate.limit != limit {
		gate.limit = limit
		close(gate.changed)
		gate.changed = make(chan struct{})
	}
	gate.mu.Unlock()
}

func (gate *chatGPTWebReloginExecutionGate) acquire(ctx context.Context) (func(), bool) {
	if gate == nil {
		return func() {}, true
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, false
		}
		gate.mu.Lock()
		if gate.active < gate.limit {
			gate.active++
			gate.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() {
					gate.mu.Lock()
					if gate.active > 0 {
						gate.active--
					}
					close(gate.changed)
					gate.changed = make(chan struct{})
					gate.mu.Unlock()
				})
			}, true
		}
		changed := gate.changed
		gate.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, false
		case <-changed:
		}
	}
}

type chatGPTWebReloginEnqueueResult uint8

const (
	chatGPTWebReloginEnqueueRejected chatGPTWebReloginEnqueueResult = iota
	chatGPTWebReloginEnqueueAccepted
	chatGPTWebReloginEnqueueDeduplicated
	chatGPTWebReloginEnqueueBackpressured
)

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
	changed  chan struct{}

	mu          sync.Mutex
	enabled     bool
	closed      bool
	workerLimit int
	workerCount int
	queueLimit  int
	sequence    uint64
	tasks       map[string]*chatGPTWebReloginQueueTask
	active      map[string]*chatGPTWebReloginQueueActive
	byAuthID    map[string]map[string]struct{}
	delayed     chatGPTWebReloginTaskHeap

	promoted                  atomic.Uint64
	deduplicated              atomic.Uint64
	canceled                  atomic.Uint64
	backpressured             atomic.Uint64
	succeeded                 atomic.Uint64
	failed                    atomic.Uint64
	exhausted                 atomic.Uint64
	dead                      atomic.Uint64
	historicalEligible        atomic.Int64
	historicalBlockedByMethod atomic.Int64
	historicalCooling         atomic.Int64
	historicalExhausted       atomic.Int64
	reconcileNeeded           atomic.Bool
	wg                        sync.WaitGroup
}

func newChatGPTWebReloginQueue(executor *ChatGPTWebExecutor, parent context.Context, workers int, queueLimits ...int) *chatGPTWebReloginQueue {
	if parent == nil {
		parent = context.Background()
	}
	if workers <= 0 {
		workers = config.DefaultChatGPTWebAutoReloginWorkers
	}
	queueLimit := config.DefaultChatGPTWebAutoReloginQueueSize
	if len(queueLimits) > 0 && queueLimits[0] > 0 {
		queueLimit = queueLimits[0]
	}
	ctx, cancel := context.WithCancel(parent)
	queue := &chatGPTWebReloginQueue{
		executor:    executor,
		ctx:         ctx,
		cancel:      cancel,
		wake:        make(chan struct{}, 1),
		work:        make(chan *chatGPTWebReloginQueueTask),
		changed:     make(chan struct{}),
		workerLimit: workers,
		queueLimit:  queueLimit,
		tasks:       make(map[string]*chatGPTWebReloginQueueTask),
		active:      make(map[string]*chatGPTWebReloginQueueActive),
		byAuthID:    make(map[string]map[string]struct{}),
	}
	heap.Init(&queue.delayed)
	queue.wg.Add(1)
	go queue.schedule()
	queue.mu.Lock()
	queue.startWorkersLocked(workers)
	queue.mu.Unlock()
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
		close(q.changed)
		q.changed = make(chan struct{})
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
	workers := q.workerLimit
	queueLimit := q.queueLimit
	q.mu.Unlock()
	q.setConfig(enabled, workers, queueLimit)
}

func (q *chatGPTWebReloginQueue) setConfig(enabled bool, workers, queueLimit int) {
	if q == nil {
		return
	}
	if workers < 1 {
		workers = config.DefaultChatGPTWebAutoReloginWorkers
	}
	if queueLimit < 1 {
		queueLimit = config.DefaultChatGPTWebAutoReloginQueueSize
	}
	sharedChatGPTWebReloginExecutionGate.resize(workers)
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	changed := q.enabled != enabled
	q.enabled = enabled
	previousQueueLimit := q.queueLimit
	q.workerLimit = workers
	q.queueLimit = queueLimit
	if changed && !enabled {
		q.clearLocked(true)
	}
	close(q.changed)
	q.changed = make(chan struct{})
	if q.workerCount < q.workerLimit {
		q.startWorkersLocked(q.workerLimit - q.workerCount)
	}
	q.mu.Unlock()
	q.notify()
	if enabled && queueLimit > previousQueueLimit && q.reconcileNeeded.Swap(false) {
		q.executor.scheduleBackgroundReloginBackpressureReconcile()
	}
}

func (q *chatGPTWebReloginQueue) enqueueAuth(auth *cliproxyauth.Auth) chatGPTWebReloginEnqueueResult {
	if q == nil || auth == nil {
		return chatGPTWebReloginEnqueueRejected
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
		return chatGPTWebReloginEnqueueRejected
	}
	q.mu.Lock()
	if q.closed || !q.enabled {
		q.mu.Unlock()
		return chatGPTWebReloginEnqueueRejected
	}
	if _, exists := q.tasks[task.generationKey]; exists {
		q.deduplicated.Add(1)
		q.mu.Unlock()
		return chatGPTWebReloginEnqueueDeduplicated
	}
	if _, exists := q.active[task.generationKey]; exists {
		q.deduplicated.Add(1)
		q.mu.Unlock()
		return chatGPTWebReloginEnqueueDeduplicated
	}
	if len(q.tasks)+len(q.active) >= q.queueLimit {
		q.backpressured.Add(1)
		q.reconcileNeeded.Store(true)
		q.mu.Unlock()
		return chatGPTWebReloginEnqueueBackpressured
	}
	q.sequence++
	task.sequence = q.sequence
	q.tasks[task.generationKey] = task
	q.indexTaskLocked(task)
	heap.Push(&q.delayed, task)
	q.mu.Unlock()
	q.notify()
	return chatGPTWebReloginEnqueueAccepted
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
		q.unindexTaskLocked(task)
		q.promoted.Add(1)
	}
	q.mu.Unlock()
	if task != nil {
		q.notify()
		q.reconcileBackpressureAfterCapacityRelease()
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
	if len(q.tasks)+len(q.active) >= q.queueLimit {
		q.backpressured.Add(1)
		q.reconcileNeeded.Store(true)
		q.mu.Unlock()
		return
	}
	q.sequence++
	task.sequence = q.sequence
	q.tasks[task.generationKey] = task
	q.indexTaskLocked(task)
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
	for key := range q.byAuthID[authID] {
		if task := q.tasks[key]; task != nil {
			if instanceID != "" && task.instanceID != instanceID {
				continue
			}
			delete(q.tasks, key)
			if task.heapIndex >= 0 {
				heap.Remove(&q.delayed, task.heapIndex)
			}
			q.unindexTaskLocked(task)
			removed++
			continue
		}
		if active := q.active[key]; active != nil && active.task != nil {
			if instanceID != "" && active.task.instanceID != instanceID {
				continue
			}
			delete(q.active, key)
			q.unindexTaskLocked(active.task)
			if active.cancel != nil {
				active.cancel()
			}
			removed++
			continue
		}
		q.unindexKeyLocked(authID, key)
	}
	q.mu.Unlock()
	if removed > 0 {
		q.canceled.Add(removed)
		q.notify()
		q.reconcileBackpressureAfterCapacityRelease()
	}
}

func (q *chatGPTWebReloginQueue) snapshot() chatgptwebauth.BackgroundReloginRuntimeSnapshot {
	if q == nil {
		return chatgptwebauth.BackgroundReloginRuntimeSnapshot{}
	}
	now := q.executor.currentTime()
	snapshot := chatgptwebauth.BackgroundReloginRuntimeSnapshot{
		Promoted:                  q.promoted.Load(),
		Deduplicated:              q.deduplicated.Load(),
		Canceled:                  q.canceled.Load(),
		Backpressured:             q.backpressured.Load(),
		Succeeded:                 q.succeeded.Load(),
		Failed:                    q.failed.Load(),
		Exhausted:                 q.exhausted.Load(),
		Dead:                      q.dead.Load(),
		HistoricalEligible:        int(q.historicalEligible.Load()),
		HistoricalBlockedByMethod: int(q.historicalBlockedByMethod.Load()),
		HistoricalCooling:         int(q.historicalCooling.Load()),
		HistoricalExhausted:       int(q.historicalExhausted.Load()),
	}
	q.mu.Lock()
	snapshot.WorkerLimit = q.workerLimit
	snapshot.Workers = q.workerCount
	snapshot.QueueLimit = q.queueLimit
	snapshot.Shrinking = len(q.tasks)+len(q.active) > q.queueLimit || q.workerCount > q.workerLimit
	for _, task := range q.tasks {
		if task.dueAt.After(now) {
			snapshot.Delayed++
		} else {
			snapshot.Queued++
		}
	}
	for _, active := range q.active {
		if active != nil && active.running {
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
	registered := true
	defer func() {
		if registered {
			q.mu.Lock()
			if q.workerCount > 0 {
				q.workerCount--
			}
			q.mu.Unlock()
		}
		q.wg.Done()
	}()
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return
		}
		if q.workerCount > q.workerLimit {
			q.workerCount--
			registered = false
			close(q.changed)
			q.changed = make(chan struct{})
			q.mu.Unlock()
			return
		}
		changed := q.changed
		q.mu.Unlock()
		select {
		case <-q.ctx.Done():
			return
		case <-changed:
			continue
		case task := <-q.work:
			if task == nil {
				continue
			}
			ctx, cancel := context.WithCancel(context.WithValue(q.ctx, chatGPTWebReloginQueueWorkerContextKey{}, true))
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
			releaseCapacity, acquired := sharedChatGPTWebReloginExecutionGate.acquire(ctx)
			if !acquired {
				cancel()
				q.finish(task, false)
				continue
			}
			q.mu.Lock()
			active = q.active[task.generationKey]
			if active != nil && active.task == task && q.enabled && !q.closed {
				active.running = true
			} else {
				active = nil
			}
			q.mu.Unlock()
			if active == nil {
				releaseCapacity()
				cancel()
				continue
			}
			retry := q.executor.executeBackgroundReloginTask(ctx, task)
			releaseCapacity()
			cancel()
			q.finish(task, retry)
		}
	}
}

func (q *chatGPTWebReloginQueue) startWorkersLocked(count int) {
	if q == nil || q.closed || count <= 0 {
		return
	}
	q.workerCount += count
	q.wg.Add(count)
	for range count {
		go q.worker()
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
	} else {
		q.unindexTaskLocked(task)
	}
	q.mu.Unlock()
	q.notify()
	if !retry {
		q.reconcileBackpressureAfterCapacityRelease()
	}
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
		q.byAuthID = make(map[string]map[string]struct{})
	} else {
		q.byAuthID = make(map[string]map[string]struct{}, len(q.active))
		for _, active := range q.active {
			if active != nil {
				q.indexTaskLocked(active.task)
			}
		}
	}
	if removed > 0 {
		q.canceled.Add(removed)
	}
}

func (q *chatGPTWebReloginQueue) indexTaskLocked(task *chatGPTWebReloginQueueTask) {
	if q == nil || task == nil || task.authID == "" || task.generationKey == "" {
		return
	}
	keys := q.byAuthID[task.authID]
	if keys == nil {
		keys = make(map[string]struct{})
		q.byAuthID[task.authID] = keys
	}
	keys[task.generationKey] = struct{}{}
}

func (q *chatGPTWebReloginQueue) unindexTaskLocked(task *chatGPTWebReloginQueueTask) {
	if task == nil {
		return
	}
	q.unindexKeyLocked(task.authID, task.generationKey)
}

func (q *chatGPTWebReloginQueue) unindexKeyLocked(authID, generationKey string) {
	keys := q.byAuthID[authID]
	if keys == nil {
		return
	}
	delete(keys, generationKey)
	if len(keys) == 0 {
		delete(q.byAuthID, authID)
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

func (q *chatGPTWebReloginQueue) reconcileBackpressureAfterCapacityRelease() {
	if q == nil || !q.reconcileNeeded.Load() {
		return
	}
	q.mu.Lock()
	tracked := len(q.tasks) + len(q.active)
	lowWater := q.queueLimit / 2
	hasCapacity := !q.closed && q.enabled && tracked <= lowWater
	q.mu.Unlock()
	if hasCapacity && q.reconcileNeeded.Swap(false) {
		q.executor.scheduleBackgroundReloginBackpressureReconcile()
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

package executor

import (
	"container/heap"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

const (
	chatGPTWebAccountInfoTaskRetention          = 24 * time.Hour
	chatGPTWebAccountInfoTaskMaxKept            = 128
	chatGPTWebAccountInfoExpiredRecoveryBackoff = 5 * time.Minute
	chatGPTWebAccountInfoMaxRetryAfter          = 5 * time.Minute
	chatGPTWebAccountInfoMaxBodyBytes           = 1 << 20
	chatGPTWebAccountInfoMaxRedirects           = 5
	chatGPTWebAmbiguousImageRecheckCooldown     = 5 * time.Minute
)

type chatGPTWebAccountInfoTriggerMode uint8

const (
	chatGPTWebAccountInfoTriggerNone chatGPTWebAccountInfoTriggerMode = iota
	chatGPTWebAccountInfoTriggerDefault
	chatGPTWebAccountInfoTriggerAutomaticRecheck
	chatGPTWebAccountInfoTriggerForce
)

func (mode chatGPTWebAccountInfoTriggerMode) forced() bool {
	return mode == chatGPTWebAccountInfoTriggerAutomaticRecheck ||
		mode == chatGPTWebAccountInfoTriggerForce
}

type chatGPTWebAccountInfoOutcome struct {
	status          string
	errorCode       string
	retryable       bool
	retryAfter      time.Duration
	retryAt         time.Time
	quotaStateKnown bool
	exhausted       bool
	quotaResetAt    time.Time
}

type chatGPTWebImageQuotaObservation struct {
	remaining      *int
	quotaState     chatgptwebauth.QuotaState
	quotaResetAt   string
	quotaUpdatedAt string
	quotaStale     bool
	quotaLastError string
	modelStates    map[string]*cliproxyauth.ModelState
}

type chatGPTWebAccountProfileObservation struct {
	accountID        string
	planType         string
	profileUpdatedAt string
}

func captureChatGPTWebAccountProfileObservation(auth *cliproxyauth.Auth) chatGPTWebAccountProfileObservation {
	if auth == nil {
		return chatGPTWebAccountProfileObservation{}
	}
	credential, errParse := chatgptwebauth.ParseCredential(auth.Metadata)
	if errParse != nil {
		return chatGPTWebAccountProfileObservation{}
	}
	return chatGPTWebAccountProfileObservation{
		accountID:        strings.TrimSpace(credential.AccountID),
		planType:         strings.TrimSpace(credential.PlanType),
		profileUpdatedAt: strings.TrimSpace(credential.ProfileUpdatedAt),
	}
}

func (observation chatGPTWebAccountProfileObservation) matches(auth *cliproxyauth.Auth) bool {
	if auth == nil {
		return false
	}
	credential, errParse := chatgptwebauth.ParseCredential(auth.Metadata)
	if errParse != nil {
		return false
	}
	return strings.TrimSpace(credential.AccountID) == observation.accountID &&
		strings.TrimSpace(credential.PlanType) == observation.planType &&
		strings.TrimSpace(credential.ProfileUpdatedAt) == observation.profileUpdatedAt
}

func captureChatGPTWebImageQuotaObservation(auth *cliproxyauth.Auth) chatGPTWebImageQuotaObservation {
	observation := chatGPTWebImageQuotaObservation{
		modelStates: cloneChatGPTWebImageModelStates(auth),
	}
	if credential, errParse := chatgptwebauth.ParseCredential(auth.Metadata); errParse == nil {
		if credential.ImageQuotaRemaining != nil {
			remaining := *credential.ImageQuotaRemaining
			observation.remaining = &remaining
		}
		observation.quotaState = credential.QuotaState
		observation.quotaResetAt = strings.TrimSpace(credential.ImageQuotaResetAt)
		observation.quotaUpdatedAt = strings.TrimSpace(credential.QuotaUpdatedAt)
		observation.quotaStale = credential.QuotaStale
		observation.quotaLastError = strings.TrimSpace(credential.QuotaLastError)
	}
	return observation
}

func (observation chatGPTWebImageQuotaObservation) matches(auth *cliproxyauth.Auth) bool {
	credential, errParse := chatgptwebauth.ParseCredential(auth.Metadata)
	if errParse != nil {
		return false
	}
	return equalOptionalInt(credential.ImageQuotaRemaining, observation.remaining) &&
		credential.QuotaState == observation.quotaState &&
		strings.TrimSpace(credential.ImageQuotaResetAt) == observation.quotaResetAt &&
		strings.TrimSpace(credential.QuotaUpdatedAt) == observation.quotaUpdatedAt &&
		credential.QuotaStale == observation.quotaStale &&
		strings.TrimSpace(credential.QuotaLastError) == observation.quotaLastError &&
		reflect.DeepEqual(cloneChatGPTWebImageModelStates(auth), observation.modelStates)
}

func cloneChatGPTWebImageModelStates(auth *cliproxyauth.Auth) map[string]*cliproxyauth.ModelState {
	if auth == nil || len(auth.ModelStates) == 0 {
		return nil
	}
	targets := cliproxyauth.ChatGPTWebImageModelIDs(auth)
	var states map[string]*cliproxyauth.ModelState
	for model, state := range auth.ModelStates {
		if state == nil {
			continue
		}
		for _, target := range targets {
			if strings.EqualFold(strings.TrimSpace(model), strings.TrimSpace(target)) {
				if states == nil {
					states = make(map[string]*cliproxyauth.ModelState)
				}
				states[model] = state.Clone()
				break
			}
		}
	}
	return states
}

type chatGPTWebAccountInfoTaskState struct {
	snapshot chatgptwebauth.AccountInfoRefreshTask
	ctx      context.Context
	cancel   context.CancelFunc
}

type chatGPTWebAccountInfoEpochRef struct {
	released bool
}

type chatGPTWebAccountInfoWork struct {
	target             chatgptwebauth.AccountInfoRefreshTarget
	sequence           uint64
	epoch              uint64
	epochRef           *chatGPTWebAccountInfoEpochRef
	taskID             string
	index              int
	force              bool
	attempt            int
	schedule           string
	automatic          bool
	independentTrigger chatGPTWebAccountInfoTriggerMode
	quotaStateKnown    bool
	exhausted          bool
	quotaResetAt       time.Time
	partialApplied     bool
}

type chatGPTWebAccountInfoSchedule struct {
	key   string
	due   time.Time
	work  chatGPTWebAccountInfoWork
	seq   uint64
	index int
}

type chatGPTWebAccountInfoCall struct {
	ctx            context.Context
	cancel         context.CancelFunc
	done           chan struct{}
	authID         string
	authInstanceID string
	runtimeKey     string
	epoch          uint64
	outcome        chatGPTWebAccountInfoOutcome
	waiters        int
	accepting      bool
	completed      bool
	force          bool
	checkFresh     bool
	retryAttempt   int
	retryAt        time.Time
}

type chatGPTWebAccountInfoPersistenceContextKey struct{}

type chatGPTWebAccountInfoScheduleHeap []*chatGPTWebAccountInfoSchedule

func (call *chatGPTWebAccountInfoCall) key() string {
	if call == nil {
		return ""
	}
	if strings.TrimSpace(call.runtimeKey) != "" {
		return call.runtimeKey
	}
	return chatGPTWebAccountInfoAuthInstanceKey(call.authID, call.authInstanceID)
}

func (h chatGPTWebAccountInfoScheduleHeap) Len() int { return len(h) }
func (h chatGPTWebAccountInfoScheduleHeap) Less(i, j int) bool {
	if h[i].due.Equal(h[j].due) {
		return h[i].seq < h[j].seq
	}
	return h[i].due.Before(h[j].due)
}
func (h chatGPTWebAccountInfoScheduleHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *chatGPTWebAccountInfoScheduleHeap) Push(value any) {
	item := value.(*chatGPTWebAccountInfoSchedule)
	item.index = len(*h)
	*h = append(*h, item)
}
func (h *chatGPTWebAccountInfoScheduleHeap) Pop() any {
	old := *h
	last := len(old) - 1
	item := old[last]
	old[last] = nil
	*h = old[:last]
	item.index = -1
	return item
}

type chatGPTWebAccountInfoRuntime struct {
	executor *ChatGPTWebExecutor

	mu                         sync.Mutex
	cond                       *sync.Cond
	cfg                        config.ResolvedChatGPTWebAccountInfoConfig
	queue                      []chatGPTWebAccountInfoWork
	queueHead                  int
	queuedByTarget             map[string]int
	tasks                      map[string]*chatGPTWebAccountInfoTaskState
	taskReservations           int
	states                     map[string]chatgptwebauth.AccountInfoAuthRuntimeState
	inflight                   map[string]int
	inflightForce              map[string]int
	inflightTask               map[string]int
	inflightRecovery           map[string]time.Time
	pendingTriggers            map[string]chatGPTWebAccountInfoTriggerMode
	ambiguousImageRecheckAfter map[string]time.Time
	workers                    map[int]chan struct{}
	retiringWorkers            map[int]struct{}
	desiredWorkers             int
	nextID                     int
	busy                       int
	waiting                    int
	authInstances              map[string]string
	authEpoch                  map[string]uint64
	authEpochRefs              map[string]int
	schedules                  chatGPTWebAccountInfoScheduleHeap
	scheduled                  map[string]*chatGPTWebAccountInfoSchedule
	scheduledByTarget          map[string]map[string]*chatGPTWebAccountInfoSchedule
	delayedTasks               int
	schedule                   uint64
	wake                       chan struct{}
	ctx                        context.Context
	cancel                     context.CancelFunc
	wg                         sync.WaitGroup
	started                    bool
	closed                     bool

	refreshCount      uint64
	retryCount        uint64
	failedCount       uint64
	workSequence      uint64
	lastErrorSequence uint64
	lastError         string
	calls             map[string]*chatGPTWebAccountInfoCall
	now               func() time.Time
	random            io.Reader

	beforePersistedRecoveryCommit func()
	beforeAccountInfoExecution    func(chatGPTWebAccountInfoWork, *chatGPTWebAccountInfoCall, bool)
}

func newChatGPTWebAccountInfoRuntime(executor *ChatGPTWebExecutor, cfg *config.Config) *chatGPTWebAccountInfoRuntime {
	ctx, cancel := context.WithCancel(context.Background())
	runtime := &chatGPTWebAccountInfoRuntime{
		executor:                   executor,
		tasks:                      make(map[string]*chatGPTWebAccountInfoTaskState),
		states:                     make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		queuedByTarget:             make(map[string]int),
		inflight:                   make(map[string]int),
		inflightForce:              make(map[string]int),
		inflightTask:               make(map[string]int),
		inflightRecovery:           make(map[string]time.Time),
		pendingTriggers:            make(map[string]chatGPTWebAccountInfoTriggerMode),
		ambiguousImageRecheckAfter: make(map[string]time.Time),
		authInstances:              make(map[string]string),
		authEpoch:                  make(map[string]uint64),
		authEpochRefs:              make(map[string]int),
		workers:                    make(map[int]chan struct{}),
		retiringWorkers:            make(map[int]struct{}),
		scheduled:                  make(map[string]*chatGPTWebAccountInfoSchedule),
		scheduledByTarget:          make(map[string]map[string]*chatGPTWebAccountInfoSchedule),
		calls:                      make(map[string]*chatGPTWebAccountInfoCall),
		wake:                       make(chan struct{}, 1),
		ctx:                        ctx,
		cancel:                     cancel,
		now:                        time.Now,
		random:                     rand.Reader,
	}
	runtime.cond = sync.NewCond(&runtime.mu)
	runtime.cfg = accountInfoConfigSnapshot(cfg)
	return runtime
}

func (runtime *chatGPTWebAccountInfoRuntime) start() {
	if runtime == nil {
		return
	}
	runtime.mu.Lock()
	if runtime.started || runtime.closed {
		runtime.mu.Unlock()
		return
	}
	runtime.started = true
	runtime.resizeWorkersLocked(runtime.cfg.RefreshWorkers)
	runtime.wg.Add(1)
	runtime.mu.Unlock()
	go runtime.schedulerLoop()
	runtime.restoreRecoverySchedules()
}

func accountInfoConfigSnapshot(cfg *config.Config) config.ResolvedChatGPTWebAccountInfoConfig {
	var resolved config.ResolvedChatGPTWebAccountInfoConfig
	if cfg == nil {
		resolved = (config.ChatGPTWebAccountInfoConfig{}).Resolved()
	} else {
		resolved = cfg.ChatGPTWeb.AccountInfo.Resolved()
	}
	resolved.RefreshWorkers = clampChatGPTWebAccountInfoValue(
		resolved.RefreshWorkers,
		1,
		config.MaxChatGPTWebAccountInfoWorkers,
	)
	resolved.RefreshQueueSize = clampChatGPTWebAccountInfoValue(
		resolved.RefreshQueueSize,
		0,
		config.MaxChatGPTWebAccountInfoQueueSize,
	)
	resolved.RefreshTTLMinutes = clampChatGPTWebAccountInfoValue(
		resolved.RefreshTTLMinutes,
		1,
		config.MaxChatGPTWebAccountInfoTTLMinutes,
	)
	resolved.RecoveryJitterSeconds = clampChatGPTWebAccountInfoValue(
		resolved.RecoveryJitterSeconds,
		0,
		config.MaxChatGPTWebAccountInfoJitterSeconds,
	)
	resolved.MaxRetries = clampChatGPTWebAccountInfoValue(
		resolved.MaxRetries,
		0,
		config.MaxChatGPTWebAccountInfoRetries,
	)
	return resolved
}

func clampChatGPTWebAccountInfoValue(value, minimum, maximum int) int {
	if value < minimum {
		return minimum
	}
	if value > maximum {
		return maximum
	}
	return value
}

func chatGPTWebAccountInfoAuthInstanceKey(authID, authInstanceID string) string {
	authID = strings.TrimSpace(authID)
	authInstanceID = strings.TrimSpace(authInstanceID)
	if authInstanceID == "" {
		return authID
	}
	return authID + "\x00" + authInstanceID
}

func chatGPTWebAccountInfoTargetKey(target chatgptwebauth.AccountInfoRefreshTarget) string {
	return chatGPTWebAccountInfoAuthInstanceKey(target.AuthID, target.AuthInstanceID)
}

func chatGPTWebAccountInfoTargetsMatch(
	target chatgptwebauth.AccountInfoRefreshTarget,
	authID string,
	authInstanceID string,
) bool {
	if strings.TrimSpace(target.AuthID) != strings.TrimSpace(authID) {
		return false
	}
	authInstanceID = strings.TrimSpace(authInstanceID)
	return authInstanceID == "" || strings.TrimSpace(target.AuthInstanceID) == authInstanceID
}

func chatGPTWebAccountInfoRuntimeKeyMatchesAuth(runtimeKey, authID string) bool {
	runtimeKey = strings.TrimSpace(runtimeKey)
	authID = strings.TrimSpace(authID)
	return runtimeKey == authID || strings.HasPrefix(runtimeKey, authID+"\x00")
}

func (runtime *chatGPTWebAccountInfoRuntime) resolveCurrentTarget(
	target chatgptwebauth.AccountInfoRefreshTarget,
) (chatgptwebauth.AccountInfoRefreshTarget, bool) {
	target.AuthID = strings.TrimSpace(target.AuthID)
	target.AuthInstanceID = strings.TrimSpace(target.AuthInstanceID)
	if target.AuthID == "" {
		return target, false
	}
	if runtime == nil || runtime.executor == nil || runtime.executor.manager == nil {
		return target, true
	}
	auth, ok := runtime.executor.manager.GetByID(target.AuthID)
	if !ok || auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), chatgptwebauth.Provider) {
		return target, false
	}
	currentInstanceID := strings.TrimSpace(auth.RuntimeInstanceID())
	if target.AuthInstanceID != "" && target.AuthInstanceID != currentInstanceID {
		return target, false
	}
	target.AuthInstanceID = currentInstanceID
	return target, true
}

func (runtime *chatGPTWebAccountInfoRuntime) bindCurrentTargetLocked(
	target chatgptwebauth.AccountInfoRefreshTarget,
) chatgptwebauth.AccountInfoRefreshTarget {
	target.AuthID = strings.TrimSpace(target.AuthID)
	target.AuthInstanceID = strings.TrimSpace(target.AuthInstanceID)
	if target.AuthID == "" {
		return target
	}
	if runtime.authInstances == nil {
		runtime.authInstances = make(map[string]string)
	}
	if target.AuthInstanceID == "" {
		target.AuthInstanceID = runtime.authInstances[target.AuthID]
		return target
	}
	runtime.authInstances[target.AuthID] = target.AuthInstanceID
	return target
}

func (runtime *chatGPTWebAccountInfoRuntime) resolveCurrentTargetLocked(
	target chatgptwebauth.AccountInfoRefreshTarget,
) (chatgptwebauth.AccountInfoRefreshTarget, bool) {
	target, current := runtime.resolveCurrentTarget(target)
	if !current {
		return target, false
	}
	return runtime.bindCurrentTargetLocked(target), true
}

func (runtime *chatGPTWebAccountInfoRuntime) prepareWorkTargetForCommitLocked(
	work *chatGPTWebAccountInfoWork,
) bool {
	if work == nil {
		return false
	}
	previousKey := chatGPTWebAccountInfoTargetKey(work.target)
	target, current := runtime.resolveCurrentTargetLocked(work.target)
	if !current {
		return false
	}
	if work.epochRef != nil && previousKey != chatGPTWebAccountInfoTargetKey(target) {
		return false
	}
	work.target = target
	return true
}

func (runtime *chatGPTWebAccountInfoRuntime) updateConfig(cfg *config.Config) {
	if runtime == nil {
		return
	}
	resolved := accountInfoConfigSnapshot(cfg)
	runtime.mu.Lock()
	if runtime.closed {
		runtime.mu.Unlock()
		return
	}
	previousEnabled := runtime.cfg.AutomaticRefreshEnabled()
	runtime.cfg = resolved
	resolvedEnabled := resolved.AutomaticRefreshEnabled()
	if runtime.started {
		if !resolvedEnabled {
			runtime.disableAutomaticRefreshLocked()
		}
		runtime.resizeWorkersLocked(resolved.RefreshWorkers)
		runtime.drainPendingTriggersLocked()
		runtime.cond.Broadcast()
	}
	started := runtime.started
	runtime.mu.Unlock()
	if started {
		runtime.signalScheduler()
		if !previousEnabled && resolvedEnabled {
			runtime.restoreRecoverySchedules()
		}
	}
}

func (runtime *chatGPTWebAccountInfoRuntime) disableAutomaticRefreshLocked() {
	if runtime == nil {
		return
	}
	for _, task := range runtime.tasks {
		if task == nil || task.snapshot.Force || task.snapshot.CompletedAt != nil {
			continue
		}
		task.snapshot.State = chatgptwebauth.AccountInfoTaskCanceling
		if task.cancel != nil {
			task.cancel()
		}
	}
	for key, call := range runtime.calls {
		if call == nil || call.completed || runtime.inflightTask[key] > 0 {
			continue
		}
		call.accepting = false
		if call.cancel != nil {
			call.cancel()
		}
	}

	activeQueue := runtime.queuedWorkLocked()
	filtered := activeQueue[:0]
	for _, work := range activeQueue {
		task := runtime.tasks[work.taskID]
		if work.taskID != "" && task != nil && task.snapshot.Force {
			filtered = append(filtered, work)
			continue
		}
		if work.taskID != "" {
			runtime.completeTaskWorkLocked(work, chatGPTWebAccountInfoOutcome{
				status: chatgptwebauth.AccountInfoResultCanceled,
			})
		}
		runtime.releaseWorkEpochLocked(work)
	}
	runtime.replaceQueuedWorkLocked(filtered)

	keys := make([]string, 0, len(runtime.scheduled))
	for key, entry := range runtime.scheduled {
		if entry == nil {
			continue
		}
		task := runtime.tasks[entry.work.taskID]
		if entry.work.taskID != "" && task != nil && task.snapshot.Force {
			continue
		}
		keys = append(keys, key)
	}
	for _, key := range keys {
		entry := runtime.removeScheduleLocked(key)
		if entry == nil {
			continue
		}
		if entry.work.taskID != "" {
			runtime.completeTaskWorkLocked(entry.work, chatGPTWebAccountInfoOutcome{
				status: chatgptwebauth.AccountInfoResultCanceled,
			})
		}
		runtime.releaseWorkEpochLocked(entry.work)
	}
	clear(runtime.pendingTriggers)
	clear(runtime.ambiguousImageRecheckAfter)
	runtime.cond.Broadcast()
}

func (runtime *chatGPTWebAccountInfoRuntime) resizeWorkersLocked(target int) {
	if target < 1 {
		target = 1
	}
	if target > config.MaxChatGPTWebAccountInfoWorkers {
		target = config.MaxChatGPTWebAccountInfoWorkers
	}
	runtime.desiredWorkers = target
	if runtime.retiringWorkers == nil {
		runtime.retiringWorkers = make(map[int]struct{})
	}
	active := len(runtime.workers) - len(runtime.retiringWorkers)
	for active > target {
		highest := -1
		for id := range runtime.workers {
			if _, retiring := runtime.retiringWorkers[id]; retiring {
				continue
			}
			if id > highest {
				highest = id
			}
		}
		if highest < 0 {
			break
		}
		runtime.retiringWorkers[highest] = struct{}{}
		close(runtime.workers[highest])
		active--
	}
	for len(runtime.workers) < target {
		id := runtime.nextID
		runtime.nextID++
		stop := make(chan struct{})
		runtime.workers[id] = stop
		runtime.wg.Add(1)
		go runtime.worker(id, stop)
	}
}

func (runtime *chatGPTWebAccountInfoRuntime) close() {
	if runtime == nil {
		return
	}
	runtime.mu.Lock()
	if runtime.closed {
		runtime.mu.Unlock()
		runtime.wg.Wait()
		return
	}
	runtime.closed = true
	for _, task := range runtime.tasks {
		if task != nil && task.cancel != nil {
			task.cancel()
		}
	}
	for id, stop := range runtime.workers {
		if _, retiring := runtime.retiringWorkers[id]; !retiring {
			close(stop)
		}
	}
	runtime.queue = nil
	runtime.queueHead = 0
	clear(runtime.queuedByTarget)
	runtime.schedules = nil
	clear(runtime.scheduled)
	for _, call := range runtime.calls {
		if call != nil && call.cancel != nil {
			call.cancel()
		}
	}
	runtime.cond.Broadcast()
	runtime.mu.Unlock()
	runtime.cancel()
	runtime.signalScheduler()
	runtime.wg.Wait()
	runtime.mu.Lock()
	clear(runtime.tasks)
	clear(runtime.states)
	clear(runtime.inflight)
	clear(runtime.inflightForce)
	clear(runtime.inflightTask)
	clear(runtime.inflightRecovery)
	clear(runtime.pendingTriggers)
	clear(runtime.ambiguousImageRecheckAfter)
	clear(runtime.authInstances)
	clear(runtime.authEpoch)
	clear(runtime.authEpochRefs)
	clear(runtime.scheduled)
	clear(runtime.scheduledByTarget)
	clear(runtime.calls)
	clear(runtime.workers)
	clear(runtime.retiringWorkers)
	runtime.mu.Unlock()
}

func (runtime *chatGPTWebAccountInfoRuntime) worker(id int, stop <-chan struct{}) {
	defer func() {
		runtime.mu.Lock()
		delete(runtime.workers, id)
		delete(runtime.retiringWorkers, id)
		if !runtime.closed {
			runtime.resizeWorkersLocked(runtime.desiredWorkers)
		}
		runtime.cond.Broadcast()
		runtime.mu.Unlock()
		runtime.wg.Done()
	}()
	for {
		runtime.mu.Lock()
		for !runtime.closed && runtime.queueLengthLocked() == 0 && !channelClosed(stop) {
			runtime.cond.Wait()
		}
		if runtime.closed || channelClosed(stop) {
			runtime.mu.Unlock()
			return
		}
		work, ok := runtime.dequeueLocked()
		if !ok {
			runtime.mu.Unlock()
			continue
		}
		runtime.beginAccountInfoWorkLocked(work)
		var call *chatGPTWebAccountInfoCall
		owner := false
		if !runtime.taskCanceledLocked(work.taskID) {
			call, owner = runtime.acquireOrCreateAccountInfoCallLocked(work)
		}
		startRegisteredCall := owner
		beforeExecution := runtime.beforeAccountInfoExecution
		runtime.mu.Unlock()

		if beforeExecution != nil {
			beforeExecution(work, call, owner)
		}
		var immediate *chatGPTWebAccountInfoOutcome
		if call == nil {
			call, owner, immediate = runtime.prepareAccountInfoExecution(work)
			startRegisteredCall = false
		}
		if immediate != nil {
			runtime.mu.Lock()
			runtime.completeAccountInfoWorkLocked(work, false, *immediate)
			runtime.mu.Unlock()
			continue
		}
		if startRegisteredCall {
			go runtime.runAccountInfoCall(call)
		}
		if !owner {
			runtime.mu.Lock()
			runtime.busy--
			runtime.waiting++
			runtime.wg.Add(1)
			runtime.mu.Unlock()
			go runtime.waitForSharedAccountInfoCall(work, call)
			continue
		}

		outcome := runtime.waitForAccountInfoCall(work, call, true)
		runtime.mu.Lock()
		runtime.completeAccountInfoWorkLocked(work, false, outcome)
		runtime.mu.Unlock()
	}
}

func (runtime *chatGPTWebAccountInfoRuntime) beginAccountInfoWorkLocked(work chatGPTWebAccountInfoWork) {
	runtimeKey := chatGPTWebAccountInfoTargetKey(work.target)
	runtime.busy++
	runtime.inflight[runtimeKey]++
	if work.taskID != "" {
		if runtime.inflightTask == nil {
			runtime.inflightTask = make(map[string]int)
		}
		runtime.inflightTask[runtimeKey]++
	}
	if work.force {
		runtime.inflightForce[runtimeKey]++
	}
	if chatGPTWebAccountInfoRecoveryWorkMatches(work, work.target, work.quotaResetAt) {
		if runtime.inflightRecovery == nil {
			runtime.inflightRecovery = make(map[string]time.Time)
		}
		runtime.inflightRecovery[runtimeKey] = work.quotaResetAt
	}
	state := runtime.states[runtimeKey]
	state.Refreshing = true
	runtime.states[runtimeKey] = state
	runtime.refreshNextScheduleStateForTargetLocked(work.target)
	runtime.markTaskRunningLocked(work)
}

func (runtime *chatGPTWebAccountInfoRuntime) completeAccountInfoWorkLocked(
	work chatGPTWebAccountInfoWork,
	waiting bool,
	outcome chatGPTWebAccountInfoOutcome,
) {
	if waiting {
		if runtime.waiting > 0 {
			runtime.waiting--
		}
	} else if runtime.busy > 0 {
		runtime.busy--
	}
	runtime.clearInflightRecoveryLocked(work)
	if !runtime.workEpochCurrentLocked(work) {
		runtime.finishWorkLocked(work, outcome)
		runtime.cond.Broadcast()
		return
	}
	runtimeKey := chatGPTWebAccountInfoTargetKey(work.target)
	runtime.inflight[runtimeKey]--
	if work.taskID != "" {
		runtime.inflightTask[runtimeKey]--
		if runtime.inflightTask[runtimeKey] <= 0 {
			delete(runtime.inflightTask, runtimeKey)
		}
	}
	if work.force {
		runtime.inflightForce[runtimeKey]--
		if runtime.inflightForce[runtimeKey] <= 0 {
			delete(runtime.inflightForce, runtimeKey)
		}
	}
	if runtime.inflight[runtimeKey] <= 0 {
		delete(runtime.inflight, runtimeKey)
		if runtime.workEpochCurrentLocked(work) {
			state := runtime.states[runtimeKey]
			state.Refreshing = false
			runtime.states[runtimeKey] = state
		}
	}
	pendingMode := runtime.pendingTriggers[runtimeKey]
	if accountInfoWorkSatisfiesTrigger(work, pendingMode) {
		if work.taskID != "" && pendingMode > work.independentTrigger {
			work.independentTrigger = pendingMode
		}
		delete(runtime.pendingTriggers, runtimeKey)
	}
	runtime.finishWorkLocked(work, outcome)
	runtime.enqueuePendingTriggerForTargetLocked(work.target)
	runtime.drainPendingTriggersLocked()
	runtime.cond.Broadcast()
}

func accountInfoWorkSatisfiesTrigger(
	work chatGPTWebAccountInfoWork,
	mode chatGPTWebAccountInfoTriggerMode,
) bool {
	if mode == chatGPTWebAccountInfoTriggerNone {
		return false
	}
	return !mode.forced() || work.force
}

func (runtime *chatGPTWebAccountInfoRuntime) waitForSharedAccountInfoCall(
	work chatGPTWebAccountInfoWork,
	call *chatGPTWebAccountInfoCall,
) {
	defer runtime.wg.Done()
	outcome := runtime.waitForAccountInfoCall(work, call, false)
	runtime.mu.Lock()
	runtime.completeAccountInfoWorkLocked(work, true, outcome)
	runtime.mu.Unlock()
}

func channelClosed(channel <-chan struct{}) bool {
	select {
	case <-channel:
		return true
	default:
		return false
	}
}

func (runtime *chatGPTWebAccountInfoRuntime) queueLengthLocked() int {
	if runtime == nil || runtime.queueHead >= len(runtime.queue) {
		return 0
	}
	return len(runtime.queue) - runtime.queueHead
}

func (runtime *chatGPTWebAccountInfoRuntime) queuedWorkLocked() []chatGPTWebAccountInfoWork {
	if runtime == nil || runtime.queueHead >= len(runtime.queue) {
		return nil
	}
	return runtime.queue[runtime.queueHead:]
}

func (runtime *chatGPTWebAccountInfoRuntime) dequeueLocked() (chatGPTWebAccountInfoWork, bool) {
	if runtime.queueLengthLocked() == 0 {
		return chatGPTWebAccountInfoWork{}, false
	}
	work := runtime.queue[runtime.queueHead]
	runtime.queue[runtime.queueHead] = chatGPTWebAccountInfoWork{}
	runtime.queueHead++
	runtime.assignWorkSequenceLocked(&work)
	runtime.removeQueuedTargetLocked(work)
	if runtime.queueHead == len(runtime.queue) {
		runtime.queue = nil
		runtime.queueHead = 0
		return work, true
	}
	if runtime.queueHead >= 1024 && runtime.queueHead*2 >= len(runtime.queue) {
		remaining := copy(runtime.queue, runtime.queue[runtime.queueHead:])
		clear(runtime.queue[remaining:])
		runtime.queue = runtime.queue[:remaining]
		runtime.queueHead = 0
	}
	return work, true
}

func (runtime *chatGPTWebAccountInfoRuntime) replaceQueuedWorkLocked(
	work []chatGPTWebAccountInfoWork,
) {
	active := runtime.queuedWorkLocked()
	if len(work) == 0 {
		clear(active)
		runtime.queue = nil
		runtime.queueHead = 0
		clear(runtime.queuedByTarget)
		runtime.pruneCompletedAccountInfoCallsLocked()
		return
	}
	clear(active[len(work):])
	runtime.queue = work
	runtime.queueHead = 0
	runtime.rebuildQueuedTargetsLocked()
	runtime.pruneCompletedAccountInfoCallsLocked()
}

func (runtime *chatGPTWebAccountInfoRuntime) addQueuedTargetLocked(work chatGPTWebAccountInfoWork) {
	runtimeKey := chatGPTWebAccountInfoTargetKey(work.target)
	if runtimeKey == "" {
		return
	}
	if runtime.queuedByTarget == nil {
		runtime.queuedByTarget = make(map[string]int)
	}
	runtime.queuedByTarget[runtimeKey]++
}

func (runtime *chatGPTWebAccountInfoRuntime) removeQueuedTargetLocked(work chatGPTWebAccountInfoWork) {
	runtimeKey := chatGPTWebAccountInfoTargetKey(work.target)
	if runtimeKey == "" || runtime.queuedByTarget == nil {
		return
	}
	count := runtime.queuedByTarget[runtimeKey]
	if count <= 1 {
		delete(runtime.queuedByTarget, runtimeKey)
		return
	}
	runtime.queuedByTarget[runtimeKey] = count - 1
}

func (runtime *chatGPTWebAccountInfoRuntime) rebuildQueuedTargetsLocked() {
	if runtime.queuedByTarget == nil {
		runtime.queuedByTarget = make(map[string]int)
	} else {
		clear(runtime.queuedByTarget)
	}
	for _, work := range runtime.queuedWorkLocked() {
		runtime.addQueuedTargetLocked(work)
	}
}

func (runtime *chatGPTWebAccountInfoRuntime) execute(work chatGPTWebAccountInfoWork) chatGPTWebAccountInfoOutcome {
	if work.epoch == 0 {
		runtime.mu.Lock()
		work.epoch = runtime.currentAuthEpochLocked(work.target)
		runtime.mu.Unlock()
	}
	call, owner, immediate := runtime.prepareAccountInfoExecution(work)
	if immediate != nil {
		return *immediate
	}
	return runtime.waitForAccountInfoCall(work, call, owner)
}

func (runtime *chatGPTWebAccountInfoRuntime) prepareAccountInfoExecution(
	work chatGPTWebAccountInfoWork,
) (*chatGPTWebAccountInfoCall, bool, *chatGPTWebAccountInfoOutcome) {
	if runtime == nil || runtime.executor == nil {
		outcome := chatGPTWebAccountInfoOutcome{status: chatgptwebauth.AccountInfoResultFailed, errorCode: "credential_unavailable"}
		return nil, false, &outcome
	}
	if runtime.taskCanceled(work.taskID) {
		runtime.pruneCompletedAccountInfoCalls()
		outcome := chatGPTWebAccountInfoOutcome{status: chatgptwebauth.AccountInfoResultCanceled, errorCode: "canceled"}
		return nil, false, &outcome
	}
	if call := runtime.acquireCompletedAccountInfoCall(work); call != nil {
		return call, false, nil
	}
	if !work.force {
		if outcome, fresh := runtime.executor.cachedChatGPTWebAccountInfoOutcomeForInstance(
			work.target.AuthID,
			work.target.AuthInstanceID,
		); fresh {
			if call := runtime.acquireCompletedAccountInfoCall(work); call != nil {
				return call, false, nil
			}
			return nil, false, &outcome
		}
	}
	call, owner := runtime.acquireAccountInfoCall(work)
	if call == nil {
		outcome := chatGPTWebAccountInfoOutcome{status: chatgptwebauth.AccountInfoResultCanceled, errorCode: "canceled"}
		return nil, false, &outcome
	}
	return call, owner, nil
}

func (runtime *chatGPTWebAccountInfoRuntime) waitForAccountInfoCall(
	work chatGPTWebAccountInfoWork,
	call *chatGPTWebAccountInfoCall,
	owner bool,
) chatGPTWebAccountInfoOutcome {
	if call == nil {
		return chatGPTWebAccountInfoOutcome{status: chatgptwebauth.AccountInfoResultCanceled, errorCode: "canceled"}
	}
	waitContext := runtime.workContext(work)
	select {
	case <-call.done:
		outcome, _ := runtime.releaseAccountInfoCall(call, false, work.attempt)
		return outcome
	default:
	}
	select {
	case <-waitContext.Done():
		outcome, waitForCompletion := runtime.releaseAccountInfoCall(call, true, work.attempt)
		if owner || waitForCompletion {
			<-call.done
			outcome = runtime.accountInfoCallOutcome(call)
		}
		outcome.status = chatgptwebauth.AccountInfoResultCanceled
		outcome.errorCode = "canceled"
		outcome.retryable = false
		outcome.retryAt = time.Time{}
		return outcome
	case <-call.done:
		outcome, _ := runtime.releaseAccountInfoCall(call, false, work.attempt)
		return outcome
	}
}

func (runtime *chatGPTWebAccountInfoRuntime) accountInfoCallOutcome(
	call *chatGPTWebAccountInfoCall,
) chatGPTWebAccountInfoOutcome {
	if runtime == nil || call == nil {
		return chatGPTWebAccountInfoOutcome{}
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return call.outcome
}

func (runtime *chatGPTWebAccountInfoRuntime) workContext(work chatGPTWebAccountInfoWork) context.Context {
	if runtime == nil {
		return context.Background()
	}
	baseContext := runtime.ctx
	if baseContext == nil {
		baseContext = context.Background()
	}
	if work.taskID == "" {
		return baseContext
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	task := runtime.tasks[work.taskID]
	if task == nil || task.ctx == nil {
		return baseContext
	}
	return task.ctx
}

func (runtime *chatGPTWebAccountInfoRuntime) acquireAccountInfoCall(work chatGPTWebAccountInfoWork) (*chatGPTWebAccountInfoCall, bool) {
	authID := work.target.AuthID
	if runtime == nil || strings.TrimSpace(authID) == "" {
		return nil, false
	}
	authID = strings.TrimSpace(authID)
	runtimeKey := chatGPTWebAccountInfoTargetKey(work.target)
	for {
		runtime.mu.Lock()
		if runtime.closed || !runtime.workEpochCurrentLocked(work) {
			runtime.mu.Unlock()
			return nil, false
		}
		if runtime.calls == nil {
			runtime.calls = make(map[string]*chatGPTWebAccountInfoCall)
		}
		if call := runtime.calls[runtimeKey]; call != nil {
			if call.accepting && call.epoch == work.epoch {
				if call.completed && !accountInfoCallSatisfiesWork(call, work) {
					delete(runtime.calls, runtimeKey)
					runtime.mu.Unlock()
					continue
				}
				call.force = call.force || work.force
				call.includeRetryAttempt(work.attempt)
				call.waiters++
				runtime.mu.Unlock()
				return call, false
			}
			if !call.completed {
				done := call.done
				runtime.mu.Unlock()
				select {
				case <-done:
					continue
				case <-runtime.workContext(work).Done():
					return nil, false
				}
			}
			if runtime.calls[runtimeKey] == call {
				delete(runtime.calls, runtimeKey)
			}
		}
		baseContext := runtime.ctx
		if baseContext == nil {
			baseContext = context.Background()
		}
		timeout := chatgptwebauth.DefaultAcquisitionTimeout
		if runtime.executor != nil && runtime.executor.accountInfoTimeout > 0 {
			timeout = runtime.executor.accountInfoTimeout
		}
		callContext, cancel := newChatGPTWebAccountInfoCallContext(baseContext, timeout)
		call := &chatGPTWebAccountInfoCall{
			ctx: callContext,
		}
		runtime.initializeAccountInfoCallLocked(call, work, cancel)
		runtime.mu.Unlock()
		go runtime.runAccountInfoCall(call)
		return call, true
	}
}

func (runtime *chatGPTWebAccountInfoRuntime) acquireOrCreateAccountInfoCallLocked(
	work chatGPTWebAccountInfoWork,
) (*chatGPTWebAccountInfoCall, bool) {
	authID := strings.TrimSpace(work.target.AuthID)
	if runtime == nil ||
		runtime.closed ||
		authID == "" ||
		!runtime.workEpochCurrentLocked(work) {
		return nil, false
	}
	if runtime.calls == nil {
		runtime.calls = make(map[string]*chatGPTWebAccountInfoCall)
	}
	runtimeKey := chatGPTWebAccountInfoTargetKey(work.target)
	call := runtime.calls[runtimeKey]
	if call != nil {
		if call.accepting &&
			call.epoch == work.epoch &&
			(!call.completed || accountInfoCallSatisfiesWork(call, work)) {
			call.force = call.force || work.force
			call.includeRetryAttempt(work.attempt)
			call.waiters++
			return call, false
		}
		if !call.completed {
			return nil, false
		}
		if runtime.calls[runtimeKey] == call {
			delete(runtime.calls, runtimeKey)
		}
	}
	baseContext := runtime.ctx
	if baseContext == nil {
		baseContext = context.Background()
	}
	timeout := chatgptwebauth.DefaultAcquisitionTimeout
	if runtime.executor != nil && runtime.executor.accountInfoTimeout > 0 {
		timeout = runtime.executor.accountInfoTimeout
	}
	callContext, cancel := newChatGPTWebAccountInfoCallContext(baseContext, timeout)
	call = &chatGPTWebAccountInfoCall{ctx: callContext}
	runtime.initializeAccountInfoCallLocked(call, work, cancel)
	return call, true
}

func newChatGPTWebAccountInfoCallContext(
	baseContext context.Context,
	timeout time.Duration,
) (context.Context, context.CancelFunc) {
	if baseContext == nil {
		baseContext = context.Background()
	}
	persistContext, cancelPersist := context.WithCancel(baseContext)
	acquisitionBase := context.WithValue(
		baseContext,
		chatGPTWebAccountInfoPersistenceContextKey{},
		persistContext,
	)
	acquisitionContext, cancelAcquisition := context.WithTimeout(acquisitionBase, timeout)
	return acquisitionContext, func() {
		cancelAcquisition()
		cancelPersist()
	}
}

func (runtime *chatGPTWebAccountInfoRuntime) initializeAccountInfoCallLocked(
	call *chatGPTWebAccountInfoCall,
	work chatGPTWebAccountInfoWork,
	cancel context.CancelFunc,
) {
	call.cancel = cancel
	call.done = make(chan struct{})
	call.authID = strings.TrimSpace(work.target.AuthID)
	call.authInstanceID = strings.TrimSpace(work.target.AuthInstanceID)
	call.runtimeKey = chatGPTWebAccountInfoTargetKey(work.target)
	call.epoch = work.epoch
	call.waiters = 1
	call.accepting = true
	call.force = work.force
	call.checkFresh = !work.force
	call.retryAttempt = work.attempt
	runtime.calls[call.runtimeKey] = call
	runtime.wg.Add(1)
}

func (runtime *chatGPTWebAccountInfoRuntime) acquireCompletedAccountInfoCall(
	work chatGPTWebAccountInfoWork,
) *chatGPTWebAccountInfoCall {
	if runtime == nil {
		return nil
	}
	runtimeKey := chatGPTWebAccountInfoTargetKey(work.target)
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	call := runtime.calls[runtimeKey]
	if call == nil || !call.completed {
		return nil
	}
	if !call.accepting || call.epoch != work.epoch || !accountInfoCallSatisfiesWork(call, work) {
		if call.waiters == 0 && runtime.calls[runtimeKey] == call {
			delete(runtime.calls, runtimeKey)
		}
		return nil
	}
	call.force = call.force || work.force
	call.includeRetryAttempt(work.attempt)
	call.waiters++
	return call
}

func (runtime *chatGPTWebAccountInfoRuntime) runAccountInfoCall(call *chatGPTWebAccountInfoCall) {
	defer runtime.wg.Done()
	if call.checkFresh {
		if outcome, fresh := runtime.executor.cachedChatGPTWebAccountInfoOutcomeForInstance(
			call.authID,
			call.authInstanceID,
		); fresh {
			runtime.mu.Lock()
			if !call.force {
				runtime.completeAccountInfoCallLocked(call, outcome)
				runtime.mu.Unlock()
				call.cancel()
				return
			}
			runtime.mu.Unlock()
		}
	}
	outcome := runtime.executor.refreshChatGPTWebAccountInfoForInstance(
		call.ctx,
		call.authID,
		call.authInstanceID,
		true,
	)
	runtime.mu.Lock()
	runtime.completeAccountInfoCallLocked(call, outcome)
	runtime.mu.Unlock()
	call.cancel()
}

func (runtime *chatGPTWebAccountInfoRuntime) completeAccountInfoCallLocked(
	call *chatGPTWebAccountInfoCall,
	outcome chatGPTWebAccountInfoOutcome,
) {
	if call == nil || call.completed {
		return
	}
	call.outcome = outcome
	call.completed = true
	callKey := call.key()
	if runtime.calls[callKey] == call && !runtime.queuedAccountInfoWorkCanJoinCallLocked(call) {
		delete(runtime.calls, callKey)
	}
	close(call.done)
}

func accountInfoCallSatisfiesWork(
	call *chatGPTWebAccountInfoCall,
	work chatGPTWebAccountInfoWork,
) bool {
	if call == nil || !call.completed {
		return true
	}
	return !work.force ||
		call.force ||
		call.outcome.status != chatgptwebauth.AccountInfoResultFresh
}

func (call *chatGPTWebAccountInfoCall) includeRetryAttempt(attempt int) {
	if call != nil && attempt > call.retryAttempt {
		call.retryAttempt = attempt
	}
}

func (runtime *chatGPTWebAccountInfoRuntime) queuedAccountInfoWorkCanJoinCallLocked(
	call *chatGPTWebAccountInfoCall,
) bool {
	if runtime == nil || call == nil {
		return false
	}
	canJoin := false
	for _, work := range runtime.queuedWorkLocked() {
		if chatGPTWebAccountInfoTargetKey(work.target) == call.key() &&
			work.epoch == call.epoch &&
			accountInfoCallSatisfiesWork(call, work) {
			call.includeRetryAttempt(work.attempt)
			canJoin = true
		}
	}
	return canJoin
}

func (runtime *chatGPTWebAccountInfoRuntime) pruneCompletedAccountInfoCalls() {
	if runtime == nil {
		return
	}
	runtime.mu.Lock()
	runtime.pruneCompletedAccountInfoCallsLocked()
	runtime.mu.Unlock()
}

func (runtime *chatGPTWebAccountInfoRuntime) pruneCompletedAccountInfoCallsLocked() {
	for key, call := range runtime.calls {
		if call != nil &&
			call.completed &&
			call.waiters == 0 &&
			!runtime.queuedAccountInfoWorkCanJoinCallLocked(call) {
			delete(runtime.calls, key)
		}
	}
}

func (runtime *chatGPTWebAccountInfoRuntime) releaseAccountInfoCall(
	call *chatGPTWebAccountInfoCall,
	canceled bool,
	attempt int,
) (chatGPTWebAccountInfoOutcome, bool) {
	if runtime == nil || call == nil {
		return chatGPTWebAccountInfoOutcome{status: chatgptwebauth.AccountInfoResultCanceled, errorCode: "canceled"}, false
	}
	runtime.mu.Lock()
	if call.waiters > 0 {
		call.waiters--
	}
	waitForCompletion := canceled && call.waiters == 0 && !call.completed
	if waitForCompletion {
		call.accepting = false
		call.cancel()
	}
	if call.completed &&
		call.waiters == 0 &&
		runtime.calls[call.key()] == call &&
		!runtime.queuedAccountInfoWorkCanJoinCallLocked(call) {
		delete(runtime.calls, call.key())
	}
	outcome := call.outcome
	if !canceled && outcome.retryable && outcome.status != chatgptwebauth.AccountInfoResultCanceled {
		call.includeRetryAttempt(attempt)
		if call.retryAt.IsZero() {
			delay := runtime.retryDelay(call.retryAttempt)
			if retryAfter := clampChatGPTWebAccountInfoRetryAfter(outcome.retryAfter); retryAfter > delay {
				delay = retryAfter
			}
			call.retryAt = runtime.currentTime().Add(delay)
		}
		outcome.retryAt = call.retryAt
	}
	runtime.mu.Unlock()
	return outcome, waitForCompletion
}

func (runtime *chatGPTWebAccountInfoRuntime) taskCanceled(taskID string) bool {
	if taskID == "" {
		return false
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.taskCanceledLocked(taskID)
}

func (runtime *chatGPTWebAccountInfoRuntime) taskCanceledLocked(taskID string) bool {
	if taskID == "" {
		return false
	}
	task := runtime.tasks[taskID]
	return task == nil || (task.ctx != nil && task.ctx.Err() != nil)
}

func (runtime *chatGPTWebAccountInfoRuntime) markTaskRunningLocked(work chatGPTWebAccountInfoWork) {
	if work.taskID == "" {
		return
	}
	task := runtime.tasks[work.taskID]
	if task == nil || work.index < 0 || work.index >= len(task.snapshot.Results) {
		return
	}
	if task.snapshot.StartedAt == nil {
		now := runtime.currentTime()
		task.snapshot.StartedAt = &now
		task.snapshot.State = chatgptwebauth.AccountInfoTaskRunning
	}
	result := &task.snapshot.Results[work.index]
	if result.Status == chatgptwebauth.AccountInfoResultQueued || result.Status == chatgptwebauth.AccountInfoResultRetrying {
		result.Status = chatgptwebauth.AccountInfoResultRunning
		result.Attempts = work.attempt
	}
}

func (runtime *chatGPTWebAccountInfoRuntime) finishWorkLocked(work chatGPTWebAccountInfoWork, outcome chatGPTWebAccountInfoOutcome) {
	if runtime.closed {
		return
	}
	runtime.assignWorkSequenceLocked(&work)
	runtime.assignWorkEpochLocked(&work)
	if !runtime.workEpochCurrentLocked(work) {
		runtime.completeTaskWorkLocked(work, chatGPTWebAccountInfoOutcome{
			status:    chatgptwebauth.AccountInfoResultFailed,
			errorCode: "credential_unavailable",
		})
		runtime.releaseWorkEpochLocked(work)
		return
	}
	if runtime.taskCanceledLocked(work.taskID) {
		outcome.status = chatgptwebauth.AccountInfoResultCanceled
		outcome.errorCode = ""
		outcome.retryable = false
		runtime.retainIndependentTriggerLocked(work)
	}
	if !runtime.cfg.AutomaticRefreshEnabled() && work.taskID == "" {
		outcome.retryable = false
		outcome.retryAt = time.Time{}
	}
	runtimeKey := chatGPTWebAccountInfoTargetKey(work.target)
	state := runtime.states[runtimeKey]
	if outcome.errorCode != "" {
		state.LastError = chatgptwebauth.SafeQuotaError(outcome.errorCode)
	} else {
		state.LastError = ""
	}
	runtime.updateLastErrorLocked(work.sequence, outcome.status, state.LastError)
	runtime.states[runtimeKey] = state

	freshQuotaState := outcome.quotaStateKnown
	if outcome.quotaStateKnown {
		work.quotaStateKnown = true
		work.exhausted = outcome.exhausted
		work.quotaResetAt = outcome.quotaResetAt
	}
	if outcome.status == chatgptwebauth.AccountInfoResultPartial {
		work.partialApplied = true
	}
	if outcome.retryable && work.attempt <= runtime.cfg.MaxRetries && outcome.status != chatgptwebauth.AccountInfoResultCanceled {
		next := outcome.retryAt
		if next.IsZero() {
			delay := runtime.retryDelay(work.attempt)
			if retryAfter := clampChatGPTWebAccountInfoRetryAfter(outcome.retryAfter); retryAfter > delay {
				delay = retryAfter
			}
			next = runtime.currentTime().Add(delay)
		}
		work.attempt++
		work.schedule = runtime.retryScheduleKey(work)
		if runtime.scheduleLocked(work.schedule, next, work) {
			runtime.retryCount++
			runtime.setTaskRetryingLocked(work, outcome.errorCode)
			return
		}
		outcome.status = chatgptwebauth.AccountInfoResultFailed
		outcome.errorCode = "refresh_failed"
	}
	if work.partialApplied && outcome.status == chatgptwebauth.AccountInfoResultFailed {
		outcome.status = chatgptwebauth.AccountInfoResultPartial
	}
	retainAutomaticRecovery := work.quotaResetAt.After(runtime.currentTime())
	if !outcome.quotaStateKnown && work.quotaStateKnown &&
		(!work.automatic || retainAutomaticRecovery) {
		outcome.quotaStateKnown = true
		outcome.exhausted = work.exhausted
		outcome.quotaResetAt = work.quotaResetAt
	}
	if freshQuotaState || outcome.quotaStateKnown {
		target, managed, exhausted, resetAt := runtime.currentQuotaScheduleStateLocked(work.target)
		if managed {
			work.target = target
			outcome.quotaStateKnown = freshQuotaState ||
				!work.automatic ||
				resetAt.After(runtime.currentTime())
			outcome.exhausted = exhausted
			outcome.quotaResetAt = resetAt
		}
	}
	if outcome.quotaStateKnown {
		if work.automatic && freshQuotaState && outcome.exhausted &&
			!outcome.quotaResetAt.After(runtime.currentTime()) {
			runtime.syncRecoveryScheduleForTargetLocked(work.target, false, time.Time{}, 0)
		} else {
			runtime.syncRecoveryScheduleForTargetLocked(
				work.target,
				outcome.exhausted,
				outcome.quotaResetAt,
				outcome.retryAfter,
			)
		}
	}

	if outcome.status == chatgptwebauth.AccountInfoResultFailed || outcome.status == chatgptwebauth.AccountInfoResultPartial {
		runtime.failedCount++
	}
	if outcome.status == chatgptwebauth.AccountInfoResultUpdated ||
		outcome.status == chatgptwebauth.AccountInfoResultUnchanged ||
		outcome.status == chatgptwebauth.AccountInfoResultPartial {
		runtime.refreshCount++
	}
	runtime.completeTaskWorkLocked(work, outcome)
	runtime.releaseWorkEpochLocked(work)
}

func (runtime *chatGPTWebAccountInfoRuntime) assignWorkSequenceLocked(work *chatGPTWebAccountInfoWork) {
	if work == nil || work.sequence != 0 {
		return
	}
	runtime.workSequence++
	work.sequence = runtime.workSequence
}

func (runtime *chatGPTWebAccountInfoRuntime) updateLastErrorLocked(sequence uint64, status, errorCode string) {
	if sequence < runtime.lastErrorSequence {
		return
	}
	if errorCode != "" {
		runtime.lastErrorSequence = sequence
		runtime.lastError = errorCode
		return
	}
	switch status {
	case chatgptwebauth.AccountInfoResultUpdated,
		chatgptwebauth.AccountInfoResultUnchanged,
		chatgptwebauth.AccountInfoResultFresh:
		runtime.lastErrorSequence = sequence
		runtime.lastError = ""
	}
}

func (runtime *chatGPTWebAccountInfoRuntime) retryScheduleKey(work chatGPTWebAccountInfoWork) string {
	if work.taskID != "" {
		return fmt.Sprintf("task:%s:%d", work.taskID, work.index)
	}
	return "retry:" + chatGPTWebAccountInfoTargetKey(work.target)
}

func (runtime *chatGPTWebAccountInfoRuntime) setTaskRetryingLocked(work chatGPTWebAccountInfoWork, errorCode string) {
	if work.taskID == "" {
		return
	}
	task := runtime.tasks[work.taskID]
	if task == nil || work.index < 0 || work.index >= len(task.snapshot.Results) {
		return
	}
	result := &task.snapshot.Results[work.index]
	result.Status = chatgptwebauth.AccountInfoResultRetrying
	result.Attempts = work.attempt - 1
	result.Error = chatgptwebauth.SafeQuotaError(errorCode)
}

func (runtime *chatGPTWebAccountInfoRuntime) completeTaskWorkLocked(work chatGPTWebAccountInfoWork, outcome chatGPTWebAccountInfoOutcome) {
	if work.taskID == "" {
		return
	}
	task := runtime.tasks[work.taskID]
	if task == nil || work.index < 0 || work.index >= len(task.snapshot.Results) {
		return
	}
	result := &task.snapshot.Results[work.index]
	if task.ctx.Err() != nil {
		outcome.status = chatgptwebauth.AccountInfoResultCanceled
		outcome.errorCode = ""
	}
	if accountInfoResultTerminal(result.Status) {
		return
	}
	result.Status = outcome.status
	result.Attempts = work.attempt
	result.Error = chatgptwebauth.SafeQuotaError(outcome.errorCode)
	task.snapshot.Processed++
	switch outcome.status {
	case chatgptwebauth.AccountInfoResultUpdated, chatgptwebauth.AccountInfoResultUnchanged,
		chatgptwebauth.AccountInfoResultFresh:
		task.snapshot.Succeeded++
	case chatgptwebauth.AccountInfoResultPartial:
		task.snapshot.Succeeded++
		task.snapshot.Partial++
	case chatgptwebauth.AccountInfoResultCanceled:
		task.snapshot.Canceled++
	default:
		task.snapshot.Failed++
	}
	runtime.finishTaskIfDoneLocked(task)
}

func accountInfoResultTerminal(status string) bool {
	switch status {
	case chatgptwebauth.AccountInfoResultUpdated, chatgptwebauth.AccountInfoResultUnchanged,
		chatgptwebauth.AccountInfoResultFresh, chatgptwebauth.AccountInfoResultPartial,
		chatgptwebauth.AccountInfoResultFailed, chatgptwebauth.AccountInfoResultCanceled:
		return true
	default:
		return false
	}
}

func (runtime *chatGPTWebAccountInfoRuntime) finishTaskIfDoneLocked(task *chatGPTWebAccountInfoTaskState) {
	if task == nil || task.snapshot.Processed < task.snapshot.Total || task.snapshot.CompletedAt != nil {
		return
	}
	now := runtime.currentTime()
	task.snapshot.CompletedAt = &now
	switch {
	case task.snapshot.Canceled == task.snapshot.Total:
		task.snapshot.State = chatgptwebauth.AccountInfoTaskCanceled
	case task.snapshot.Partial > 0 || task.snapshot.Failed > 0 || task.snapshot.Canceled > 0:
		task.snapshot.State = chatgptwebauth.AccountInfoTaskCompletedWithErrors
	default:
		task.snapshot.State = chatgptwebauth.AccountInfoTaskCompleted
	}
	if task.cancel != nil {
		task.cancel()
		task.cancel = nil
	}
}

func (runtime *chatGPTWebAccountInfoRuntime) enqueueLocked(work chatGPTWebAccountInfoWork) bool {
	enqueued, _ := runtime.enqueueForCurrentInstanceLocked(work)
	return enqueued
}

func (runtime *chatGPTWebAccountInfoRuntime) enqueueForCurrentInstanceLocked(
	work chatGPTWebAccountInfoWork,
) (bool, bool) {
	if runtime.closed {
		return false, true
	}
	if !runtime.prepareWorkTargetForCommitLocked(&work) {
		return false, false
	}
	acquiredEpoch := work.epochRef == nil
	runtime.assignWorkEpochLocked(&work)
	if !runtime.workEpochCurrentLocked(work) {
		if acquiredEpoch {
			runtime.releaseWorkEpochLocked(work)
		}
		return false, false
	}
	if !runtime.hasAccountInfoCapacityLocked() {
		if acquiredEpoch {
			runtime.releaseWorkEpochLocked(work)
		}
		return false, true
	}
	runtime.queue = append(runtime.queue, work)
	runtime.addQueuedTargetLocked(work)
	runtime.cond.Signal()
	return true, true
}

func (runtime *chatGPTWebAccountInfoRuntime) scheduleLocked(key string, due time.Time, work chatGPTWebAccountInfoWork) bool {
	key = strings.TrimSpace(key)
	if runtime.closed || key == "" {
		return false
	}
	if !runtime.prepareWorkTargetForCommitLocked(&work) {
		return false
	}
	acquiredEpoch := work.epochRef == nil
	runtime.assignWorkEpochLocked(&work)
	if !runtime.workEpochCurrentLocked(work) {
		if acquiredEpoch {
			runtime.releaseWorkEpochLocked(work)
		}
		return false
	}
	runtime.schedule++
	if entry := runtime.scheduled[key]; entry != nil {
		wasTask := entry.work.taskID != ""
		isTask := work.taskID != ""
		if !wasTask && isTask && runtime.delayedTasks >= runtime.accountInfoCapacityLocked() {
			if acquiredEpoch {
				runtime.releaseWorkEpochLocked(work)
			}
			return false
		}
		previousTarget := entry.work.target
		previousWork := entry.work
		runtime.unindexScheduleTargetLocked(entry)
		entry.due = due
		entry.work = work
		entry.seq = runtime.schedule
		runtime.indexScheduleTargetLocked(entry)
		if wasTask != isTask {
			if isTask {
				runtime.delayedTasks++
			} else {
				runtime.delayedTasks--
			}
		}
		if entry.index >= 0 && entry.index < len(runtime.schedules) {
			heap.Fix(&runtime.schedules, entry.index)
		} else {
			heap.Push(&runtime.schedules, entry)
		}
		if chatGPTWebAccountInfoTargetKey(previousTarget) != chatGPTWebAccountInfoTargetKey(work.target) {
			runtime.refreshNextScheduleStateForTargetLocked(previousTarget)
		}
		if previousWork.epochRef != work.epochRef {
			runtime.releaseWorkEpochLocked(previousWork)
		}
		runtime.refreshNextScheduleStateForTargetLocked(work.target)
		runtime.signalScheduler()
		return true
	}
	if work.taskID != "" && runtime.delayedTasks >= runtime.accountInfoCapacityLocked() {
		if acquiredEpoch {
			runtime.releaseWorkEpochLocked(work)
		}
		return false
	}
	entry := &chatGPTWebAccountInfoSchedule{key: key, due: due, work: work, seq: runtime.schedule, index: -1}
	runtime.scheduled[key] = entry
	runtime.indexScheduleTargetLocked(entry)
	if work.taskID != "" {
		runtime.delayedTasks++
	}
	heap.Push(&runtime.schedules, entry)
	runtime.refreshNextScheduleStateForTargetLocked(work.target)
	runtime.signalScheduler()
	return true
}

func (runtime *chatGPTWebAccountInfoRuntime) accountInfoCapacityLocked() int {
	capacity := runtime.cfg.RefreshQueueSize + runtime.cfg.RefreshWorkers
	if capacity < 1 {
		return 1
	}
	return capacity
}

func (runtime *chatGPTWebAccountInfoRuntime) accountInfoWorkCountLocked() int {
	return runtime.busy + runtime.waiting + runtime.queueLengthLocked()
}

func (runtime *chatGPTWebAccountInfoRuntime) currentAuthEpochLocked(
	target chatgptwebauth.AccountInfoRefreshTarget,
) uint64 {
	runtimeKey := chatGPTWebAccountInfoTargetKey(target)
	if runtimeKey == "" {
		return 0
	}
	if runtime.authEpoch == nil {
		runtime.authEpoch = make(map[string]uint64)
	}
	epoch := runtime.authEpoch[runtimeKey]
	if epoch == 0 {
		epoch = 1
		runtime.authEpoch[runtimeKey] = epoch
	}
	return epoch
}

func (runtime *chatGPTWebAccountInfoRuntime) assignWorkEpochLocked(work *chatGPTWebAccountInfoWork) {
	if work == nil {
		return
	}
	if work.epoch == 0 {
		work.epoch = runtime.currentAuthEpochLocked(work.target)
	}
	if work.epoch == 0 || work.epochRef != nil {
		return
	}
	if runtime.authEpochRefs == nil {
		runtime.authEpochRefs = make(map[string]int)
	}
	runtimeKey := chatGPTWebAccountInfoTargetKey(work.target)
	if runtimeKey == "" {
		return
	}
	runtime.authEpochRefs[runtimeKey]++
	work.epochRef = &chatGPTWebAccountInfoEpochRef{}
}

func (runtime *chatGPTWebAccountInfoRuntime) releaseWorkEpochLocked(work chatGPTWebAccountInfoWork) {
	if work.epochRef == nil || work.epochRef.released {
		return
	}
	runtimeKey := chatGPTWebAccountInfoTargetKey(work.target)
	if runtimeKey == "" {
		return
	}
	references := runtime.authEpochRefs[runtimeKey]
	if references <= 0 {
		return
	}
	work.epochRef.released = true
	if references > 1 {
		runtime.authEpochRefs[runtimeKey] = references - 1
		return
	}
	delete(runtime.authEpochRefs, runtimeKey)
	delete(runtime.authEpoch, runtimeKey)
}

func (runtime *chatGPTWebAccountInfoRuntime) cleanupAuthEpochLocked(runtimeKey string) {
	runtimeKey = strings.TrimSpace(runtimeKey)
	if runtimeKey == "" || runtime.authEpochRefs[runtimeKey] > 0 {
		return
	}
	delete(runtime.authEpochRefs, runtimeKey)
	delete(runtime.authEpoch, runtimeKey)
}

func (runtime *chatGPTWebAccountInfoRuntime) workEpochCurrentLocked(work chatGPTWebAccountInfoWork) bool {
	return work.epoch != 0 && work.epoch == runtime.currentAuthEpochLocked(work.target)
}

func (runtime *chatGPTWebAccountInfoRuntime) hasAccountInfoCapacityLocked() bool {
	return runtime.accountInfoWorkCountLocked() < runtime.accountInfoCapacityLocked()
}

func (runtime *chatGPTWebAccountInfoRuntime) removeScheduleLocked(key string) *chatGPTWebAccountInfoSchedule {
	entry := runtime.scheduled[strings.TrimSpace(key)]
	if entry == nil {
		return nil
	}
	delete(runtime.scheduled, entry.key)
	runtime.unindexScheduleTargetLocked(entry)
	if entry.work.taskID != "" && runtime.delayedTasks > 0 {
		runtime.delayedTasks--
	}
	if entry.index >= 0 && entry.index < len(runtime.schedules) {
		heap.Remove(&runtime.schedules, entry.index)
	}
	runtime.refreshNextScheduleStateForTargetLocked(entry.work.target)
	runtime.signalScheduler()
	return entry
}

func (runtime *chatGPTWebAccountInfoRuntime) refreshNextScheduleStateLocked(authID string) {
	target := chatgptwebauth.AccountInfoRefreshTarget{AuthID: strings.TrimSpace(authID)}
	target = runtime.bindCurrentTargetLocked(target)
	runtime.refreshNextScheduleStateForTargetLocked(target)
}

func (runtime *chatGPTWebAccountInfoRuntime) refreshNextScheduleStateForTargetLocked(
	target chatgptwebauth.AccountInfoRefreshTarget,
) {
	runtimeKey := chatGPTWebAccountInfoTargetKey(target)
	if runtimeKey == "" {
		return
	}
	var earliest time.Time
	for _, entry := range runtime.scheduledByTarget[runtimeKey] {
		if entry == nil {
			continue
		}
		if earliest.IsZero() || entry.due.Before(earliest) {
			earliest = entry.due
		}
	}
	state := runtime.states[runtimeKey]
	state.NextRefreshAt = earliest
	runtime.states[runtimeKey] = state
}

func (runtime *chatGPTWebAccountInfoRuntime) indexScheduleTargetLocked(entry *chatGPTWebAccountInfoSchedule) {
	if runtime == nil || entry == nil {
		return
	}
	runtimeKey := chatGPTWebAccountInfoTargetKey(entry.work.target)
	if runtimeKey == "" {
		return
	}
	if runtime.scheduledByTarget == nil {
		runtime.scheduledByTarget = make(map[string]map[string]*chatGPTWebAccountInfoSchedule)
	}
	bucket := runtime.scheduledByTarget[runtimeKey]
	if bucket == nil {
		bucket = make(map[string]*chatGPTWebAccountInfoSchedule)
		runtime.scheduledByTarget[runtimeKey] = bucket
	}
	bucket[entry.key] = entry
}

func (runtime *chatGPTWebAccountInfoRuntime) unindexScheduleTargetLocked(entry *chatGPTWebAccountInfoSchedule) {
	if runtime == nil || entry == nil {
		return
	}
	runtimeKey := chatGPTWebAccountInfoTargetKey(entry.work.target)
	bucket := runtime.scheduledByTarget[runtimeKey]
	if bucket == nil {
		return
	}
	delete(bucket, entry.key)
	if len(bucket) == 0 {
		delete(runtime.scheduledByTarget, runtimeKey)
	}
}

func (runtime *chatGPTWebAccountInfoRuntime) scheduleRecoveryLocked(authID string, resetAt time.Time) bool {
	target := chatgptwebauth.AccountInfoRefreshTarget{Name: authID, AuthID: authID}
	target = runtime.bindCurrentTargetLocked(target)
	return runtime.scheduleRecoveryForTargetLocked(target, resetAt)
}

func (runtime *chatGPTWebAccountInfoRuntime) scheduleRecoveryForTargetLocked(
	target chatgptwebauth.AccountInfoRefreshTarget,
	resetAt time.Time,
) bool {
	return runtime.scheduleRecoveryForTargetAtLocked(target, resetAt, resetAt)
}

func (runtime *chatGPTWebAccountInfoRuntime) scheduleRecoveryForTargetAtLocked(
	target chatgptwebauth.AccountInfoRefreshTarget,
	resetAt time.Time,
	due time.Time,
) bool {
	if !runtime.cfg.AutomaticRefreshEnabled() || strings.TrimSpace(target.AuthID) == "" {
		return false
	}
	var current bool
	target, current = runtime.resolveCurrentTargetLocked(target)
	if !current {
		return false
	}
	scheduleKey := "recovery:" + chatGPTWebAccountInfoTargetKey(target)
	if runtime.hasRecoveryWorkLocked(target, resetAt) {
		runtime.refreshNextScheduleStateForTargetLocked(target)
		return true
	}
	now := runtime.currentTime()
	if !due.After(now) {
		due = now
	}
	if jitter := runtime.recoveryJitter(); jitter > 0 {
		due = due.Add(jitter)
	}
	work := chatGPTWebAccountInfoWork{
		target:          target,
		force:           true,
		attempt:         1,
		automatic:       true,
		schedule:        scheduleKey,
		quotaStateKnown: true,
		exhausted:       true,
		quotaResetAt:    resetAt,
	}
	return runtime.scheduleLocked(work.schedule, due, work)
}

func chatGPTWebAccountInfoRecoveryWorkMatches(
	work chatGPTWebAccountInfoWork,
	target chatgptwebauth.AccountInfoRefreshTarget,
	resetAt time.Time,
) bool {
	return work.automatic &&
		work.quotaStateKnown &&
		work.exhausted &&
		chatGPTWebAccountInfoTargetKey(work.target) == chatGPTWebAccountInfoTargetKey(target) &&
		work.quotaResetAt.Equal(resetAt)
}

func (runtime *chatGPTWebAccountInfoRuntime) hasRecoveryWorkLocked(
	target chatgptwebauth.AccountInfoRefreshTarget,
	resetAt time.Time,
) bool {
	runtimeKey := chatGPTWebAccountInfoTargetKey(target)
	if inflightResetAt, ok := runtime.inflightRecovery[runtimeKey]; ok && inflightResetAt.Equal(resetAt) {
		return true
	}
	for _, work := range runtime.queuedWorkLocked() {
		if chatGPTWebAccountInfoRecoveryWorkMatches(work, target, resetAt) {
			return true
		}
	}
	for _, entry := range runtime.scheduledByTarget[runtimeKey] {
		if entry != nil && chatGPTWebAccountInfoRecoveryWorkMatches(entry.work, target, resetAt) {
			return true
		}
	}
	return false
}

func (runtime *chatGPTWebAccountInfoRuntime) clearInflightRecoveryLocked(
	work chatGPTWebAccountInfoWork,
) {
	if !work.automatic || !work.quotaStateKnown || !work.exhausted {
		return
	}
	runtimeKey := chatGPTWebAccountInfoTargetKey(work.target)
	if resetAt, ok := runtime.inflightRecovery[runtimeKey]; ok && resetAt.Equal(work.quotaResetAt) {
		delete(runtime.inflightRecovery, runtimeKey)
	}
}

func (runtime *chatGPTWebAccountInfoRuntime) syncRecoveryScheduleLocked(authID string, exhausted bool, resetAt time.Time) {
	target := chatgptwebauth.AccountInfoRefreshTarget{Name: authID, AuthID: authID}
	target = runtime.bindCurrentTargetLocked(target)
	runtime.syncRecoveryScheduleForTargetLocked(target, exhausted, resetAt, 0)
}

func (runtime *chatGPTWebAccountInfoRuntime) syncRecoveryScheduleForTargetLocked(
	target chatgptwebauth.AccountInfoRefreshTarget,
	exhausted bool,
	resetAt time.Time,
	retryAfter time.Duration,
) {
	if strings.TrimSpace(target.AuthID) == "" {
		return
	}
	key := "recovery:" + chatGPTWebAccountInfoTargetKey(target)
	if !runtime.cfg.AutomaticRefreshEnabled() {
		if entry := runtime.removeScheduleLocked(key); entry != nil {
			runtime.releaseWorkEpochLocked(entry.work)
		}
		return
	}
	if !exhausted {
		if entry := runtime.removeScheduleLocked(key); entry != nil {
			runtime.releaseWorkEpochLocked(entry.work)
		}
		return
	}
	now := runtime.currentTime()
	due := resetAt
	if due.IsZero() || !due.After(now) {
		delay := chatGPTWebAccountInfoExpiredRecoveryBackoff
		if retryAfter = clampChatGPTWebAccountInfoRetryAfter(retryAfter); retryAfter > delay {
			delay = retryAfter
		}
		due = now.Add(delay)
	}
	runtime.scheduleRecoveryForTargetAtLocked(target, resetAt, due)
}

func (runtime *chatGPTWebAccountInfoRuntime) syncCanceledWorkRecoveryLocked(
	work chatGPTWebAccountInfoWork,
) {
	if !work.quotaStateKnown {
		return
	}
	target, managed, exhausted, resetAt := runtime.currentQuotaScheduleStateLocked(work.target)
	if !managed {
		target = work.target
		exhausted = work.exhausted
		resetAt = work.quotaResetAt
	}
	runtime.syncRecoveryScheduleForTargetLocked(target, exhausted, resetAt, 0)
}

func (runtime *chatGPTWebAccountInfoRuntime) schedulerLoop() {
	defer runtime.wg.Done()
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	for {
		runtime.mu.Lock()
		next, ok := runtime.nextScheduleLocked()
		var nextDue time.Time
		if ok {
			nextDue = next.due
		}
		runtime.mu.Unlock()
		if !ok {
			select {
			case <-runtime.ctx.Done():
				return
			case <-runtime.wake:
				continue
			}
		}
		delay := time.Until(nextDue)
		if delay < 0 {
			delay = 0
		}
		timer.Reset(delay)
		select {
		case <-runtime.ctx.Done():
			return
		case <-runtime.wake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			continue
		case <-timer.C:
		}

		runtime.mu.Lock()
		now := runtime.currentTime()
		for {
			entry, ready := runtime.nextScheduleLocked()
			if !ready || entry.due.After(now) {
				break
			}
			heap.Pop(&runtime.schedules)
			current := runtime.scheduled[entry.key]
			if current != entry {
				continue
			}
			delete(runtime.scheduled, entry.key)
			runtime.unindexScheduleTargetLocked(entry)
			if entry.work.taskID != "" && runtime.delayedTasks > 0 {
				runtime.delayedTasks--
			}
			runtime.refreshNextScheduleStateForTargetLocked(entry.work.target)
			if !runtime.workEpochCurrentLocked(entry.work) {
				runtime.completeTaskWorkLocked(entry.work, chatGPTWebAccountInfoOutcome{
					status:    chatgptwebauth.AccountInfoResultFailed,
					errorCode: "credential_unavailable",
				})
				runtime.releaseWorkEpochLocked(entry.work)
				continue
			}
			enqueued, instanceCurrent := runtime.enqueueForCurrentInstanceLocked(entry.work)
			if !instanceCurrent {
				runtime.completeTaskWorkLocked(entry.work, chatGPTWebAccountInfoOutcome{
					status:    chatgptwebauth.AccountInfoResultFailed,
					errorCode: "credential_unavailable",
				})
				runtime.releaseWorkEpochLocked(entry.work)
				continue
			}
			if !enqueued {
				entry.due = now.Add(30 * time.Second)
				runtime.schedule++
				entry.seq = runtime.schedule
				entry.index = -1
				runtime.scheduled[entry.key] = entry
				runtime.indexScheduleTargetLocked(entry)
				if entry.work.taskID != "" {
					runtime.delayedTasks++
				}
				heap.Push(&runtime.schedules, entry)
				runtime.refreshNextScheduleStateForTargetLocked(entry.work.target)
			}
		}
		runtime.mu.Unlock()
	}
}

func (runtime *chatGPTWebAccountInfoRuntime) nextScheduleLocked() (*chatGPTWebAccountInfoSchedule, bool) {
	for len(runtime.schedules) > 0 {
		entry := runtime.schedules[0]
		if runtime.scheduled[entry.key] == entry {
			return entry, true
		}
		heap.Pop(&runtime.schedules)
	}
	return nil, false
}

func (runtime *chatGPTWebAccountInfoRuntime) signalScheduler() {
	select {
	case runtime.wake <- struct{}{}:
	default:
	}
}

func (runtime *chatGPTWebAccountInfoRuntime) retryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := 30 * time.Second
	for index := 1; index < attempt && delay < chatGPTWebAccountInfoMaxRetryAfter; index++ {
		delay *= 2
	}
	if delay > chatGPTWebAccountInfoMaxRetryAfter {
		delay = chatGPTWebAccountInfoMaxRetryAfter
	}
	jitter := runtime.fraction(0.2)
	delay += time.Duration(float64(delay) * jitter)
	if delay > chatGPTWebAccountInfoMaxRetryAfter {
		return chatGPTWebAccountInfoMaxRetryAfter
	}
	return delay
}

func (runtime *chatGPTWebAccountInfoRuntime) recoveryJitter() time.Duration {
	maximum := runtime.cfg.RecoveryJitterSeconds
	if maximum <= 0 {
		return 0
	}
	return time.Duration(runtime.fraction(1) * float64(maximum) * float64(time.Second))
}

func (runtime *chatGPTWebAccountInfoRuntime) fraction(scale float64) float64 {
	var data [8]byte
	if _, err := io.ReadFull(runtime.random, data[:]); err != nil {
		return 0
	}
	value := binary.LittleEndian.Uint64(data[:]) >> 11
	return (float64(value) / float64(uint64(1)<<53)) * scale
}

func (runtime *chatGPTWebAccountInfoRuntime) currentTime() time.Time {
	if runtime != nil && runtime.now != nil {
		return runtime.now()
	}
	return time.Now()
}

func (runtime *chatGPTWebAccountInfoRuntime) currentQuotaScheduleStateLocked(
	target chatgptwebauth.AccountInfoRefreshTarget,
) (chatgptwebauth.AccountInfoRefreshTarget, bool, bool, time.Time) {
	if runtime == nil || runtime.executor == nil || runtime.executor.manager == nil {
		return target, false, false, time.Time{}
	}
	auth, ok := runtime.executor.manager.GetByID(strings.TrimSpace(target.AuthID))
	if !ok || auth == nil {
		return target, true, false, time.Time{}
	}
	currentInstanceID := strings.TrimSpace(auth.RuntimeInstanceID())
	if target.AuthInstanceID != "" && strings.TrimSpace(target.AuthInstanceID) != currentInstanceID {
		return target, true, false, time.Time{}
	}
	target.AuthInstanceID = currentInstanceID
	target = runtime.bindCurrentTargetLocked(target)
	if !strings.EqualFold(strings.TrimSpace(auth.Provider), chatgptwebauth.Provider) ||
		auth.Disabled ||
		auth.Status == cliproxyauth.StatusDisabled ||
		!auth.LifecycleRefreshable() {
		return target, true, false, time.Time{}
	}
	credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
	if errCredential != nil || credential.QuotaState != chatgptwebauth.QuotaStateExhausted {
		return target, true, false, time.Time{}
	}
	resetAt, _ := time.Parse(time.RFC3339Nano, strings.TrimSpace(credential.ImageQuotaResetAt))
	return target, true, true, resetAt
}

func (runtime *chatGPTWebAccountInfoRuntime) persistenceContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if parent, ok := ctx.Value(chatGPTWebAccountInfoPersistenceContextKey{}).(context.Context); ok && parent != nil {
		return context.WithCancel(parent)
	}
	persistContext, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stopRequestCancellation := context.AfterFunc(ctx, func() {
		if !errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
			cancel()
		}
	})
	if ctx.Err() != nil && !errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
		cancel()
	}
	if runtime == nil || runtime.ctx == nil {
		return persistContext, func() {
			stopRequestCancellation()
			cancel()
		}
	}
	stopLifecycleCancellation := context.AfterFunc(runtime.ctx, cancel)
	if runtime.ctx.Err() != nil {
		cancel()
	}
	return persistContext, func() {
		stopRequestCancellation()
		stopLifecycleCancellation()
		cancel()
	}
}

func (runtime *chatGPTWebAccountInfoRuntime) restoreRecoverySchedules() {
	if runtime == nil || runtime.executor == nil || runtime.executor.manager == nil {
		return
	}
	runtime.mu.Lock()
	enabled := runtime.cfg.AutomaticRefreshEnabled()
	runtime.mu.Unlock()
	if !enabled {
		return
	}
	for _, auth := range runtime.executor.manager.List() {
		runtime.syncPersistedRecovery(auth)
	}
}

func (runtime *chatGPTWebAccountInfoRuntime) syncPersistedRecovery(auth *cliproxyauth.Auth) {
	if runtime == nil || auth == nil || strings.TrimSpace(auth.ID) == "" {
		return
	}
	authID := strings.TrimSpace(auth.ID)
	target, current := runtime.resolveCurrentTarget(chatgptwebauth.AccountInfoRefreshTarget{
		Name:           authID,
		AuthID:         authID,
		AuthInstanceID: auth.RuntimeInstanceID(),
	})
	if !current {
		return
	}
	if runtime.beforePersistedRecoveryCommit != nil {
		runtime.beforePersistedRecoveryCommit()
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	target, managed, exhausted, resetAt := runtime.currentQuotaScheduleStateLocked(target)
	if !managed {
		target = runtime.bindCurrentTargetLocked(target)
		if strings.EqualFold(strings.TrimSpace(auth.Provider), chatgptwebauth.Provider) &&
			!auth.Disabled &&
			auth.Status != cliproxyauth.StatusDisabled &&
			auth.LifecycleRefreshable() {
			credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
			if errCredential == nil && credential.QuotaState == chatgptwebauth.QuotaStateExhausted {
				exhausted = true
				resetAt, _ = time.Parse(time.RFC3339Nano, strings.TrimSpace(credential.ImageQuotaResetAt))
			}
		}
	}
	if exhausted {
		runtime.scheduleRecoveryForTargetLocked(target, resetAt)
		return
	}
	if entry := runtime.removeScheduleLocked("recovery:" + chatGPTWebAccountInfoTargetKey(target)); entry != nil {
		runtime.releaseWorkEpochLocked(entry.work)
	}
}

func (runtime *chatGPTWebAccountInfoRuntime) removeAuth(authID string) {
	runtime.removeAuthInstance(authID, "")
}

func (runtime *chatGPTWebAccountInfoRuntime) removeAuthInstance(authID, authInstanceID string) {
	if runtime == nil {
		return
	}
	authID = strings.TrimSpace(authID)
	authInstanceID = strings.TrimSpace(authInstanceID)
	if authID == "" {
		return
	}
	runtime.mu.Lock()
	if runtime.closed {
		runtime.mu.Unlock()
		return
	}
	targetKey := chatGPTWebAccountInfoAuthInstanceKey(authID, authInstanceID)
	matchesRuntimeKey := func(runtimeKey string) bool {
		if authInstanceID != "" {
			return runtimeKey == targetKey
		}
		return chatGPTWebAccountInfoRuntimeKeyMatchesAuth(runtimeKey, authID)
	}
	affectedRuntimeKeys := make(map[string]struct{})
	if authInstanceID != "" {
		affectedRuntimeKeys[targetKey] = struct{}{}
	}
	collectRuntimeKey := func(runtimeKey string) {
		if matchesRuntimeKey(runtimeKey) {
			affectedRuntimeKeys[runtimeKey] = struct{}{}
		}
	}
	for runtimeKey := range runtime.states {
		collectRuntimeKey(runtimeKey)
	}
	for runtimeKey := range runtime.inflight {
		collectRuntimeKey(runtimeKey)
	}
	for runtimeKey := range runtime.inflightForce {
		collectRuntimeKey(runtimeKey)
	}
	for runtimeKey := range runtime.inflightTask {
		collectRuntimeKey(runtimeKey)
	}
	for runtimeKey := range runtime.inflightRecovery {
		collectRuntimeKey(runtimeKey)
	}
	for runtimeKey := range runtime.pendingTriggers {
		collectRuntimeKey(runtimeKey)
	}
	for runtimeKey := range runtime.ambiguousImageRecheckAfter {
		collectRuntimeKey(runtimeKey)
	}
	for runtimeKey := range runtime.authEpoch {
		collectRuntimeKey(runtimeKey)
	}
	for runtimeKey := range runtime.authEpochRefs {
		collectRuntimeKey(runtimeKey)
	}
	for runtimeKey, call := range runtime.calls {
		if call != nil && chatGPTWebAccountInfoTargetsMatch(
			chatgptwebauth.AccountInfoRefreshTarget{
				AuthID:         call.authID,
				AuthInstanceID: call.authInstanceID,
			},
			authID,
			authInstanceID,
		) {
			affectedRuntimeKeys[runtimeKey] = struct{}{}
		}
	}
	for _, work := range runtime.queuedWorkLocked() {
		if chatGPTWebAccountInfoTargetsMatch(work.target, authID, authInstanceID) {
			affectedRuntimeKeys[chatGPTWebAccountInfoTargetKey(work.target)] = struct{}{}
		}
	}
	for _, entry := range runtime.scheduled {
		if entry != nil && chatGPTWebAccountInfoTargetsMatch(entry.work.target, authID, authInstanceID) {
			affectedRuntimeKeys[chatGPTWebAccountInfoTargetKey(entry.work.target)] = struct{}{}
		}
	}
	for runtimeKey := range affectedRuntimeKeys {
		epoch := runtime.authEpoch[runtimeKey]
		if epoch == 0 {
			epoch = 1
		}
		runtime.authEpoch[runtimeKey] = epoch + 1
	}
	activeQueue := runtime.queuedWorkLocked()
	filtered := activeQueue[:0]
	for _, work := range activeQueue {
		if !chatGPTWebAccountInfoTargetsMatch(work.target, authID, authInstanceID) {
			filtered = append(filtered, work)
			continue
		}
		runtime.completeTaskWorkLocked(work, chatGPTWebAccountInfoOutcome{
			status:    chatgptwebauth.AccountInfoResultFailed,
			errorCode: "credential_unavailable",
		})
		runtime.releaseWorkEpochLocked(work)
	}
	runtime.replaceQueuedWorkLocked(filtered)
	scheduleKeys := make([]string, 0)
	for key, entry := range runtime.scheduled {
		if entry != nil && chatGPTWebAccountInfoTargetsMatch(entry.work.target, authID, authInstanceID) {
			scheduleKeys = append(scheduleKeys, key)
		}
	}
	for _, key := range scheduleKeys {
		entry := runtime.removeScheduleLocked(key)
		if entry != nil {
			runtime.completeTaskWorkLocked(entry.work, chatGPTWebAccountInfoOutcome{
				status:    chatgptwebauth.AccountInfoResultFailed,
				errorCode: "credential_unavailable",
			})
			runtime.releaseWorkEpochLocked(entry.work)
		}
	}
	for runtimeKey, call := range runtime.calls {
		if call == nil || !chatGPTWebAccountInfoTargetsMatch(
			chatgptwebauth.AccountInfoRefreshTarget{
				AuthID:         call.authID,
				AuthInstanceID: call.authInstanceID,
			},
			authID,
			authInstanceID,
		) {
			continue
		}
		call.accepting = false
		delete(runtime.calls, runtimeKey)
		call.cancel()
	}
	for runtimeKey := range affectedRuntimeKeys {
		delete(runtime.pendingTriggers, runtimeKey)
		delete(runtime.ambiguousImageRecheckAfter, runtimeKey)
		delete(runtime.inflight, runtimeKey)
		delete(runtime.inflightForce, runtimeKey)
		delete(runtime.inflightTask, runtimeKey)
		delete(runtime.inflightRecovery, runtimeKey)
		delete(runtime.states, runtimeKey)
		runtime.cleanupAuthEpochLocked(runtimeKey)
	}
	if authInstanceID == "" || runtime.authInstances[authID] == authInstanceID {
		delete(runtime.authInstances, authID)
	}
	runtime.cond.Broadcast()
	runtime.mu.Unlock()
	runtime.signalScheduler()
}

func (runtime *chatGPTWebAccountInfoRuntime) startTask(targets []chatgptwebauth.AccountInfoRefreshTarget, force bool) (*chatgptwebauth.AccountInfoRefreshTask, error) {
	if runtime == nil {
		return nil, errors.New("chatgpt web account info is unavailable")
	}
	if len(targets) == 0 {
		return nil, errors.New("at least one auth name is required")
	}
	if len(targets) > chatgptwebauth.AccountInfoMaxTargets {
		return nil, fmt.Errorf("at most %d auth names are allowed", chatgptwebauth.AccountInfoMaxTargets)
	}

	runtime.mu.Lock()
	if runtime.closed {
		runtime.mu.Unlock()
		return nil, errors.New("chatgpt web account info is unavailable")
	}
	if !force && !runtime.cfg.AutomaticRefreshEnabled() {
		runtime.mu.Unlock()
		return nil, chatgptwebauth.ErrAccountInfoAutoRefreshDisabled
	}
	runtime.pruneTasksLocked()
	if len(runtime.tasks)+runtime.taskReservations >= chatGPTWebAccountInfoTaskMaxKept {
		runtime.mu.Unlock()
		return nil, fmt.Errorf(
			"%w: maximum %d tasks",
			chatgptwebauth.ErrAccountInfoTaskLimitReached,
			chatGPTWebAccountInfoTaskMaxKept,
		)
	}
	runtime.taskReservations++
	runtime.mu.Unlock()

	reservationHeld := true
	defer func() {
		if !reservationHeld {
			return
		}
		runtime.mu.Lock()
		runtime.taskReservations--
		runtime.mu.Unlock()
	}()

	resolvedTargets := make([]chatgptwebauth.AccountInfoRefreshTarget, len(targets))
	currentTargets := make([]bool, len(targets))
	for index, target := range targets {
		resolvedTargets[index], currentTargets[index] = runtime.resolveCurrentTarget(target)
	}
	now := runtime.currentTime()
	taskCtx, cancel := context.WithCancel(runtime.ctx)
	task := &chatGPTWebAccountInfoTaskState{
		snapshot: chatgptwebauth.AccountInfoRefreshTask{
			ID:        uuid.NewString(),
			State:     chatgptwebauth.AccountInfoTaskQueued,
			Force:     force,
			CreatedAt: now,
			Total:     len(targets),
			Results:   make([]chatgptwebauth.AccountInfoRefreshResult, len(targets)),
		},
		ctx:    taskCtx,
		cancel: cancel,
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		runtime.taskReservations--
		reservationHeld = false
		cancel()
		return nil, errors.New("chatgpt web account info is unavailable")
	}
	if !force && !runtime.cfg.AutomaticRefreshEnabled() {
		runtime.taskReservations--
		reservationHeld = false
		cancel()
		return nil, chatgptwebauth.ErrAccountInfoAutoRefreshDisabled
	}
	runtime.taskReservations--
	reservationHeld = false
	runtime.tasks[task.snapshot.ID] = task
	for index, target := range resolvedTargets {
		task.snapshot.Results[index] = chatgptwebauth.AccountInfoRefreshResult{
			Name:      target.Name,
			AuthIndex: target.AuthIndex,
			Status:    chatgptwebauth.AccountInfoResultQueued,
		}
		if !currentTargets[index] {
			task.snapshot.Results[index].Status = chatgptwebauth.AccountInfoResultFailed
			task.snapshot.Results[index].Error = "credential_unavailable"
			task.snapshot.Processed++
			task.snapshot.Failed++
			continue
		}
		work := chatGPTWebAccountInfoWork{target: target, taskID: task.snapshot.ID, index: index, force: force, attempt: 1}
		enqueued, current := runtime.enqueueForCurrentInstanceLocked(work)
		if enqueued {
			continue
		}
		task.snapshot.Results[index].Status = chatgptwebauth.AccountInfoResultFailed
		if current {
			task.snapshot.Results[index].Error = "refresh_queue_full"
		} else {
			task.snapshot.Results[index].Error = "credential_unavailable"
		}
		task.snapshot.Processed++
		task.snapshot.Failed++
	}
	runtime.finishTaskIfDoneLocked(task)
	snapshot := cloneChatGPTWebAccountInfoTask(&task.snapshot)
	return snapshot, nil
}

func (runtime *chatGPTWebAccountInfoRuntime) task(id string) (*chatgptwebauth.AccountInfoRefreshTask, bool) {
	if runtime == nil {
		return nil, false
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.pruneTasksLocked()
	task := runtime.tasks[strings.TrimSpace(id)]
	if task == nil {
		return nil, false
	}
	return cloneChatGPTWebAccountInfoTask(&task.snapshot), true
}

func (runtime *chatGPTWebAccountInfoRuntime) cancelTask(id string) (*chatgptwebauth.AccountInfoRefreshTask, bool) {
	if runtime == nil {
		return nil, false
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	task := runtime.tasks[strings.TrimSpace(id)]
	if task == nil {
		return nil, false
	}
	if task.snapshot.CompletedAt != nil {
		return cloneChatGPTWebAccountInfoTask(&task.snapshot), true
	}
	task.snapshot.State = chatgptwebauth.AccountInfoTaskCanceling
	if task.cancel != nil {
		task.cancel()
	}
	activeQueue := runtime.queuedWorkLocked()
	filtered := activeQueue[:0]
	for _, work := range activeQueue {
		if work.taskID != task.snapshot.ID {
			filtered = append(filtered, work)
			continue
		}
		runtime.syncCanceledWorkRecoveryLocked(work)
		runtime.retainIndependentTriggerLocked(work)
		result := &task.snapshot.Results[work.index]
		if !accountInfoResultTerminal(result.Status) {
			result.Status = chatgptwebauth.AccountInfoResultCanceled
			result.Error = ""
			task.snapshot.Processed++
			task.snapshot.Canceled++
		}
		runtime.releaseWorkEpochLocked(work)
	}
	runtime.replaceQueuedWorkLocked(filtered)
	scheduledKeys := make([]string, 0)
	for key, entry := range runtime.scheduled {
		if entry.work.taskID != task.snapshot.ID {
			continue
		}
		scheduledKeys = append(scheduledKeys, key)
	}
	for _, key := range scheduledKeys {
		entry := runtime.removeScheduleLocked(key)
		if entry == nil {
			continue
		}
		runtime.syncCanceledWorkRecoveryLocked(entry.work)
		runtime.retainIndependentTriggerLocked(entry.work)
		result := &task.snapshot.Results[entry.work.index]
		if !accountInfoResultTerminal(result.Status) {
			result.Status = chatgptwebauth.AccountInfoResultCanceled
			result.Error = ""
			task.snapshot.Processed++
			task.snapshot.Canceled++
		}
		runtime.releaseWorkEpochLocked(entry.work)
	}
	runtime.drainPendingTriggersLocked()
	runtime.finishTaskIfDoneLocked(task)
	return cloneChatGPTWebAccountInfoTask(&task.snapshot), true
}

func (runtime *chatGPTWebAccountInfoRuntime) pruneTasksLocked() {
	now := runtime.currentTime()
	for id, task := range runtime.tasks {
		if task == nil || task.snapshot.CompletedAt == nil {
			continue
		}
		if now.Sub(*task.snapshot.CompletedAt) > chatGPTWebAccountInfoTaskRetention {
			delete(runtime.tasks, id)
		}
	}
	for len(runtime.tasks) >= chatGPTWebAccountInfoTaskMaxKept {
		oldestID := ""
		var oldest time.Time
		for id, task := range runtime.tasks {
			if task == nil || task.snapshot.CompletedAt == nil {
				continue
			}
			if oldestID == "" || task.snapshot.CreatedAt.Before(oldest) {
				oldestID = id
				oldest = task.snapshot.CreatedAt
			}
		}
		if oldestID == "" {
			break
		}
		delete(runtime.tasks, oldestID)
	}
}

func cloneChatGPTWebAccountInfoTask(task *chatgptwebauth.AccountInfoRefreshTask) *chatgptwebauth.AccountInfoRefreshTask {
	if task == nil {
		return nil
	}
	cloned := *task
	cloned.Results = append([]chatgptwebauth.AccountInfoRefreshResult(nil), task.Results...)
	if task.StartedAt != nil {
		value := *task.StartedAt
		cloned.StartedAt = &value
	}
	if task.CompletedAt != nil {
		value := *task.CompletedAt
		cloned.CompletedAt = &value
	}
	return &cloned
}

func (runtime *chatGPTWebAccountInfoRuntime) snapshot() chatgptwebauth.AccountInfoRuntimeSnapshot {
	if runtime == nil {
		return chatgptwebauth.AccountInfoRuntimeSnapshot{}
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	queued := runtime.queueLengthLocked() + runtime.waiting
	for _, mode := range runtime.pendingTriggers {
		if mode != chatGPTWebAccountInfoTriggerNone {
			queued++
		}
	}
	return chatgptwebauth.AccountInfoRuntimeSnapshot{
		Busy:         runtime.busy,
		Queued:       queued,
		Scheduled:    len(runtime.scheduled),
		Inflight:     len(runtime.inflight),
		RefreshCount: runtime.refreshCount,
		RetryCount:   runtime.retryCount,
		FailedCount:  runtime.failedCount,
		LastError:    chatgptwebauth.SafeQuotaError(runtime.lastError),
	}
}

func (runtime *chatGPTWebAccountInfoRuntime) authState(authID string) chatgptwebauth.AccountInfoAuthRuntimeState {
	if runtime == nil {
		return chatgptwebauth.AccountInfoAuthRuntimeState{}
	}
	target, current := runtime.resolveCurrentTarget(chatgptwebauth.AccountInfoRefreshTarget{AuthID: authID})
	if !current {
		return chatgptwebauth.AccountInfoAuthRuntimeState{}
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	target = runtime.bindCurrentTargetLocked(target)
	runtimeKey := chatGPTWebAccountInfoTargetKey(target)
	state := runtime.states[runtimeKey]
	if runtime.inflight[runtimeKey] > 0 ||
		runtime.pendingTriggers[runtimeKey] != chatGPTWebAccountInfoTriggerNone ||
		runtime.targetQueuedLocked(runtimeKey) {
		state.Refreshing = true
	}
	return state
}

func (runtime *chatGPTWebAccountInfoRuntime) targetQueuedLocked(runtimeKey string) bool {
	if runtime.queuedByTarget == nil && runtime.queueLengthLocked() > 0 {
		runtime.rebuildQueuedTargetsLocked()
	}
	return runtime.queuedByTarget[runtimeKey] > 0
}

func (runtime *chatGPTWebAccountInfoRuntime) hasPassiveAuthState(authID, authInstanceID string) bool {
	if runtime == nil {
		return false
	}
	authID = strings.TrimSpace(authID)
	authInstanceID = strings.TrimSpace(authInstanceID)
	if authID == "" {
		return false
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtimeKey := chatGPTWebAccountInfoAuthInstanceKey(authID, authInstanceID)
	if authInstanceID != "" {
		if runtime.authInstances[authID] == authInstanceID {
			return true
		}
		if _, ok := runtime.states[runtimeKey]; ok {
			return true
		}
		if runtime.inflight[runtimeKey] > 0 ||
			runtime.pendingTriggers[runtimeKey] != chatGPTWebAccountInfoTriggerNone ||
			runtime.authEpochRefs[runtimeKey] > 0 ||
			runtime.calls[runtimeKey] != nil {
			return true
		}
	} else {
		if _, ok := runtime.authInstances[authID]; ok {
			return true
		}
		for stateKey := range runtime.states {
			if chatGPTWebAccountInfoRuntimeKeyMatchesAuth(stateKey, authID) {
				return true
			}
		}
		for stateKey, count := range runtime.inflight {
			if count > 0 && chatGPTWebAccountInfoRuntimeKeyMatchesAuth(stateKey, authID) {
				return true
			}
		}
		for stateKey, pending := range runtime.pendingTriggers {
			if pending != chatGPTWebAccountInfoTriggerNone &&
				chatGPTWebAccountInfoRuntimeKeyMatchesAuth(stateKey, authID) {
				return true
			}
		}
		for stateKey, references := range runtime.authEpochRefs {
			if references > 0 && chatGPTWebAccountInfoRuntimeKeyMatchesAuth(stateKey, authID) {
				return true
			}
		}
		for _, call := range runtime.calls {
			if call != nil && strings.TrimSpace(call.authID) == authID {
				return true
			}
		}
	}
	for _, work := range runtime.queuedWorkLocked() {
		if chatGPTWebAccountInfoTargetsMatch(work.target, authID, authInstanceID) {
			return true
		}
	}
	for _, entry := range runtime.scheduled {
		if entry != nil && chatGPTWebAccountInfoTargetsMatch(entry.work.target, authID, authInstanceID) {
			return true
		}
	}
	return false
}

func (runtime *chatGPTWebAccountInfoRuntime) trigger(authID string, force bool) bool {
	mode := chatGPTWebAccountInfoTriggerDefault
	if force {
		mode = chatGPTWebAccountInfoTriggerForce
	}
	return runtime.triggerWithMode(authID, mode)
}

func (runtime *chatGPTWebAccountInfoRuntime) triggerAutomaticRecheck(authID string) bool {
	return runtime.triggerWithMode(authID, chatGPTWebAccountInfoTriggerAutomaticRecheck)
}

func (runtime *chatGPTWebAccountInfoRuntime) triggerAmbiguousImageRecheck(authID string) bool {
	if runtime == nil || strings.TrimSpace(authID) == "" {
		return false
	}
	target, current := runtime.resolveCurrentTarget(chatgptwebauth.AccountInfoRefreshTarget{
		Name:   strings.TrimSpace(authID),
		AuthID: strings.TrimSpace(authID),
	})
	if !current {
		return false
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	target, current = runtime.resolveCurrentTargetLocked(target)
	if !current {
		return false
	}
	runtimeKey := chatGPTWebAccountInfoTargetKey(target)
	now := runtime.currentTime()
	if next := runtime.ambiguousImageRecheckAfter[runtimeKey]; next.After(now) {
		return false
	}
	if !runtime.triggerTargetLocked(target, chatGPTWebAccountInfoTriggerAutomaticRecheck) {
		return false
	}
	if runtime.ambiguousImageRecheckAfter == nil {
		runtime.ambiguousImageRecheckAfter = make(map[string]time.Time)
	}
	runtime.ambiguousImageRecheckAfter[runtimeKey] = now.Add(chatGPTWebAmbiguousImageRecheckCooldown)
	return true
}

func (runtime *chatGPTWebAccountInfoRuntime) triggerWithMode(
	authID string,
	mode chatGPTWebAccountInfoTriggerMode,
) bool {
	if runtime == nil || strings.TrimSpace(authID) == "" {
		return false
	}
	target, current := runtime.resolveCurrentTarget(chatgptwebauth.AccountInfoRefreshTarget{
		Name:   strings.TrimSpace(authID),
		AuthID: strings.TrimSpace(authID),
	})
	if !current {
		return false
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	target, current = runtime.resolveCurrentTargetLocked(target)
	if !current {
		return false
	}
	return runtime.triggerTargetLocked(target, mode)
}

func (runtime *chatGPTWebAccountInfoRuntime) triggerTargetLocked(
	target chatgptwebauth.AccountInfoRefreshTarget,
	mode chatGPTWebAccountInfoTriggerMode,
) bool {
	runtimeKey := chatGPTWebAccountInfoTargetKey(target)
	if mode != chatGPTWebAccountInfoTriggerForce && !runtime.cfg.AutomaticRefreshEnabled() {
		delete(runtime.pendingTriggers, runtimeKey)
		return false
	}
	if runtime.pendingTriggers == nil {
		runtime.pendingTriggers = make(map[string]chatGPTWebAccountInfoTriggerMode)
	}
	if runtime.inflightForce == nil {
		runtime.inflightForce = make(map[string]int)
	}
	if runtime.inflightTask == nil {
		runtime.inflightTask = make(map[string]int)
	}
	if runtime.inflight[runtimeKey] > 0 {
		needsPending := runtime.inflightTask[runtimeKey] > 0 ||
			(mode.forced() && runtime.inflightForce[runtimeKey] == 0)
		if needsPending && mode > runtime.pendingTriggers[runtimeKey] {
			runtime.pendingTriggers[runtimeKey] = mode
		}
		return true
	}
	queuedWork := runtime.queuedWorkLocked()
	for index := range queuedWork {
		work := &queuedWork[index]
		if chatGPTWebAccountInfoTargetKey(work.target) == runtimeKey {
			runtime.trackIndependentTriggerLocked(work, mode)
			work.force = work.force || mode.forced()
			return true
		}
	}
	now := runtime.currentTime()
	if runtime.handleScheduledTriggerLocked(target, mode, now) {
		return true
	}
	work := chatGPTWebAccountInfoWork{
		target:    target,
		force:     mode.forced(),
		attempt:   1,
		automatic: true,
	}
	enqueued, _ := runtime.enqueueForCurrentInstanceLocked(work)
	if enqueued {
		return true
	}
	return false
}

func (runtime *chatGPTWebAccountInfoRuntime) handleScheduledTriggerLocked(
	target chatgptwebauth.AccountInfoRefreshTarget,
	mode chatGPTWebAccountInfoTriggerMode,
	now time.Time,
) bool {
	runtimeKey := chatGPTWebAccountInfoTargetKey(target)
	scheduledForTarget := runtime.scheduledByTarget[runtimeKey]
	if mode == chatGPTWebAccountInfoTriggerAutomaticRecheck {
		if retry := runtime.scheduled["retry:"+runtimeKey]; retry != nil &&
			chatGPTWebAccountInfoTargetKey(retry.work.target) == runtimeKey {
			retry.work.force = true
			runtime.refreshNextScheduleStateForTargetLocked(target)
			return true
		}
		for _, scheduled := range scheduledForTarget {
			if scheduled != nil && scheduled.work.taskID != "" {
				runtime.trackIndependentTriggerLocked(&scheduled.work, mode)
				scheduled.work.force = true
				runtime.refreshNextScheduleStateForTargetLocked(target)
				return true
			}
		}
		for _, key := range []string{"recovery:" + runtimeKey, "force:" + runtimeKey} {
			if scheduled := runtime.scheduled[key]; scheduled != nil &&
				chatGPTWebAccountInfoTargetKey(scheduled.work.target) == runtimeKey {
				runtime.promoteAccountInfoScheduleLocked(scheduled, target, now)
				return true
			}
		}
	}
	for _, scheduled := range scheduledForTarget {
		if scheduled != nil {
			runtime.trackIndependentTriggerLocked(&scheduled.work, mode)
			if mode.forced() && mode != chatGPTWebAccountInfoTriggerAutomaticRecheck {
				runtime.promoteAccountInfoScheduleLocked(scheduled, target, now)
			} else {
				runtime.refreshNextScheduleStateForTargetLocked(target)
			}
			return true
		}
	}
	return false
}

func (runtime *chatGPTWebAccountInfoRuntime) trackIndependentTriggerLocked(
	work *chatGPTWebAccountInfoWork,
	mode chatGPTWebAccountInfoTriggerMode,
) {
	if work == nil || work.taskID == "" || mode <= work.independentTrigger {
		return
	}
	work.independentTrigger = mode
}

func (runtime *chatGPTWebAccountInfoRuntime) retainIndependentTriggerLocked(
	work chatGPTWebAccountInfoWork,
) {
	mode := work.independentTrigger
	runtimeKey := chatGPTWebAccountInfoTargetKey(work.target)
	if mode == chatGPTWebAccountInfoTriggerNone || runtimeKey == "" {
		return
	}
	if mode != chatGPTWebAccountInfoTriggerForce && !runtime.cfg.AutomaticRefreshEnabled() {
		delete(runtime.pendingTriggers, runtimeKey)
		return
	}
	if runtime.pendingTriggers == nil {
		runtime.pendingTriggers = make(map[string]chatGPTWebAccountInfoTriggerMode)
	}
	if mode > runtime.pendingTriggers[runtimeKey] {
		runtime.pendingTriggers[runtimeKey] = mode
	}
}

func (runtime *chatGPTWebAccountInfoRuntime) promoteAccountInfoScheduleLocked(
	scheduled *chatGPTWebAccountInfoSchedule,
	target chatgptwebauth.AccountInfoRefreshTarget,
	now time.Time,
) {
	scheduled.work.force = true
	scheduled.due = now
	runtime.schedule++
	scheduled.seq = runtime.schedule
	heap.Fix(&runtime.schedules, scheduled.index)
	runtime.refreshNextScheduleStateForTargetLocked(target)
	runtime.signalScheduler()
}

func (runtime *chatGPTWebAccountInfoRuntime) enqueuePendingTriggerLocked(authID string) {
	target := chatgptwebauth.AccountInfoRefreshTarget{Name: authID, AuthID: authID}
	target = runtime.bindCurrentTargetLocked(target)
	runtime.enqueuePendingTriggerForTargetLocked(target)
}

func (runtime *chatGPTWebAccountInfoRuntime) enqueuePendingTriggerForTargetLocked(
	target chatgptwebauth.AccountInfoRefreshTarget,
) {
	runtimeKey := chatGPTWebAccountInfoTargetKey(target)
	mode := runtime.pendingTriggers[runtimeKey]
	if runtimeKey == "" || runtime.inflight[runtimeKey] > 0 ||
		mode == chatGPTWebAccountInfoTriggerNone {
		return
	}
	if runtime.triggerTargetLocked(target, mode) {
		delete(runtime.pendingTriggers, runtimeKey)
	}
}

func (runtime *chatGPTWebAccountInfoRuntime) drainPendingTriggersLocked() {
	for runtimeKey, mode := range runtime.pendingTriggers {
		if runtimeKey == "" || mode == chatGPTWebAccountInfoTriggerNone ||
			runtime.inflight[runtimeKey] > 0 {
			continue
		}
		authID, authInstanceID, _ := strings.Cut(runtimeKey, "\x00")
		target, current := runtime.resolveCurrentTargetLocked(chatgptwebauth.AccountInfoRefreshTarget{
			Name:           authID,
			AuthID:         authID,
			AuthInstanceID: authInstanceID,
		})
		if !current {
			delete(runtime.pendingTriggers, runtimeKey)
			continue
		}
		if runtime.triggerTargetLocked(target, mode) {
			delete(runtime.pendingTriggers, runtimeKey)
		}
	}
}

// AccountInfoSnapshot returns bounded refresh runtime state.
func (e *ChatGPTWebExecutor) AccountInfoSnapshot() chatgptwebauth.AccountInfoRuntimeSnapshot {
	if e == nil || e.accountInfo == nil {
		return chatgptwebauth.AccountInfoRuntimeSnapshot{}
	}
	return e.accountInfo.snapshot()
}

// HasPassiveAuthInstanceState reports account-info state created outside request execution.
func (e *ChatGPTWebExecutor) HasPassiveAuthInstanceState(authID string, authInstanceID string) bool {
	return e != nil && e.accountInfo != nil && e.accountInfo.hasPassiveAuthState(authID, authInstanceID)
}

// AccountInfoAuthState returns transient state for one credential.
func (e *ChatGPTWebExecutor) AccountInfoAuthState(authID string) chatgptwebauth.AccountInfoAuthRuntimeState {
	if e == nil || e.accountInfo == nil {
		return chatgptwebauth.AccountInfoAuthRuntimeState{}
	}
	return e.accountInfo.authState(authID)
}

// StartAccountInfoRefreshTask queues bounded account profile and quota refreshes.
func (e *ChatGPTWebExecutor) StartAccountInfoRefreshTask(targets []chatgptwebauth.AccountInfoRefreshTarget, force bool) (*chatgptwebauth.AccountInfoRefreshTask, error) {
	if e == nil || e.accountInfo == nil {
		return nil, errors.New("chatgpt web account info is unavailable")
	}
	return e.accountInfo.startTask(targets, force)
}

// AccountInfoRefreshTask returns one task snapshot.
func (e *ChatGPTWebExecutor) AccountInfoRefreshTask(id string) (*chatgptwebauth.AccountInfoRefreshTask, bool) {
	if e == nil || e.accountInfo == nil {
		return nil, false
	}
	return e.accountInfo.task(id)
}

// CancelAccountInfoRefreshTask cancels pending work for one task.
func (e *ChatGPTWebExecutor) CancelAccountInfoRefreshTask(id string) (*chatgptwebauth.AccountInfoRefreshTask, bool) {
	if e == nil || e.accountInfo == nil {
		return nil, false
	}
	return e.accountInfo.cancelTask(id)
}

// TriggerAccountInfoRefresh schedules a provider-owned refresh without a management task.
func (e *ChatGPTWebExecutor) TriggerAccountInfoRefresh(authID string, force bool) bool {
	return e != nil && e.accountInfo != nil && e.accountInfo.trigger(authID, force)
}

// TriggerAutomaticAccountInfoRefresh schedules a quota recheck without advancing a pending retry.
func (e *ChatGPTWebExecutor) TriggerAutomaticAccountInfoRefresh(authID string) bool {
	return e != nil && e.accountInfo != nil && e.accountInfo.triggerAutomaticRecheck(authID)
}

func (e *ChatGPTWebExecutor) triggerAmbiguousImageAccountInfoRefresh(authID string) bool {
	return e != nil && e.accountInfo != nil && e.accountInfo.triggerAmbiguousImageRecheck(authID)
}

// SyncAccountInfoRecovery reconciles one persisted image quota reset schedule.
func (e *ChatGPTWebExecutor) SyncAccountInfoRecovery(auth *cliproxyauth.Auth) {
	if e == nil || e.accountInfo == nil {
		return
	}
	e.accountInfo.syncPersistedRecovery(auth)
}

func (e *ChatGPTWebExecutor) refreshChatGPTWebAccountInfo(ctx context.Context, authID string, force bool) chatGPTWebAccountInfoOutcome {
	return e.refreshChatGPTWebAccountInfoForInstance(ctx, authID, "", force)
}

func (e *ChatGPTWebExecutor) refreshChatGPTWebAccountInfoForInstance(
	ctx context.Context,
	authID string,
	authInstanceID string,
	force bool,
) chatGPTWebAccountInfoOutcome {
	outcome := chatGPTWebAccountInfoOutcome{status: chatgptwebauth.AccountInfoResultFailed}
	if e == nil || e.manager == nil {
		outcome.errorCode = "credential_unavailable"
		return outcome
	}
	auth, ok := e.manager.GetByID(strings.TrimSpace(authID))
	if !ok || auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), chatgptwebauth.Provider) {
		outcome.errorCode = "credential_unavailable"
		return outcome
	}
	authInstanceID = strings.TrimSpace(authInstanceID)
	if authInstanceID != "" && strings.TrimSpace(auth.RuntimeInstanceID()) != authInstanceID {
		outcome.errorCode = "credential_unavailable"
		return outcome
	}
	if auth.Disabled || auth.Status == cliproxyauth.StatusDisabled {
		outcome.errorCode = "credential_unavailable"
		return outcome
	}
	credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
	if errCredential != nil || !auth.LifecycleRefreshable() {
		outcome.errorCode = "credential_unavailable"
		return outcome
	}
	cfg := accountInfoConfigSnapshot(e.configSnapshot())
	if !force &&
		!credential.QuotaStale &&
		accountInfoFresh(credential.ProfileUpdatedAt, e.currentTime(), cfg.RefreshTTLMinutes) &&
		accountInfoFresh(credential.QuotaUpdatedAt, e.currentTime(), cfg.RefreshTTLMinutes) {
		outcome.status = chatgptwebauth.AccountInfoResultFresh
		outcome.quotaStateKnown = true
		outcome.exhausted = credential.QuotaState == chatgptwebauth.QuotaStateExhausted
		outcome.quotaResetAt, _ = time.Parse(time.RFC3339Nano, credential.ImageQuotaResetAt)
		return outcome
	}

	profile, quota, profileErr, quotaErr, baselineCookies, cookies, persona, refreshedAuth := e.fetchChatGPTWebAccountInfo(ctx, auth)
	if refreshedAuth != nil {
		if authInstanceID != "" && strings.TrimSpace(refreshedAuth.RuntimeInstanceID()) != authInstanceID {
			outcome.errorCode = "credential_unavailable"
			return outcome
		}
		auth = refreshedAuth
		credential, _ = chatgptwebauth.ParseCredential(auth.Metadata)
	}
	profileObservation := captureChatGPTWebAccountProfileObservation(auth)
	observedAuth, observed := e.manager.GetByID(auth.ID)
	if !observed || observedAuth == nil ||
		!strings.EqualFold(strings.TrimSpace(observedAuth.Provider), chatgptwebauth.Provider) ||
		strings.TrimSpace(observedAuth.RuntimeInstanceID()) != strings.TrimSpace(auth.RuntimeInstanceID()) {
		outcome.errorCode = "credential_unavailable"
		return outcome
	}
	if authInstanceID != "" && strings.TrimSpace(observedAuth.RuntimeInstanceID()) != authInstanceID {
		outcome.errorCode = "credential_unavailable"
		return outcome
	}
	auth = observedAuth
	credential, errCredential = chatgptwebauth.ParseCredential(auth.Metadata)
	if errCredential != nil {
		outcome.errorCode = "credential_unavailable"
		return outcome
	}
	quotaObservation := captureChatGPTWebImageQuotaObservation(auth)
	if errors.Is(ctx.Err(), context.Canceled) {
		outcome.status = chatgptwebauth.AccountInfoResultCanceled
		outcome.errorCode = "canceled"
		return outcome
	}
	if errors.Is(profileErr, chatgptwebauth.ErrAccountProfileIdentityMismatch) {
		outcome.errorCode = "identity_mismatch"
		e.persistChatGPTWebAccountInfoFailure(ctx, auth, quotaObservation, outcome.errorCode)
		return outcome
	}
	if profileErr != nil && quotaErr != nil {
		outcome.errorCode, outcome.retryable = classifyChatGPTWebAccountInfoErrors(profileErr, quotaErr)
		outcome.retryAfter = chatGPTWebAccountInfoRetryAfter(profileErr, quotaErr)
		e.persistChatGPTWebAccountInfoFailure(ctx, auth, quotaObservation, outcome.errorCode)
		return outcome
	}
	if profileErr == nil && credential != nil && profile.AccountID != "" &&
		credential.AccountID != "" &&
		profile.AccountID != credential.AccountID {
		outcome.errorCode = "identity_mismatch"
		e.persistChatGPTWebAccountInfoFailure(ctx, auth, quotaObservation, outcome.errorCode)
		return outcome
	}
	if quotaErr == nil && !quota.FeaturePresent && credential != nil &&
		credential.QuotaState != chatgptwebauth.QuotaStateUnknown {
		quotaErr = errors.New("image quota is missing from conversation init")
	}

	now := e.currentTime().UTC()
	profileChanged := false
	quotaChanged := false
	if profileErr == nil && credential != nil {
		profileChanged = strings.TrimSpace(profile.PlanType) != strings.TrimSpace(credential.PlanType) ||
			(credential.AccountID == "" && profile.AccountID != "")
	}
	if quotaErr == nil && credential != nil {
		nextState := chatgptwebauth.QuotaStateUnknown
		var nextRemaining *int
		if quota.Present {
			remaining := quota.Remaining
			nextRemaining = &remaining
			if remaining > 0 {
				nextState = chatgptwebauth.QuotaStateAvailable
			} else {
				nextState = chatgptwebauth.QuotaStateExhausted
			}
		}
		quotaChanged = !equalOptionalInt(credential.ImageQuotaRemaining, nextRemaining) ||
			credential.QuotaState != nextState ||
			!sameAccountInfoTime(credential.ImageQuotaResetAt, quota.ResetAt) ||
			credential.QuotaStale || credential.QuotaLastError != ""
		outcome.exhausted = nextState == chatgptwebauth.QuotaStateExhausted
		outcome.quotaResetAt = quota.ResetAt
	}

	profileObservationCurrent := true
	quotaObservationCurrent := true
	mutateAccountInfo := func(currentAuth *cliproxyauth.Auth) {
		currentCredential, errParse := chatgptwebauth.ParseCredential(currentAuth.Metadata)
		if errParse != nil {
			return
		}
		currentQuotaObservation := quotaObservation.matches(currentAuth)
		if !currentQuotaObservation {
			quotaObservationCurrent = false
		}
		currentProfileObservation := profileObservation.matches(currentAuth)
		if !currentProfileObservation {
			profileObservationCurrent = false
		}
		if profileErr == nil && currentProfileObservation {
			currentCredential.PlanType = strings.TrimSpace(profile.PlanType)
			if currentCredential.AccountID == "" {
				currentCredential.AccountID = profile.AccountID
			}
			currentCredential.ProfileUpdatedAt = now.Format(time.RFC3339Nano)
		}
		if quotaErr == nil && currentQuotaObservation {
			if quota.Present {
				remaining := quota.Remaining
				currentCredential.ImageQuotaRemaining = &remaining
				if remaining > 0 {
					currentCredential.QuotaState = chatgptwebauth.QuotaStateAvailable
				} else {
					currentCredential.QuotaState = chatgptwebauth.QuotaStateExhausted
				}
				currentCredential.ImageQuotaResetAt = formatOptionalAccountInfoTime(quota.ResetAt)
			} else {
				currentCredential.ImageQuotaRemaining = nil
				currentCredential.QuotaState = chatgptwebauth.QuotaStateUnknown
				currentCredential.ImageQuotaResetAt = ""
			}
			currentCredential.QuotaUpdatedAt = now.Format(time.RFC3339Nano)
			currentCredential.QuotaStale = false
			currentCredential.QuotaLastError = ""
		} else if quotaErr != nil && currentQuotaObservation {
			currentCredential.QuotaStale = true
			currentCredential.QuotaLastError, _ = classifyChatGPTWebAccountInfoError(quotaErr)
		}
		if len(cookies) > 0 {
			currentCredential.Cookies = mergeChatGPTWebCookieDelta(currentCredential.Cookies, baselineCookies, cookies)
		}
		if strings.TrimSpace(persona.Profile) != "" {
			currentCredential.Persona = persona
		}
		currentCredential.ApplyToMetadata(currentAuth.Metadata)
	}
	persistContext, cancelPersist := e.accountInfo.persistenceContext(ctx)
	defer cancelPersist()
	var current bool
	var errPersist error
	if quotaErr == nil && quota.Present && quota.Remaining > 0 {
		_, current, errPersist = e.manager.MutateRuntimeMetadataAndClearModelCooldownsIfCurrent(
			persistContext,
			auth,
			cliproxyauth.ChatGPTWebImageModelIDs(auth),
			"chatgpt_web_image_quota",
			mutateAccountInfo,
		)
	} else {
		_, current, errPersist = e.manager.MutateRuntimeMetadataIfCurrent(persistContext, auth, mutateAccountInfo)
	}
	if errPersist != nil || !current {
		outcome.errorCode = "refresh_failed"
		outcome.retryable = true
		return outcome
	}
	if !quotaObservationCurrent {
		outcome.errorCode = "stale_quota_observation"
		outcome.retryable = true
		return outcome
	}
	if !profileObservationCurrent {
		profileChanged = false
	}
	if quotaErr == nil {
		outcome.quotaStateKnown = true
	}
	e.manager.RefreshSchedulerEntry(auth.ID)
	if profileErr != nil || quotaErr != nil {
		if quotaErr != nil {
			outcome.errorCode, outcome.retryable = classifyChatGPTWebAccountInfoError(quotaErr)
		} else {
			outcome.errorCode, outcome.retryable = classifyChatGPTWebAccountInfoError(profileErr)
		}
		outcome.retryAfter = chatGPTWebAccountInfoRetryAfter(profileErr, quotaErr)
		outcome.status = chatgptwebauth.AccountInfoResultPartial
		return outcome
	}
	if profileChanged || quotaChanged {
		outcome.status = chatgptwebauth.AccountInfoResultUpdated
	} else {
		outcome.status = chatgptwebauth.AccountInfoResultUnchanged
	}
	return outcome
}

func (e *ChatGPTWebExecutor) cachedChatGPTWebAccountInfoOutcome(authID string) (chatGPTWebAccountInfoOutcome, bool) {
	return e.cachedChatGPTWebAccountInfoOutcomeForInstance(authID, "")
}

func (e *ChatGPTWebExecutor) cachedChatGPTWebAccountInfoOutcomeForInstance(
	authID string,
	authInstanceID string,
) (chatGPTWebAccountInfoOutcome, bool) {
	outcome := chatGPTWebAccountInfoOutcome{status: chatgptwebauth.AccountInfoResultFresh}
	if e == nil || e.manager == nil {
		return outcome, false
	}
	auth, ok := e.manager.GetByID(strings.TrimSpace(authID))
	if !ok || auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), chatgptwebauth.Provider) {
		return outcome, false
	}
	authInstanceID = strings.TrimSpace(authInstanceID)
	if authInstanceID != "" && strings.TrimSpace(auth.RuntimeInstanceID()) != authInstanceID {
		return outcome, false
	}
	credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
	if errCredential != nil || !auth.LifecycleRefreshable() {
		return outcome, false
	}
	cfg := accountInfoConfigSnapshot(e.configSnapshot())
	if credential.QuotaStale ||
		!accountInfoFresh(credential.ProfileUpdatedAt, e.currentTime(), cfg.RefreshTTLMinutes) ||
		!accountInfoFresh(credential.QuotaUpdatedAt, e.currentTime(), cfg.RefreshTTLMinutes) {
		return outcome, false
	}
	outcome.exhausted = credential.QuotaState == chatgptwebauth.QuotaStateExhausted
	outcome.quotaStateKnown = true
	outcome.quotaResetAt, _ = time.Parse(time.RFC3339Nano, credential.ImageQuotaResetAt)
	return outcome, true
}

func (e *ChatGPTWebExecutor) fetchChatGPTWebAccountInfo(ctx context.Context, auth *cliproxyauth.Auth) (
	profile chatgptwebauth.AccountProfile,
	quota chatgptwebauth.ImageQuota,
	profileErr error,
	quotaErr error,
	baselineCookies []chatgptwebauth.Cookie,
	cookies []chatgptwebauth.Cookie,
	persona chatgptwebauth.Persona,
	current *cliproxyauth.Auth,
) {
	current = auth
	for refreshAttempt := 0; refreshAttempt < 2; refreshAttempt++ {
		resolved, errResolve := e.manager.ResolveProxyAuth(ctx, current)
		if errResolve != nil {
			profileErr = errResolve
			quotaErr = errResolve
			return
		}
		client, credential, errClient := e.newRuntimeClientForAcquisition(resolved, true)
		if errClient != nil {
			profileErr = errClient
			quotaErr = errClient
			return
		}
		baselineCookies = append(baselineCookies[:0], client.ExportCookies()...)
		quotaClient, errQuotaClient := chatgptwebauth.NewAccessTokenAcquisitionClient(
			client.Persona(),
			client.ProxyURL(),
			baselineCookies,
			e.accountInfoTimeout,
		)
		if errQuotaClient != nil {
			client.CloseIdleConnections()
			profileErr = errQuotaClient
			quotaErr = errQuotaClient
			return
		}
		profile, quota, profileErr, quotaErr = e.fetchChatGPTWebAccountInfoPair(ctx, client, quotaClient, credential)
		if errContext := ctx.Err(); errContext != nil {
			client.CloseIdleConnections()
			quotaClient.CloseIdleConnections()
			profileErr = errContext
			quotaErr = errContext
			return
		}
		cookies = mergeChatGPTWebCookieDelta(baselineCookies, baselineCookies, client.ExportCookies())
		cookies = mergeChatGPTWebCookieDelta(cookies, baselineCookies, quotaClient.ExportCookies())
		persona = client.Persona()
		client.CloseIdleConnections()
		quotaClient.CloseIdleConnections()
		if refreshAttempt > 0 || !accountInfoUnauthorized(profileErr, quotaErr) {
			return
		}
		installed, errRefresh := e.refreshChatGPTWebAccountInfoCredential(ctx, current)
		if installed != nil {
			current = installed
		}
		if errRefresh != nil {
			if accountInfoUnauthorized(profileErr) {
				profileErr = errRefresh
			}
			if accountInfoUnauthorized(quotaErr) {
				quotaErr = errRefresh
			}
			return
		}
	}
	return
}

func (e *ChatGPTWebExecutor) refreshChatGPTWebAccountInfoCredential(
	ctx context.Context,
	current *cliproxyauth.Auth,
) (*cliproxyauth.Auth, error) {
	if e == nil || e.manager == nil || current == nil {
		return current, errors.New("chatgpt web refresh is unavailable")
	}
	installed, errRefresh := e.manager.RefreshChatGPTWebForRequest(ctx, current)
	if installed == nil {
		installed = current
	}
	return installed, errRefresh
}

func (e *ChatGPTWebExecutor) fetchChatGPTWebAccountInfoPair(
	ctx context.Context,
	profileClient *chatgptwebauth.Client,
	quotaClient *chatgptwebauth.Client,
	credential *chatgptwebauth.Credential,
) (chatgptwebauth.AccountProfile, chatgptwebauth.ImageQuota, error, error) {
	stopCancellation := context.AfterFunc(ctx, func() {
		profileClient.CloseActiveAcquisitionConnections()
		quotaClient.CloseActiveAcquisitionConnections()
	})
	defer stopCancellation()

	type profileResult struct {
		profile chatgptwebauth.AccountProfile
		err     error
	}
	type quotaResult struct {
		quota chatgptwebauth.ImageQuota
		err   error
	}
	profileResults := make(chan profileResult, 1)
	quotaResults := make(chan quotaResult, 1)
	timezone := e.chatGPTWebTimezone()
	accountID := credential.AccountID
	if strings.TrimSpace(accountID) != accountID {
		accountID = ""
	}
	go func() {
		path := chatgptwebauth.AccountCheckPath
		queryPath := fmt.Sprintf("%s?timezone_offset_min=%d", path, timezone.OffsetMinutes)
		headers := e.chatGPTWebHeaders(credential, path, map[string]string{
			"chatgpt-account-id": accountID,
		})
		e.recordChatGPTWebRequest(ctx, credential, http.MethodGet, queryPath, headers, nil)
		response, errRequest := profileClient.DoSameOriginRedirectStream(
			ctx,
			http.MethodGet,
			e.chatGPTWebBaseURL()+queryPath,
			headers,
			chatGPTWebAccountInfoMaxRedirects,
		)
		if errRequest != nil {
			profileResults <- profileResult{err: errRequest}
			return
		}
		payload, errRead := readChatGPTWebResponseBody(response, chatGPTWebAccountInfoMaxBodyBytes)
		if errRead != nil {
			profileResults <- profileResult{err: errRead}
			return
		}
		helps.RecordAPIResponseMetadata(ctx, e.configSnapshot(), response.StatusCode, chatGPTWebResponseLogHeaders(response.Header))
		helps.AppendAPIResponseChunk(ctx, e.configSnapshot(), chatGPTWebResponseLogBody(path, payload))
		if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
			profileResults <- profileResult{err: newChatGPTWebStatusError(response.StatusCode, path, payload, response.Header)}
			return
		}
		preferredAccountID := credential.AccountID
		if strings.TrimSpace(preferredAccountID) != preferredAccountID {
			preferredAccountID = ""
		}
		profile, errParse := chatgptwebauth.ParseAccountProfileForAccount(payload, preferredAccountID)
		profileResults <- profileResult{profile: profile, err: errParse}
	}()
	go func() {
		path := chatgptwebauth.ConversationInitPath
		headers := e.chatGPTWebHeaders(credential, path, map[string]string{
			"accept":             "application/json",
			"content-type":       "application/json",
			"chatgpt-account-id": accountID,
		})
		response, payload, errRequest := e.doChatGPTWebJSONWithHeadersAndMaxBody(ctx, quotaClient, credential, path, headers, map[string]any{
			"gizmo_id":                nil,
			"requested_default_model": nil,
			"conversation_id":         nil,
			"timezone_offset_min":     timezone.OffsetMinutes,
			"system_hints":            []string{"picture_v2"},
		}, chatGPTWebAccountInfoMaxBodyBytes)
		if errRequest != nil {
			quotaResults <- quotaResult{err: errRequest}
			return
		}
		if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
			quotaResults <- quotaResult{err: newChatGPTWebStatusError(response.StatusCode, path, payload, response.Header)}
			return
		}
		quota, errParse := chatgptwebauth.ParseImageQuota(payload)
		quotaResults <- quotaResult{quota: quota, err: errParse}
	}()

	var profileResultValue profileResult
	var quotaResultValue quotaResult
	for profileResults != nil || quotaResults != nil {
		select {
		case result := <-profileResults:
			profileResultValue = result
			profileResults = nil
		case result := <-quotaResults:
			quotaResultValue = result
			quotaResults = nil
		}
	}
	return profileResultValue.profile, quotaResultValue.quota, profileResultValue.err, quotaResultValue.err
}

func (e *ChatGPTWebExecutor) persistChatGPTWebAccountInfoFailure(
	ctx context.Context,
	auth *cliproxyauth.Auth,
	observation chatGPTWebImageQuotaObservation,
	errorCode string,
) {
	if e == nil || e.manager == nil || auth == nil {
		return
	}
	errorCode = chatgptwebauth.SafeQuotaError(errorCode)
	persistContext, cancelPersist := e.accountInfo.persistenceContext(ctx)
	defer cancelPersist()
	applied := false
	_, _, errPersist := e.manager.MutateRuntimeMetadataIfCurrent(persistContext, auth, func(current *cliproxyauth.Auth) {
		if !observation.matches(current) {
			return
		}
		credential, errParse := chatgptwebauth.ParseCredential(current.Metadata)
		if errParse != nil {
			return
		}
		credential.QuotaStale = true
		credential.QuotaLastError = errorCode
		credential.ApplyToMetadata(current.Metadata)
		applied = true
	})
	if errPersist != nil {
		log.WithField("auth_id", auth.ID).Warnf("chatgpt web executor: persist account info refresh failure: %v", errPersist)
		return
	}
	if applied {
		e.manager.RefreshSchedulerEntry(auth.ID)
	}
}

func accountInfoFresh(updatedAt string, now time.Time, ttlMinutes int) bool {
	updatedAt = strings.TrimSpace(updatedAt)
	if updatedAt == "" || ttlMinutes < 1 {
		return false
	}
	parsed, errParse := time.Parse(time.RFC3339Nano, updatedAt)
	if errParse != nil || parsed.After(now) {
		return false
	}
	return now.Sub(parsed) < time.Duration(ttlMinutes)*time.Minute
}

func accountInfoUnauthorized(errs ...error) bool {
	for _, err := range errs {
		var status interface{ StatusCode() int }
		if errors.As(err, &status) && status.StatusCode() == http.StatusUnauthorized {
			return true
		}
	}
	return false
}

func classifyChatGPTWebAccountInfoError(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	if authError, ok := chatgptwebauth.AsAuthError(err); ok {
		code := chatgptwebauth.SafeQuotaError(authError.Code)
		if code == "refresh_failed" && authError.Terminal {
			code = "unauthorized"
		}
		return code, authError.Retryable
	}
	if errors.Is(err, context.Canceled) {
		return "canceled", false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "network_error", true
	}
	var status interface{ StatusCode() int }
	if errors.As(err, &status) {
		switch code := status.StatusCode(); {
		case code == http.StatusUnauthorized || code == http.StatusForbidden:
			return "unauthorized", false
		case code == http.StatusTooManyRequests:
			return "rate_limited", true
		case code >= http.StatusInternalServerError:
			return "upstream_unavailable", true
		default:
			return "invalid_response", false
		}
	}
	lower := strings.ToLower(err.Error())
	if strings.Contains(lower, "identity") {
		return "identity_mismatch", false
	}
	if strings.Contains(lower, "decode") || strings.Contains(lower, "missing") || strings.Contains(lower, "invalid") {
		return "invalid_response", true
	}
	return "network_error", true
}

func classifyChatGPTWebAccountInfoErrors(errs ...error) (string, bool) {
	type classification struct {
		code      string
		retryable bool
	}
	classified := make([]classification, 0, len(errs))
	for _, err := range errs {
		if err == nil {
			continue
		}
		code, retryable := classifyChatGPTWebAccountInfoError(err)
		classified = append(classified, classification{code: code, retryable: retryable})
		if code == "unauthorized" || code == "identity_mismatch" {
			return code, false
		}
	}
	for _, item := range classified {
		if !item.retryable {
			return item.code, false
		}
	}
	if len(classified) > 0 {
		return classified[0].code, classified[0].retryable
	}
	return "", false
}

func chatGPTWebAccountInfoRetryAfter(errs ...error) time.Duration {
	var longest time.Duration
	for _, err := range errs {
		if err == nil {
			continue
		}
		var provider interface{ RetryAfter() *time.Duration }
		if !errors.As(err, &provider) {
			continue
		}
		retryAfter := provider.RetryAfter()
		if retryAfter != nil && *retryAfter > longest {
			longest = *retryAfter
		}
	}
	return clampChatGPTWebAccountInfoRetryAfter(longest)
}

func clampChatGPTWebAccountInfoRetryAfter(delay time.Duration) time.Duration {
	if delay <= 0 {
		return 0
	}
	if delay > chatGPTWebAccountInfoMaxRetryAfter {
		return chatGPTWebAccountInfoMaxRetryAfter
	}
	return delay
}

func equalOptionalInt(left, right *int) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func sameAccountInfoTime(current string, next time.Time) bool {
	current = strings.TrimSpace(current)
	if current == "" || next.IsZero() {
		return current == "" && next.IsZero()
	}
	parsed, errParse := time.Parse(time.RFC3339Nano, current)
	return errParse == nil && parsed.Equal(next)
}

func formatOptionalAccountInfoTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

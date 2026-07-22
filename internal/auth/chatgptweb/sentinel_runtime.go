package chatgptweb

import (
	"container/list"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fastschema/qjs"
	wazerosys "github.com/tetratelabs/wazero/sys"
)

const (
	sentinelSDKMemoryLimit             = 32 << 20
	sentinelSDKStackLimit              = 1 << 20
	sentinelSDKMaxSourceBytes          = 2 << 20
	sentinelSDKRandomBytes             = 64 << 10
	sentinelSDKCompileTimeout          = 10 * time.Second
	sentinelSDKInitializeTimeout       = 15 * time.Second
	sentinelSDKTurnstileTimeout        = 5 * time.Second
	sentinelSDKObserverTimeout         = 65 * time.Second
	sentinelSDKSourceTTL               = 30 * time.Minute
	sentinelSDKPreferredTTL            = 30 * time.Minute
	sentinelSDKCircuitBreakerTTL       = time.Minute
	sentinelSDKPreferredMax            = 1024
	sentinelSDKCircuitMax              = 16
	sentinelSDKSourceCircuitMax        = 4096
	sentinelSDKSourceFetchMax          = 4
	sentinelSDKSourceWaiterMax         = 64
	sentinelSDKReclaimMinDelay         = 10 * time.Millisecond
	sentinelSDKReclaimMaxDelay         = time.Second
	sentinelSDKMaxWorkers              = 16
	sentinelSDKMaxQueueSize            = 1024
	sentinelSDKMaxCacheVersions        = 5
	sentinelSDKAdapterVersion          = "exports-v2"
	sentinelSDKExportMarker            = "t.init=we,t.sessionObserverToken=async function(t){"
	sentinelSDKExportInjection         = "globalThis.__sentinelInternals={P,_n,Nt,Et,D};"
	sentinelSDKVerifiedExportsV2SHA256 = sentinelSDKSHA256
)

type sentinelSDKAdapter struct {
	version   string
	marker    string
	injection string
}

type sentinelSDKAdapterResolver func(string, []byte) (sentinelSDKAdapter, bool)

var sentinelSDKExportsV2Adapter = sentinelSDKAdapter{
	version:   sentinelSDKAdapterVersion,
	marker:    sentinelSDKExportMarker,
	injection: sentinelSDKExportInjection,
}

func resolveKnownSentinelSDKAdapter(hash string) (sentinelSDKAdapter, bool) {
	switch strings.ToLower(strings.TrimSpace(hash)) {
	case sentinelSDKVerifiedExportsV2SHA256:
		return sentinelSDKExportsV2Adapter, true
	default:
		return sentinelSDKAdapter{}, false
	}
}

func resolveSentinelSDKAdapter(hash string, source []byte) (sentinelSDKAdapter, bool) {
	if adapter, known := resolveKnownSentinelSDKAdapter(hash); known {
		if sentinelSDKAdapterMatchesSource(source, adapter) {
			return adapter, true
		}
		return sentinelSDKAdapter{}, false
	}
	if sentinelSDKAdapterMatchesSource(source, sentinelSDKExportsV2Adapter) {
		return sentinelSDKExportsV2Adapter, true
	}
	return sentinelSDKAdapter{}, false
}

func sentinelSDKAdapterMatchesSource(source []byte, adapter sentinelSDKAdapter) bool {
	if len(source) == 0 || adapter.marker == "" {
		return false
	}
	text := string(source)
	if strings.Count(text, adapter.marker) != 1 {
		return false
	}
	for _, declaration := range []string{"function _n(", "function Nt(", "function Et(", "function D("} {
		if strings.Count(text, declaration) != 1 {
			return false
		}
	}
	variableDeclarations := 0
	for _, declaration := range []string{"var P=", "let P=", "const P="} {
		variableDeclarations += strings.Count(text, declaration)
	}
	return variableDeclarations == 1
}

// SentinelRuntimeConfig contains effective SDK runtime settings.
type SentinelRuntimeConfig struct {
	Enabled       bool
	Workers       int
	QueueSize     int
	CacheVersions int
}

// SentinelRuntimeSnapshot is safe to expose through the management API.
type SentinelRuntimeSnapshot struct {
	SDKRuntimeEnabled    bool   `json:"sdk-runtime-enabled"`
	SDKWorkers           int    `json:"sdk-workers"`
	SDKQueueSize         int    `json:"sdk-queue-size"`
	SDKCacheVersions     int    `json:"sdk-cache-versions"`
	Initialized          bool   `json:"initialized"`
	Available            bool   `json:"available"`
	WorkerLimit          int    `json:"worker_limit"`
	Busy                 int    `json:"busy"`
	Queued               int    `json:"queued"`
	SourcePending        int    `json:"source_pending"`
	SourceWaiters        int    `json:"source_waiters"`
	BytecodeWaiters      int    `json:"bytecode_waiters"`
	ObserverSessions     int    `json:"observer_sessions"`
	SDKVersion           string `json:"sdk_version,omitempty"`
	SDKSHA256            string `json:"sdk_sha256,omitempty"`
	SourceCacheEntries   int    `json:"source_cache_entries"`
	BytecodeCacheEntries int    `json:"bytecode_cache_entries"`
	FallbackCount        uint64 `json:"fallback_count"`
	LastError            string `json:"last_error,omitempty"`
}

// SentinelRuntimeError is a local SDK scheduling or availability failure.
type SentinelRuntimeError struct {
	Code       string
	RetryAfter time.Duration
	Err        error
}

func (err *SentinelRuntimeError) Error() string {
	if err == nil {
		return "sentinel SDK error"
	}
	if err.Err != nil {
		return err.Code + ": " + err.Err.Error()
	}
	return err.Code
}

func (err *SentinelRuntimeError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Err
}

// SentinelSDKFetcher downloads one trusted SDK URL through the current
// credential's transport. Implementations must not return secret headers.
type SentinelSDKFetcher func(context.Context, string, int64) ([]byte, string, string, error)

// SentinelSDKRequest describes one SDK-backed challenge.
type SentinelSDKRequest struct {
	BaseURL           string
	SDKURL            string
	ScriptSources     []string
	ExpectedSHA256    string
	IntegrityRequired bool
	TransportKey      string
	Challenge         map[string]any
	RequirementsToken string
	Environment       ConversationTurnstileEnvironment
	DeviceID          string
	Flow              string
	Fetcher           SentinelSDKFetcher
}

type sentinelTaskPriority uint8

const (
	sentinelPriorityObserverSnapshot sentinelTaskPriority = iota
	sentinelPriorityFallback
	sentinelPriorityObserverCollector
	sentinelPriorityPrecompile
	sentinelPriorityCount
)

type sentinelRuntimeWaiter struct {
	ready    chan error
	priority sentinelTaskPriority
}

type sentinelRuntimeReservation struct {
	manager  *SentinelRuntimeManager
	observer bool
	waiter   *sentinelRuntimeWaiter
	granted  bool
}

type sentinelRuntimeLease struct {
	manager  *SentinelRuntimeManager
	observer bool
	once     sync.Once
	workerMu sync.Mutex
	worker   *sentinelQJSWorker
	broken   bool
}

func (lease *sentinelRuntimeLease) release() {
	if lease == nil || lease.manager == nil {
		return
	}
	lease.once.Do(func() { lease.manager.release(lease) })
}

func (lease *sentinelRuntimeLease) markWorkerBroken() {
	if lease == nil {
		return
	}
	lease.workerMu.Lock()
	lease.broken = true
	lease.workerMu.Unlock()
}

type sentinelSourceCacheEntry struct {
	key        string
	url        string
	version    string
	hash       string
	source     []byte
	fetchedAt  time.Time
	generation uint64
}

type sentinelBytecodeCacheEntry struct {
	key      string
	hash     string
	bytecode []byte
}

type sentinelBytecodeFlight struct {
	done          chan struct{}
	bytecode      []byte
	err           error
	ownerCanceled bool
}

type sentinelSourceFlight struct {
	key       string
	done      chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc
	waiters   int
	completed bool
	entry     *sentinelSourceCacheEntry
	err       error
}

type sentinelPreferredKey struct {
	hash      string
	program   SentinelProgramKind
	signature string
}

type sentinelPreferredHintKey struct {
	program            SentinelProgramKind
	dxHash             uint64
	requirementsHash   uint64
	dxLength           int
	requirementsLength int
}

type sentinelPreferredHintEntry struct {
	signature string
	expiresAt time.Time
	candidate bool
}

// SentinelRuntimeManager owns the lazy SDK scheduler and in-memory caches.
// It starts no goroutine and creates no QJS runtime until an SDK task is used.
type SentinelRuntimeManager struct {
	mu          sync.Mutex
	lifecycleMu sync.Mutex

	config           SentinelRuntimeConfig
	workerLimit      int
	busy             int
	observerBusy     int
	queued           int
	queues           [sentinelPriorityCount][]*sentinelRuntimeWaiter
	closed           bool
	initialized      bool
	observerSessions int
	activeTasks      int
	sourcePending    int
	sourceWaiters    int
	bytecodeWaiters  int
	fallbackCount    uint64
	lastError        string
	latestVersion    string
	latestHash       string

	sourceCache     map[string]*list.Element
	sourceLRU       *list.List
	bytecodeCache   map[string]*list.Element
	bytecodeLRU     *list.List
	preferred       map[sentinelPreferredKey]time.Time
	preferredHints  map[sentinelPreferredHintKey]sentinelPreferredHintEntry
	circuits        map[string]time.Time
	sourceCircuits  map[string]time.Time
	idleWorkers     []*sentinelQJSWorker
	observers       map[*SentinelObserver]struct{}
	workerCount     int
	cacheGeneration uint64
	clearWhenIdle   bool

	sourceFlights   map[string]*sentinelSourceFlight
	sourceContext   context.Context
	cancelSources   context.CancelFunc
	bytecodeFlights map[string]*sentinelBytecodeFlight
	now             func() time.Time
	random          io.Reader
	closeRuntime    func(*qjs.Runtime) bool
	createRuntime   func(qjs.Option) (*qjs.Runtime, error)
	adapterResolver sentinelSDKAdapterResolver
	activeQJS       atomic.Int64
	maxActiveQJS    atomic.Int64
	sdkTaskWG       sync.WaitGroup
	reclaimMu       sync.Mutex
	reclaimClosed   bool
	reclaimWG       sync.WaitGroup
	enabled         atomic.Bool
	closedFlag      atomic.Bool
	hasPreferred    atomic.Bool
}

// NewSentinelRuntimeManager creates a lazy SDK runtime manager.
func NewSentinelRuntimeManager(config SentinelRuntimeConfig) *SentinelRuntimeManager {
	return newSentinelRuntimeManager(config, resolveSentinelSDKAdapter)
}

func newSentinelRuntimeManager(config SentinelRuntimeConfig, adapterResolver sentinelSDKAdapterResolver) *SentinelRuntimeManager {
	config = normalizeSentinelRuntimeConfig(config)
	if adapterResolver == nil {
		adapterResolver = resolveSentinelSDKAdapter
	}
	sourceContext, cancelSources := context.WithCancel(context.Background())
	manager := &SentinelRuntimeManager{
		config:          config,
		workerLimit:     resolveSentinelWorkerLimit(config.Workers),
		sourceCache:     make(map[string]*list.Element),
		sourceLRU:       list.New(),
		bytecodeCache:   make(map[string]*list.Element),
		bytecodeLRU:     list.New(),
		bytecodeFlights: make(map[string]*sentinelBytecodeFlight),
		preferred:       make(map[sentinelPreferredKey]time.Time),
		preferredHints:  make(map[sentinelPreferredHintKey]sentinelPreferredHintEntry),
		circuits:        make(map[string]time.Time),
		sourceCircuits:  make(map[string]time.Time),
		sourceFlights:   make(map[string]*sentinelSourceFlight),
		observers:       make(map[*SentinelObserver]struct{}),
		sourceContext:   sourceContext,
		cancelSources:   cancelSources,
		now:             time.Now,
		random:          rand.Reader,
		closeRuntime:    safeCloseSentinelQJS,
		createRuntime: func(option qjs.Option) (*qjs.Runtime, error) {
			return qjs.New(option)
		},
		adapterResolver: adapterResolver,
	}
	manager.enabled.Store(config.Enabled)
	return manager
}

func normalizeSentinelRuntimeConfig(config SentinelRuntimeConfig) SentinelRuntimeConfig {
	if config.Workers < 0 {
		config.Workers = 0
	}
	if config.Workers > sentinelSDKMaxWorkers {
		config.Workers = sentinelSDKMaxWorkers
	}
	if config.CacheVersions <= 0 {
		config.CacheVersions = 3
	}
	if config.CacheVersions > sentinelSDKMaxCacheVersions {
		config.CacheVersions = sentinelSDKMaxCacheVersions
	}
	if config.QueueSize < 0 {
		config.QueueSize = 0
	}
	if config.QueueSize > sentinelSDKMaxQueueSize {
		config.QueueSize = sentinelSDKMaxQueueSize
	}
	return config
}

func resolveSentinelWorkerLimit(workers int) int {
	if workers > 0 {
		return workers
	}
	workers = (runtime.GOMAXPROCS(0) + 1) / 2
	if workers < 1 {
		workers = 1
	}
	if workers > 4 {
		workers = 4
	}
	return workers
}

// UpdateConfig applies runtime limits without eagerly initializing the SDK.
func (manager *SentinelRuntimeManager) UpdateConfig(config SentinelRuntimeConfig) {
	if manager == nil {
		return
	}
	manager.lifecycleMu.Lock()
	defer manager.lifecycleMu.Unlock()
	config = normalizeSentinelRuntimeConfig(config)
	manager.mu.Lock()
	wasEnabled := manager.config.Enabled
	manager.config = config
	manager.workerLimit = resolveSentinelWorkerLimit(config.Workers)
	manager.trimCachesLocked()
	workersToClose := manager.trimIdleWorkersLocked()
	if wasEnabled && !config.Enabled {
		manager.enabled.Store(false)
		manager.clearWhenIdle = manager.hasAcceptedWorkLocked()
		if manager.clearWhenIdle {
			manager.grantQueuedLocked()
		} else {
			manager.clearCachesLocked()
			workersToClose = append(workersToClose, manager.drainIdleWorkersLocked()...)
		}
	} else if config.Enabled && manager.clearWhenIdle {
		manager.enabled.Store(false)
	} else if config.Enabled {
		manager.enabled.Store(true)
		manager.grantQueuedLocked()
	} else {
		manager.enabled.Store(false)
	}
	manager.mu.Unlock()
	closeSentinelQJSWorkers(workersToClose)
}

// Close rejects new work and releases all in-memory SDK state after active
// requests finish.
func (manager *SentinelRuntimeManager) Close() {
	if manager == nil {
		return
	}
	manager.lifecycleMu.Lock()
	defer manager.lifecycleMu.Unlock()
	manager.mu.Lock()
	if manager.closed {
		manager.mu.Unlock()
		return
	}
	manager.closed = true
	manager.closedFlag.Store(true)
	manager.enabled.Store(false)
	manager.clearCachesLocked()
	manager.rejectQueuedLocked(newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK runtime is closed")))
	observers := make([]*SentinelObserver, 0, len(manager.observers))
	for observer := range manager.observers {
		observers = append(observers, observer)
	}
	workersToClose := manager.drainIdleWorkersLocked()
	manager.mu.Unlock()
	for _, observer := range observers {
		observer.Close()
	}
	closeSentinelQJSWorkers(workersToClose)
	manager.sdkTaskWG.Wait()
	manager.stopQJSReclaimers()
}

// Snapshot returns a non-blocking runtime status snapshot.
func (manager *SentinelRuntimeManager) Snapshot() SentinelRuntimeSnapshot {
	if manager == nil {
		return SentinelRuntimeSnapshot{}
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	now := manager.now()
	available := manager.config.Enabled && !manager.closed && !manager.clearWhenIdle
	if manager.latestHash != "" && manager.circuits[manager.latestHash].After(now) {
		available = false
	}
	return SentinelRuntimeSnapshot{
		SDKRuntimeEnabled:    manager.config.Enabled,
		SDKWorkers:           manager.config.Workers,
		SDKQueueSize:         manager.config.QueueSize,
		SDKCacheVersions:     manager.config.CacheVersions,
		Initialized:          manager.initialized,
		Available:            available,
		WorkerLimit:          manager.workerLimit,
		Busy:                 manager.busy,
		Queued:               manager.queued,
		SourcePending:        manager.sourcePending,
		SourceWaiters:        manager.sourceWaiters,
		BytecodeWaiters:      manager.bytecodeWaiters,
		ObserverSessions:     manager.observerSessions,
		SDKVersion:           manager.latestVersion,
		SDKSHA256:            manager.latestHash,
		SourceCacheEntries:   len(manager.sourceCache),
		BytecodeCacheEntries: len(manager.bytecodeCache),
		FallbackCount:        manager.fallbackCount,
		LastError:            manager.lastError,
	}
}

func (manager *SentinelRuntimeManager) beginSDKTask() (func(), error) {
	manager.mu.Lock()
	if manager.closed || !manager.config.Enabled || manager.clearWhenIdle {
		manager.mu.Unlock()
		return nil, newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK runtime is disabled"))
	}
	manager.sdkTaskWG.Add(1)
	manager.activeTasks++
	manager.mu.Unlock()
	return manager.finishSDKTask, nil
}

func (manager *SentinelRuntimeManager) reserveObserver(observer *SentinelObserver) error {
	if manager == nil || observer == nil {
		return newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK Observer is unavailable"))
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.closed || !manager.config.Enabled || manager.clearWhenIdle {
		return newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK runtime is disabled"))
	}
	reservation, err := manager.reserveLocked(sentinelPriorityObserverCollector)
	if err != nil {
		return err
	}
	observer.reservation = reservation
	manager.observers[observer] = struct{}{}
	return nil
}

func (manager *SentinelRuntimeManager) finishSDKTask() {
	if manager == nil {
		return
	}
	defer manager.sdkTaskWG.Done()
	manager.mu.Lock()
	if manager.activeTasks > 0 {
		manager.activeTasks--
	}
	workersToClose := manager.finishDrainLocked()
	manager.mu.Unlock()
	closeSentinelQJSWorkers(workersToClose)
}

func (manager *SentinelRuntimeManager) hasAcceptedWorkLocked() bool {
	return manager.activeTasks > 0 || manager.busy > 0 || manager.queued > 0 ||
		manager.sourcePending > 0 || manager.sourceWaiters > 0 || manager.bytecodeWaiters > 0 ||
		len(manager.sourceFlights) > 0 || len(manager.bytecodeFlights) > 0 || len(manager.observers) > 0
}

func (manager *SentinelRuntimeManager) finishDrainLocked() []*sentinelQJSWorker {
	if !manager.clearWhenIdle || manager.hasAcceptedWorkLocked() {
		return nil
	}
	manager.clearWhenIdle = false
	manager.clearCachesLocked()
	workersToClose := manager.drainIdleWorkersLocked()
	manager.enabled.Store(manager.config.Enabled && !manager.closed)
	if manager.config.Enabled && !manager.closed {
		manager.grantQueuedLocked()
	}
	return workersToClose
}

func (manager *SentinelRuntimeManager) reserve(ctx context.Context, priority sentinelTaskPriority) (*sentinelRuntimeReservation, error) {
	if manager == nil {
		return nil, newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK runtime is unavailable"))
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return manager.reserveLocked(priority)
}

func (manager *SentinelRuntimeManager) reserveLocked(priority sentinelTaskPriority) (*sentinelRuntimeReservation, error) {
	if manager.closed || (!manager.config.Enabled && !manager.clearWhenIdle) {
		return nil, newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK runtime is disabled"))
	}
	observer := priority == sentinelPriorityObserverCollector
	if manager.canGrantLocked(observer) {
		manager.busy++
		if observer {
			manager.observerBusy++
		}
		manager.initialized = true
		return &sentinelRuntimeReservation{manager: manager, observer: observer, granted: true}, nil
	}
	if manager.queued >= manager.config.QueueSize {
		return nil, newSentinelRuntimeError("sentinel_sdk_busy", time.Second, errors.New("Sentinel SDK runtime queue is full"))
	}
	waiter := &sentinelRuntimeWaiter{ready: make(chan error, 1), priority: priority}
	manager.queues[priority] = append(manager.queues[priority], waiter)
	manager.queued++
	return &sentinelRuntimeReservation{manager: manager, observer: observer, waiter: waiter}, nil
}

func (reservation *sentinelRuntimeReservation) wait(ctx context.Context) (*sentinelRuntimeLease, error) {
	if reservation == nil || reservation.manager == nil {
		return nil, newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK runtime reservation is unavailable"))
	}
	if reservation.granted {
		return &sentinelRuntimeLease{manager: reservation.manager, observer: reservation.observer}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	waiter := reservation.waiter
	if waiter == nil {
		return nil, newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK runtime reservation is invalid"))
	}

	select {
	case err := <-waiter.ready:
		if err != nil {
			return nil, err
		}
		return &sentinelRuntimeLease{manager: reservation.manager, observer: reservation.observer}, nil
	case <-ctx.Done():
		reservation.manager.mu.Lock()
		removed := reservation.manager.removeWaiterLocked(waiter)
		workersToClose := reservation.manager.finishDrainLocked()
		reservation.manager.mu.Unlock()
		closeSentinelQJSWorkers(workersToClose)
		if !removed {
			err := <-waiter.ready
			if err == nil {
				reservation.manager.release(&sentinelRuntimeLease{manager: reservation.manager, observer: reservation.observer})
			}
		}
		return nil, ctx.Err()
	}
}

func (manager *SentinelRuntimeManager) acquire(ctx context.Context, priority sentinelTaskPriority) (*sentinelRuntimeLease, error) {
	reservation, err := manager.reserve(ctx, priority)
	if err != nil {
		return nil, err
	}
	return reservation.wait(ctx)
}

func (manager *SentinelRuntimeManager) release(lease *sentinelRuntimeLease) {
	if manager == nil || lease == nil {
		return
	}
	lease.workerMu.Lock()
	worker := lease.worker
	broken := lease.broken
	lease.worker = nil
	lease.workerMu.Unlock()
	if worker != nil && !worker.reusable() {
		broken = true
	}
	var workersToClose []*sentinelQJSWorker
	manager.mu.Lock()
	if manager.busy > 0 {
		manager.busy--
	}
	if lease.observer && manager.observerBusy > 0 {
		manager.observerBusy--
	}
	if worker != nil {
		if broken || manager.closed || (!manager.config.Enabled && !manager.clearWhenIdle) || manager.workerCount > manager.workerLimit {
			if manager.workerCount > 0 {
				manager.workerCount--
			}
			workersToClose = append(workersToClose, worker)
		} else {
			manager.idleWorkers = append(manager.idleWorkers, worker)
		}
	}
	if manager.clearWhenIdle {
		manager.grantQueuedLocked()
	}
	workersToClose = append(workersToClose, manager.finishDrainLocked()...)
	if manager.closed || (!manager.config.Enabled && !manager.clearWhenIdle) {
		manager.mu.Unlock()
		closeSentinelQJSWorkers(workersToClose)
		return
	}
	manager.grantQueuedLocked()
	manager.mu.Unlock()
	closeSentinelQJSWorkers(workersToClose)
}

func (manager *SentinelRuntimeManager) grantQueuedLocked() {
	for manager.busy < manager.workerLimit && manager.queued > 0 {
		waiter := manager.popGrantableWaiterLocked()
		if waiter == nil {
			return
		}
		manager.busy++
		if waiter.priority == sentinelPriorityObserverCollector {
			manager.observerBusy++
		}
		manager.initialized = true
		waiter.ready <- nil
	}
}

func (manager *SentinelRuntimeManager) popGrantableWaiterLocked() *sentinelRuntimeWaiter {
	for priority := sentinelTaskPriority(0); priority < sentinelPriorityCount; priority++ {
		queue := manager.queues[priority]
		if len(queue) == 0 {
			continue
		}
		if !manager.canGrantLocked(priority == sentinelPriorityObserverCollector) {
			continue
		}
		return manager.popWaiterLocked(priority)
	}
	return nil
}

func (manager *SentinelRuntimeManager) popWaiterLocked(priority sentinelTaskPriority) *sentinelRuntimeWaiter {
	queue := manager.queues[priority]
	if len(queue) == 0 {
		return nil
	}
	waiter := queue[0]
	copy(queue, queue[1:])
	queue[len(queue)-1] = nil
	manager.queues[priority] = queue[:len(queue)-1]
	manager.queued--
	return waiter
}

func (manager *SentinelRuntimeManager) canGrantLocked(observer bool) bool {
	if manager.busy >= manager.workerLimit {
		return false
	}
	if observer && manager.workerLimit > 1 && manager.observerBusy >= manager.workerLimit-1 {
		return false
	}
	return true
}

func (manager *SentinelRuntimeManager) removeWaiterLocked(target *sentinelRuntimeWaiter) bool {
	if target == nil {
		return false
	}
	queue := manager.queues[target.priority]
	for index, waiter := range queue {
		if waiter != target {
			continue
		}
		copy(queue[index:], queue[index+1:])
		queue[len(queue)-1] = nil
		manager.queues[target.priority] = queue[:len(queue)-1]
		manager.queued--
		return true
	}
	return false
}

func (manager *SentinelRuntimeManager) rejectQueuedLocked(err error) {
	for priority := sentinelTaskPriority(0); priority < sentinelPriorityCount; priority++ {
		for _, waiter := range manager.queues[priority] {
			waiter.ready <- err
		}
		manager.queues[priority] = nil
	}
	manager.queued = 0
}

func newSentinelRuntimeError(code string, retryAfter time.Duration, err error) *SentinelRuntimeError {
	return &SentinelRuntimeError{Code: code, RetryAfter: retryAfter, Err: err}
}

func (manager *SentinelRuntimeManager) generationActiveLocked(generation uint64) bool {
	return manager.cacheGeneration == generation && !manager.closed && (manager.config.Enabled || manager.clearWhenIdle)
}

func (manager *SentinelRuntimeManager) requireGeneration(generation uint64) error {
	manager.mu.Lock()
	active := manager.generationActiveLocked(generation)
	manager.mu.Unlock()
	if active {
		return nil
	}
	return newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK task belongs to a stale runtime generation"))
}

func (manager *SentinelRuntimeManager) recordError(generation uint64, code string) {
	manager.mu.Lock()
	if manager.generationActiveLocked(generation) {
		manager.lastError = code
	}
	manager.mu.Unlock()
}

func (manager *SentinelRuntimeManager) recordFallbackSuccess(generation uint64) {
	manager.mu.Lock()
	if manager.generationActiveLocked(generation) {
		manager.fallbackCount++
		manager.lastError = ""
	}
	manager.mu.Unlock()
}

func (manager *SentinelRuntimeManager) recordCircuit(generation uint64, hash string) {
	if hash == "" {
		return
	}
	manager.mu.Lock()
	if !manager.generationActiveLocked(generation) {
		manager.mu.Unlock()
		return
	}
	now := manager.now()
	for key, expiresAt := range manager.circuits {
		if !expiresAt.After(now) {
			delete(manager.circuits, key)
		}
	}
	if _, exists := manager.circuits[hash]; !exists && len(manager.circuits) >= sentinelSDKCircuitMax {
		oldestKey := ""
		var oldest time.Time
		for key, expiresAt := range manager.circuits {
			if oldestKey == "" || expiresAt.Before(oldest) {
				oldestKey = key
				oldest = expiresAt
			}
		}
		delete(manager.circuits, oldestKey)
	}
	manager.circuits[hash] = now.Add(sentinelSDKCircuitBreakerTTL)
	manager.lastError = "sentinel_sdk_execute_failed"
	manager.mu.Unlock()
}

func recordSentinelCircuitForError(manager *SentinelRuntimeManager, ctx context.Context, generation uint64, hash string, err error) {
	if manager == nil || err == nil {
		return
	}
	if ctx != nil && ctx.Err() != nil {
		return
	}
	if errors.Is(err, context.Canceled) {
		return
	}
	var runtimeErr *SentinelRuntimeError
	if errors.As(err, &runtimeErr) && runtimeErr.Code == "sentinel_sdk_busy" {
		return
	}
	manager.recordCircuit(generation, hash)
}

func (manager *SentinelRuntimeManager) circuitRetryAfter(hash string) (time.Duration, bool) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	expiresAt := manager.circuits[hash]
	if expiresAt.IsZero() {
		return 0, false
	}
	now := manager.now()
	if !expiresAt.After(now) {
		delete(manager.circuits, hash)
		return 0, false
	}
	return expiresAt.Sub(now), true
}

func (manager *SentinelRuntimeManager) circuitOpen(hash string) bool {
	_, open := manager.circuitRetryAfter(hash)
	return open
}

func (manager *SentinelRuntimeManager) sourceCircuitRetryAfter(key string) (time.Duration, bool) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	expiresAt := manager.sourceCircuits[key]
	if expiresAt.IsZero() {
		return 0, false
	}
	now := manager.now()
	if !expiresAt.After(now) {
		delete(manager.sourceCircuits, key)
		return 0, false
	}
	return expiresAt.Sub(now), true
}

func (manager *SentinelRuntimeManager) recordSourceFailure(generation uint64, key string, err error) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.cacheGeneration != generation || !manager.config.Enabled || manager.closed {
		return newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK source belongs to a stale runtime generation"))
	}
	now := manager.now()
	for candidate, expiresAt := range manager.sourceCircuits {
		if !expiresAt.After(now) {
			delete(manager.sourceCircuits, candidate)
		}
	}
	if _, exists := manager.sourceCircuits[key]; !exists && len(manager.sourceCircuits) >= sentinelSDKSourceCircuitMax {
		oldestKey := ""
		var oldest time.Time
		for candidate, expiresAt := range manager.sourceCircuits {
			if oldestKey == "" || expiresAt.Before(oldest) {
				oldestKey = candidate
				oldest = expiresAt
			}
		}
		delete(manager.sourceCircuits, oldestKey)
	}
	retryAfter := sentinelSDKCircuitBreakerTTL
	var retryable interface{ RetryAfter() *time.Duration }
	if errors.As(err, &retryable) {
		if value := retryable.RetryAfter(); value != nil && *value >= 0 {
			retryAfter = *value
		}
	}
	expiresAt := now.Add(retryAfter)
	if retryAfter > 0 {
		manager.sourceCircuits[key] = expiresAt
	} else {
		delete(manager.sourceCircuits, key)
	}
	manager.lastError = "sentinel_sdk_download_failed"
	return newSentinelRuntimeError("sentinel_sdk_unavailable", retryAfter, err)
}

func (manager *SentinelRuntimeManager) markPreferred(generation uint64, hash string, program SentinelProgramKind, signature string) {
	if hash == "" || signature == "" {
		return
	}
	manager.mu.Lock()
	if !manager.generationActiveLocked(generation) {
		manager.mu.Unlock()
		return
	}
	now := manager.now()
	for key, expiresAt := range manager.preferred {
		if !expiresAt.After(now) {
			delete(manager.preferred, key)
		}
	}
	key := sentinelPreferredKey{hash: hash, program: program, signature: signature}
	if _, exists := manager.preferred[key]; !exists && len(manager.preferred) >= sentinelSDKPreferredMax {
		var oldestKey sentinelPreferredKey
		var oldest time.Time
		oldestSet := false
		for candidate, expiresAt := range manager.preferred {
			if !oldestSet || expiresAt.Before(oldest) {
				oldestKey = candidate
				oldest = expiresAt
				oldestSet = true
			}
		}
		delete(manager.preferred, oldestKey)
	}
	manager.preferred[key] = now.Add(sentinelSDKPreferredTTL)
	for hint, entry := range manager.preferredHints {
		if entry.signature == signature {
			entry.candidate = true
			manager.preferredHints[hint] = entry
		}
	}
	manager.hasPreferred.Store(true)
	manager.mu.Unlock()
}

func sentinelPreferredHint(program SentinelProgramKind, dx, requirementsToken string) sentinelPreferredHintKey {
	return sentinelPreferredHintKey{
		program:            program,
		dxHash:             sentinelStringFingerprint(dx),
		requirementsHash:   sentinelStringFingerprint(requirementsToken),
		dxLength:           len(dx),
		requirementsLength: len(requirementsToken),
	}
}

func sentinelStringFingerprint(value string) uint64 {
	const (
		offset = uint64(14695981039346656037)
		prime  = uint64(1099511628211)
	)
	hash := offset
	for index := 0; index < len(value); index++ {
		hash ^= uint64(value[index])
		hash *= prime
	}
	return hash
}

func (manager *SentinelRuntimeManager) markPreferredForChallenge(generation uint64, hash string, program SentinelProgramKind, signature, dx, requirementsToken string) {
	if hash == "" || signature == "" || dx == "" || requirementsToken == "" {
		return
	}
	manager.markPreferred(generation, hash, program, signature)
	manager.cacheChallengeSignature(generation, program, signature, dx, requirementsToken, true)
}

func (manager *SentinelRuntimeManager) cacheChallengeSignature(generation uint64, program SentinelProgramKind, signature, dx, requirementsToken string, candidate bool) {
	if signature == "" || dx == "" || requirementsToken == "" {
		return
	}
	hint := sentinelPreferredHint(program, dx, requirementsToken)
	manager.mu.Lock()
	if !manager.generationActiveLocked(generation) {
		manager.mu.Unlock()
		return
	}
	now := manager.now()
	for key, entry := range manager.preferredHints {
		if !entry.expiresAt.After(now) {
			delete(manager.preferredHints, key)
		}
	}
	if _, exists := manager.preferredHints[hint]; !exists && len(manager.preferredHints) >= sentinelSDKPreferredMax {
		var oldestKey sentinelPreferredHintKey
		var oldest time.Time
		oldestSet := false
		for candidate, entry := range manager.preferredHints {
			if !oldestSet || entry.expiresAt.Before(oldest) {
				oldestKey = candidate
				oldest = entry.expiresAt
				oldestSet = true
			}
		}
		delete(manager.preferredHints, oldestKey)
	}
	manager.preferredHints[hint] = sentinelPreferredHintEntry{
		signature: signature,
		expiresAt: now.Add(sentinelSDKPreferredTTL),
		candidate: candidate,
	}
	manager.mu.Unlock()
}

func (manager *SentinelRuntimeManager) preferredChallengeSignature(program SentinelProgramKind, dx, requirementsToken string) (string, bool, bool) {
	if dx == "" || requirementsToken == "" {
		return "", false, false
	}
	hint := sentinelPreferredHint(program, dx, requirementsToken)
	manager.mu.Lock()
	defer manager.mu.Unlock()
	entry := manager.preferredHints[hint]
	if entry.expiresAt.After(manager.now()) {
		return entry.signature, entry.candidate, true
	}
	delete(manager.preferredHints, hint)
	return "", false, false
}

func (manager *SentinelRuntimeManager) isPreferred(hash string, program SentinelProgramKind, signature string) bool {
	if hash == "" || signature == "" {
		return false
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	now := manager.now()
	for candidate, expiresAt := range manager.preferred {
		if !expiresAt.After(now) {
			delete(manager.preferred, candidate)
		}
	}
	key := sentinelPreferredKey{hash: hash, program: program, signature: signature}
	expiresAt := manager.preferred[key]
	if expiresAt.After(now) {
		return true
	}
	delete(manager.preferred, key)
	if len(manager.preferred) == 0 {
		manager.hasPreferred.Store(false)
	}
	return false
}

func (manager *SentinelRuntimeManager) hasActivePreferred() bool {
	if manager == nil || !manager.hasPreferred.Load() {
		return false
	}
	manager.mu.Lock()
	now := manager.now()
	for key, expiresAt := range manager.preferred {
		if !expiresAt.After(now) {
			delete(manager.preferred, key)
		}
	}
	active := len(manager.preferred) > 0
	manager.hasPreferred.Store(active)
	manager.mu.Unlock()
	return active
}

func (manager *SentinelRuntimeManager) latestSourceForURL(sourceURL string, expectedHashes []string) *sentinelSourceCacheEntry {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	for element := manager.sourceLRU.Front(); element != nil; element = element.Next() {
		entry := element.Value.(*sentinelSourceCacheEntry)
		if entry.url != sourceURL || !sentinelHashAllowed(entry.hash, expectedHashes) {
			continue
		}
		if manager.now().Sub(entry.fetchedAt) > sentinelSDKSourceTTL {
			continue
		}
		manager.sourceLRU.MoveToFront(element)
		return entry
	}
	return nil
}

func (manager *SentinelRuntimeManager) latestSourceForGeneration(sourceURL string, expectedHashes []string, generation uint64) (*sentinelSourceCacheEntry, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if !manager.generationActiveLocked(generation) {
		return nil, newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK source belongs to a stale runtime generation"))
	}
	for element := manager.sourceLRU.Front(); element != nil; element = element.Next() {
		entry := element.Value.(*sentinelSourceCacheEntry)
		if entry.generation != generation || entry.url != sourceURL || !sentinelHashAllowed(entry.hash, expectedHashes) {
			continue
		}
		if manager.now().Sub(entry.fetchedAt) > sentinelSDKSourceTTL {
			continue
		}
		manager.sourceLRU.MoveToFront(element)
		return entry, nil
	}
	return nil, nil
}

func (manager *SentinelRuntimeManager) cachedSourceForRequest(request SentinelSDKRequest) (*sentinelSourceCacheEntry, error) {
	sourceURL, _, err := resolveSentinelSDKRequestSource(request)
	if err != nil {
		return nil, err
	}
	expectedHashes, err := expectedSentinelHashes(request)
	if err != nil {
		return nil, err
	}
	return manager.latestSourceForURL(sourceURL, expectedHashes), nil
}

func (manager *SentinelRuntimeManager) sourceForTask(ctx context.Context, request SentinelSDKRequest) (*sentinelSourceCacheEntry, error) {
	return manager.source(ctx, request)
}

func (manager *SentinelRuntimeManager) source(ctx context.Context, request SentinelSDKRequest) (*sentinelSourceCacheEntry, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sourceURL, version, err := resolveSentinelSDKRequestSource(request)
	if err != nil {
		return nil, err
	}
	expectedHashes, err := expectedSentinelHashes(request)
	if err != nil {
		return nil, err
	}
	if cached := manager.latestSourceForURL(sourceURL, expectedHashes); cached != nil {
		return cached, nil
	}
	if request.Fetcher == nil {
		return nil, errors.New("Sentinel SDK fetcher is unavailable")
	}
	transportKey := strings.TrimSpace(request.TransportKey)
	if transportKey == "" {
		transportKey = "default"
	}
	circuitKey := sourceURL + "\x00" + strings.Join(expectedHashes, ",") + "\x00" + transportKey
	if retryAfter, open := manager.sourceCircuitRetryAfter(circuitKey); open {
		return nil, newSentinelRuntimeError("sentinel_sdk_unavailable", retryAfter, errors.New("Sentinel SDK source circuit breaker is open"))
	}
	manager.mu.Lock()
	if (!manager.config.Enabled && !manager.clearWhenIdle) || manager.closed || manager.sourceContext == nil {
		manager.mu.Unlock()
		return nil, newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK runtime is unavailable"))
	}
	generation := manager.cacheGeneration
	sourceContext := manager.sourceContext
	manager.mu.Unlock()
	flightKey := circuitKey + "\x00" + strconv.FormatUint(generation, 10)
	flight, leader, err := manager.beginSourceFlight(flightKey, sourceContext)
	if err != nil {
		return nil, err
	}
	defer manager.releaseSourceWaiter(flight)
	if leader {
		go manager.runSourceFlight(
			flightKey,
			flight,
			generation,
			request,
			sourceURL,
			version,
			expectedHashes,
			circuitKey,
		)
	}
	select {
	case <-flight.done:
		err = flight.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		var runtimeErr *SentinelRuntimeError
		if !errors.As(err, &runtimeErr) {
			manager.recordError(generation, "sentinel_sdk_download_failed")
		}
		return nil, err
	}
	if flight.entry == nil {
		return nil, newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK source flight returned no source"))
	}
	if flight.entry.generation != generation {
		return nil, newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK source flight returned a stale generation"))
	}
	if err = manager.requireGeneration(generation); err != nil {
		return nil, err
	}
	return flight.entry, nil
}

func (manager *SentinelRuntimeManager) beginSourceFlight(key string, sourceContext context.Context) (*sentinelSourceFlight, bool, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.closed || (!manager.config.Enabled && !manager.clearWhenIdle) {
		return nil, false, newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK runtime is disabled"))
	}
	waiterLimit := manager.workerLimit + manager.config.QueueSize
	if waiterLimit < 1 {
		waiterLimit = 1
	}
	if waiterLimit > sentinelSDKSourceWaiterMax {
		waiterLimit = sentinelSDKSourceWaiterMax
	}
	if manager.sourceWaiters >= waiterLimit {
		return nil, false, newSentinelRuntimeError("sentinel_sdk_busy", time.Second, errors.New("Sentinel SDK source waiters are full"))
	}
	if flight := manager.sourceFlights[key]; flight != nil {
		flight.waiters++
		manager.sourceWaiters++
		return flight, false, nil
	}
	fetchLimit := manager.workerLimit + manager.config.QueueSize
	if fetchLimit < 1 {
		fetchLimit = 1
	}
	if fetchLimit > sentinelSDKSourceFetchMax {
		fetchLimit = sentinelSDKSourceFetchMax
	}
	if manager.sourcePending >= fetchLimit {
		return nil, false, newSentinelRuntimeError("sentinel_sdk_busy", time.Second, errors.New("Sentinel SDK source queue is full"))
	}
	flightContext, cancelFlight := context.WithCancel(sourceContext)
	flight := &sentinelSourceFlight{
		key:     key,
		done:    make(chan struct{}),
		ctx:     flightContext,
		cancel:  cancelFlight,
		waiters: 1,
	}
	manager.sourceFlights[key] = flight
	manager.sourcePending++
	manager.sourceWaiters++
	return flight, true, nil
}

func (manager *SentinelRuntimeManager) releaseSourceWaiter(flight *sentinelSourceFlight) {
	var cancelFlight context.CancelFunc
	manager.mu.Lock()
	if manager.sourceWaiters > 0 {
		manager.sourceWaiters--
	}
	if flight != nil && flight.waiters > 0 {
		flight.waiters--
		if flight.waiters == 0 && !flight.completed {
			if manager.sourceFlights[flight.key] == flight {
				delete(manager.sourceFlights, flight.key)
			}
			cancelFlight = flight.cancel
		}
	}
	workersToClose := manager.finishDrainLocked()
	manager.mu.Unlock()
	if cancelFlight != nil {
		cancelFlight()
	}
	closeSentinelQJSWorkers(workersToClose)
}

func (manager *SentinelRuntimeManager) completeSourceFlight(key string, flight *sentinelSourceFlight, entry *sentinelSourceCacheEntry, err error) {
	var cancelFlight context.CancelFunc
	manager.mu.Lock()
	flight.completed = true
	flight.entry = entry
	flight.err = err
	cancelFlight = flight.cancel
	flight.cancel = nil
	if manager.sourceFlights[key] == flight {
		delete(manager.sourceFlights, key)
	}
	if manager.sourcePending > 0 {
		manager.sourcePending--
	}
	close(flight.done)
	workersToClose := manager.finishDrainLocked()
	manager.mu.Unlock()
	if cancelFlight != nil {
		cancelFlight()
	}
	closeSentinelQJSWorkers(workersToClose)
}

func (manager *SentinelRuntimeManager) runSourceFlight(
	flightKey string,
	flight *sentinelSourceFlight,
	generation uint64,
	request SentinelSDKRequest,
	sourceURL string,
	version string,
	expectedHashes []string,
	circuitKey string,
) {
	entry, err := manager.fetchSource(
		flight.ctx,
		generation,
		request,
		sourceURL,
		version,
		expectedHashes,
		circuitKey,
	)
	manager.completeSourceFlight(flightKey, flight, entry, err)
}

func (manager *SentinelRuntimeManager) fetchSource(
	fetchCtx context.Context,
	generation uint64,
	request SentinelSDKRequest,
	sourceURL string,
	version string,
	expectedHashes []string,
	circuitKey string,
) (*sentinelSourceCacheEntry, error) {
	if cached, cacheErr := manager.latestSourceForGeneration(sourceURL, expectedHashes, generation); cacheErr != nil {
		return nil, cacheErr
	} else if cached != nil {
		return cached, nil
	}
	if retryAfter, open := manager.sourceCircuitRetryAfter(circuitKey); open {
		return nil, newSentinelRuntimeError("sentinel_sdk_unavailable", retryAfter, errors.New("Sentinel SDK source circuit breaker is open"))
	}
	// The fetch is shared by all waiters and has no total deadline. Its context
	// is canceled only when the runtime generation changes or no waiters remain.
	fail := func(fetchErr error) (*sentinelSourceCacheEntry, error) {
		return nil, manager.recordSourceFailure(generation, circuitKey, fetchErr)
	}
	source, contentType, finalURL, fetchErr := request.Fetcher(fetchCtx, sourceURL, sentinelSDKMaxSourceBytes)
	if fetchErr != nil {
		if fetchCtx.Err() != nil && (errors.Is(fetchErr, context.Canceled) || errors.Is(fetchErr, context.DeadlineExceeded)) {
			return nil, fetchCtx.Err()
		}
		return fail(fmt.Errorf("fetch Sentinel SDK: %w", fetchErr))
	}
	if fetchErr = fetchCtx.Err(); fetchErr != nil {
		return nil, fetchErr
	}
	parsedFinalURL, parseFinalErr := url.Parse(strings.TrimSpace(finalURL))
	if parseFinalErr != nil || validateSentinelSDKURL(parsedFinalURL) != nil {
		return fail(errors.New("Sentinel SDK final URL is not trusted"))
	}
	parsedSourceURL, parseSourceErr := url.Parse(sourceURL)
	if parseSourceErr != nil || sentinelSDKVersionFromPath(parsedSourceURL.EscapedPath()) != sentinelSDKVersionFromPath(parsedFinalURL.EscapedPath()) {
		return fail(errors.New("Sentinel SDK redirect changed the requested version"))
	}
	if len(source) == 0 || len(source) > sentinelSDKMaxSourceBytes {
		return fail(errors.New("Sentinel SDK source size is invalid"))
	}
	if !validSentinelSDKContentType(contentType) {
		return fail(errors.New("Sentinel SDK content type is invalid"))
	}
	digest := sha256.Sum256(source)
	hash := hex.EncodeToString(digest[:])
	if !sentinelHashAllowed(hash, expectedHashes) {
		return fail(errors.New("Sentinel SDK SHA-256 mismatch"))
	}
	if _, known := manager.resolveSDKAdapter(hash, source); !known {
		return fail(errors.New("Sentinel SDK source has no compatible export adapter"))
	}
	entry := &sentinelSourceCacheEntry{
		key:        sourceURL + "\x00" + hash,
		url:        sourceURL,
		version:    version,
		hash:       hash,
		source:     append([]byte(nil), source...),
		fetchedAt:  manager.now(),
		generation: generation,
	}
	manager.mu.Lock()
	if !manager.generationActiveLocked(generation) {
		manager.mu.Unlock()
		return nil, newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK source belongs to a stale runtime generation"))
	}
	manager.putSourceLocked(entry)
	delete(manager.sourceCircuits, circuitKey)
	manager.latestVersion = version
	manager.latestHash = hash
	manager.lastError = ""
	manager.mu.Unlock()
	return entry, nil
}

func (manager *SentinelRuntimeManager) bytecode(ctx context.Context, source *sentinelSourceCacheEntry, priority sentinelTaskPriority) ([]byte, error) {
	return manager.bytecodeWithLease(ctx, source, priority, nil)
}

func (manager *SentinelRuntimeManager) bytecodeWithLease(
	ctx context.Context,
	source *sentinelSourceCacheEntry,
	priority sentinelTaskPriority,
	reservedLease *sentinelRuntimeLease,
) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if source == nil {
		return nil, errors.New("Sentinel SDK source is unavailable")
	}
	if err := manager.requireGeneration(source.generation); err != nil {
		return nil, err
	}
	adapter, known := manager.resolveSDKAdapter(source.hash, source.source)
	if !known {
		return nil, newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK source has no compatible export adapter"))
	}
	key := source.hash + ":" + adapter.version
	return manager.bytecodeFlight(ctx, source, priority, reservedLease, key, adapter)
}

func (manager *SentinelRuntimeManager) bytecodeFlight(ctx context.Context, source *sentinelSourceCacheEntry, priority sentinelTaskPriority, lease *sentinelRuntimeLease, key string, adapter sentinelSDKAdapter) ([]byte, error) {
	for {
		manager.mu.Lock()
		if !manager.generationActiveLocked(source.generation) {
			manager.mu.Unlock()
			return nil, newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK source belongs to a stale runtime generation"))
		}
		if element := manager.bytecodeCache[key]; element != nil {
			manager.bytecodeLRU.MoveToFront(element)
			entry := element.Value.(*sentinelBytecodeCacheEntry)
			manager.mu.Unlock()
			return entry.bytecode, nil
		}
		generation := source.generation
		flightKey := key + "\x00" + strconv.FormatUint(generation, 10)
		// A speculative precompile must never make a live request wait behind its
		// lower-priority flight. Both paths still publish to the shared cache.
		if lease == nil && priority == sentinelPriorityPrecompile {
			flightKey += "\x00precompile"
		}
		flight := manager.bytecodeFlights[flightKey]
		leader := flight == nil
		if leader {
			flight = &sentinelBytecodeFlight{done: make(chan struct{})}
			manager.bytecodeFlights[flightKey] = flight
		} else if manager.bytecodeWaiters >= manager.config.QueueSize {
			manager.mu.Unlock()
			return nil, newSentinelRuntimeError("sentinel_sdk_busy", time.Second, errors.New("Sentinel SDK bytecode waiters are full"))
		} else {
			manager.bytecodeWaiters++
		}
		manager.mu.Unlock()

		if leader {
			if lease != nil {
				bytecode, compileErr := manager.compileBytecodeWithLease(ctx, source, priority, lease, key, generation, adapter)
				manager.completeBytecodeFlight(flightKey, flight, bytecode, compileErr, ctx.Err() != nil)
			} else {
				go func() {
					bytecode, compileErr := manager.compileBytecodeWithLease(context.Background(), source, priority, nil, key, generation, adapter)
					manager.completeBytecodeFlight(flightKey, flight, bytecode, compileErr, false)
				}()
			}
		}
		select {
		case <-flight.done:
			if !leader {
				manager.releaseBytecodeWaiter()
			}
			if flight.ownerCanceled && ctx.Err() == nil {
				continue
			}
			if flight.err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				var runtimeErr *SentinelRuntimeError
				if errors.As(flight.err, &runtimeErr) {
					return nil, flight.err
				}
				manager.recordError(source.generation, "sentinel_sdk_compile_failed")
				recordSentinelCircuitForError(manager, ctx, source.generation, source.hash, flight.err)
				return nil, flight.err
			}
			return flight.bytecode, nil
		case <-ctx.Done():
			if !leader {
				manager.releaseBytecodeWaiter()
			}
			return nil, ctx.Err()
		}
	}
}

func (manager *SentinelRuntimeManager) releaseBytecodeWaiter() {
	manager.mu.Lock()
	if manager.bytecodeWaiters > 0 {
		manager.bytecodeWaiters--
	}
	workersToClose := manager.finishDrainLocked()
	manager.mu.Unlock()
	closeSentinelQJSWorkers(workersToClose)
}

func (manager *SentinelRuntimeManager) compileBytecodeWithLease(ctx context.Context, source *sentinelSourceCacheEntry, priority sentinelTaskPriority, reservedLease *sentinelRuntimeLease, key string, generation uint64, adapter sentinelSDKAdapter) ([]byte, error) {
	compileParent := context.Background()
	if reservedLease != nil {
		compileParent = ctx
	}
	compileCtx, cancel := context.WithTimeout(compileParent, sentinelSDKCompileTimeout)
	defer cancel()
	compileLease := reservedLease
	if compileLease == nil {
		var acquireErr error
		compileLease, acquireErr = manager.acquire(compileCtx, priority)
		if acquireErr != nil {
			if errors.Is(acquireErr, context.DeadlineExceeded) {
				return nil, newSentinelRuntimeError("sentinel_sdk_busy", time.Second, errors.New("Sentinel SDK compile queue timed out"))
			}
			return nil, acquireErr
		}
		defer compileLease.release()
	}
	manager.mu.Lock()
	if element := manager.bytecodeCache[key]; element != nil {
		manager.bytecodeLRU.MoveToFront(element)
		cached := element.Value.(*sentinelBytecodeCacheEntry).bytecode
		manager.mu.Unlock()
		return cached, nil
	}
	manager.mu.Unlock()
	patched, patchErr := patchSentinelSDK(source.source, adapter)
	if patchErr != nil {
		return nil, patchErr
	}
	script := "(function(){\n" + strings.Replace(sentinelSDKRuntimeSource, "/*__SENTINEL_SDK__*/", string(patched), 1) + "\n})();"
	worker, runtimeErr := compileLease.runtimeWorker(compileCtx)
	if runtimeErr != nil {
		return nil, runtimeErr
	}
	var compiled []byte
	compileErr := worker.run(compileCtx, source.generation, func(runtimeQJS *qjs.Runtime) error {
		var errCompile error
		compiled, errCompile = runtimeQJS.Compile("sentinel-sdk-runtime.js", qjs.Code(script))
		return errCompile
	})
	if compileErr != nil {
		return nil, fmt.Errorf("compile Sentinel SDK: %w", compileErr)
	}
	entry := &sentinelBytecodeCacheEntry{key: key, hash: source.hash, bytecode: append([]byte(nil), compiled...)}
	manager.mu.Lock()
	if !manager.generationActiveLocked(generation) {
		manager.mu.Unlock()
		return nil, newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK bytecode belongs to a stale runtime generation"))
	}
	manager.putBytecodeLocked(entry)
	manager.mu.Unlock()
	return entry.bytecode, nil
}

func (manager *SentinelRuntimeManager) completeBytecodeFlight(key string, flight *sentinelBytecodeFlight, bytecode []byte, err error, ownerCanceled bool) {
	manager.mu.Lock()
	flight.bytecode = bytecode
	flight.err = err
	flight.ownerCanceled = ownerCanceled
	delete(manager.bytecodeFlights, key)
	close(flight.done)
	workersToClose := manager.finishDrainLocked()
	manager.mu.Unlock()
	closeSentinelQJSWorkers(workersToClose)
}

func (manager *SentinelRuntimeManager) putSourceLocked(entry *sentinelSourceCacheEntry) {
	if existing := manager.sourceCache[entry.key]; existing != nil {
		manager.sourceLRU.Remove(existing)
	}
	element := manager.sourceLRU.PushFront(entry)
	manager.sourceCache[entry.key] = element
	manager.trimCachesLocked()
}

func (manager *SentinelRuntimeManager) putBytecodeLocked(entry *sentinelBytecodeCacheEntry) {
	if existing := manager.bytecodeCache[entry.key]; existing != nil {
		manager.bytecodeLRU.Remove(existing)
	}
	element := manager.bytecodeLRU.PushFront(entry)
	manager.bytecodeCache[entry.key] = element
	manager.trimCachesLocked()
}

func (manager *SentinelRuntimeManager) trimCachesLocked() {
	limit := manager.config.CacheVersions
	if limit < 1 {
		limit = 1
	}
	for manager.sourceLRU.Len() > limit {
		element := manager.sourceLRU.Back()
		entry := element.Value.(*sentinelSourceCacheEntry)
		delete(manager.sourceCache, entry.key)
		manager.sourceLRU.Remove(element)
	}
	for manager.bytecodeLRU.Len() > limit {
		element := manager.bytecodeLRU.Back()
		entry := element.Value.(*sentinelBytecodeCacheEntry)
		delete(manager.bytecodeCache, entry.key)
		manager.bytecodeLRU.Remove(element)
	}
}

func (manager *SentinelRuntimeManager) clearCachesLocked() {
	if manager.cancelSources != nil {
		manager.cancelSources()
	}
	manager.sourceContext = nil
	manager.cancelSources = nil
	if !manager.closed {
		manager.sourceContext, manager.cancelSources = context.WithCancel(context.Background())
	}
	manager.cacheGeneration++
	manager.sourceCache = make(map[string]*list.Element)
	manager.sourceLRU.Init()
	manager.bytecodeCache = make(map[string]*list.Element)
	manager.bytecodeLRU.Init()
	manager.preferred = make(map[sentinelPreferredKey]time.Time)
	manager.preferredHints = make(map[sentinelPreferredHintKey]sentinelPreferredHintEntry)
	manager.circuits = make(map[string]time.Time)
	manager.sourceCircuits = make(map[string]time.Time)
	manager.latestVersion = ""
	manager.latestHash = ""
	manager.lastError = ""
	manager.hasPreferred.Store(false)
}

func (manager *SentinelRuntimeManager) trimIdleWorkersLocked() []*sentinelQJSWorker {
	if manager == nil {
		return nil
	}
	var workers []*sentinelQJSWorker
	for manager.workerCount > manager.workerLimit && len(manager.idleWorkers) > 0 {
		index := len(manager.idleWorkers) - 1
		workers = append(workers, manager.idleWorkers[index])
		manager.idleWorkers[index] = nil
		manager.idleWorkers = manager.idleWorkers[:index]
		manager.workerCount--
	}
	return workers
}

func (manager *SentinelRuntimeManager) drainIdleWorkersLocked() []*sentinelQJSWorker {
	if manager == nil || len(manager.idleWorkers) == 0 {
		return nil
	}
	workers := manager.idleWorkers
	manager.idleWorkers = nil
	manager.workerCount -= len(workers)
	if manager.workerCount < 0 {
		manager.workerCount = 0
	}
	return workers
}

func closeSentinelQJSWorkers(workers []*sentinelQJSWorker) {
	for _, worker := range workers {
		if worker != nil {
			worker.close()
		}
	}
}

type sentinelQJSWorker struct {
	manager *SentinelRuntimeManager
	mu      sync.Mutex
	closed  bool
}

type sentinelQJSRuntime struct {
	manager              *SentinelRuntimeManager
	runtime              *qjs.Runtime
	cancel               context.CancelFunc
	emptyDir             string
	generation           uint64
	mu                   sync.Mutex
	closed               bool
	contextClosedRuntime atomic.Bool
}

func (lease *sentinelRuntimeLease) runtimeWorker(ctx context.Context) (*sentinelQJSWorker, error) {
	if lease == nil || lease.manager == nil {
		return nil, errors.New("Sentinel SDK runtime lease is unavailable")
	}
	lease.workerMu.Lock()
	defer lease.workerMu.Unlock()
	if lease.worker != nil {
		return lease.worker, nil
	}
	manager := lease.manager
	manager.mu.Lock()
	if count := len(manager.idleWorkers); count > 0 {
		worker := manager.idleWorkers[count-1]
		manager.idleWorkers[count-1] = nil
		manager.idleWorkers = manager.idleWorkers[:count-1]
		manager.mu.Unlock()
		lease.worker = worker
		return worker, nil
	}
	manager.workerCount++
	manager.mu.Unlock()
	worker, err := manager.newQJSWorker(ctx)
	if err != nil {
		manager.mu.Lock()
		if manager.workerCount > 0 {
			manager.workerCount--
		}
		manager.mu.Unlock()
		return nil, err
	}
	lease.worker = worker
	return worker, nil
}

func (manager *SentinelRuntimeManager) newQJSWorker(ctx context.Context) (*sentinelQJSWorker, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &sentinelQJSWorker{manager: manager}, nil
}

func (worker *sentinelQJSWorker) newRuntime(ctx context.Context, generation uint64) (*sentinelQJSRuntime, error) {
	if worker == nil || worker.manager == nil {
		return nil, errors.New("Sentinel SDK worker is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	worker.mu.Lock()
	closed := worker.closed
	worker.mu.Unlock()
	if closed {
		return nil, errors.New("Sentinel SDK worker is closed")
	}
	manager := worker.manager
	active, err := manager.reserveActiveQJS()
	if err != nil {
		return nil, err
	}
	for {
		previous := manager.maxActiveQJS.Load()
		if active <= previous || manager.maxActiveQJS.CompareAndSwap(previous, active) {
			break
		}
	}
	// qjs.New reuses fastschema/qjs's process-wide compiled Wazero module while
	// creating a fresh QuickJS heap for each task. Reusing the heap itself would
	// leak SDK globals and pending promises across credentials.
	emptyDir, err := os.MkdirTemp("", "cliproxy-sentinel-qjs-")
	if err != nil {
		manager.activeQJS.Add(-1)
		return nil, fmt.Errorf("create Sentinel SDK runtime directory: %w", err)
	}
	runtimeContext, cancelRuntime := context.WithCancel(context.WithoutCancel(ctx))
	createRuntime := manager.createRuntime
	if createRuntime == nil {
		createRuntime = func(option qjs.Option) (*qjs.Runtime, error) { return qjs.New(option) }
	}
	runtimeQJS, err := createRuntime(qjs.Option{
		CWD:                emptyDir,
		Context:            runtimeContext,
		CloseOnContextDone: true,
		MemoryLimit:        sentinelSDKMemoryLimit,
		MaxStackSize:       sentinelSDKStackLimit,
		Stdout:             io.Discard,
		Stderr:             io.Discard,
	})
	if err != nil {
		cancelRuntime()
		_ = os.RemoveAll(emptyDir)
		worker.mu.Lock()
		worker.closed = true
		worker.mu.Unlock()
		manager.activeQJS.Add(-1)
		manager.recordError(generation, "sentinel_sdk_runtime_create_failed")
		return nil, fmt.Errorf("create Sentinel SDK runtime: %w", err)
	}
	isolatedRuntime := &sentinelQJSRuntime{
		manager:    manager,
		runtime:    runtimeQJS,
		cancel:     cancelRuntime,
		emptyDir:   emptyDir,
		generation: generation,
	}
	if err = ctx.Err(); err != nil {
		if !isolatedRuntime.close() {
			worker.mu.Lock()
			worker.closed = true
			worker.mu.Unlock()
		}
		return nil, err
	}
	return isolatedRuntime, nil
}

func (worker *sentinelQJSWorker) run(ctx context.Context, generation uint64, task func(*qjs.Runtime) error) (err error) {
	if worker == nil || task == nil {
		return errors.New("Sentinel SDK worker is unavailable")
	}
	runtimeQJS, err := worker.newRuntime(ctx, generation)
	if err != nil {
		return err
	}
	defer func() {
		if !runtimeQJS.close() {
			worker.mu.Lock()
			worker.closed = true
			worker.mu.Unlock()
			if err == nil {
				err = errors.New("Sentinel SDK runtime cleanup failed")
			}
		}
	}()
	return runtimeQJS.run(ctx, task)
}

func (runtimeQJS *sentinelQJSRuntime) run(ctx context.Context, task func(*qjs.Runtime) error) (err error) {
	if runtimeQJS == nil || task == nil {
		return errors.New("Sentinel SDK runtime is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	runtimeQJS.mu.Lock()
	defer runtimeQJS.mu.Unlock()
	if runtimeQJS.closed || runtimeQJS.runtime == nil {
		return errors.New("Sentinel SDK runtime is closed")
	}
	// Wazero reads this stable lifetime context during each function call.
	// Canceling it interrupts an in-flight QuickJS loop and permanently retires
	// the isolated runtime, while successful calls can share Observer state.
	cancellationDone := make(chan struct{})
	stopCancellation := context.AfterFunc(ctx, func() {
		runtimeQJS.cancel()
		close(cancellationDone)
	})
	defer func() {
		cancellationStarted := !stopCancellation()
		if cancellationStarted {
			<-cancellationDone
		}
		if recovered := recover(); recovered != nil {
			recoveredErr := qjs.AnyToError(recovered)
			if sentinelQJSRuntimeClosedByContext(recoveredErr) {
				runtimeQJS.contextClosedRuntime.Store(true)
			}
			err = fmt.Errorf("Sentinel SDK runtime panic: %w", recoveredErr)
		}
		if cancellationStarted && !runtimeQJS.contextClosedRuntime.Load() && sentinelQJSRuntimeObserveCancellation(runtimeQJS.runtime) {
			runtimeQJS.contextClosedRuntime.Store(true)
		}
		if ctx.Err() != nil && err == nil {
			err = ctx.Err()
		}
	}()
	return task(runtimeQJS.runtime)
}

func sentinelQJSRuntimeObserveCancellation(runtimeQJS *qjs.Runtime) (closed bool) {
	if runtimeQJS == nil {
		return false
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			closed = sentinelQJSRuntimeClosedByContext(qjs.AnyToError(recovered))
		}
	}()
	// The callback may cancel the lifetime context after the task's final WASM
	// call. A harmless call makes Wazero observe that cancellation and close the
	// module, so cleanup can release the hard runtime slot with proof of closure.
	runtimeQJS.Call("QJS_GetContext", runtimeQJS.Raw())
	return false
}

func sentinelQJSRuntimeClosedByContext(err error) bool {
	var exitErr *wazerosys.ExitError
	return errors.As(err, &exitErr) && (errors.Is(exitErr, context.Canceled) || errors.Is(exitErr, context.DeadlineExceeded))
}

func (worker *sentinelQJSWorker) close() bool {
	if worker == nil {
		return true
	}
	worker.mu.Lock()
	if worker.closed {
		worker.mu.Unlock()
		return true
	}
	worker.closed = true
	worker.mu.Unlock()
	return true
}

func (worker *sentinelQJSWorker) reusable() bool {
	if worker == nil {
		return false
	}
	worker.mu.Lock()
	defer worker.mu.Unlock()
	return !worker.closed
}

func (manager *SentinelRuntimeManager) reserveActiveQJS() (int64, error) {
	manager.mu.Lock()
	limit := manager.workerLimit
	if manager.busy > limit {
		limit = manager.busy
	}
	manager.mu.Unlock()
	for {
		active := manager.activeQJS.Load()
		if active >= int64(limit) {
			return 0, newSentinelRuntimeError("sentinel_sdk_busy", time.Second, errors.New("Sentinel SDK runtime capacity is full"))
		}
		if manager.activeQJS.CompareAndSwap(active, active+1) {
			return active + 1, nil
		}
	}
}

func (runtimeQJS *sentinelQJSRuntime) close() bool {
	if runtimeQJS == nil {
		return true
	}
	runtimeQJS.mu.Lock()
	if runtimeQJS.closed {
		runtimeQJS.mu.Unlock()
		return true
	}
	runtimeQJS.closed = true
	qjsRuntime := runtimeQJS.runtime
	cancelRuntime := runtimeQJS.cancel
	runtimeQJS.runtime = nil
	runtimeQJS.cancel = nil
	runtimeQJS.mu.Unlock()
	closeRuntime := safeCloseSentinelQJS
	if runtimeQJS.manager != nil && runtimeQJS.manager.closeRuntime != nil {
		closeRuntime = runtimeQJS.manager.closeRuntime
	}
	ok := closeRuntime(qjsRuntime)
	if cancelRuntime != nil {
		cancelRuntime()
	}
	_ = os.RemoveAll(runtimeQJS.emptyDir)
	// WithCloseOnContextDone closes the Wazero module before an interrupted
	// operation returns. A canceled context alone is insufficient: the runtime
	// may have been created without ever entering an operation.
	released := ok || runtimeQJS.contextClosedRuntime.Load()
	if released && runtimeQJS.manager != nil {
		runtimeQJS.manager.activeQJS.Add(-1)
	}
	if !released && runtimeQJS.manager != nil {
		runtimeQJS.manager.recordError(runtimeQJS.generation, "sentinel_sdk_runtime_close_failed")
		runtimeQJS.manager.reclaimFailedQJS(qjsRuntime, closeRuntime, runtimeQJS.generation)
	}
	return released
}

func (manager *SentinelRuntimeManager) reclaimFailedQJS(runtimeQJS *qjs.Runtime, closeRuntime func(*qjs.Runtime) bool, generation uint64) {
	if manager == nil {
		return
	}
	if closeRuntime == nil {
		closeRuntime = safeCloseSentinelQJS
	}
	manager.reclaimMu.Lock()
	if manager.reclaimClosed {
		manager.reclaimMu.Unlock()
		manager.reclaimQJSUntilClosed(runtimeQJS, closeRuntime, generation)
		return
	}
	manager.reclaimWG.Add(1)
	manager.reclaimMu.Unlock()
	go func() {
		defer manager.reclaimWG.Done()
		manager.reclaimQJSUntilClosed(runtimeQJS, closeRuntime, generation)
	}()
}

func (manager *SentinelRuntimeManager) reclaimQJSUntilClosed(runtimeQJS *qjs.Runtime, closeRuntime func(*qjs.Runtime) bool, generation uint64) {
	delay := sentinelSDKReclaimMinDelay
	for {
		time.Sleep(delay)
		if closeRuntime(runtimeQJS) || sentinelQJSRuntimeObserveCancellation(runtimeQJS) {
			manager.activeQJS.Add(-1)
			return
		}
		manager.recordError(generation, "sentinel_sdk_runtime_close_failed")
		if delay < sentinelSDKReclaimMaxDelay {
			delay *= 2
			if delay > sentinelSDKReclaimMaxDelay {
				delay = sentinelSDKReclaimMaxDelay
			}
		}
	}
}

func (manager *SentinelRuntimeManager) stopQJSReclaimers() {
	if manager == nil {
		return
	}
	manager.reclaimMu.Lock()
	if !manager.reclaimClosed {
		manager.reclaimClosed = true
	}
	manager.reclaimMu.Unlock()
	manager.reclaimWG.Wait()
}

func safeCloseSentinelQJS(runtimeQJS *qjs.Runtime) (ok bool) {
	if runtimeQJS == nil {
		return true
	}
	ok = true
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	runtimeQJS.Close()
	return ok
}

func validSentinelSDKContentType(value string) bool {
	mediaType, _, err := mime.ParseMediaType(strings.TrimSpace(value))
	if err != nil {
		return false
	}
	switch strings.ToLower(mediaType) {
	case "application/javascript", "text/javascript", "application/x-javascript":
		return true
	default:
		return false
	}
}

func normalizeSentinelHash(value string) string {
	value = strings.TrimSpace(value)
	if len(value) >= len("sha256-") && strings.EqualFold(value[:len("sha256-")], "sha256-") {
		value = value[len("sha256-"):]
	}
	if decoded, err := base64.StdEncoding.DecodeString(value); err == nil && len(decoded) == sha256.Size {
		return hex.EncodeToString(decoded)
	}
	value = strings.ToLower(value)
	if len(value) != sha256.Size*2 {
		return ""
	}
	if _, err := hex.DecodeString(value); err != nil {
		return ""
	}
	return value
}

func normalizeSentinelHashes(value string) ([]string, error) {
	fields := strings.Fields(value)
	if len(fields) == 0 {
		return nil, nil
	}
	hashes := make([]string, 0, len(fields))
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		hash := normalizeSentinelHash(field)
		if hash == "" {
			return nil, errors.New("Sentinel SDK SHA-256 is invalid")
		}
		if _, exists := seen[hash]; exists {
			continue
		}
		seen[hash] = struct{}{}
		hashes = append(hashes, hash)
	}
	sort.Strings(hashes)
	return hashes, nil
}

func expectedSentinelHashes(request SentinelSDKRequest) ([]string, error) {
	hashes, err := normalizeSentinelHashes(request.ExpectedSHA256)
	if err != nil {
		return nil, err
	}
	if request.IntegrityRequired && len(hashes) == 0 {
		return nil, errors.New("Sentinel SDK integrity does not include a supported SHA-256 digest")
	}
	return hashes, nil
}

func sentinelHashAllowed(hash string, expected []string) bool {
	if len(expected) == 0 {
		return true
	}
	for _, candidate := range expected {
		if candidate == hash {
			return true
		}
	}
	return false
}

func (manager *SentinelRuntimeManager) resolveSDKAdapter(hash string, source []byte) (sentinelSDKAdapter, bool) {
	if manager == nil || manager.adapterResolver == nil {
		return sentinelSDKAdapter{}, false
	}
	return manager.adapterResolver(hash, source)
}

func resolveSentinelSDKSource(baseURL string, sources []string) (string, string, error) {
	base, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || base == nil || base.Scheme == "" || base.Host == "" {
		base, _ = url.Parse("https://chatgpt.com")
	}
	candidates := make([]*url.URL, 0, len(sources)+1)
	for _, source := range sources {
		parsed, parseErr := url.Parse(strings.TrimSpace(source))
		if parseErr != nil || parsed == nil {
			continue
		}
		parsed = base.ResolveReference(parsed)
		path := strings.ToLower(parsed.EscapedPath())
		if strings.HasSuffix(path, "/sdk.js") && strings.Contains(path, "/sentinel/") {
			candidates = append(candidates, parsed)
		}
	}
	for _, candidate := range candidates {
		if err := validateSentinelSDKURL(candidate); err != nil {
			continue
		}
		candidate.Fragment = ""
		version := sentinelSDKVersionFromPath(candidate.EscapedPath())
		return candidate.String(), version, nil
	}
	return "", "", errors.New("Sentinel SDK URL is not trusted")
}

func resolveSentinelSDKRequestSource(request SentinelSDKRequest) (string, string, error) {
	sources := request.ScriptSources
	if sdkURL := strings.TrimSpace(request.SDKURL); sdkURL != "" {
		sources = make([]string, 0, len(request.ScriptSources)+1)
		sources = append(sources, sdkURL)
		sources = append(sources, request.ScriptSources...)
	}
	return resolveSentinelSDKSource(request.BaseURL, sources)
}

func validateSentinelSDKURL(value *url.URL) error {
	if value == nil || !strings.EqualFold(value.Scheme, "https") || value.User != nil {
		return errors.New("Sentinel SDK URL must use HTTPS")
	}
	if value.ForceQuery || value.RawQuery != "" || value.Fragment != "" || value.RawFragment != "" {
		return errors.New("Sentinel SDK URL must not contain a query or fragment")
	}
	if port := value.Port(); port != "" && port != "443" {
		return errors.New("Sentinel SDK URL must use the default HTTPS port")
	}
	host := strings.ToLower(value.Hostname())
	if host != "sentinel.openai.com" && host != "chatgpt.com" {
		return errors.New("Sentinel SDK host is not trusted")
	}
	version, backendAPI := conversationSentinelSDKVersionFromPath(value.EscapedPath())
	if version == "" || backendAPI && host != "chatgpt.com" {
		return errors.New("Sentinel SDK path is not trusted")
	}
	return nil
}

func sentinelSDKVersionFromPath(path string) string {
	version, _ := conversationSentinelSDKVersionFromPath(path)
	return version
}

func patchSentinelSDK(source []byte, adapter sentinelSDKAdapter) ([]byte, error) {
	if adapter.version == "" || adapter.marker == "" || adapter.injection == "" {
		return nil, errors.New("Sentinel SDK export adapter is invalid")
	}
	if strings.Count(string(source), adapter.marker) != 1 {
		return nil, errors.New("Sentinel SDK export adapter does not match this SDK hash")
	}
	patched := strings.Replace(
		string(source),
		adapter.marker,
		adapter.injection+adapter.marker,
		1,
	)
	return []byte(patched), nil
}

func (manager *SentinelRuntimeManager) runtimeBootstrap(request SentinelSDKRequest, source *sentinelSourceCacheEntry) (string, error) {
	persona := normalizePersona(request.Environment.Persona)
	randomBytes := make([]byte, sentinelSDKRandomBytes)
	reader := manager.random
	if reader == nil {
		reader = rand.Reader
	}
	if _, err := io.ReadFull(reader, randomBytes); err != nil {
		return "", fmt.Errorf("read Sentinel SDK random pool: %w", err)
	}
	languages := []string{persona.Language}
	for _, part := range strings.Split(persona.AcceptLanguage, ",") {
		language := strings.TrimSpace(strings.SplitN(part, ";", 2)[0])
		if language == "" || strings.EqualFold(language, persona.Language) {
			continue
		}
		languages = append(languages, language)
	}
	payload, err := json.Marshal(map[string]any{
		"sdk_url":              source.url,
		"location":             strings.TrimSpace(request.Environment.Location),
		"device_id":            strings.TrimSpace(request.DeviceID),
		"user_agent":           persona.UserAgent,
		"language":             persona.Language,
		"languages":            languages,
		"platform":             persona.Platform,
		"hardware_concurrency": persona.HardwareConcurrency,
		"screen_width":         persona.ScreenWidth,
		"screen_height":        persona.ScreenHeight,
		"random_b64":           base64.StdEncoding.EncodeToString(randomBytes),
		"script_sources":       append([]string(nil), request.Environment.ScriptSources...),
		"local_storage_keys":   append([]string(nil), request.Environment.LocalStorageKeys...),
	})
	if err != nil {
		return "", fmt.Errorf("encode Sentinel SDK environment: %w", err)
	}
	return string(payload), nil
}

// SolveTurnstile keeps the Go VM on the hot path and uses the SDK only for a
// typed compatibility failure or a temporary sdk_preferred cache hit.
func (manager *SentinelRuntimeManager) SolveTurnstile(
	ctx context.Context,
	goRequest ConversationTurnstileSolveRequest,
	sdkRequest SentinelSDKRequest,
	observer *SentinelObserver,
) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if manager == nil {
		return BuildConversationTurnstileTokenWithEnvironment(
			ctx, goRequest.DX, goRequest.RequirementsToken, goRequest.Environment, goRequest.Reader, goRequest.Now,
		)
	}
	if manager.closedFlag.Load() || (!manager.enabled.Load() && observer == nil) {
		return BuildConversationTurnstileTokenWithEnvironment(
			ctx, goRequest.DX, goRequest.RequirementsToken, goRequest.Environment, goRequest.Reader, goRequest.Now,
		)
	}
	manager.mu.Lock()
	generation := manager.cacheGeneration
	manager.mu.Unlock()

	var prepared *conversationTurnstilePreparedProgram
	if manager.hasActivePreferred() {
		signature, candidate, exactHint := manager.preferredChallengeSignature(SentinelProgramTurnstile, goRequest.DX, goRequest.RequirementsToken)
		if exactHint && !candidate {
			signature = ""
		}
		if !exactHint {
			var signatureErr error
			prepared, signature, signatureErr = prepareConversationSentinelProgramSignature(ctx, goRequest.DX, goRequest.RequirementsToken)
			if signatureErr != nil {
				signature = ""
			}
		}
		if signature != "" {
			preferred := false
			sourceURL, _, resolveErr := resolveSentinelSDKRequestSource(sdkRequest)
			if resolveErr == nil {
				expectedHashes, hashErr := expectedSentinelHashes(sdkRequest)
				if hashErr == nil {
					if cached := manager.latestSourceForURL(sourceURL, expectedHashes); cached != nil {
						preferred = manager.isPreferred(cached.hash, SentinelProgramTurnstile, signature)
						if preferred {
							return manager.solveTurnstileWithSDK(ctx, sdkRequest, cached, signature, true, observer, goRequest.DX, goRequest.RequirementsToken)
						}
					}
				}
			}
			if !preferred {
				manager.cacheChallengeSignature(generation, SentinelProgramTurnstile, signature, goRequest.DX, goRequest.RequirementsToken, false)
			}
		}
	}

	var token string
	var err error
	if prepared != nil {
		token, err = solvePreparedConversationTurnstile(ctx, prepared, goRequest)
	} else {
		token, err = (GoConversationTurnstileSolver{}).Solve(ctx, goRequest)
	}
	if err == nil {
		return token, nil
	}
	var compatibility *SentinelCompatibilityError
	if !errors.As(err, &compatibility) {
		return "", err
	}
	signature := compatibility.OpcodeSignature
	if signature == "" {
		var signatureErr error
		if prepared != nil {
			signature, signatureErr = conversationSentinelProgramSignature(ctx, prepared.program)
		} else {
			signature, signatureErr = conversationSentinelProgramSignatureForDX(ctx, goRequest.DX, goRequest.RequirementsToken)
		}
		if signatureErr != nil {
			signature = ""
		}
	}
	signature = conversationSentinelCompatibilitySignature(signature, compatibility)
	return manager.solveTurnstileWithSDK(ctx, sdkRequest, nil, signature, signature != "", observer, goRequest.DX, goRequest.RequirementsToken)
}

func (manager *SentinelRuntimeManager) solveTurnstileWithSDK(
	ctx context.Context,
	request SentinelSDKRequest,
	source *sentinelSourceCacheEntry,
	signature string,
	cachePreferred bool,
	observer *SentinelObserver,
	dx string,
	requirementsToken string,
) (string, error) {
	var finishTask func()
	if observer == nil {
		var err error
		finishTask, err = manager.beginSDKTask()
		if err != nil {
			return "", err
		}
		defer finishTask()
	}
	if observer != nil {
		if token, err := observer.solveTurnstile(ctx); err == nil {
			observer.mu.Lock()
			hash := ""
			generation := uint64(0)
			if observer.source != nil {
				hash = observer.source.hash
				generation = observer.source.generation
			}
			observer.mu.Unlock()
			if cachePreferred {
				manager.markPreferredForChallenge(generation, hash, SentinelProgramTurnstile, signature, dx, requirementsToken)
			}
			manager.recordFallbackSuccess(generation)
			return token, nil
		} else {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return "", err
			}
			observer.mu.Lock()
			hasInstance := observer.instance != nil && observer.lease != nil
			hash := ""
			generation := uint64(0)
			if observer.source != nil {
				hash = observer.source.hash
				generation = observer.source.generation
			}
			observer.mu.Unlock()
			if hasInstance {
				recordSentinelCircuitForError(manager, ctx, generation, hash, err)
				return "", newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK Turnstile solve failed"))
			}
		}
		var beginErr error
		finishTask, beginErr = manager.beginSDKTask()
		if beginErr != nil {
			return "", beginErr
		}
		defer finishTask()
	}
	if source == nil {
		loadedSource, sourceErr := manager.sourceForTask(ctx, request)
		if sourceErr != nil {
			var runtimeErr *SentinelRuntimeError
			if errors.As(sourceErr, &runtimeErr) {
				return "", sourceErr
			}
			return "", newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK source is unavailable"))
		}
		source = loadedSource
	}
	if retryAfter, open := manager.circuitRetryAfter(source.hash); open {
		return "", newSentinelRuntimeError("sentinel_sdk_unavailable", retryAfter, errors.New("Sentinel SDK circuit breaker is open"))
	}
	lease, err := manager.acquire(ctx, sentinelPriorityFallback)
	if err != nil {
		return "", err
	}
	defer lease.release()
	bytecode, err := manager.bytecodeWithLease(ctx, source, sentinelPriorityFallback, lease)
	if err != nil {
		var runtimeErr *SentinelRuntimeError
		if errors.As(err, &runtimeErr) {
			return "", err
		}
		return "", newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK bytecode is unavailable"))
	}
	instance, err := manager.newSDKInstance(ctx, lease, request, source, bytecode)
	if err != nil {
		recordSentinelCircuitForError(manager, ctx, source.generation, source.hash, err)
		var runtimeErr *SentinelRuntimeError
		if errors.As(err, &runtimeErr) {
			return "", err
		}
		return "", newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK runtime initialization failed"))
	}
	defer instance.close()
	token, err := instance.call(ctx, "solveTurnstile", map[string]any{
		"challenge":          request.Challenge,
		"requirements_token": request.RequirementsToken,
	})
	if err != nil {
		recordSentinelCircuitForError(manager, ctx, source.generation, source.hash, err)
		return "", newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK Turnstile solve failed"))
	}
	token = strings.TrimSpace(token)
	if token == "" {
		err = errors.New("Sentinel SDK Turnstile token is empty")
		recordSentinelCircuitForError(manager, ctx, source.generation, source.hash, err)
		return "", newSentinelRuntimeError("sentinel_sdk_unavailable", 0, err)
	}
	if cachePreferred {
		manager.markPreferredForChallenge(source.generation, source.hash, SentinelProgramTurnstile, signature, dx, requirementsToken)
	}
	manager.recordFallbackSuccess(source.generation)
	return token, nil
}

// SentinelObserver retains one isolated SDK instance for a single request.
type SentinelObserver struct {
	manager *SentinelRuntimeManager
	request SentinelSDKRequest
	ctx     context.Context
	cancel  context.CancelFunc
	ready   chan struct{}

	initialSource *sentinelSourceCacheEntry

	mu          sync.Mutex
	reservation *sentinelRuntimeReservation
	lease       *sentinelRuntimeLease
	source      *sentinelSourceCacheEntry
	instance    *sentinelQJSInstance
	err         error
	closed      bool
	counted     bool
	closeOnce   sync.Once
}

// BeginObserver starts collector work only when the challenge requires both
// collector and snapshot programs. It returns immediately.
func (manager *SentinelRuntimeManager) BeginObserver(ctx context.Context, request SentinelSDKRequest) (*SentinelObserver, error) {
	if manager == nil || !sentinelObserverRequired(request.Challenge) {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	observerCtx, cancel := context.WithCancel(ctx)
	initialSource, err := manager.cachedSourceForRequest(request)
	if err != nil {
		cancel()
		return nil, err
	}
	observer := &SentinelObserver{
		manager:       manager,
		request:       request,
		ctx:           observerCtx,
		cancel:        cancel,
		ready:         make(chan struct{}),
		initialSource: initialSource,
	}
	if err = manager.reserveObserver(observer); err != nil {
		cancel()
		var runtimeErr *SentinelRuntimeError
		if errors.As(err, &runtimeErr) && runtimeErr.Code == "sentinel_sdk_unavailable" {
			return nil, nil
		}
		return nil, err
	}
	go observer.initialize()
	return observer, nil
}

func sentinelObserverRequired(challenge map[string]any) bool {
	observer, _ := challenge["so"].(map[string]any)
	return boolValue(observer["required"]) && stringValue(observer["collector_dx"]) != "" && stringValue(observer["snapshot_dx"]) != ""
}

func (observer *SentinelObserver) initialize() {
	defer close(observer.ready)
	lease, err := observer.reservation.wait(observer.ctx)
	if err != nil {
		observer.setError(err)
		return
	}
	keepLease := false
	defer func() {
		if !keepLease {
			lease.release()
		}
	}()
	source := observer.initialSource
	if source == nil {
		source, err = observer.manager.sourceForTask(observer.ctx, observer.request)
		if err != nil {
			var runtimeErr *SentinelRuntimeError
			if errors.As(err, &runtimeErr) {
				observer.setError(err)
				return
			}
			observer.setError(newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK source is unavailable")))
			return
		}
	}
	if retryAfter, open := observer.manager.circuitRetryAfter(source.hash); open {
		observer.setError(newSentinelRuntimeError("sentinel_sdk_unavailable", retryAfter, errors.New("Sentinel SDK circuit breaker is open")))
		return
	}
	bytecode, err := observer.manager.bytecodeWithLease(observer.ctx, source, sentinelPriorityObserverCollector, lease)
	if err != nil {
		var runtimeErr *SentinelRuntimeError
		if errors.As(err, &runtimeErr) {
			observer.setError(err)
			return
		}
		observer.setError(newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK bytecode is unavailable")))
		return
	}
	instance, err := observer.manager.newSDKInstance(observer.ctx, lease, observer.request, source, bytecode)
	if err != nil {
		recordSentinelCircuitForError(observer.manager, observer.ctx, source.generation, source.hash, err)
		var runtimeErr *SentinelRuntimeError
		if errors.As(err, &runtimeErr) {
			observer.setError(err)
			return
		}
		observer.setError(newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK Observer initialization failed")))
		return
	}
	if _, err = instance.call(observer.ctx, "startObserver", map[string]any{
		"challenge":          observer.request.Challenge,
		"requirements_token": observer.request.RequirementsToken,
	}); err != nil {
		recordSentinelCircuitForError(observer.manager, observer.ctx, source.generation, source.hash, err)
		instance.close()
		observer.setError(newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK collector failed")))
		return
	}
	observer.manager.mu.Lock()
	if !observer.manager.generationActiveLocked(source.generation) {
		observer.manager.mu.Unlock()
		instance.close()
		observer.setError(newSentinelRuntimeError("sentinel_sdk_unavailable", 0, errors.New("Sentinel SDK Observer belongs to a stale runtime generation")))
		return
	}
	observer.mu.Lock()
	if observer.closed {
		observer.mu.Unlock()
		observer.manager.mu.Unlock()
		instance.close()
		return
	}
	observer.lease = lease
	observer.source = source
	observer.instance = instance
	observer.counted = true
	keepLease = true
	observer.mu.Unlock()
	observer.manager.observerSessions++
	observer.manager.lastError = ""
	observer.manager.mu.Unlock()
}

func (observer *SentinelObserver) setError(err error) {
	observer.mu.Lock()
	observer.err = err
	observer.mu.Unlock()
}

func (observer *SentinelObserver) wait(ctx context.Context) error {
	if observer == nil {
		return errors.New("Sentinel Observer is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-observer.ready:
		observer.mu.Lock()
		defer observer.mu.Unlock()
		if observer.err != nil {
			return observer.err
		}
		if observer.closed || observer.instance == nil {
			return errors.New("Sentinel Observer is closed")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (observer *SentinelObserver) solveTurnstile(ctx context.Context) (string, error) {
	if err := observer.wait(ctx); err != nil {
		return "", err
	}
	observer.mu.Lock()
	instance := observer.instance
	request := observer.request
	observer.mu.Unlock()
	return instance.call(ctx, "solveTurnstile", map[string]any{
		"challenge":          request.Challenge,
		"requirements_token": request.RequirementsToken,
	})
}

// Snapshot returns the one-request OpenAI-Sentinel-So-Token payload.
func (observer *SentinelObserver) Snapshot(ctx context.Context) (string, error) {
	if err := observer.wait(ctx); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return "", err
		}
		var runtimeErr *SentinelRuntimeError
		if errors.As(err, &runtimeErr) {
			if runtimeErr.Code == "sentinel_sdk_busy" {
				return "", err
			}
			return "", newSentinelRuntimeError("sentinel_session_observer_unavailable", runtimeErr.RetryAfter, errors.New("Sentinel SDK Observer is unavailable"))
		}
		return "", newSentinelRuntimeError("sentinel_session_observer_unavailable", 0, errors.New("Sentinel SDK Observer is unavailable"))
	}
	observer.mu.Lock()
	instance := observer.instance
	hash := ""
	generation := uint64(0)
	if observer.source != nil {
		hash = observer.source.hash
		generation = observer.source.generation
	}
	request := observer.request
	observer.mu.Unlock()
	snapshot, err := instance.call(ctx, "snapshotObserver", map[string]any{"challenge": request.Challenge})
	if err != nil {
		recordSentinelCircuitForError(observer.manager, ctx, generation, hash, err)
		return "", newSentinelRuntimeError("sentinel_session_observer_unavailable", 0, errors.New("Sentinel SDK snapshot failed"))
	}
	snapshot = strings.TrimSpace(snapshot)
	if snapshot == "" {
		recordSentinelCircuitForError(observer.manager, ctx, generation, hash, errors.New("Sentinel SDK snapshot is empty"))
		return "", newSentinelRuntimeError("sentinel_session_observer_unavailable", 0, errors.New("Sentinel SDK snapshot is empty"))
	}
	challengeToken := stringValue(request.Challenge["token"])
	if challengeToken == "" {
		return snapshot, nil
	}
	payload, err := json.Marshal(map[string]any{
		"so":   snapshot,
		"c":    challengeToken,
		"id":   strings.TrimSpace(request.DeviceID),
		"flow": strings.TrimSpace(request.Flow),
	})
	if err != nil {
		return "", newSentinelRuntimeError("sentinel_session_observer_unavailable", 0, errors.New("encode Sentinel Observer token"))
	}
	return string(payload), nil
}

// Close cancels and releases a request-scoped Observer instance.
func (observer *SentinelObserver) Close() {
	if observer == nil {
		return
	}
	observer.closeOnce.Do(func() {
		if observer.cancel != nil {
			observer.cancel()
		}
		<-observer.ready
		observer.mu.Lock()
		observer.closed = true
		instance := observer.instance
		lease := observer.lease
		counted := observer.counted
		observer.instance = nil
		observer.lease = nil
		observer.counted = false
		observer.mu.Unlock()
		if instance != nil {
			instance.close()
		}
		if lease != nil {
			lease.release()
		}
		if counted {
			observer.manager.mu.Lock()
			if observer.manager.observerSessions > 0 {
				observer.manager.observerSessions--
			}
			delete(observer.manager.observers, observer)
			workersToClose := observer.manager.finishDrainLocked()
			observer.manager.mu.Unlock()
			closeSentinelQJSWorkers(workersToClose)
			return
		}
		observer.manager.mu.Lock()
		delete(observer.manager.observers, observer)
		workersToClose := observer.manager.finishDrainLocked()
		observer.manager.mu.Unlock()
		closeSentinelQJSWorkers(workersToClose)
	})
}

type sentinelQJSInstance struct {
	runtime *sentinelQJSRuntime
	lease   *sentinelRuntimeLease
	ctx     context.Context
	mu      sync.Mutex
	closed  bool
}

func (manager *SentinelRuntimeManager) newSDKInstance(ctx context.Context, lease *sentinelRuntimeLease, request SentinelSDKRequest, source *sentinelSourceCacheEntry, bytecode []byte) (*sentinelQJSInstance, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if source == nil {
		return nil, errors.New("Sentinel SDK source is unavailable")
	}
	if err := manager.requireGeneration(source.generation); err != nil {
		return nil, err
	}
	bootstrap, err := manager.runtimeBootstrap(request, source)
	if err != nil {
		return nil, err
	}
	initializeCtx, cancelInitialize := context.WithTimeout(ctx, sentinelSDKInitializeTimeout)
	defer cancelInitialize()
	worker, err := lease.runtimeWorker(initializeCtx)
	if err != nil {
		return nil, err
	}
	isolatedRuntime, err := worker.newRuntime(initializeCtx, source.generation)
	if err != nil {
		return nil, err
	}
	instance := &sentinelQJSInstance{runtime: isolatedRuntime, lease: lease, ctx: ctx}
	bootstrapScript := "delete globalThis.navigator;globalThis.__sentinelBootstrap=JSON.parse(" + strconv.Quote(bootstrap) + ");"
	err = isolatedRuntime.run(initializeCtx, func(runtimeQJS *qjs.Runtime) error {
		if errBootstrap := evalSentinelQJS(runtimeQJS, "sentinel-bootstrap.js", bootstrapScript); errBootstrap != nil {
			return fmt.Errorf("initialize Sentinel SDK environment: %w", errBootstrap)
		}
		value, errEval := runtimeQJS.Eval("sentinel-sdk-runtime.bytecode", qjs.Bytecode(bytecode))
		if errEval != nil {
			return fmt.Errorf("load Sentinel SDK bytecode: %w", errEval)
		}
		if !safeFreeSentinelQJSValue(value) {
			return errors.New("release Sentinel SDK initialization value")
		}
		return nil
	})
	if err != nil {
		if !isolatedRuntime.close() {
			lease.markWorkerBroken()
		}
		return nil, err
	}
	return instance, nil
}

func (instance *sentinelQJSInstance) close() {
	if instance == nil {
		return
	}
	instance.mu.Lock()
	if instance.closed {
		instance.mu.Unlock()
		return
	}
	instance.closed = true
	runtimeQJS := instance.runtime
	lease := instance.lease
	instance.runtime = nil
	instance.mu.Unlock()
	if runtimeQJS == nil || !runtimeQJS.close() {
		if lease != nil {
			lease.markWorkerBroken()
		}
	}
}

func (instance *sentinelQJSInstance) call(ctx context.Context, method string, payload any) (string, error) {
	if instance == nil {
		return "", errors.New("Sentinel SDK instance is unavailable")
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode Sentinel SDK call: %w", err)
	}
	instance.mu.Lock()
	defer instance.mu.Unlock()
	if instance.closed || instance.runtime == nil {
		return "", errors.New("Sentinel SDK instance is closed")
	}
	if ctx == nil {
		ctx = instance.ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := sentinelSDKTurnstileTimeout
	if method == "startObserver" || method == "snapshotObserver" {
		timeout = sentinelSDKObserverTimeout
	}
	callCtx, cancelCall := context.WithTimeout(ctx, timeout)
	defer cancelCall()
	if instance.ctx != nil {
		stopLifetimeCancellation := context.AfterFunc(instance.ctx, cancelCall)
		defer stopLifetimeCancellation()
		if err = instance.ctx.Err(); err != nil {
			return "", err
		}
	}
	script := "globalThis.__sentinelBridge[" + strconv.Quote(method) + "](JSON.parse(" + strconv.Quote(string(data)) + "))"
	result := ""
	err = instance.runtime.run(callCtx, func(runtimeQJS *qjs.Runtime) error {
		value, errEval := runtimeQJS.Eval("sentinel-call.js", qjs.Code(script), qjs.FlagAsync())
		if errEval != nil {
			return errEval
		}
		if value.IsPromise() {
			awaited, awaitErr := value.Await()
			if awaitErr != nil {
				return awaitErr
			}
			result = awaited.String()
			if awaited.Raw() == value.Raw() {
				if !safeFreeSentinelQJSValue(awaited) {
					return errors.New("release Sentinel SDK async result")
				}
				return nil
			}
			if !safeFreeSentinelQJSValue(awaited) {
				return errors.New("release Sentinel SDK awaited result")
			}
			// QuickJS js_std_await calls JS_FreeValue on fulfilled or rejected
			// promises. The Go wrapper retains a stale handle, so freeing value here
			// would double-free it; only the returned result remains owned by Go.
			return nil
		}
		result = value.String()
		if !safeFreeSentinelQJSValue(value) {
			return errors.New("release Sentinel SDK result")
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return result, nil
}

func evalSentinelQJS(runtimeQJS *qjs.Runtime, filename, source string) error {
	value, err := runtimeQJS.Eval(filename, qjs.Code(source))
	if err != nil {
		return err
	}
	if !safeFreeSentinelQJSValue(value) {
		return errors.New("release Sentinel SDK bootstrap value")
	}
	return nil
}

func safeFreeSentinelQJSValue(value *qjs.Value) (ok bool) {
	if value == nil {
		return true
	}
	ok = true
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	value.Free()
	return ok
}

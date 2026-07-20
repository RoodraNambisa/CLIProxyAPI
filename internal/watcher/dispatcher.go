// dispatcher.go implements auth update dispatching and queue management.
// It batches, deduplicates, and delivers auth updates to registered consumers.
package watcher

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/watcher/synthesizer"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

var snapshotCoreAuthsFunc = snapshotCoreAuths

// SeedCurrentFileAuths records file-backed auths already loaded by the runtime
// manager so the first watcher snapshot can emit deletions for quarantined or
// removed files instead of treating every entry as new.
func (w *Watcher) SeedCurrentFileAuths(auths []*coreauth.Auth) {
	if w == nil || len(auths) == 0 {
		return
	}
	authDirValue := strings.TrimSpace(w.authDir)
	if authDirValue == "" {
		return
	}
	authDir, errAuthDir := filepath.Abs(authDirValue)
	if errAuthDir != nil {
		return
	}
	w.clientsMutex.Lock()
	defer w.clientsMutex.Unlock()
	if w.currentAuths == nil {
		w.currentAuths = make(map[string]*coreauth.Auth)
	}
	for _, auth := range auths {
		if auth == nil || auth.ID == "" || !fileAuthBelongsToDirectory(auth, authDir) {
			continue
		}
		w.currentAuths[auth.ID] = auth.Clone()
	}
}

func fileAuthBelongsToDirectory(auth *coreauth.Auth, authDir string) bool {
	if auth.Attributes != nil && strings.EqualFold(strings.TrimSpace(auth.Attributes["runtime_only"]), "true") {
		return false
	}
	path := strings.TrimSpace(auth.FileName)
	if path == "" && auth.Attributes != nil {
		path = strings.TrimSpace(auth.Attributes["path"])
		if path == "" {
			path = strings.TrimSpace(auth.Attributes["source"])
		}
	}
	if path == "" || strings.HasPrefix(strings.ToLower(path), "config:") || !strings.HasSuffix(strings.ToLower(path), ".json") {
		return false
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(authDir, filepath.FromSlash(path))
	}
	absolutePath, errAbs := filepath.Abs(path)
	if errAbs != nil {
		return false
	}
	authDirIdentity := authfileguard.PathIdentity(authDir)
	pathIdentity := authfileguard.PathIdentity(absolutePath)
	if authDirIdentity == "" || pathIdentity == "" {
		return false
	}
	relativePath, errRel := filepath.Rel(authDirIdentity, pathIdentity)
	return errRel == nil && relativePath != "." && relativePath != ".." && !strings.HasPrefix(relativePath, ".."+string(filepath.Separator))
}

func (w *Watcher) setAuthUpdateQueue(queue chan<- AuthUpdate) {
	if w == nil {
		return
	}
	w.dispatchLifecycleMu.Lock()
	defer w.dispatchLifecycleMu.Unlock()
	w.stopDispatchLoopLocked()

	w.clientsMutex.Lock()
	w.authQueue = queue
	w.clientsMutex.Unlock()
	if queue == nil {
		return
	}

	w.dispatchMu.Lock()
	if w.dispatchCond == nil {
		w.dispatchCond = sync.NewCond(&w.dispatchMu)
	}
	w.dispatchMu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	w.dispatchCancel = cancel
	w.dispatchDone = done
	go func() {
		defer close(done)
		w.dispatchLoop(ctx)
	}()
}

func (w *Watcher) dispatchRuntimeAuthUpdate(update AuthUpdate) RuntimeAuthUpdateResult {
	if w == nil {
		return RuntimeAuthUpdateResult{}
	}
	w.clientsMutex.Lock()
	if w.runtimeAuths == nil {
		w.runtimeAuths = make(map[string]*coreauth.Auth)
	}
	switch update.Action {
	case AuthUpdateActionAdd, AuthUpdateActionModify:
		if update.Auth != nil && update.Auth.ID != "" {
			retiredAuth := coreauth.IsRetiredGeminiCLIAuth(update.Auth)
			if retiredAuth {
				coreauth.WarnRetiredGeminiCLIAuthIgnored()
			}
			if retiredAuth || w.authUsesRetiredPathLocked(update.Auth) {
				existing, exists := w.runtimeAuths[update.Auth.ID]
				if !exists {
					w.clientsMutex.Unlock()
					return RuntimeAuthUpdateResult{Consumed: true}
				}
				delete(w.runtimeAuths, update.Auth.ID)
				if w.currentAuths != nil {
					delete(w.currentAuths, update.Auth.ID)
				}
				var deletedAuth *coreauth.Auth
				if existing != nil {
					deletedAuth = existing.Clone()
				}
				w.clientsMutex.Unlock()
				fallback := AuthUpdate{Action: AuthUpdateActionDelete, ID: update.Auth.ID, Auth: deletedAuth}
				if w.dispatchAuthUpdatesWithDependencyReconcile([]AuthUpdate{fallback}) {
					return RuntimeAuthUpdateResult{Enqueued: true, Consumed: true}
				}
				return RuntimeAuthUpdateResult{Fallback: &fallback}
			}
			clone := update.Auth.Clone()
			w.runtimeAuths[clone.ID] = clone
			if w.currentAuths == nil {
				w.currentAuths = make(map[string]*coreauth.Auth)
			}
			w.currentAuths[clone.ID] = clone.Clone()
		}
	case AuthUpdateActionDelete:
		id := update.ID
		if id == "" && update.Auth != nil {
			id = update.Auth.ID
		}
		if id != "" {
			delete(w.runtimeAuths, id)
			if w.currentAuths != nil {
				delete(w.currentAuths, id)
			}
		}
	}
	w.clientsMutex.Unlock()
	enqueued := w.dispatchAuthUpdatesWithDependencyReconcile([]AuthUpdate{update})
	return RuntimeAuthUpdateResult{Enqueued: enqueued, Consumed: enqueued}
}

func (w *Watcher) refreshAuthState(force bool) {
	w.clientsMutex.RLock()
	cfg := w.config
	authDir := w.authDir
	w.clientsMutex.RUnlock()
	auths := snapshotCoreAuthsFunc(cfg, authDir)
	w.clientsMutex.Lock()
	if len(w.runtimeAuths) > 0 {
		for id, a := range w.runtimeAuths {
			if a == nil {
				continue
			}
			if coreauth.IsRetiredGeminiCLIAuth(a) || w.authUsesRetiredPathLocked(a) {
				delete(w.runtimeAuths, id)
				continue
			}
			auths = append(auths, a.Clone())
		}
	}
	auths = w.filterRetiredPathAuthsLocked(auths)
	updates := w.prepareAuthUpdatesLocked(auths, force)
	w.clientsMutex.Unlock()
	w.dispatchAuthUpdatesWithDependencyReconcile(updates)
}

func (w *Watcher) filterRetiredPathAuthsLocked(auths []*coreauth.Auth) []*coreauth.Auth {
	if w == nil || len(w.retiredAuthPaths) == 0 || len(auths) == 0 {
		return auths
	}
	filtered := make([]*coreauth.Auth, 0, len(auths))
	for _, auth := range auths {
		if auth == nil {
			continue
		}
		if w.authUsesRetiredPathLocked(auth) {
			continue
		}
		filtered = append(filtered, auth)
	}
	return filtered
}

func (w *Watcher) authUsesRetiredPathLocked(auth *coreauth.Auth) bool {
	if w == nil || auth == nil || len(w.retiredAuthPaths) == 0 {
		return false
	}
	path := ""
	if auth.Attributes != nil {
		if strings.EqualFold(strings.TrimSpace(auth.Attributes["runtime_only"]), "true") {
			return false
		}
		path = strings.TrimSpace(auth.Attributes["source"])
		if path == "" {
			path = strings.TrimSpace(auth.Attributes["path"])
		}
	}
	if path == "" {
		path = strings.TrimSpace(auth.FileName)
		if !strings.HasSuffix(strings.ToLower(path), ".json") {
			return false
		}
	}
	if path == "" || strings.HasPrefix(strings.ToLower(path), "config:") {
		return false
	}
	if !filepath.IsAbs(path) && strings.TrimSpace(w.authDir) != "" {
		path = filepath.Join(w.authDir, filepath.FromSlash(path))
	}
	normalized := w.normalizeAuthPath(path)
	if _, retired := w.retiredAuthPaths[normalized]; retired {
		return true
	}
	identity := authfileguard.PathIdentity(normalized)
	if identity == "" {
		return false
	}
	for retiredPath := range w.retiredAuthPaths {
		if authfileguard.PathIdentity(retiredPath) == identity {
			return true
		}
	}
	return false
}

func (w *Watcher) prepareAuthUpdatesLocked(auths []*coreauth.Auth, force bool) []AuthUpdate {
	newState := make(map[string]*coreauth.Auth, len(auths))
	for _, auth := range auths {
		if auth == nil || auth.ID == "" {
			continue
		}
		newState[auth.ID] = auth.Clone()
	}
	if w.currentAuths == nil {
		w.currentAuths = newState
		if w.authQueue == nil {
			return nil
		}
		updates := make([]AuthUpdate, 0, len(newState))
		for id, auth := range newState {
			updates = append(updates, AuthUpdate{Action: AuthUpdateActionAdd, ID: id, Auth: auth.Clone()})
		}
		return updates
	}
	if w.authQueue == nil {
		w.currentAuths = newState
		return nil
	}
	updates := make([]AuthUpdate, 0, len(newState)+len(w.currentAuths))
	for id, auth := range newState {
		if existing, ok := w.currentAuths[id]; !ok {
			updates = append(updates, AuthUpdate{Action: AuthUpdateActionAdd, ID: id, Auth: auth.Clone()})
		} else if force || !authEqual(existing, auth) {
			updates = append(updates, AuthUpdate{Action: AuthUpdateActionModify, ID: id, Auth: auth.Clone()})
		}
	}
	for id := range w.currentAuths {
		if _, ok := newState[id]; !ok {
			var deletedAuth *coreauth.Auth
			if existing := w.currentAuths[id]; existing != nil {
				deletedAuth = existing.Clone()
			}
			updates = append(updates, AuthUpdate{Action: AuthUpdateActionDelete, ID: id, Auth: deletedAuth})
		}
	}
	w.currentAuths = newState
	return updates
}

func (w *Watcher) dispatchAuthUpdates(updates []AuthUpdate) bool {
	if len(updates) == 0 {
		return false
	}
	w.dispatchLifecycleMu.Lock()
	defer w.dispatchLifecycleMu.Unlock()
	return w.dispatchAuthUpdatesLocked(updates)
}

func (w *Watcher) dispatchAuthUpdatesWithDependencyReconcile(updates []AuthUpdate) bool {
	if w == nil {
		return false
	}
	w.dispatchLifecycleMu.Lock()
	defer w.dispatchLifecycleMu.Unlock()
	if w.dispatchDone == nil {
		updates = append(updates, AuthUpdate{Action: AuthUpdateActionReconcileChatGPTWebDependencies})
		return w.dispatchAuthUpdatesLocked(updates)
	}
	dispatched := w.dispatchAuthUpdatesLocked(updates)
	w.scheduleDependencyReconcileLocked()
	return dispatched || w.dependencyPending
}

func (w *Watcher) scheduleDependencyReconcileLocked() {
	delay := w.dependencyDebounce
	if delay <= 0 {
		delay = authDependencyDebounce
	}
	maxDelay := w.dependencyMaxDelay
	if maxDelay <= 0 {
		maxDelay = authDependencyMaxDelay
	}
	now := time.Now()
	if !w.dependencyPending || w.dependencyFirstAt.IsZero() {
		w.dependencyFirstAt = now
	}
	remaining := maxDelay - now.Sub(w.dependencyFirstAt)
	if remaining <= 0 {
		w.flushDependencyReconcileLocked()
		return
	}
	if delay > remaining {
		delay = remaining
	}
	if w.dependencyTimer != nil {
		w.dependencyTimer.Stop()
	}
	w.dependencyPending = true
	w.dependencyGeneration++
	generation := w.dependencyGeneration
	w.dependencyTimer = time.AfterFunc(delay, func() {
		w.dispatchLifecycleMu.Lock()
		defer w.dispatchLifecycleMu.Unlock()
		if !w.dependencyPending || w.dependencyGeneration != generation {
			return
		}
		w.dependencyPending = false
		w.dependencyTimer = nil
		w.dependencyFirstAt = time.Time{}
		w.dispatchAuthUpdatesLocked([]AuthUpdate{{Action: AuthUpdateActionReconcileChatGPTWebDependencies}})
	})
}

func (w *Watcher) flushDependencyReconcileLocked() {
	if !w.dependencyPending {
		return
	}
	if w.dependencyTimer != nil {
		w.dependencyTimer.Stop()
		w.dependencyTimer = nil
	}
	w.dependencyPending = false
	w.dependencyFirstAt = time.Time{}
	w.dispatchAuthUpdatesLocked([]AuthUpdate{{Action: AuthUpdateActionReconcileChatGPTWebDependencies}})
}

func (w *Watcher) dispatchAuthUpdatesLocked(updates []AuthUpdate) bool {
	queue := w.getAuthQueue()
	if queue == nil {
		return false
	}
	baseTS := time.Now().UnixNano()
	w.dispatchMu.Lock()
	if w.pendingUpdates == nil {
		w.pendingUpdates = make(map[string]AuthUpdate)
	}
	for idx, update := range updates {
		key := w.authUpdateKey(update, baseTS+int64(idx))
		if _, exists := w.pendingUpdates[key]; !exists {
			w.pendingOrder = append(w.pendingOrder, key)
		}
		w.pendingUpdates[key] = update
	}
	if w.dispatchCond != nil {
		w.dispatchCond.Signal()
	}
	w.dispatchMu.Unlock()
	return true
}

// WaitForAuthUpdates waits until all updates dispatched before this call have
// been applied by the registered consumer.
func (w *Watcher) WaitForAuthUpdates(ctx context.Context) error {
	if w == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	applied := make(chan struct{})
	w.dispatchLifecycleMu.Lock()
	dispatchDone := w.dispatchDone
	w.flushDependencyReconcileLocked()
	enqueued := dispatchDone != nil && w.dispatchAuthUpdatesLocked([]AuthUpdate{{Action: AuthUpdateActionBarrier, Applied: applied}})
	w.dispatchLifecycleMu.Unlock()
	if !enqueued {
		return fmt.Errorf("auth update queue is unavailable")
	}
	select {
	case <-applied:
		return nil
	case <-dispatchDone:
		select {
		case <-applied:
			return nil
		default:
			return fmt.Errorf("auth update queue stopped before updates were applied")
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Watcher) authUpdateKey(update AuthUpdate, ts int64) string {
	if update.ID != "" {
		return update.ID
	}
	if update.Action == AuthUpdateActionBarrier && update.Applied != nil {
		return fmt.Sprintf("%s:%p", update.Action, update.Applied)
	}
	return fmt.Sprintf("%s:%d", update.Action, ts)
}

func (w *Watcher) dispatchLoop(ctx context.Context) {
	for {
		batch, ok := w.nextPendingBatch(ctx)
		if !ok {
			return
		}
		queue := w.getAuthQueue()
		if queue == nil {
			if ctx.Err() != nil {
				return
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		for _, update := range batch {
			select {
			case queue <- update:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (w *Watcher) nextPendingBatch(ctx context.Context) ([]AuthUpdate, bool) {
	w.dispatchMu.Lock()
	defer w.dispatchMu.Unlock()
	if ctx.Err() != nil {
		return nil, false
	}
	for len(w.pendingOrder) == 0 {
		if ctx.Err() != nil {
			return nil, false
		}
		w.dispatchCond.Wait()
		if ctx.Err() != nil {
			return nil, false
		}
	}
	batch := make([]AuthUpdate, 0, len(w.pendingOrder))
	for _, key := range w.pendingOrder {
		batch = append(batch, w.pendingUpdates[key])
		delete(w.pendingUpdates, key)
	}
	w.pendingOrder = w.pendingOrder[:0]
	return batch, true
}

func (w *Watcher) getAuthQueue() chan<- AuthUpdate {
	w.clientsMutex.RLock()
	defer w.clientsMutex.RUnlock()
	return w.authQueue
}

func (w *Watcher) stopDispatch() {
	if w == nil {
		return
	}
	w.dispatchLifecycleMu.Lock()
	defer w.dispatchLifecycleMu.Unlock()
	w.stopDispatchLoopLocked()
}

func (w *Watcher) stopDispatchLoopLocked() {
	if w.dependencyTimer != nil {
		w.dependencyTimer.Stop()
		w.dependencyTimer = nil
	}
	w.dependencyPending = false
	w.dependencyFirstAt = time.Time{}
	w.clientsMutex.Lock()
	w.authQueue = nil
	w.clientsMutex.Unlock()

	if w.dispatchCancel != nil {
		w.dispatchCancel()
	}
	w.dispatchMu.Lock()
	if w.dispatchCond != nil {
		w.dispatchCond.Broadcast()
	}
	w.dispatchMu.Unlock()
	if w.dispatchDone != nil {
		<-w.dispatchDone
	}
	w.dispatchCancel = nil
	w.dispatchDone = nil

	w.dispatchMu.Lock()
	w.pendingOrder = nil
	w.pendingUpdates = nil
	w.dispatchMu.Unlock()
}

func authEqual(a, b *coreauth.Auth) bool {
	return reflect.DeepEqual(normalizeAuth(a), normalizeAuth(b))
}

func normalizeAuth(a *coreauth.Auth) *coreauth.Auth {
	if a == nil {
		return nil
	}
	clone := a.CloneWithoutRuntimeInstance()
	clone.CreatedAt = time.Time{}
	clone.UpdatedAt = time.Time{}
	clone.LastRefreshedAt = time.Time{}
	clone.NextRefreshAfter = time.Time{}
	clone.Runtime = nil
	clone.Quota.NextRecoverAt = time.Time{}
	return clone
}

func snapshotCoreAuths(cfg *config.Config, authDir string) []*coreauth.Auth {
	ctx := &synthesizer.SynthesisContext{
		Config:      cfg,
		AuthDir:     authDir,
		Now:         time.Now(),
		IDGenerator: synthesizer.NewStableIDGenerator(),
	}

	var out []*coreauth.Auth

	configSynth := synthesizer.NewConfigSynthesizer()
	if auths, err := configSynth.Synthesize(ctx); err == nil {
		out = append(out, auths...)
	}

	fileSynth := synthesizer.NewFileSynthesizer()
	if auths, err := fileSynth.Synthesize(ctx); err == nil {
		out = append(out, auths...)
	}

	return out
}

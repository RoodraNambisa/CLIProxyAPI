package cliproxy

import (
	"context"
	"strings"
	"sync"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

var chatGPTWebFallbackModelIDs = []string{
	"gpt-5",
	"gpt-5-1",
	"gpt-5-2",
	"gpt-5-3",
	"gpt-5-3-mini",
	"gpt-5-5",
	"gpt-5-mini",
}

type chatGPTWebModelCatalogCacheEntry struct {
	RuntimeInstanceID   string
	CredentialIdentity  string
	CredentialReference coreauth.ChatGPTWebCredentialReference
	Models              []*registry.ModelInfo
}

type chatGPTWebModelFetchLockEntry struct {
	mu         sync.Mutex
	references int
}

type chatGPTWebRegistryStateAction uint8

const (
	chatGPTWebRegistryStateNone chatGPTWebRegistryStateAction = iota
	chatGPTWebRegistryStateRefresh
	chatGPTWebRegistryStatePrune
	chatGPTWebRegistryStateReconcile
)

type chatGPTWebModelFetcher interface {
	FetchModels(context.Context, *coreauth.Auth) ([]chatgptwebauth.CatalogModel, error)
}

func chatGPTWebBuiltinModels() []*registry.ModelInfo {
	models := make([]*registry.ModelInfo, 0, len(chatGPTWebFallbackModelIDs)+1)
	for _, modelID := range chatGPTWebFallbackModelIDs {
		models = append(models, chatGPTWebTextModelInfo(modelID, modelID, 0, "openai"))
	}
	return upsertModelInfo(models, chatGPTWebImageModelInfo())
}

func chatGPTWebTextModelInfo(modelID, displayName string, created int64, ownedBy string) *registry.ModelInfo {
	modelID = strings.TrimSpace(modelID)
	if modelID == "" {
		return nil
	}
	if displayName = strings.TrimSpace(displayName); displayName == "" {
		displayName = modelID
	}
	if ownedBy = strings.TrimSpace(ownedBy); ownedBy == "" {
		ownedBy = "openai"
	}
	if created <= 0 {
		created = 1704067200
	}
	return &registry.ModelInfo{
		ID:                  modelID,
		UpstreamID:          modelID,
		Object:              "model",
		Created:             created,
		OwnedBy:             ownedBy,
		Type:                "openai",
		DisplayName:         displayName,
		Version:             modelID,
		SupportedParameters: []string{"tools"},
		Thinking:            &registry.ThinkingSupport{Levels: []string{"low", "medium", "high", "xhigh"}},
	}
}

func chatGPTWebImageModelInfo() *registry.ModelInfo {
	return &registry.ModelInfo{
		ID:                        "gpt-image-2",
		UpstreamID:                "gpt-image-2",
		Object:                    "model",
		Created:                   1704067200,
		OwnedBy:                   "openai",
		Type:                      registry.OpenAIImageModelType,
		DisplayName:               "GPT Image 2",
		Version:                   "gpt-image-2",
		SupportedParameters:       []string{"tools"},
		SupportedInputModalities:  []string{"TEXT", "IMAGE"},
		SupportedOutputModalities: []string{"IMAGE"},
	}
}

func chatGPTWebCatalogModelInfos(models []chatgptwebauth.CatalogModel) []*registry.ModelInfo {
	output := make([]*registry.ModelInfo, 0, len(models)+1)
	seen := make(map[string]struct{}, len(models))
	for _, model := range models {
		modelID := strings.TrimSpace(model.Slug)
		key := strings.ToLower(modelID)
		if key == "" || key == "gpt-image-2" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		if info := chatGPTWebTextModelInfo(modelID, model.DisplayName, model.Created, model.OwnedBy); info != nil {
			output = append(output, info)
		}
	}
	return upsertModelInfo(output, chatGPTWebImageModelInfo())
}

func (s *Service) chatGPTWebModelsForAuth(auth *coreauth.Auth) []*registry.ModelInfo {
	if s == nil || auth == nil {
		return nil
	}
	if cached, ok := s.chatGPTWebModelCatalog.Load(auth.ID); ok {
		if entry, okEntry := cached.(*chatGPTWebModelCatalogCacheEntry); okEntry && entry != nil &&
			entry.RuntimeInstanceID != "" && entry.RuntimeInstanceID == auth.RuntimeInstanceID() &&
			chatGPTWebCatalogEntryMatchesAuth(entry, auth) {
			currentIdentity := chatGPTWebCatalogCredentialIdentity(auth)
			if entry.CredentialIdentity != currentIdentity {
				s.chatGPTWebModelCatalog.Store(auth.ID, &chatGPTWebModelCatalogCacheEntry{
					RuntimeInstanceID:   entry.RuntimeInstanceID,
					CredentialIdentity:  currentIdentity,
					CredentialReference: chatGPTWebCatalogReference(entry, auth),
					Models:              cloneChatGPTWebModelInfos(entry.Models),
				})
			}
			return cloneChatGPTWebModelInfos(entry.Models)
		} else if okEntry && entry != nil {
			s.chatGPTWebModelCatalog.CompareAndDelete(auth.ID, entry)
		}
	}
	return chatGPTWebBuiltinModels()
}

func chatGPTWebCatalogCredentialIdentity(auth *coreauth.Auth) string {
	return coreauth.ChatGPTWebCatalogCredentialKey(auth)
}

func chatGPTWebCatalogEntryMatchesAuth(entry *chatGPTWebModelCatalogCacheEntry, auth *coreauth.Auth) bool {
	if entry == nil || auth == nil {
		return false
	}
	if !entry.CredentialReference.Empty() {
		return entry.CredentialReference.Matches(auth)
	}
	return entry.CredentialIdentity != "" && entry.CredentialIdentity == chatGPTWebCatalogCredentialIdentity(auth)
}

func chatGPTWebCatalogReference(entry *chatGPTWebModelCatalogCacheEntry, auth *coreauth.Auth) coreauth.ChatGPTWebCredentialReference {
	if entry != nil && !entry.CredentialReference.Empty() {
		return entry.CredentialReference
	}
	return coreauth.NewChatGPTWebCredentialReference(auth)
}

func (s *Service) migrateOpaqueChatGPTWebCatalogAfterRefreshLocked(auth *coreauth.Auth) {
	if s == nil || auth == nil || coreauth.ChatGPTWebCredentialIdentity(auth) != "" {
		return
	}
	currentIdentity := chatGPTWebCatalogCredentialIdentity(auth)
	if currentIdentity == "" {
		return
	}
	cached, ok := s.chatGPTWebModelCatalog.Load(auth.ID)
	if !ok {
		return
	}
	entry, ok := cached.(*chatGPTWebModelCatalogCacheEntry)
	if !ok || entry == nil || entry.CredentialIdentity == "" || !entry.CredentialReference.Empty() {
		return
	}
	s.chatGPTWebModelCatalog.Store(auth.ID, &chatGPTWebModelCatalogCacheEntry{
		RuntimeInstanceID:  auth.RuntimeInstanceID(),
		CredentialIdentity: currentIdentity,
		Models:             cloneChatGPTWebModelInfos(entry.Models),
	})
}

func preserveChatGPTWebReplacementRuntimeState(existing, next *coreauth.Auth) {
	if existing == nil || next == nil || !isNativeChatGPTWebAuth(existing) || !isNativeChatGPTWebAuth(next) {
		return
	}
	existingIdentity := coreauth.ChatGPTWebCredentialIdentity(existing)
	nextIdentity := coreauth.ChatGPTWebCredentialIdentity(next)
	if existingIdentity == "" || nextIdentity == "" || coreauth.ChatGPTWebCredentialIdentityChanged(existing, next) {
		return
	}
	if !existing.LifecycleSelectable() || !next.LifecycleSelectable() {
		return
	}
	next.Unavailable = existing.Unavailable
	if (next.Status == "" || next.Status == coreauth.StatusActive) && existing.Status != "" && existing.Status != coreauth.StatusActive {
		next.Status = existing.Status
	}
	next.StatusMessage = existing.StatusMessage
	next.LastError = existing.LastError
	next.NextRetryAfter = existing.NextRetryAfter
	next.CooldownScope = existing.CooldownScope
	next.ModelStates = existing.ModelStates
}

func chatGPTWebCredentialIdentityChanged(existing, next *coreauth.Auth) bool {
	return coreauth.ChatGPTWebCredentialIdentityChanged(existing, next)
}

func authHasTransientState(auth *coreauth.Auth) bool {
	return auth != nil && (auth.Unavailable || !auth.NextRetryAfter.IsZero() || auth.CooldownScope != "" ||
		auth.LastError != nil || len(auth.ModelStates) > 0)
}

func cloneChatGPTWebModelInfos(models []*registry.ModelInfo) []*registry.ModelInfo {
	output := make([]*registry.ModelInfo, 0, len(models))
	for _, model := range models {
		if model == nil {
			continue
		}
		clone := *model
		clone.SupportedParameters = append([]string(nil), model.SupportedParameters...)
		clone.SupportedInputModalities = append([]string(nil), model.SupportedInputModalities...)
		clone.SupportedOutputModalities = append([]string(nil), model.SupportedOutputModalities...)
		if model.Thinking != nil {
			thinking := *model.Thinking
			thinking.Levels = append([]string(nil), model.Thinking.Levels...)
			clone.Thinking = &thinking
		}
		output = append(output, &clone)
	}
	return output
}

func (s *Service) fetchChatGPTWebModelCatalog(ctx context.Context, auth *coreauth.Auth) ([]*registry.ModelInfo, bool) {
	if s == nil || s.coreManager == nil || auth == nil {
		return nil, false
	}
	registered, ok := s.coreManager.Executor(chatgptwebauth.Provider)
	if !ok {
		return nil, false
	}
	providerExecutor, ok := registered.(chatGPTWebModelFetcher)
	if !ok {
		return nil, false
	}
	models, err := providerExecutor.FetchModels(ctx, auth)
	if err != nil {
		log.Warnf("chatgpt web model catalog refresh failed for %s: %v", auth.ID, err)
		return nil, false
	}
	return chatGPTWebCatalogModelInfos(models), true
}

func (s *Service) currentAuthForChatGPTWebCatalog(source *coreauth.Auth) (*coreauth.Auth, bool) {
	if s == nil || s.coreManager == nil || source == nil || strings.TrimSpace(source.ID) == "" {
		return nil, false
	}
	current, ok := s.coreManager.GetByID(source.ID)
	if !ok || current == nil {
		return nil, false
	}
	if current.Disabled || current.Status == coreauth.StatusDisabled ||
		!isNativeChatGPTWebAuth(current) ||
		!current.LifecycleSelectable() {
		return current, false
	}
	expectedInstanceID := strings.TrimSpace(source.RuntimeInstanceID())
	if expectedInstanceID == "" || expectedInstanceID != strings.TrimSpace(current.RuntimeInstanceID()) {
		return current, false
	}
	return current, !coreauth.ChatGPTWebCredentialIdentityChanged(source, current)
}

func (s *Service) reconcileChatGPTWebAuthState(ctx context.Context, auth *coreauth.Auth) {
	if s == nil || auth == nil {
		return
	}
	unlockTransition := s.lockAuthModelTransition(auth.ID)
	if s.coreManager != nil {
		current, ok := s.coreManager.CurrentAuthInstallation(auth)
		if !ok {
			unlockTransition()
			return
		}
		auth = current
	}
	action := s.reconcileChatGPTWebAuthStateLocked(ctx, auth, false, false)
	unlockTransition()
	s.applyChatGPTWebRegistryState(ctx, auth, action)
}

func (s *Service) reconcileChatGPTWebAuthStateLocked(_ context.Context, auth *coreauth.Auth, force, resetTransientState bool) chatGPTWebRegistryStateAction {
	if s == nil || auth == nil || strings.TrimSpace(auth.ID) == "" {
		return chatGPTWebRegistryStateNone
	}
	if !isNativeChatGPTWebAuth(auth) {
		s.chatGPTWebModelCatalog.Delete(auth.ID)
		if force {
			s.registerModelsForAuth(auth)
			return chatGPTWebRegistryStateReconcile
		}
		return chatGPTWebRegistryStateNone
	}
	if auth.Disabled || auth.Status == coreauth.StatusDisabled {
		s.chatGPTWebModelCatalog.Delete(auth.ID)
		GlobalModelRegistry().UnregisterClient(auth.ID)
		return chatGPTWebRegistryStateRefresh
	}
	if cached, ok := s.chatGPTWebModelCatalog.Load(auth.ID); ok {
		entry, okEntry := cached.(*chatGPTWebModelCatalogCacheEntry)
		currentIdentity := chatGPTWebCatalogCredentialIdentity(auth)
		switch {
		case !okEntry || entry == nil:
			s.chatGPTWebModelCatalog.Delete(auth.ID)
			force = true
		case !chatGPTWebCatalogEntryMatchesAuth(entry, auth):
			s.chatGPTWebModelCatalog.Delete(auth.ID)
			force = true
			resetTransientState = true
		case entry.RuntimeInstanceID != auth.RuntimeInstanceID() ||
			entry.CredentialIdentity != currentIdentity:
			s.chatGPTWebModelCatalog.Store(auth.ID, &chatGPTWebModelCatalogCacheEntry{
				RuntimeInstanceID:   auth.RuntimeInstanceID(),
				CredentialIdentity:  currentIdentity,
				CredentialReference: chatGPTWebCatalogReference(entry, auth),
				Models:              cloneChatGPTWebModelInfos(entry.Models),
			})
		}
	}
	registeredProvider := registry.GetGlobalRegistry().GetProviderForClient(auth.ID)
	if registeredProvider != "" && !strings.EqualFold(registeredProvider, chatgptwebauth.Provider) {
		force = true
	}
	if !auth.LifecycleSelectable() || len(registry.GetGlobalRegistry().GetModelsForClient(auth.ID)) == 0 {
		force = true
	}
	if !force {
		return chatGPTWebRegistryStateNone
	}
	preserveTransientState := !resetTransientState && authHasTransientState(auth) &&
		strings.EqualFold(registeredProvider, chatgptwebauth.Provider)
	if preserveTransientState {
		s.registerModelsForAuthPreservingState(auth)
		return chatGPTWebRegistryStatePrune
	}
	s.registerModelsForAuth(auth)
	return chatGPTWebRegistryStateReconcile
}

func (s *Service) applyChatGPTWebRegistryState(ctx context.Context, expected *coreauth.Auth, action chatGPTWebRegistryStateAction) {
	if s == nil || s.coreManager == nil || expected == nil || action == chatGPTWebRegistryStateNone {
		return
	}
	applied := true
	switch action {
	case chatGPTWebRegistryStateRefresh:
		_, applied = s.coreManager.CurrentAuthInstallation(expected)
	case chatGPTWebRegistryStatePrune:
		applied = s.coreManager.PruneRegistryModelStatesIfCurrent(ctx, expected)
	case chatGPTWebRegistryStateReconcile:
		applied = s.coreManager.ReconcileRegistryModelStatesIfCurrent(ctx, expected)
	}
	if applied {
		s.coreManager.RefreshSchedulerEntry(expected.ID)
	}
}

func (s *Service) syncChatGPTWebModels(ctx context.Context, source *coreauth.Auth) {
	if s == nil || source == nil {
		return
	}
	rawUnlockFetch := s.lockChatGPTWebModelFetch(source.ID)
	var unlockFetchOnce sync.Once
	unlockFetch := func() {
		unlockFetchOnce.Do(rawUnlockFetch)
	}
	defer unlockFetch()

	unlockTransition := s.lockAuthModelTransition(source.ID)
	current, currentSource := s.currentAuthForChatGPTWebCatalog(source)
	if !currentSource {
		action := chatGPTWebRegistryStateNone
		expected := source
		if current == nil {
			s.chatGPTWebModelCatalog.Delete(source.ID)
			GlobalModelRegistry().UnregisterClient(source.ID)
		} else {
			expected = current
			action = s.reconcileChatGPTWebAuthStateLocked(ctx, current, true, false)
		}
		unlockTransition()
		s.applyChatGPTWebRegistryState(ctx, expected, action)
		return
	}
	action := s.reconcileChatGPTWebAuthStateLocked(ctx, current, false, false)
	selectable := !current.Disabled && current.Status != coreauth.StatusDisabled && current.LifecycleSelectable()
	unlockTransition()
	s.applyChatGPTWebRegistryState(ctx, current, action)
	if !selectable {
		return
	}

	fetchCtx, releaseFetch, active := current.BeginRuntimeExecution(ctx)
	if !active {
		return
	}
	defer releaseFetch()
	models, okFetch := s.fetchChatGPTWebModelCatalog(fetchCtx, current)
	if !okFetch {
		return
	}

	unlockTransition = s.lockAuthModelTransition(current.ID)
	latest, stillCurrent := s.currentAuthForChatGPTWebCatalog(current)
	if !stillCurrent {
		expected, staleAction, syncInline := s.reconcileStaleChatGPTWebCatalogLocked(ctx, current.ID, latest)
		releaseFetch()
		unlockTransition()
		s.applyChatGPTWebRegistryState(ctx, expected, staleAction)
		unlockFetch()
		s.runChatGPTWebCatalogSyncInline(ctx, expected, syncInline)
		return
	}
	if s.chatGPTWebCatalogCommitObserved != nil {
		s.chatGPTWebCatalogCommitObserved(latest)
	}
	entry := &chatGPTWebModelCatalogCacheEntry{
		RuntimeInstanceID:   latest.RuntimeInstanceID(),
		CredentialIdentity:  chatGPTWebCatalogCredentialIdentity(latest),
		CredentialReference: coreauth.NewChatGPTWebCredentialReference(latest),
		Models:              cloneChatGPTWebModelInfos(models),
	}
	s.chatGPTWebModelCatalog.Store(latest.ID, entry)
	s.registerModelsForAuthPreservingState(latest)

	latestAfterCommit, commitStillCurrent := s.currentAuthForChatGPTWebCatalog(latest)
	retired := releaseFetch()
	if !commitStillCurrent || retired {
		expected, staleAction, syncInline := s.reconcileStaleChatGPTWebCatalogLocked(ctx, latest.ID, latestAfterCommit)
		unlockTransition()
		s.applyChatGPTWebRegistryState(ctx, expected, staleAction)
		unlockFetch()
		s.runChatGPTWebCatalogSyncInline(ctx, expected, syncInline)
		return
	}
	unlockTransition()
	s.applyChatGPTWebRegistryState(ctx, latest, chatGPTWebRegistryStatePrune)
}

func (s *Service) refreshChatGPTWebModelRegistration(ctx context.Context, source *coreauth.Auth) {
	if s == nil || source == nil {
		return
	}
	unlockTransition := s.lockAuthModelTransition(source.ID)

	current, stillCurrent := s.currentAuthForChatGPTWebCatalog(source)
	if !stillCurrent {
		expected, action, syncInline := s.reconcileStaleChatGPTWebCatalogLocked(ctx, source.ID, current)
		unlockTransition()
		s.applyChatGPTWebRegistryState(ctx, expected, action)
		s.runChatGPTWebCatalogSyncInline(ctx, expected, syncInline)
		return
	}
	s.registerModelsForAuthPreservingState(current)
	unlockTransition()
	s.applyChatGPTWebRegistryState(ctx, current, chatGPTWebRegistryStatePrune)
}

func (s *Service) reconcileStaleChatGPTWebCatalogLocked(ctx context.Context, authID string, latest *coreauth.Auth) (*coreauth.Auth, chatGPTWebRegistryStateAction, bool) {
	if latest == nil {
		s.chatGPTWebModelCatalog.Delete(authID)
		GlobalModelRegistry().UnregisterClient(authID)
		return nil, chatGPTWebRegistryStateNone, false
	}
	action := s.reconcileChatGPTWebAuthStateLocked(ctx, latest, true, false)
	syncInline := false
	if !latest.Disabled && latest.Status != coreauth.StatusDisabled && latest.LifecycleSelectable() {
		syncInline = !s.enqueueModelSync(latest.ID)
	}
	return latest, action, syncInline
}

func (s *Service) runChatGPTWebCatalogSyncInline(ctx context.Context, auth *coreauth.Auth, enabled bool) {
	if !enabled || auth == nil {
		return
	}
	s.syncAuthModelsInline(ctx, auth.ID)
}

func (s *Service) lockChatGPTWebModelFetch(authID string) func() {
	authID = strings.TrimSpace(authID)
	if authID == "" {
		var lock sync.Mutex
		lock.Lock()
		return lock.Unlock
	}
	s.chatGPTWebModelFetchMu.Lock()
	if s.chatGPTWebModelFetchLocks == nil {
		s.chatGPTWebModelFetchLocks = make(map[string]*chatGPTWebModelFetchLockEntry)
	}
	entry := s.chatGPTWebModelFetchLocks[authID]
	if entry == nil {
		entry = &chatGPTWebModelFetchLockEntry{}
		s.chatGPTWebModelFetchLocks[authID] = entry
	}
	entry.references++
	s.chatGPTWebModelFetchMu.Unlock()

	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		s.chatGPTWebModelFetchMu.Lock()
		entry.references--
		if entry.references == 0 && s.chatGPTWebModelFetchLocks[authID] == entry {
			delete(s.chatGPTWebModelFetchLocks, authID)
		}
		s.chatGPTWebModelFetchMu.Unlock()
	}
}

func (s *Service) retireChatGPTWebModelFetchLock(authID string) {
	if s == nil {
		return
	}
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return
	}
	s.chatGPTWebModelFetchMu.Lock()
	defer s.chatGPTWebModelFetchMu.Unlock()
	entry := s.chatGPTWebModelFetchLocks[authID]
	if entry == nil {
		return
	}
	if entry.references == 0 {
		delete(s.chatGPTWebModelFetchLocks, authID)
	}
}

func (s *Service) cleanupChatGPTWebModelResourcesAfterDelete(authID, removedRuntimeInstanceID string) {
	if s == nil {
		return
	}
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return
	}

	unlockTransition := s.lockAuthModelTransition(authID)
	defer unlockTransition()

	if s.coreManager != nil {
		if current, ok := s.coreManager.GetByID(authID); ok && current != nil {
			return
		}
	}
	s.chatGPTWebModelCatalog.Delete(authID)
	s.retireChatGPTWebModelFetchLock(authID)
	GlobalModelRegistry().UnregisterClient(authID)
}

func isNativeChatGPTWebAuth(auth *coreauth.Auth) bool {
	if auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), chatgptwebauth.Provider) {
		return false
	}
	_, _, isCompat := openAICompatInfoFromAuth(auth)
	return !isCompat
}

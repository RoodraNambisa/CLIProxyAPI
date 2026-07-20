package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const (
	ChatGPTWebDeletionStateRetained = "retained_for_dependents"

	chatGPTWebCredentialUIDKey            = "credential_uid"
	chatGPTWebRefreshStrategyKey          = "refresh_strategy"
	chatGPTWebSourceCredentialUIDKey      = "source_credential_uid"
	chatGPTWebSourceAuthIDKey             = "source_auth_id"
	chatGPTWebSourceProxyURLKey           = "source_proxy_url"
	chatGPTWebDeletionStateKey            = "deletion_state"
	chatGPTWebDeletionRequestedAtKey      = "deletion_requested_at"
	chatGPTWebDeletionPreviousDisabledKey = "deletion_previous_disabled"
	chatGPTWebDependencyReservationsKey   = "chatgpt_web_dependency_reservations"
	chatGPTWebDependencyReservationTTL    = 15 * time.Minute
)

// ChatGPTWebDependencyReservation protects the interval between validating a
// Codex source and persisting its linked Web credential.
type ChatGPTWebDependencyReservation struct {
	ID            string `json:"id,omitempty"`
	AuthID        string `json:"auth_id"`
	CredentialUID string `json:"credential_uid"`
	ExpiresAt     string `json:"expires_at"`
}

// ChatGPTWebDependencyGraph indexes linked Web credentials by their stable
// Codex source credential UID. Duplicate source UIDs are treated as ambiguous.
type ChatGPTWebDependencyGraph struct {
	sourcesByUID    map[string]*Auth
	duplicateUIDs   map[string]struct{}
	dependentsByUID map[string][]*Auth
}

func isChatGPTWebDependencyProvider(auth *Auth) bool {
	if auth == nil {
		return false
	}
	provider := strings.ToLower(strings.TrimSpace(auth.Provider))
	return provider == "codex" || provider == "chatgpt-web"
}

type chatGPTWebDependencyMutationContextKey struct{}

type chatGPTWebDependencyMutationToken struct {
	manager *Manager
	parent  *chatGPTWebDependencyMutationToken
	active  atomic.Bool
	once    sync.Once
}

func chatGPTWebDependencyTokenForManager(ctx context.Context, manager *Manager) *chatGPTWebDependencyMutationToken {
	token, _ := ctx.Value(chatGPTWebDependencyMutationContextKey{}).(*chatGPTWebDependencyMutationToken)
	for current := token; current != nil; current = current.parent {
		if current.manager == manager && current.active.Load() {
			return current
		}
	}
	return nil
}

func (m *Manager) lockChatGPTWebDependencyMutationContext(ctx context.Context, authID string, incoming *Auth, force bool) (context.Context, func()) {
	if m == nil {
		return ctx, func() {}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if chatGPTWebDependencyTokenForManager(ctx, m) != nil {
		return ctx, func() {}
	}
	lockRequired := force || isChatGPTWebDependencyProvider(incoming)
	if !lockRequired && strings.TrimSpace(authID) != "" {
		m.mu.RLock()
		lockRequired = isChatGPTWebDependencyProvider(m.auths[strings.TrimSpace(authID)])
		m.mu.RUnlock()
	}
	if !lockRequired {
		return ctx, func() {}
	}
	m.chatGPTWebDependencyMutation.Lock()
	parent, _ := ctx.Value(chatGPTWebDependencyMutationContextKey{}).(*chatGPTWebDependencyMutationToken)
	token := &chatGPTWebDependencyMutationToken{manager: m, parent: parent}
	token.active.Store(true)
	lockedCtx := context.WithValue(ctx, chatGPTWebDependencyMutationContextKey{}, token)
	return lockedCtx, func() {
		token.once.Do(func() {
			token.active.Store(false)
			m.chatGPTWebDependencyMutation.Unlock()
		})
	}
}

// WithChatGPTWebDependencyMutation serializes a dependency graph decision with
// all Codex and ChatGPT Web credential mutations performed through lockedCtx.
func (m *Manager) WithChatGPTWebDependencyMutation(ctx context.Context, operation func(context.Context) error) error {
	if operation == nil {
		return nil
	}
	if m == nil {
		return operation(ctx)
	}
	lockedCtx, unlock := m.lockChatGPTWebDependencyMutationContext(ctx, "", nil, true)
	defer unlock()
	return operation(lockedCtx)
}

// BuildChatGPTWebDependencyGraph builds an immutable dependency view from auth snapshots.
func BuildChatGPTWebDependencyGraph(auths []*Auth) *ChatGPTWebDependencyGraph {
	graph := &ChatGPTWebDependencyGraph{
		sourcesByUID:    make(map[string]*Auth),
		duplicateUIDs:   make(map[string]struct{}),
		dependentsByUID: make(map[string][]*Auth),
	}
	for _, auth := range auths {
		if auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
			continue
		}
		uid := ChatGPTWebCredentialUID(auth)
		if uid == "" {
			continue
		}
		if _, exists := graph.sourcesByUID[uid]; exists {
			delete(graph.sourcesByUID, uid)
			graph.duplicateUIDs[uid] = struct{}{}
			continue
		}
		if _, duplicate := graph.duplicateUIDs[uid]; !duplicate {
			graph.sourcesByUID[uid] = auth
		}
	}
	for _, auth := range auths {
		uid := ChatGPTWebLinkedSourceUID(auth)
		if uid == "" {
			continue
		}
		if source := graph.sourcesByUID[uid]; source != nil && !ChatGPTWebLinkedSourceMatches(auth, source) {
			continue
		}
		graph.dependentsByUID[uid] = append(graph.dependentsByUID[uid], auth)
	}
	for uid := range graph.dependentsByUID {
		sort.Slice(graph.dependentsByUID[uid], func(i, j int) bool {
			return graph.dependentsByUID[uid][i].ID < graph.dependentsByUID[uid][j].ID
		})
	}
	return graph
}

// ChatGPTWebCredentialUID returns the stable credential UID stored on an auth.
func ChatGPTWebCredentialUID(auth *Auth) string {
	if auth == nil {
		return ""
	}
	return chatGPTWebIdentityMetadataString(auth.Metadata, chatGPTWebCredentialUIDKey)
}

// ChatGPTWebLinkedSourceUID returns the Codex source UID for a linked Web auth.
func ChatGPTWebLinkedSourceUID(auth *Auth) string {
	if auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), "chatgpt-web") ||
		!strings.EqualFold(chatGPTWebIdentityMetadataString(auth.Metadata, chatGPTWebRefreshStrategyKey), "codex_source") {
		return ""
	}
	return chatGPTWebIdentityMetadataString(auth.Metadata, chatGPTWebSourceCredentialUIDKey)
}

// ChatGPTWebLinkedSourceID returns the runtime ID of a linked Codex source.
func ChatGPTWebLinkedSourceID(auth *Auth) string {
	if ChatGPTWebLinkedSourceUID(auth) == "" {
		return ""
	}
	return chatGPTWebIdentityMetadataString(auth.Metadata, chatGPTWebSourceAuthIDKey)
}

// ChatGPTWebActiveDependencyReservations returns unexpired linked credential
// reservations. Reservations are transient and are not a second dependency
// index; persisted Web credentials remain the authoritative dependency source.
func ChatGPTWebActiveDependencyReservations(auth *Auth, now time.Time) []ChatGPTWebDependencyReservation {
	if auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") || auth.Metadata == nil {
		return nil
	}
	now = now.UTC()
	reservations := parseChatGPTWebDependencyReservations(auth.Metadata[chatGPTWebDependencyReservationsKey])
	active := make([]ChatGPTWebDependencyReservation, 0, len(reservations))
	seen := make(map[string]struct{}, len(reservations))
	for _, reservation := range reservations {
		expiresAt, errParse := time.Parse(time.RFC3339Nano, strings.TrimSpace(reservation.ExpiresAt))
		if errParse != nil || !expiresAt.After(now) {
			continue
		}
		reservation.ID = strings.TrimSpace(reservation.ID)
		reservation.AuthID = strings.TrimSpace(reservation.AuthID)
		reservation.CredentialUID = strings.TrimSpace(reservation.CredentialUID)
		if reservation.AuthID == "" || reservation.CredentialUID == "" {
			continue
		}
		reservation.ExpiresAt = expiresAt.UTC().Format(time.RFC3339Nano)
		key := reservation.ID
		if key == "" {
			key = reservation.AuthID + "\x00" + reservation.CredentialUID
		}
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		active = append(active, reservation)
	}
	sort.Slice(active, func(i, j int) bool {
		if active[i].AuthID != active[j].AuthID {
			return active[i].AuthID < active[j].AuthID
		}
		return active[i].CredentialUID < active[j].CredentialUID
	})
	return active
}

func parseChatGPTWebDependencyReservations(value any) []ChatGPTWebDependencyReservation {
	if value == nil {
		return nil
	}
	payload, errMarshal := json.Marshal(value)
	if errMarshal != nil {
		return nil
	}
	var reservations []ChatGPTWebDependencyReservation
	if errUnmarshal := json.Unmarshal(payload, &reservations); errUnmarshal != nil {
		return nil
	}
	return reservations
}

func setChatGPTWebDependencyReservations(auth *Auth, reservations []ChatGPTWebDependencyReservation) {
	if auth == nil {
		return
	}
	if auth.Metadata == nil {
		auth.Metadata = make(map[string]any)
	}
	if len(reservations) == 0 {
		delete(auth.Metadata, chatGPTWebDependencyReservationsKey)
		return
	}
	auth.Metadata[chatGPTWebDependencyReservationsKey] = reservations
}

func addChatGPTWebDependencyReservation(auth *Auth, reservation ChatGPTWebDependencyReservation, now time.Time) *Auth {
	updated := auth.Clone()
	reservations := ChatGPTWebActiveDependencyReservations(updated, now)
	for _, current := range reservations {
		if chatGPTWebDependencyReservationMatches(current, reservation) {
			setChatGPTWebDependencyReservations(updated, reservations)
			return updated
		}
	}
	reservations = append(reservations, reservation)
	setChatGPTWebDependencyReservations(updated, reservations)
	return updated
}

func chatGPTWebDependencyReservationMatches(current, expected ChatGPTWebDependencyReservation) bool {
	currentID := strings.TrimSpace(current.ID)
	expectedID := strings.TrimSpace(expected.ID)
	if currentID != "" || expectedID != "" {
		return currentID != "" && currentID == expectedID
	}
	return strings.TrimSpace(current.AuthID) == strings.TrimSpace(expected.AuthID) &&
		strings.TrimSpace(current.CredentialUID) == strings.TrimSpace(expected.CredentialUID)
}

func chatGPTWebDependencyReservationExists(auth *Auth, expected ChatGPTWebDependencyReservation) bool {
	if auth == nil || auth.Metadata == nil {
		return false
	}
	for _, current := range parseChatGPTWebDependencyReservations(auth.Metadata[chatGPTWebDependencyReservationsKey]) {
		if chatGPTWebDependencyReservationMatches(current, expected) {
			return true
		}
	}
	return false
}

func chatGPTWebActiveDependencyReservationExists(auth *Auth, expected ChatGPTWebDependencyReservation, now time.Time) bool {
	for _, current := range ChatGPTWebActiveDependencyReservations(auth, now) {
		if chatGPTWebDependencyReservationMatches(current, expected) {
			return true
		}
	}
	return false
}

func renewChatGPTWebDependencyReservation(auth *Auth, expected ChatGPTWebDependencyReservation, now time.Time) (*Auth, ChatGPTWebDependencyReservation, bool) {
	if !chatGPTWebActiveDependencyReservationExists(auth, expected, now) {
		return auth, ChatGPTWebDependencyReservation{}, false
	}
	renewed := expected
	renewed.ID = strings.TrimSpace(renewed.ID)
	renewed.AuthID = strings.TrimSpace(renewed.AuthID)
	renewed.CredentialUID = strings.TrimSpace(renewed.CredentialUID)
	renewed.ExpiresAt = now.UTC().Add(chatGPTWebDependencyReservationTTL).Format(time.RFC3339Nano)
	reservations := ChatGPTWebActiveDependencyReservations(auth, now)
	remaining := make([]ChatGPTWebDependencyReservation, 0, len(reservations)+1)
	for _, current := range reservations {
		if !chatGPTWebDependencyReservationMatches(current, expected) {
			remaining = append(remaining, current)
		}
	}
	remaining = append(remaining, renewed)
	updated := auth.Clone()
	setChatGPTWebDependencyReservations(updated, remaining)
	return updated, renewed, true
}

func removeExactChatGPTWebDependencyReservation(auth *Auth, expected ChatGPTWebDependencyReservation, now time.Time) (*Auth, bool) {
	if !chatGPTWebDependencyReservationExists(auth, expected) {
		return auth, false
	}
	reservations := ChatGPTWebActiveDependencyReservations(auth, now)
	remaining := make([]ChatGPTWebDependencyReservation, 0, len(reservations))
	for _, current := range reservations {
		if !chatGPTWebDependencyReservationMatches(current, expected) {
			remaining = append(remaining, current)
		}
	}
	updated := auth.Clone()
	setChatGPTWebDependencyReservations(updated, remaining)
	return updated, true
}

// ChatGPTWebLinkedSourceMatches validates a linked Web credential against the
// source ID, stable UID, and stored account identity when one is available.
func ChatGPTWebLinkedSourceMatches(dependent, source *Auth) bool {
	if dependent == nil || source == nil ||
		!strings.EqualFold(strings.TrimSpace(source.Provider), "codex") ||
		ChatGPTWebLinkedSourceUID(dependent) == "" ||
		ChatGPTWebLinkedSourceUID(dependent) != ChatGPTWebCredentialUID(source) ||
		ChatGPTWebLinkedSourceID(dependent) != source.ID {
		return false
	}
	reference := chatGPTWebIdentityMetadataString(dependent.Metadata, "source_identity")
	if reference == "" {
		return true
	}
	candidate := source.Clone()
	candidate.Provider = "chatgpt-web"
	return ChatGPTWebCredentialReferenceMatches(reference, candidate)
}

// ChatGPTWebAuthRetainedForDependents reports whether a Codex auth is pending deletion.
func ChatGPTWebAuthRetainedForDependents(auth *Auth) bool {
	return auth != nil && strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") &&
		strings.EqualFold(chatGPTWebIdentityMetadataString(auth.Metadata, chatGPTWebDeletionStateKey), ChatGPTWebDeletionStateRetained)
}

func normalizeChatGPTWebDependencyState(auth *Auth) {
	if !ChatGPTWebAuthRetainedForDependents(auth) {
		return
	}
	if auth.Metadata == nil {
		auth.Metadata = make(map[string]any)
	}
	auth.Metadata["disabled"] = true
	auth.Disabled = true
	auth.Status = StatusDisabled
	auth.StatusMessage = ChatGPTWebDeletionStateRetained
}

// SourceByUID resolves a unique Codex source. The second result reports ambiguity.
func (graph *ChatGPTWebDependencyGraph) SourceByUID(uid string) (*Auth, bool) {
	if graph == nil {
		return nil, false
	}
	uid = strings.TrimSpace(uid)
	if _, duplicate := graph.duplicateUIDs[uid]; duplicate {
		return nil, true
	}
	return graph.sourcesByUID[uid], false
}

// DependentsForSource returns linked Web auths and whether the source UID is ambiguous.
func (graph *ChatGPTWebDependencyGraph) DependentsForSource(source *Auth) ([]*Auth, bool) {
	uid := ChatGPTWebCredentialUID(source)
	if graph == nil || uid == "" {
		return nil, false
	}
	if _, duplicate := graph.duplicateUIDs[uid]; duplicate {
		return nil, true
	}
	dependents := graph.dependentsByUID[uid]
	return append([]*Auth(nil), dependents...), false
}

// ReconcileChatGPTWebDependencies removes retained Codex sources that no
// longer have linked Web dependents. Ambiguous source UIDs fail closed.
func (m *Manager) ReconcileChatGPTWebDependencies(ctx context.Context) ([]string, error) {
	if m == nil {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	auths := m.List()
	deleted := make([]string, 0)
	var reconcileErr error
	for _, source := range auths {
		if !ChatGPTWebAuthRetainedForDependents(source) {
			continue
		}
		uid := ChatGPTWebCredentialUID(source)
		if uid == "" {
			reconcileErr = errors.Join(reconcileErr, fmt.Errorf("retained codex auth %s has no credential UID", source.ID))
			continue
		}
		removed, errDelete := m.deleteRetainedCodexSourceIfOrphan(ctx, source.ID, uid)
		if errDelete != nil {
			reconcileErr = errors.Join(reconcileErr, fmt.Errorf("delete retained codex auth %s: %w", source.ID, errDelete))
			continue
		}
		if removed {
			deleted = append(deleted, source.ID)
		}
	}
	return deleted, reconcileErr
}

// PersistedAuthSnapshot returns the authoritative store view.
func (m *Manager) PersistedAuthSnapshot(ctx context.Context) ([]*Auth, error) {
	if m == nil {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if m.store == nil {
		return nil, nil
	}
	persistedAuths, errList := m.store.List(ctx)
	if errList != nil {
		return nil, errList
	}
	auths := make([]*Auth, 0, len(persistedAuths))
	for _, auth := range persistedAuths {
		if auth != nil && strings.TrimSpace(auth.ID) != "" {
			auths = append(auths, auth.Clone())
		}
	}
	return auths, nil
}

// CompleteAuthSnapshot returns current runtime credentials plus persisted-only
// records. Runtime records win when an ID exists in both views.
func (m *Manager) CompleteAuthSnapshot(ctx context.Context) ([]*Auth, error) {
	if m == nil {
		return nil, nil
	}
	runtimeAuths := m.List()
	persistedAuths, errList := m.PersistedAuthSnapshot(ctx)
	if errList != nil {
		return nil, errList
	}
	byID := make(map[string]*Auth, len(persistedAuths)+len(runtimeAuths))
	for _, auth := range persistedAuths {
		if auth != nil && strings.TrimSpace(auth.ID) != "" {
			byID[auth.ID] = auth.Clone()
		}
	}
	for _, auth := range runtimeAuths {
		if auth == nil || strings.TrimSpace(auth.ID) == "" {
			continue
		}
		byID[auth.ID] = auth.Clone()
	}
	auths := make([]*Auth, 0, len(byID))
	for _, auth := range byID {
		auths = append(auths, auth)
	}
	return auths, nil
}

// UpdateIfCurrentSourceHash persists and installs updated only when expected is
// still current and its persisted source generation has not changed.
func (m *Manager) UpdateIfCurrentSourceHash(ctx context.Context, expected, updated *Auth) (*Auth, bool, error) {
	if m == nil || expected == nil || updated == nil {
		return nil, false, nil
	}
	if _, supported := m.store.(SourceConditionalSaveStore); !supported {
		return nil, false, errors.New("auth store does not support source-conditional save")
	}
	expectedHash := authSourceHash(expected)
	if expectedHash == "" {
		return nil, false, fmt.Errorf("auth %s has no source generation", expected.ID)
	}
	ctx = WithSourceHashSavePrecondition(ctx, expectedHash)
	return m.UpdateIfCurrent(ctx, expected, updated)
}

// UpdatePersistedIfCurrentSourceHash conditionally updates a credential that is
// present in the store but not installed in this manager's runtime view.
func (m *Manager) UpdatePersistedIfCurrentSourceHash(ctx context.Context, expected, updated *Auth) (*Auth, bool, error) {
	if m == nil || expected == nil || updated == nil || strings.TrimSpace(expected.ID) == "" || expected.ID != updated.ID {
		return nil, false, nil
	}
	if !m.SupportsSourceConditionalSave() {
		return nil, false, errors.New("auth store does not support source-conditional save")
	}
	expectedHash := authSourceHash(expected)
	if expectedHash == "" {
		return nil, false, fmt.Errorf("auth %s has no source generation", expected.ID)
	}
	unlockPersist, errLock := m.lockAuthIDMutationContext(ctx, expected.ID)
	if errLock != nil {
		return nil, false, errLock
	}
	defer unlockPersist()
	m.mu.RLock()
	runtimeCurrent := m.auths[expected.ID]
	m.mu.RUnlock()
	if runtimeCurrent != nil {
		return nil, false, nil
	}
	updated = updated.Clone()
	if errPersist := m.persistWithoutLock(WithSourceHashSavePrecondition(ctx, expectedHash), updated, false); errPersist != nil {
		return nil, false, errPersist
	}
	return updated.Clone(), true, nil
}

// SupportsSourceConditionalSave reports whether the configured auth store can
// reject an update when its persisted source generation changed externally.
func (m *Manager) SupportsSourceConditionalSave() bool {
	if m == nil {
		return false
	}
	_, supported := m.store.(SourceConditionalSaveStore)
	return supported
}

// DeleteIfCurrentSourceHash removes exactly the persisted credential version
// represented by expected. It is used to compensate a conversion whose source
// reservation could not be finalized without deleting a concurrent replacement.
func (m *Manager) DeleteIfCurrentSourceHash(ctx context.Context, expected *Auth) (bool, error) {
	if m == nil || expected == nil || strings.TrimSpace(expected.ID) == "" {
		return false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	expectedHash := authSourceHash(expected)
	if expectedHash == "" {
		return false, fmt.Errorf("auth %s has no source generation", expected.ID)
	}
	store, ok := m.store.(SourceConditionalDeleteStore)
	if !ok || store == nil {
		return false, errors.New("auth store does not support source-conditional deletion")
	}
	lockedDependencyCtx, unlockDependency := m.lockChatGPTWebDependencyMutationContext(ctx, expected.ID, expected, false)
	ctx = lockedDependencyCtx
	unlockPersist, errLock := m.lockAuthIDMutationContext(ctx, expected.ID)
	if errLock != nil {
		unlockDependency()
		return false, errLock
	}
	m.mu.RLock()
	currentRuntime := m.auths[expected.ID]
	var removed *Auth
	if currentRuntime != nil {
		removed = currentRuntime.Clone()
	}
	m.mu.RUnlock()
	if currentRuntime != nil && authSourceHash(currentRuntime) != expectedHash {
		unlockPersist()
		unlockDependency()
		return false, nil
	}
	errDelete := store.DeleteIfSourceHashMatches(ctx, expected.ID, expectedHash)
	if errDelete != nil {
		if outcome, explicit := DeleteOutcomeFromError(errDelete); !explicit || outcome != DeleteOutcomeCommitted {
			unlockPersist()
			unlockDependency()
			return false, errDelete
		}
	}
	removedRuntime := false
	if currentRuntime != nil {
		m.mu.Lock()
		if m.auths[expected.ID] == currentRuntime {
			m.beginAuthInstanceCleanupLocked(expected.ID)
			delete(m.auths, expected.ID)
			removedRuntime = true
		}
		m.mu.Unlock()
	}
	if removedRuntime {
		m.cleanupRemovedAuthRuntimeStateAfterQuarantine(expected.ID)
		m.rebuildAPIKeyModelAliasFromRuntimeConfig()
	}
	unlockPersist()
	unlockDependency()
	if removedRuntime {
		m.finishAuthSessionCleanup(expected.ID, removed, "auth_removed", nil)
	}
	return true, nil
}

// ReserveChatGPTWebDependent conditionally records an in-flight linked Web
// credential before it is persisted. The source hash CAS makes source deletion
// and linked credential creation mutually exclusive across processes.
func (m *Manager) ReserveChatGPTWebDependent(ctx context.Context, expectedSource *Auth, dependentID, dependentCredentialUID string, now time.Time) (*Auth, ChatGPTWebDependencyReservation, error) {
	var empty ChatGPTWebDependencyReservation
	if m == nil || expectedSource == nil || !strings.EqualFold(strings.TrimSpace(expectedSource.Provider), "codex") {
		return nil, empty, errors.New("codex source credential is unavailable")
	}
	dependentID = strings.TrimSpace(dependentID)
	dependentCredentialUID = strings.TrimSpace(dependentCredentialUID)
	if dependentID == "" || dependentCredentialUID == "" {
		return nil, empty, errors.New("linked Web credential identity is incomplete")
	}
	if ChatGPTWebAuthRetainedForDependents(expectedSource) {
		return nil, empty, errors.New("codex source credential is pending deletion")
	}
	now = now.UTC()
	reservation := ChatGPTWebDependencyReservation{
		ID:            uuid.NewString(),
		AuthID:        dependentID,
		CredentialUID: dependentCredentialUID,
		ExpiresAt:     now.Add(chatGPTWebDependencyReservationTTL).Format(time.RFC3339Nano),
	}
	updated := addChatGPTWebDependencyReservation(expectedSource, reservation, now)
	installed, current, errUpdate := m.UpdateIfCurrentSourceHash(ctx, expectedSource, updated)
	if errUpdate != nil {
		return nil, empty, errUpdate
	}
	if !current || installed == nil {
		return nil, empty, errors.New("codex source credential changed before dependency reservation")
	}
	return installed, reservation, nil
}

// RenewChatGPTWebDependentReservation extends an in-flight conversion lease.
// The source-hash CAS prevents a renewal from recreating a deleted or replaced
// source credential.
func (m *Manager) RenewChatGPTWebDependentReservation(ctx context.Context, sourceID, sourceUID string, reservation ChatGPTWebDependencyReservation, now time.Time) (*Auth, ChatGPTWebDependencyReservation, error) {
	if m == nil {
		return nil, ChatGPTWebDependencyReservation{}, errors.New("auth manager is unavailable")
	}
	sourceID = strings.TrimSpace(sourceID)
	sourceUID = strings.TrimSpace(sourceUID)
	if sourceID == "" || sourceUID == "" || strings.TrimSpace(reservation.AuthID) == "" || strings.TrimSpace(reservation.CredentialUID) == "" {
		return nil, ChatGPTWebDependencyReservation{}, errors.New("dependency reservation identity is incomplete")
	}
	now = now.UTC()
	for range 3 {
		current, exists := m.GetByID(sourceID)
		if !exists || current == nil || !strings.EqualFold(strings.TrimSpace(current.Provider), "codex") ||
			ChatGPTWebCredentialUID(current) != sourceUID || ChatGPTWebAuthRetainedForDependents(current) {
			return nil, ChatGPTWebDependencyReservation{}, errors.New("codex source credential changed before dependency reservation renewal")
		}
		updated, renewed, found := renewChatGPTWebDependencyReservation(current, reservation, now)
		if !found {
			return nil, ChatGPTWebDependencyReservation{}, errors.New("dependency reservation disappeared before renewal")
		}
		installed, stillCurrent, errUpdate := m.UpdateIfCurrentSourceHash(ctx, current, updated)
		if errUpdate != nil {
			if outcome, explicit := SaveOutcomeFromError(errUpdate); explicit && outcome == SaveOutcomeRolledBack {
				continue
			}
			return nil, ChatGPTWebDependencyReservation{}, errUpdate
		}
		if stillCurrent && installed != nil {
			return installed, renewed, nil
		}
	}
	return nil, ChatGPTWebDependencyReservation{}, errors.New("codex source credential changed while renewing dependency reservation")
}

// FinalizeChatGPTWebDependentReservation removes the exact conversion lease.
// Unlike best-effort release, an expired or missing reservation is an error so
// the caller cannot report success after the source was concurrently deleted.
func (m *Manager) FinalizeChatGPTWebDependentReservation(ctx context.Context, sourceID, sourceUID string, reservation ChatGPTWebDependencyReservation, now time.Time) error {
	if m == nil {
		return errors.New("auth manager is unavailable")
	}
	sourceID = strings.TrimSpace(sourceID)
	sourceUID = strings.TrimSpace(sourceUID)
	if sourceID == "" || sourceUID == "" || strings.TrimSpace(reservation.AuthID) == "" || strings.TrimSpace(reservation.CredentialUID) == "" {
		return errors.New("dependency reservation identity is incomplete")
	}
	now = now.UTC()
	for range 3 {
		current, exists := m.GetByID(sourceID)
		if !exists || current == nil || !strings.EqualFold(strings.TrimSpace(current.Provider), "codex") ||
			ChatGPTWebCredentialUID(current) != sourceUID {
			return errors.New("codex source credential changed before dependency reservation finalization")
		}
		if !chatGPTWebActiveDependencyReservationExists(current, reservation, now) {
			return errors.New("dependency reservation expired or disappeared before finalization")
		}
		updated, removed := removeExactChatGPTWebDependencyReservation(current, reservation, now)
		if !removed {
			return errors.New("dependency reservation disappeared before finalization")
		}
		installed, stillCurrent, errUpdate := m.UpdateIfCurrentSourceHash(ctx, current, updated)
		if errUpdate != nil {
			if outcome, explicit := SaveOutcomeFromError(errUpdate); explicit && outcome == SaveOutcomeRolledBack {
				continue
			}
			return errUpdate
		}
		if stillCurrent && installed != nil {
			return nil
		}
	}
	return errors.New("codex source credential changed while finalizing dependency reservation")
}

// ReleaseChatGPTWebDependentReservation removes a completed or abandoned
// conversion reservation. A failed release is safe because the reservation
// expires and the persisted Web credential remains the dependency authority.
func (m *Manager) ReleaseChatGPTWebDependentReservation(ctx context.Context, sourceID, sourceUID string, reservation ChatGPTWebDependencyReservation, now time.Time) error {
	if m == nil {
		return nil
	}
	sourceID = strings.TrimSpace(sourceID)
	sourceUID = strings.TrimSpace(sourceUID)
	if sourceID == "" || sourceUID == "" || strings.TrimSpace(reservation.AuthID) == "" || strings.TrimSpace(reservation.CredentialUID) == "" {
		return nil
	}
	now = now.UTC()
	for range 3 {
		current, exists := m.GetByID(sourceID)
		if !exists || current == nil {
			return errors.New("codex source credential disappeared before dependency reservation release")
		}
		if !strings.EqualFold(strings.TrimSpace(current.Provider), "codex") || ChatGPTWebCredentialUID(current) != sourceUID {
			return errors.New("codex source credential identity changed before dependency reservation release")
		}
		updated, removed := removeExactChatGPTWebDependencyReservation(current, reservation, now)
		if !removed {
			return nil
		}
		installed, stillCurrent, errUpdate := m.UpdateIfCurrentSourceHash(ctx, current, updated)
		if errUpdate != nil {
			if outcome, explicit := SaveOutcomeFromError(errUpdate); explicit && outcome == SaveOutcomeRolledBack {
				continue
			}
			return errUpdate
		}
		if stillCurrent && installed != nil {
			return nil
		}
	}
	return errors.New("codex source credential changed while releasing dependency reservation")
}

func (m *Manager) deleteRetainedCodexSourceIfOrphan(ctx context.Context, sourceID, sourceUID string) (bool, error) {
	lockedCtx, unlockDependency := m.lockChatGPTWebDependencyMutationContext(ctx, sourceID, &Auth{Provider: "codex"}, false)
	ctx = lockedCtx
	unlockPersist, errLock := m.lockAuthIDMutationContext(ctx, sourceID)
	if errLock != nil {
		unlockDependency()
		return false, errLock
	}

	m.mu.RLock()
	currentRuntime := m.auths[sourceID]
	var current *Auth
	if currentRuntime != nil {
		current = currentRuntime.Clone()
	}
	runtimeAuths := make([]*Auth, 0, len(m.auths))
	for _, auth := range m.auths {
		if auth != nil {
			runtimeAuths = append(runtimeAuths, auth.Clone())
		}
	}
	m.mu.RUnlock()
	if current == nil || !ChatGPTWebAuthRetainedForDependents(current) || ChatGPTWebCredentialUID(current) != sourceUID {
		unlockPersist()
		unlockDependency()
		return false, nil
	}
	if m.store == nil {
		unlockPersist()
		unlockDependency()
		return false, errors.New("auth store is unavailable")
	}
	persistedAuths, errList := m.store.List(ctx)
	if errList != nil {
		unlockPersist()
		unlockDependency()
		return false, fmt.Errorf("list persisted auth dependencies: %w", errList)
	}
	byID := make(map[string]*Auth, len(persistedAuths)+len(runtimeAuths))
	for _, auth := range persistedAuths {
		if auth != nil && strings.TrimSpace(auth.ID) != "" {
			byID[auth.ID] = auth.Clone()
		}
	}
	for _, auth := range runtimeAuths {
		if auth == nil || strings.TrimSpace(auth.ID) == "" {
			continue
		}
		if _, persisted := byID[auth.ID]; !persisted {
			byID[auth.ID] = auth
		}
	}
	auths := make([]*Auth, 0, len(byID))
	for _, auth := range byID {
		auths = append(auths, auth)
	}
	graph := BuildChatGPTWebDependencyGraph(auths)
	dependents, ambiguous := graph.DependentsForSource(current)
	if ambiguous {
		unlockPersist()
		unlockDependency()
		return false, fmt.Errorf("retained codex auth %s has a duplicate credential UID", sourceID)
	}
	if len(dependents) > 0 {
		unlockPersist()
		unlockDependency()
		return false, nil
	}
	reservationSource := byID[sourceID]
	if reservationSource == nil {
		reservationSource = current
	}
	if len(ChatGPTWebActiveDependencyReservations(reservationSource, time.Now())) > 0 {
		unlockPersist()
		unlockDependency()
		return false, nil
	}
	store, ok := m.store.(SourceConditionalDeleteStore)
	if !ok || store == nil {
		unlockPersist()
		unlockDependency()
		return false, errors.New("auth store does not support source-conditional deletion")
	}
	expectedHash := authSourceHash(current)
	if expectedHash == "" {
		unlockPersist()
		unlockDependency()
		return false, fmt.Errorf("retained codex auth %s has no source generation", sourceID)
	}
	errDelete := store.DeleteIfSourceHashMatches(ctx, sourceID, expectedHash)
	if errDelete != nil {
		if outcome, explicit := DeleteOutcomeFromError(errDelete); !explicit || outcome != DeleteOutcomeCommitted {
			unlockPersist()
			unlockDependency()
			return false, errDelete
		}
	}

	removed := false
	m.mu.Lock()
	if m.auths[sourceID] == currentRuntime {
		m.beginAuthInstanceCleanupLocked(sourceID)
		delete(m.auths, sourceID)
		removed = true
	}
	m.mu.Unlock()
	if removed {
		m.cleanupRemovedAuthRuntimeStateAfterQuarantine(sourceID)
		m.rebuildAPIKeyModelAliasFromRuntimeConfig()
	}
	unlockPersist()
	unlockDependency()
	if removed {
		m.finishAuthSessionCleanup(sourceID, current, "auth_removed", nil)
	}
	return removed, nil
}

func markCodexAuthRetainedForDependents(auth *Auth, now time.Time) *Auth {
	if auth == nil {
		return nil
	}
	updated := auth.Clone()
	if updated.Metadata == nil {
		updated.Metadata = make(map[string]any)
	}
	if !ChatGPTWebAuthRetainedForDependents(auth) {
		updated.Metadata[chatGPTWebDeletionPreviousDisabledKey] = auth.Disabled
		updated.Metadata[chatGPTWebDeletionRequestedAtKey] = now.UTC().Format(time.RFC3339Nano)
	}
	updated.Metadata[chatGPTWebDeletionStateKey] = ChatGPTWebDeletionStateRetained
	updated.Metadata["disabled"] = true
	updated.Disabled = true
	updated.Status = StatusDisabled
	updated.StatusMessage = ChatGPTWebDeletionStateRetained
	updated.UpdatedAt = now.UTC()
	return updated
}

// RetainCodexAuthForChatGPTWebDependents marks a source as non-routable while preserving it for linked refreshes.
func RetainCodexAuthForChatGPTWebDependents(auth *Auth, now time.Time) *Auth {
	return markCodexAuthRetainedForDependents(auth, now)
}

// RestoreCodexAuthFromChatGPTWebRetention clears the dependency-retained deletion state.
func RestoreCodexAuthFromChatGPTWebRetention(auth *Auth, now time.Time) (*Auth, bool) {
	if !ChatGPTWebAuthRetainedForDependents(auth) {
		return auth, false
	}
	updated := auth.Clone()
	previousDisabled, _ := updated.Metadata[chatGPTWebDeletionPreviousDisabledKey].(bool)
	delete(updated.Metadata, chatGPTWebDeletionStateKey)
	delete(updated.Metadata, chatGPTWebDeletionRequestedAtKey)
	delete(updated.Metadata, chatGPTWebDeletionPreviousDisabledKey)
	updated.Metadata["disabled"] = previousDisabled
	updated.Disabled = previousDisabled
	if previousDisabled {
		updated.Status = StatusDisabled
		updated.StatusMessage = "disabled"
	} else if updated.Unavailable {
		updated.Status = StatusError
		updated.StatusMessage = ""
	} else {
		updated.Status = StatusActive
		updated.StatusMessage = ""
	}
	updated.UpdatedAt = now.UTC()
	return updated, true
}

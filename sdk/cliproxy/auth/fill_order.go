package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/fillorder"
)

// ConfigureFillOrder must run before the manager starts accepting mutations.
func (m *Manager) ConfigureFillOrder(configPath string) error {
	path := ""
	if configPath != "" {
		path = filepath.Join(filepath.Dir(configPath), ".cli-proxy-api", "fill-order.snapshot")
	}
	s, err := fillorder.Open(path)
	if err != nil {
		return err
	}
	if m.fillOrder != nil {
		_ = s.Close()
		return errors.New("fill order already configured")
	}
	m.fillOrder = s
	return nil
}
func (m *Manager) InitializeFillOrder() error {
	if m == nil || m.fillOrder == nil {
		return nil
	}
	m.fillOrderMutation.Lock()
	defer m.fillOrderMutation.Unlock()
	unlock, errBarrier := m.lockPersistBarrierWrite(context.Background())
	if errBarrier != nil {
		return errBarrier
	}
	defer unlock()
	auths := m.List()
	inputs := make([]fillorder.Input, 0, len(auths))
	for _, a := range auths {
		inputs = append(inputs, m.fillOrderInput(a))
	}
	if err := m.fillOrder.Reconcile(inputs); err != nil {
		return err
	}
	m.fillOrderReady.Store(true)
	m.applyFillOrderRecords()
	return m.fillOrder.Compact()
}
func (m *Manager) CloseFillOrder() error {
	if m == nil || m.fillOrder == nil {
		return nil
	}
	return errors.Join(m.fillOrder.Compact(), m.fillOrder.Close())
}
func (m *Manager) fillOrderInput(a *Auth) fillorder.Input {
	base := strings.TrimRight(chatGPTWebIdentityMetadataString(a.Metadata, "base_url"), "/")
	if value := strings.TrimSpace(a.Attributes["base_url"]); value != "" {
		base = strings.TrimRight(value, "/")
	}
	if strings.EqualFold(a.Provider, "codex") {
		base = ResolveCodexUpstream(a, m.currentConfig()).BaseURL
	}
	scope := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(a.Provider)) + "\x00" + base))
	return fillorder.Input{Key: a.ID, Priority: authPriority(a), Identity: fillOrderIdentity(a, base), Scope: hex.EncodeToString(scope[:])}
}

// Identity describes a principal, not a token generation. Unknown principals stay source-scoped.
func fillOrderIdentity(a *Auth, upstream ...string) string {
	if a == nil {
		return ""
	}
	provider := strings.ToLower(strings.TrimSpace(a.Provider))
	base := strings.TrimSpace(a.Attributes["base_url"])
	if base == "" {
		base = chatGPTWebIdentityMetadataString(a.Metadata, "base_url")
	}
	base = strings.TrimRight(base, "/")
	if provider == "codex" {
		base = ResolveCodexUpstream(a, nil).BaseURL
	}
	if len(upstream) > 0 {
		base = upstream[0]
	}
	workspace, user, email := "", "", ""
	if provider == "codex" {
		workspace = codex.EffectiveRequestAccountID(a.Metadata)
		user, email = fillOrderCodexSubject(a)
	} else if provider == "chatgpt-web" {
		claims := chatGPTWebCredentialIdentityClaims(a)
		workspace = claims.accountID
		user = claims.userID
		if user == "" {
			user = claims.subject
		}
		email = strings.ToLower(chatGPTWebIdentityMetadataString(a.Metadata, "email"))
	} else {
		return ""
	}
	if workspace == "" || (user == "" && email == "") {
		return ""
	}
	subject := "user:" + user
	if user == "" {
		subject = "email:" + email
	}
	raw, _ := json.Marshal([]string{provider, base, workspace, subject})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

type trustedFillRefreshKey struct{}

func (m *Manager) trackFillOrder(ctx context.Context, previous, a *Auth) error {
	if m == nil || m.fillOrder == nil || !m.fillOrderReady.Load() || a == nil {
		return nil
	}
	trusted, _ := ctx.Value(trustedFillRefreshKey{}).(bool)
	r, err := m.fillOrder.Upsert(m.fillOrderInput(a), trusted && fillOrderRefreshCompatible(previous, a))
	if err != nil {
		return fmt.Errorf("update fill order: %w", err)
	}
	a.fillOrderRecord = &r
	return nil
}
func (m *Manager) forgetFillOrder(id string) error {
	if m == nil || m.fillOrder == nil || !m.fillOrderReady.Load() {
		return nil
	}
	return m.fillOrder.Delete(id)
}
func (m *Manager) FillOrderSupported() bool {
	return m != nil && m.fillOrder != nil && m.fillOrderReady.Load()
}
func (m *Manager) FillOrderRevision() string {
	if !m.FillOrderSupported() {
		return ""
	}
	return m.fillOrder.Revision()
}

func (a *Auth) FillOrderRank() uint64 {
	if a == nil || a.fillOrderRecord == nil {
		return 0
	}
	return a.fillOrderRecord.Rank
}
func fillOrderLess(a, b *Auth) bool {
	if a.fillOrderRecord != nil && b.fillOrderRecord != nil && a.fillOrderRecord.Rank != b.fillOrderRecord.Rank {
		return a.fillOrderRecord.Rank < b.fillOrderRecord.Rank
	}
	return a.ID < b.ID
}
func (m *Manager) PreviewFillOrder(move fillorder.Move) (fillorder.Preview, error) {
	if !m.FillOrderSupported() {
		return fillorder.Preview{}, errors.New("fill order unavailable")
	}
	return m.fillOrder.Preview(move)
}
func (m *Manager) MoveFillOrder(ctx context.Context, move fillorder.Move) (fillorder.Preview, error) {
	if !m.FillOrderSupported() {
		return fillorder.Preview{}, errors.New("fill order unavailable")
	}
	if err := ctx.Err(); err != nil {
		return fillorder.Preview{}, err
	}
	m.fillOrderMutation.Lock()
	defer m.fillOrderMutation.Unlock()
	unlock, errBarrier := m.lockPersistBarrierWrite(ctx)
	if errBarrier != nil {
		return fillorder.Preview{}, errBarrier
	}
	defer unlock()
	p, err := m.fillOrder.Move(move)
	if err != nil {
		return p, err
	}
	m.applyFillOrderRecords()
	return p, nil
}
func (m *Manager) applyFillOrderRecords() {
	m.mu.Lock()
	for id, a := range m.auths {
		if r, ok := m.fillOrder.Lookup(id); ok {
			if a.fillOrderRecord != nil && *a.fillOrderRecord == r {
				continue
			}
			next := a.Clone()
			next.fillOrderRecord = &r
			m.auths[id] = next
			m.updateManagementAuthCatalogLocked(next)
		}
	}
	m.authIndexRevision++
	m.mu.Unlock()
	m.updateSchedulerFillOrder()
}

func (m *Manager) persistWithoutLock(ctx context.Context, a *Auth, syncHash bool) error {
	return m.persistWithFillOrder(ctx, a, func() error { return m.persistAuthWithoutLock(ctx, a, syncHash) })
}
func (m *Manager) persistNewWithoutLock(ctx context.Context, a *Auth) error {
	return m.persistWithFillOrder(ctx, a, func() error { return m.persistNewAuthWithoutLock(ctx, a) })
}
func (m *Manager) persistWithFillOrder(ctx context.Context, a *Auth, save func() error) error {
	if !m.FillOrderSupported() {
		return save()
	}
	previous, _ := m.GetByID(a.ID)
	if err := save(); err != nil {
		return err
	}
	rollback := func(cause error) error {
		if !m.shouldPersistAuth(ctx, a) {
			return cause
		}
		if previous != nil {
			return errors.Join(cause, m.persistAuthWithoutLock(WithSourceHashSavePrecondition(ctx, authSourceHash(a)), previous, false))
		}
		if store, ok := m.store.(SourceConditionalDeleteStore); ok && authSourceHash(a) != "" {
			return errors.Join(cause, store.DeleteIfSourceHashMatches(ctx, a.ID, authSourceHash(a)))
		}
		return errors.Join(cause, m.store.Delete(ctx, a.ID))
	}
	if _, pending := m.fillOrderDeleted.Load(a.ID); pending {
		if err := m.fillOrder.Delete(a.ID); err != nil {
			return rollback(err)
		}
		m.fillOrderDeleted.Delete(a.ID)
	}
	if err := m.trackFillOrder(ctx, previous, a); err != nil {
		return rollback(err)
	}
	return nil
}

// The caller holds the same source mutation lock used by registration.
func (m *Manager) forgetFillOrderAfterDelete(id string) error {
	if !m.FillOrderSupported() {
		return nil
	}
	m.fillOrderDeleted.Store(id, true)
	err := m.forgetFillOrder(id)
	if err == nil {
		m.fillOrderDeleted.Delete(id)
	}
	return err
}

// Rebuild only filling views; round-robin cursors and capacity counters survive moves.
func (m *Manager) updateSchedulerFillOrder() {
	s := m.scheduler
	if s == nil {
		return
	}
	records := m.fillOrder.Records()
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, provider := range s.providers {
		provider.mu.Lock()
		for _, r := range records {
			if meta := provider.auths[r.Key]; meta != nil {
				// Scheduler auth snapshots are immutable; only the ordering pointer changes.
				a := *meta.auth
				a.fillOrderRecord = &r
				meta.auth = &a
			}
		}
		for _, shard := range provider.modelShards {
			members := make(map[int][]*scheduledAuth)
			membersWS := make(map[int][]*scheduledAuth)
			ready := make(map[int][]*scheduledAuth)
			readyWS := make(map[int][]*scheduledAuth)
			for _, r := range records {
				entry := shard.entries[r.Key]
				if entry == nil {
					continue
				}
				a := *entry.auth
				a.fillOrderRecord = &r
				entry.auth = &a
				tier := entry.meta.priority
				if entryCandidateForPriority(entry, nil) {
					members[tier] = append(members[tier], entry)
					if entry.meta.websocketEnabled {
						membersWS[tier] = append(membersWS[tier], entry)
					}
				}
				if entry.state == scheduledStateReady {
					ready[tier] = append(ready[tier], entry)
					if entry.meta.websocketEnabled {
						readyWS[tier] = append(readyWS[tier], entry)
					}
				}
			}
			shard.fillMembers, shard.fillMembersWS = members, membersWS
			for tier, bucket := range shard.readyByPriority {
				bucket.fill = buildReadyView(ready[tier])
				bucket.fillWS = buildReadyView(readyWS[tier])
			}
		}
		provider.mu.Unlock()
	}
}

func (m *Manager) FillOrderSummary(a *Auth) map[string]string {
	if !m.FillOrderSupported() || a == nil {
		return nil
	}
	r, ok := m.fillOrder.Lookup(a.ID)
	if !ok {
		return nil
	}
	return map[string]string{"record_id": r.ID, "joined_sequence": strconv.FormatUint(r.Sequence, 10), "position": strconv.FormatUint(r.Position, 10)}
}

// Only a refresh already authenticated against the current runtime generation may
// enrich missing identity claims while retaining its record. Known conflicts win.
func fillOrderRefreshCompatible(old, next *Auth) bool {
	if old == nil || next == nil || old.Provider != next.Provider || old.Attributes["base_url"] != next.Attributes["base_url"] {
		return false
	}
	if old.Provider != "codex" {
		return fillOrderIdentity(old) == fillOrderIdentity(next)
	}
	a, b := codex.EffectiveRequestAccountID(old.Metadata), codex.EffectiveRequestAccountID(next.Metadata)
	if a != "" && b != "" && a != b {
		return false
	}

	u, e := fillOrderCodexSubject(old)
	v, f := fillOrderCodexSubject(next)
	if u != "" && v != "" {
		return u == v
	}
	return e == "" || f == "" || e == f
}

func fillOrderCodexSubject(a *Auth) (user, email string) {
	email = strings.ToLower(chatGPTWebIdentityMetadataString(a.Metadata, "email"))
	if claims, err := codex.ParseAccessTokenIdentity(chatGPTWebIdentityMetadataString(a.Metadata, "access_token")); err == nil {
		if email == "" {
			email = strings.ToLower(strings.TrimSpace(claims.Email))
		}
		if claims.ChatGPTUserID != "" {
			return strings.TrimSpace(claims.ChatGPTUserID), email
		}
	}
	if claims := codex.IDTokenClaimsFromMetadata(a.Metadata); claims != nil {
		if email == "" {
			email = strings.ToLower(strings.TrimSpace(claims.Email))
		}
		for _, value := range []string{claims.CodexAuthInfo.ChatgptUserID, claims.CodexAuthInfo.UserID, claims.Sub} {
			if strings.TrimSpace(value) != "" {
				return strings.TrimSpace(value), email
			}
		}
	}
	return chatGPTWebIdentityMetadataString(a.Metadata, "chatgpt_user_id", "user_id", "sub"), email
}

package auth

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	log "github.com/sirupsen/logrus"
)

const codexQuotaDisableReasonKey = "codex_quota_auto_disable_reason"

func cloneCodexQuotaAutoDisable(value config.CodexQuotaAutoDisableConfig) config.CodexQuotaAutoDisableConfig {
	value.Rules = slices.Clone(value.Rules)
	for i := range value.Rules {
		rule := &value.Rules[i]
		rule.Providers = slices.Clone(rule.Providers)
		rule.AuthPriorities = slices.Clone(rule.AuthPriorities)
		rule.CredentialIDs = slices.Clone(rule.CredentialIDs)
		if rule.WeeklyRemainingPercent != nil {
			threshold := *rule.WeeklyRemainingPercent
			rule.WeeklyRemainingPercent = &threshold
		}
		if rule.FiveHourRemainingPercent != nil {
			threshold := *rule.FiveHourRemainingPercent
			rule.FiveHourRemainingPercent = &threshold
		}
	}
	return value
}

type codexQuotaDisableMatch struct {
	rule      int
	minutes   int
	remaining float64
	threshold float64
}

func codexQuotaDisableMatchForAuth(policy config.CodexQuotaAutoDisableConfig, auth *Auth, pool CodexQuotaPool) *codexQuotaDisableMatch {
	if !policy.Enabled || auth == nil || auth.Disabled || auth.Status == StatusDisabled ||
		!strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") || pool.ID != "codex" {
		return nil
	}
	for index, rule := range policy.Rules {
		if !responseModelRuleMatchesAuth(config.ResponseModelRewriteRule{
			Providers: rule.Providers, AuthPriorities: rule.AuthPriorities, CredentialIDs: rule.CredentialIDs,
		}, auth) {
			continue
		}
		for _, kind := range []string{"primary", "secondary"} {
			get := func(field string) string {
				return pool.Signals[http.CanonicalHeaderKey("x-codex-"+kind+"-"+field)]
			}
			minutes, errMinutes := strconv.Atoi(strings.TrimSpace(get("window-minutes")))
			var threshold *float64
			switch minutes {
			case 300:
				threshold = rule.FiveHourRemainingPercent
			case 10080:
				threshold = rule.WeeklyRemainingPercent
			}
			if errMinutes != nil || threshold == nil {
				continue
			}
			used, errUsed := strconv.ParseFloat(strings.TrimSpace(get("used-percent")), 64)
			if errUsed != nil || math.IsNaN(used) || math.IsInf(used, 0) || used < 0 || used > 100 {
				continue
			}
			if remaining := 100 - used; used > 100-*threshold {
				return &codexQuotaDisableMatch{rule: index + 1, minutes: minutes, remaining: remaining, threshold: *threshold}
			}
		}
	}
	return nil
}

// applyCodexQuotaAutoDisable never retires the instance: the admitted request
// can finish, while the scheduler immediately stops selecting the credential.
// Save under the normal credential lock so manual edits and token refreshes
// cannot be overwritten by an older observation.
func (m *Manager) applyCodexQuotaAutoDisable(ctx context.Context, policy config.CodexQuotaAutoDisableConfig, authID, instanceID string, observation *CodexQuotaObservation) {
	if m == nil || !policy.Enabled || len(policy.Rules) == 0 || observation == nil {
		return
	}
	var main *CodexQuotaPool
	for _, pool := range extractCodexQuotaPools(observation) {
		if pool.ID == "codex" {
			main = &pool
			break
		}
	}
	if main == nil {
		return
	}
	// Most observations do not cross a threshold and need no persistence lock.
	m.mu.RLock()
	match := codexQuotaDisableMatchForAuth(policy, m.auths[authID], *main)
	m.mu.RUnlock()
	if match == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = context.WithoutCancel(ctx)
	unlock, errLock := m.lockAuthIDMutationContext(ctx, authID)
	if errLock != nil {
		logEntryWithRequestID(ctx).WithError(errLock).Warn("failed to lock Codex quota auto-disable")
		return
	}
	defer unlock()
	m.mu.Lock()
	auth := m.auths[authID]
	if auth == nil || auth.instanceID != instanceID || auth.RuntimeInstanceRetired() || m.sessionCleanupPendingLocked(authID) {
		m.mu.Unlock()
		return
	}
	// Pool freshness is independent of the newest raw response (which may be Spark).
	if previous := auth.codexQuotaObservation; previous != nil {
		for _, pool := range previous.Pools {
			if pool.ID == "codex" && pool.ObservedAt.After(main.ObservedAt) {
				m.mu.Unlock()
				return
			}
		}
	}
	match = codexQuotaDisableMatchForAuth(policy, auth, *main)
	if match == nil {
		m.mu.Unlock()
		return
	}
	window := "weekly"
	if match.minutes == 300 {
		window = "5-hour"
	}
	reason := fmt.Sprintf("Codex quota auto-disable: %s remaining %.6g%% < %.6g%% (rule %d)", window, match.remaining, match.threshold, match.rule)
	auth.Disabled = true
	auth.Status = StatusDisabled
	auth.StatusMessage = reason
	auth.Unavailable = true
	auth.UpdatedAt = time.Now()
	if auth.Metadata == nil {
		auth.Metadata = make(map[string]any)
	}
	auth.Metadata["disabled"] = true
	auth.Metadata[codexQuotaDisableReasonKey] = reason
	m.updateManagementAuthCatalogLocked(auth)
	snapshot := auth.Clone()
	m.mu.Unlock()
	m.upsertCurrentResultAuthState(snapshot)
	m.queueRefreshReschedule(authID)
	if _, errPersist := m.snapshotCurrentAuthForPersistenceLocked(ctx, snapshot); errPersist != nil {
		logEntryWithRequestID(ctx).WithError(errPersist).WithField("auth_id", authID).Error("Codex quota auto-disable is active in memory but could not be persisted")
	}
	logEntryWithRequestID(ctx).WithFields(log.Fields{
		"auth_id": authID, "rule": match.rule, "window_minutes": match.minutes,
		"remaining_percent": match.remaining, "threshold": match.threshold, "source": observation.Source,
	}).Info("credential disabled by Codex quota observation")
}

// CodexQuotaAutoDisableReason restores a bounded reason after loading a credential.
func CodexQuotaAutoDisableReason(auth *Auth) string {
	if auth == nil || !auth.Disabled || !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
		return ""
	}
	reason, _ := auth.Metadata[codexQuotaDisableReasonKey].(string)
	if !strings.HasPrefix(reason, "Codex quota auto-disable:") || len(reason) > 256 || strings.ContainsAny(reason, "\r\n\x00") {
		return ""
	}
	return reason
}

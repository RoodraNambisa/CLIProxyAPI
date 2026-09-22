package auth

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/match"
)

const clientResponseModelKey = "client_response_model"
const responseModelRecentLimit = 20

// PreviewResponseModelName resolves configured public naming without recording a rewrite.
func (m *Manager) PreviewResponseModelName(a *Auth, requested, original string) string {
	opts := core.Options{Metadata: map[string]any{clientResponseModelKey: requested}}
	if rewrite := m.responseModelRewriteOptions(context.Background(), a, opts, false); rewrite != nil && original != "" {
		return rewrite.RewriteModel
	}
	if alias := m.resolveExecutionAliasResult(a, requested); original != "" && alias.ForceMapping && alias.OriginalAlias != "" {
		return alias.OriginalAlias
	}
	return original
}

// PreviewUpstreamModel performs alias resolution without rotating a model pool.
func (m *Manager) PreviewUpstreamModel(a *Auth, requested string) string {
	if alias := m.resolveExecutionAliasResult(a, requested); alias.UpstreamModel != "" {
		return thinking.ParseSuffix(alias.UpstreamModel).ModelName
	}
	return thinking.ParseSuffix(rewriteModelForAuth(requested, a)).ModelName
}

type ResponseModelRewriteRecord struct {
	Validation     *core.ResponseGuardRecord `json:"validation,omitempty"`
	At             time.Time                 `json:"at"`
	RequestedModel string                    `json:"requested_model"`
	OriginalModel  string                    `json:"original_model"`
	ResponseModel  string                    `json:"response_model"`
	Rule           int                       `json:"rule"`
	Stream         bool                      `json:"stream"`
}

type ResponseModelRewriteScope struct {
	Rule   int      `json:"rule"`
	Models []string `json:"models"`
}

type ResponseModelRewriteSummary struct {
	GuardEnabled bool                         `json:"guard_enabled"`
	Blocked      uint64                       `json:"blocked"`
	Observed     uint64                       `json:"observed"`
	Enabled      bool                         `json:"enabled"`
	Conditional  bool                         `json:"conditional"`
	Total        uint64                       `json:"total"`
	Since        time.Time                    `json:"since"`
	LastAt       *time.Time                   `json:"last_at,omitempty"`
	Rules        []ResponseModelRewriteScope  `json:"rules"`
	Recent       []ResponseModelRewriteRecord `json:"recent,omitempty"`
}

type responseModelAuthStats struct {
	blocked  uint64
	observed uint64
	total    uint64
	recent   []ResponseModelRewriteRecord
}

type responseModelStats struct {
	mu    sync.Mutex
	since time.Time
	auths map[string]*responseModelAuthStats
}

func cloneResponseModelRewrite(value config.ResponseModelRewriteConfig) config.ResponseModelRewriteConfig {
	value.Rules = slices.Clone(value.Rules)
	for i := range value.Rules {
		rule := &value.Rules[i]
		rule.Providers = slices.Clone(rule.Providers)
		rule.AuthPriorities = slices.Clone(rule.AuthPriorities)
		rule.CredentialIDs = slices.Clone(rule.CredentialIDs)
		rule.RequestModels = slices.Clone(rule.RequestModels)
	}
	return value
}

func responseModelRuleMatchesAuth(rule config.ResponseModelRewriteRule, auth *Auth) bool {
	if auth == nil {
		return false
	}
	if len(rule.Providers) > 0 && !slices.ContainsFunc(rule.Providers, func(provider string) bool {
		return strings.EqualFold(strings.TrimSpace(provider), auth.ExecutionProvider())
	}) {
		return false
	}
	if len(rule.AuthPriorities) > 0 && !slices.Contains(rule.AuthPriorities, authPriority(auth)) {
		return false
	}
	if len(rule.CredentialIDs) > 0 && !slices.ContainsFunc(rule.CredentialIDs, func(id string) bool {
		return strings.TrimSpace(id) == auth.Index
	}) {
		return false
	}
	return true
}

func ensureClientResponseModelMetadata(opts core.Options, fallback string) core.Options {
	if _, exists := opts.Metadata[clientResponseModelKey]; exists {
		return opts
	}
	model := strings.TrimSpace(gjson.GetBytes(opts.OriginalRequest, "model").String())
	if model == "" {
		model = strings.TrimSpace(fallback)
	}
	metadata := make(map[string]any, len(opts.Metadata)+1)
	for key, value := range opts.Metadata {
		metadata[key] = value
	}
	metadata[clientResponseModelKey] = strings.Clone(model)
	opts.Metadata = metadata
	return opts
}

func (m *Manager) responseModelRewriteOptions(ctx context.Context, auth *Auth, opts core.Options, stream bool) *StreamRewriteOptions {
	if m == nil || auth == nil || (core.SingleAttempt(ctx) && !sdkaccess.CredentialTargetRewritesResponseModel(ctx)) {
		return nil
	}
	policy := m.selectionPolicy(ctx)
	if policy == nil || !policy.responseModelRewrite.Enabled {
		return nil
	}
	requested, _ := opts.Metadata[clientResponseModelKey].(string)
	if requested == "" || len(requested) > 256 || strings.ContainsAny(requested, "\r\n\x00") {
		return nil
	}
	for i, rule := range policy.responseModelRewrite.Rules {
		if !responseModelRuleMatchesAuth(rule, auth) {
			continue
		}
		if len(rule.RequestModels) > 0 && !slices.ContainsFunc(rule.RequestModels, func(pattern string) bool {
			pattern = strings.TrimSpace(pattern)
			matched, _ := match.MatchLimit(requested, pattern, 10000)
			if matched {
				return true
			}
			matched, _ = match.MatchLimit(thinking.ParseSuffix(requested).ModelName, pattern, 10000)
			return matched
		}) {
			continue
		}
		var once sync.Once
		return &StreamRewriteOptions{RewriteModel: requested, StrictModelFields: true, OnRewrite: func(original string) {
			once.Do(func() {
				if _, active := opts.ResponseGuard.Snapshot(); active {
					opts.ResponseGuard.SetClientModel(requested, true, i+1)
					return
				}
				m.recordResponseModelRewrite(auth, ResponseModelRewriteRecord{
					At: time.Now().UTC(), RequestedModel: strings.Clone(requested), OriginalModel: strings.Clone(original),
					ResponseModel: strings.Clone(requested), Rule: i + 1, Stream: stream,
				})
			})
		}}
	}
	return nil
}

func (m *Manager) rewriteClientResponseModel(ctx context.Context, auth *Auth, opts core.Options, response *core.Response, alias OAuthModelAliasResult) {
	defer func() {
		if model := gjson.GetBytes(response.Payload, "model").String(); model != "" {
			opts.ResponseGuard.SetClientModel(model, false, 0)
		}
	}()
	if options := m.responseModelRewriteOptions(ctx, auth, opts, false); options != nil {
		response.Payload = rewriteModelWithOptions(response.Payload, *options)
		return
	}
	if !core.SingleAttempt(ctx) {
		rewriteForceMappedResponse(response, alias)
	}
}

func (m *Manager) recordResponseModelRewrite(auth *Auth, record ResponseModelRewriteRecord) {
	if len(record.OriginalModel) > 256 {
		return
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	current := m.auths[auth.ID]
	if current == nil || current.RuntimeInstanceID() != auth.RuntimeInstanceID() {
		return
	}
	stats := &m.responseModelStats
	stats.mu.Lock()
	defer stats.mu.Unlock()
	if stats.auths == nil {
		stats.auths = make(map[string]*responseModelAuthStats)
	}
	entry := stats.auths[auth.ID]
	if entry == nil {
		entry = &responseModelAuthStats{}
		stats.auths[auth.ID] = entry
	}
	entry.total++
	entry.recent = append(entry.recent, record)
	if len(entry.recent) > responseModelRecentLimit {
		copy(entry.recent, entry.recent[len(entry.recent)-responseModelRecentLimit:])
		entry.recent = entry.recent[:responseModelRecentLimit]
	}
	log.WithFields(log.Fields{"auth_index": auth.Index, "provider": auth.ExecutionProvider(), "requested_model": record.RequestedModel, "original_response_model": record.OriginalModel, "client_response_model": record.ResponseModel, "rule": record.Rule}).Debug("response model name rewritten")
}

// AuthResponseModelRewriteSummary reads bounded, process-local counters without credential secrets.
func (m *Manager) AuthResponseModelRewriteSummary(auth *Auth, details bool) ResponseModelRewriteSummary {
	result := ResponseModelRewriteSummary{Rules: []ResponseModelRewriteScope{}}
	if m == nil || auth == nil {
		return result
	}
	policy := m.selectionPolicy()
	if policy != nil && auth.ExecutionProvider() == "codex" {
		guard := policy.codexResponseGuard
		models := map[string]bool{"": true}
		if guard.Rules != nil {
			for _, rule := range *guard.Rules {
				for _, model := range rule.Models {
					models[model] = true
				}
				for _, o := range rule.ModelOverrides {
					for _, model := range o.Models {
						models[model] = true
					}
				}
			}
		}
		for model := range models {
			if guard.PolicyFor(ResponseGuardCredentialScope(auth, model)).Mode != "off" {
				result.GuardEnabled = true
				break
			}
		}
	}
	if policy != nil && policy.responseModelRewrite.Enabled {
		result.Conditional = true
		for i, rule := range policy.responseModelRewrite.Rules {
			if !responseModelRuleMatchesAuth(rule, auth) {
				continue
			}
			result.Enabled = true
			if len(rule.RequestModels) == 0 || slices.Contains(rule.RequestModels, "*") {
				result.Conditional = false
			}
			result.Rules = append(result.Rules, ResponseModelRewriteScope{Rule: i + 1, Models: slices.Clone(rule.RequestModels)})
		}
		result.Conditional = result.Enabled && result.Conditional
	}
	stats := &m.responseModelStats
	stats.mu.Lock()
	defer stats.mu.Unlock()
	result.Since = stats.since
	if entry := stats.auths[auth.ID]; entry != nil {
		result.Total = entry.total
		result.Blocked, result.Observed = entry.blocked, entry.observed
		if len(entry.recent) > 0 {
			at := entry.recent[len(entry.recent)-1].At
			result.LastAt = &at
		}
		if details {
			result.Recent = slices.Clone(entry.recent)
			slices.Reverse(result.Recent)
		}
	}
	return result
}

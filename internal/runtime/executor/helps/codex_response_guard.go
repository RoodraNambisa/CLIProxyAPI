package helps

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

// CodexResponseGuard consumes original provider metadata before protocol or
// model rewriting. One instance belongs to one actual upstream attempt.
type CodexResponseGuard struct {
	policy      config.CodexResponseGuardPolicy
	model       string
	evidence    config.CodexResponseEvidence
	attempt     *core.ResponseGuardAttempt
	committed   bool
	failed      bool
	checked     bool
	observed    bool
	rejected    bool
	lastVerdict config.CodexResponseVerdict
	onReject    func(config.CodexResponseEvidence)
}

func NewCodexResponseGuard(ctx context.Context, cfg *config.Config, a *auth.Auth, model string, opts core.Options, stream bool, transport string, status int, headers http.Header, onReject func(config.CodexResponseEvidence)) *CodexResponseGuard {
	if cfg == nil || a == nil || a.ExecutionProvider() != "codex" || IsStateProbe(ctx) || opts.Alt == "responses/compact" {
		return nil
	}
	mode := core.ResponseGuardMode(ctx)
	if mode == "off" {
		return nil
	}
	if core.SingleAttempt(ctx) && mode == "" && !sdkaccess.CredentialTargetAppliesResponseGuard(ctx) {
		return nil
	}
	c := cfg.Codex.ResponseGuard
	if opts.ResponseGuard != nil {
		c = opts.ResponseGuard.Config
	}
	if mode != "" {
		c.Enabled = true
	}
	scope := StateCredential(a, model).Scope()
	if opts.ResponseGuard != nil {
		r, _ := opts.ResponseGuard.Snapshot()
		scope.Aliases = []string{r.RequestedModel, thinking.ParseSuffix(r.RequestedModel).ModelName}
	}
	policy := c.PolicyFor(scope)
	if mode != "" && (c.Rules == nil || policy.Rule > 0) {
		policy.Mode = mode
	}
	if policy.Mode == "off" {
		return nil
	}
	if core.SingleAttempt(ctx) || core.RequiredUpstreamWebsocket(ctx) || !RequestBodyReplayable(ctx, opts) {
		policy.OnReject = "error"
	}
	attempt := opts.ResponseGuard
	if attempt == nil {
		attempt = core.NewResponseGuardAttempt(c, model, 1, nil)
	}
	attempt.Start(policy, scope.Model, stream, transport, status)
	g := &CodexResponseGuard{policy: policy, model: scope.Model, attempt: attempt, onReject: onReject}
	state := headers.Get("X-Codex-Turn-State")
	g.evidence.StatePresent, g.evidence.StateLength = state != "", len(state)
	attempt.Update(func(r *core.ResponseGuardRecord) {
		r.StatePresent, r.StateLength = g.evidence.StatePresent, g.evidence.StateLength
		r.Phase = "headers"
	})
	return g
}

func (g *CodexResponseGuard) Enforces() bool { return g != nil && g.policy.Mode == "enforce" }

func (g *CodexResponseGuard) SetStateEvidence(e config.CodexResponseEvidence) {
	if g == nil {
		return
	}
	g.evidence.StatePresent, g.evidence.StateLength = e.StatePresent, e.StateLength
	g.attempt.Update(func(r *core.ResponseGuardRecord) { r.StatePresent, r.StateLength = e.StatePresent, e.StateLength })
}
func (g *CodexResponseGuard) Commit() {
	if g != nil {
		g.committed = true
	}
}

func responseGuardFailure(root gjson.Result) bool {
	return root.Get("type").String() == "error" || root.Get("type").String() == "response.failed" || root.Get("error").IsObject() || root.Get("response.error").IsObject() || root.Get("response.status").String() == "failed"
}

func (g *CodexResponseGuard) Observe(data []byte, boundary bool) error {
	if g == nil {
		return nil
	}
	root := gjson.ParseBytes(data)
	if responseGuardFailure(root) {
		g.failed = true
		g.attempt.Update(func(r *core.ResponseGuardRecord) {
			r.Outcome, r.Phase, r.Error = "upstream_error", "upstream", "upstream_error"
			status := CodexTerminalHTTPStatus(data)
			if status == 0 {
				status = CodexBootstrapOverloadStatus(data)
			}
			if !g.committed && status > 0 {
				r.Status = status
			}
		})
		return nil
	}
	if g.failed {
		return nil
	}
	response := root
	if root.Get("response").IsObject() {
		response = root.Get("response")
	}
	modelChanged := response.Get("model").Type == gjson.String && response.Get("model").String() != "" && response.Get("model").String() != g.evidence.Model
	if model := response.Get("model"); model.Type == gjson.String && model.String() != "" {
		g.evidence.Model = strings.Clone(model.String())
	}
	completed := response.Get("status").String() == "completed" || root.Get("type").String() == "response.completed" || root.Get("type").String() == "response.done"
	if g.checked && !modelChanged && !completed && !boundary && !response.Get("usage").IsObject() {
		return nil
	}
	g.checked = true
	verdict := g.policy.Evaluate(g.model, g.evidence, boundary || completed)
	bad := !verdict.Accepted()
	phase := "admission"
	if g.committed {
		phase = "stream"
	}
	if completed && !g.committed {
		phase = "completion"
	}
	outcome := "allowed"
	if bad {
		g.lastVerdict = verdict
		g.observed = true
	}
	if g.observed {
		outcome = "observed"
		if !bad {
			verdict = g.lastVerdict
		}
	}
	block := bad && g.Enforces() && (!g.committed || g.policy.LateMismatch == "abort")
	if block {
		outcome = "blocked"
		if g.committed {
			outcome = "aborted"
		}
		g.rejected = true
	}
	g.attempt.Update(func(r *core.ResponseGuardRecord) {
		r.OriginalModel = safeGuardModel(g.evidence.Model)
		if !r.ClientModelSet && !block {
			r.ResponseModel = r.OriginalModel
		}
		r.Verdict, r.Outcome, r.Phase = verdict, outcome, phase
		r.Completed = r.Completed || completed
		if block && !g.committed {
			r.ResponseModel = ""
			r.Status = 429
		}
		if usage := response.Get("usage"); usage.IsObject() {
			r.Usage = guardUsage(usage)
		}
	})
	if block {
		if g.onReject != nil {
			g.onReject(g.evidence)
		}
		return &core.ResponseGuardError{Policy: g.policy, Committed: g.committed}
	}
	return nil
}

func safeGuardModel(s string) string {
	s = strings.Map(func(r rune) rune {
		if r < 32 || r == 127 {
			return -1
		}
		return r
	}, s)
	if len(s) > 256 {
		return s[:256]
	}
	return s
}

func guardUsage(usage gjson.Result) map[string]int64 {
	result := map[string]int64{}
	for _, key := range []string{"input_tokens", "output_tokens", "total_tokens", "input_tokens_details.cached_tokens", "output_tokens_details.reasoning_tokens", "prompt_tokens", "completion_tokens"} {
		if value := usage.Get(key); value.Type == gjson.Number {
			result[key] = value.Int()
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func (g *CodexResponseGuard) Finish(err error) {
	if g == nil || err == nil || g.rejected {
		return
	}
	g.attempt.Update(func(r *core.ResponseGuardRecord) {
		if !g.observed && !g.failed {
			r.Outcome = "error"
		}
		// Provider messages may contain arbitrary input; retain only a safe category.
		r.Error = "request_failed_or_incomplete"
		var status core.StatusError
		if !g.committed && errors.As(err, &status) {
			r.Status = status.StatusCode()
		}
	})
}

func (g *CodexResponseGuard) UpstreamFailure(status int) {
	if g == nil {
		return
	}
	g.failed = true
	g.attempt.Update(func(r *core.ResponseGuardRecord) {
		r.Outcome, r.Phase, r.Error = "upstream_error", "upstream", "upstream_error"
		if status > 0 {
			r.UpstreamStatus = status
			if !g.committed {
				r.Status = status
			}
		}
	})
}

package auth

import (
	"context"
	"fmt"
	"strings"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

type credentialProbeProxyKey struct{}

// WithCredentialProbeProxy isolates a diagnostic probe from inference proxy pools.
func WithCredentialProbeProxy(ctx context.Context, proxyURL string) context.Context {
	return context.WithValue(ctx, credentialProbeProxyKey{}, proxyURL)
}

// ProbeCredential prepares one explicitly selected credential without invoking
// the scheduler, retrying another account, or changing cooldown/affinity state.
// The callback must finish consuming its stream before returning. Preparation
// still persists required identity metadata through the normal guarded path.
func (m *Manager) ProbeCredential(ctx context.Context, expected *Auth, executor ProviderExecutor, req core.Request, opts core.Options, probe func(context.Context, *Auth, core.Request, core.Options) error) (err error) {
	if m == nil || expected == nil || executor == nil || probe == nil {
		return fmt.Errorf("credential probe is unavailable")
	}
	provider := strings.ToLower(executorKeyFromAuth(expected))
	if IsRetiredGeminiCLIAuth(expected) || provider == "qwen" || provider == "iflow" || !strings.EqualFold(executor.Identifier(), provider) {
		return fmt.Errorf("credential probe requires its active provider executor")
	}
	ctx = m.WithRoutingPolicySnapshot(core.WithSingleAttempt(coreusage.WithStreamDefault(ctx, opts.Stream)))
	ctx = m.withCodexQuotaObservation(ctx)
	ctx, _, releaseProducer, err := m.beginResultPersistenceProducer(ctx)
	if err != nil {
		return err
	}
	defer releaseProducer()
	if !m.authInstallationCurrent(expected) || expected.RuntimeInstanceRetired() {
		return runtimeAuthInstanceRetiredError()
	}
	opts = ensureRequestedModelMetadata(opts, req.Model)
	if preparer, ok := executor.(ProviderRequestPreparer); ok {
		operation := core.RequestOperationExecute
		if opts.Stream {
			operation = core.RequestOperationStream
		}
		prepared, errPrepare := preparer.PrepareProviderRequest(ctx, req, opts, operation)
		if errPrepare != nil {
			return errPrepare
		}
		opts = core.WithProviderPreparedRequest(opts, provider, prepared)
	}
	var auth *Auth
	if proxyURL, override := ctx.Value(credentialProbeProxyKey{}).(string); override {
		auth = expected.Clone()
		auth.ProxyURL = proxyURL
		auth.RuntimeProxyURL = ""
		auth.RuntimeProxyBindingID = ""
	} else {
		auth, err = m.ResolveProxyAuth(ctx, expected)
		if err != nil {
			return err
		}
	}
	if rt := m.roundTripperFor(auth); rt != nil {
		ctx = context.WithValue(ctx, roundTripperContextKey{}, rt)
		ctx = context.WithValue(ctx, "cliproxy.roundtripper", rt)
	}
	auth, err = m.prepareRequestAuth(ctx, executor, auth, opts)
	if err != nil {
		return err
	}
	routing := m.loadAPIKeyModelRouting()
	models := m.executionModelCandidates(auth, req.Model, routing)
	if len(models) == 0 {
		return fmt.Errorf("credential has no upstream model mapping")
	}
	requested := req.Model
	req.Model = models[0]
	req = attachResolvedAPIKeyModelInfo(routing, req, auth, requested, req.Model)
	opts = withSelectedAuthInstanceMetadata(opts, auth)
	ctx, release, active := m.beginCurrentAuthExecution(ctx, auth, executor)
	if !active {
		return runtimeAuthInstanceRetiredError()
	}
	defer func() {
		if release() {
			err = runtimeAuthInstanceRetiredError()
		}
	}()
	opts = m.withResponseGuardAttempt(ctx, auth, req, opts)
	return probe(core.WithResponseGuardAttempt(ctx, opts.ResponseGuard), auth, req, opts)
}

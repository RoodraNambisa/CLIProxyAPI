package cliproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/managementdiag"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

func (s *Service) startCodexState(ctx context.Context) {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	if s.codexStateStopped || s.codexStateCancel != nil {
		return
	}
	stateCtx, cancel := context.WithCancel(ctx)
	s.codexStateCancel = cancel
	s.syncCodexState(s.cfg)
	s.codexStateWG.Add(1)
	go func() { defer s.codexStateWG.Done(); s.runCodexState(stateCtx) }()
}

func (s *Service) runCodexState(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	defer func() {
		codexstate.Default.Sync(internalconfig.CodexStateOverrideConfig{}, nil)
		codexstate.Diagnostic.Sync(internalconfig.CodexStateOverrideConfig{}, nil)
		codexstate.Default.Wait()
		codexstate.Diagnostic.Wait()
	}()
	for {
		s.cfgMu.RLock()
		s.syncCodexState(s.cfg)
		capacity := 1
		if s.cfg != nil {
			capacity = s.cfg.Codex.StateOverride.Resolved().Concurrency
		}
		s.cfgMu.RUnlock()
		codexstate.Diagnostic.TickWithCapacity(ctx, time.Now(), s.acquireCodexState, capacity-codexstate.Default.Running())
		codexstate.Default.TickWithCapacity(ctx, time.Now(), s.acquireCodexState, capacity-codexstate.Diagnostic.Running())
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) syncCodexState(cfg *internalconfig.Config) {
	if cfg == nil || s.coreManager == nil {
		return
	}
	var credentials []codexstate.Credential
	var manualScopes []codexstate.Credential
	var diagnosticScopes []codexstate.Credential
	for _, a := range s.coreManager.List() {
		if helps.StateCredentialAvailable(a) {
			diagnosticScopes = append(diagnosticScopes, helps.StateCredential(a, ""))
		}
		if cfg.Codex.StateOverride.Enabled {
			credentials = append(credentials, helps.ManagedStateModels(cfg, a)...)
			if helps.ManagedStateCredentialEligible(cfg, a) {
				manualScopes = append(manualScopes, helps.StateCredential(a, ""))
			}
		}
	}
	codexstate.Default.Sync(cfg.Codex.StateOverride, credentials, manualScopes...)
	codexstate.Diagnostic.Sync(cfg.Codex.StateOverride, nil, diagnosticScopes...)
}

func (s *Service) acquireCodexState(ctx context.Context, credential codexstate.Credential, policy internalconfig.CodexStateOverrideConfig) (result codexstate.Result, err error) {
	defer func() {
		fields := log.Fields{"auth_id": credential.ID, "model": credential.Model, "status": result.Status, "completed": result.Completed, "returned_model": result.Model, "state_length": len(result.State)}
		if err != nil {
			fields["reason"] = safeStateError(err)
			log.WithFields(fields).Warn("Codex state acquisition failed")
		} else {
			log.WithFields(fields).Info("Codex state acquisition response")
		}
	}()
	a, ok := s.coreManager.GetByID(credential.ID)
	current := helps.StateCredential(a, credential.Model)
	if !ok || current.Owner != credential.Owner || current.Plan != credential.Plan || a.RuntimeInstanceID() != credential.Instance || a.Disabled {
		return result, fmt.Errorf("credential no longer available")
	}
	s.cfgMu.RLock()
	cfg, err := internalconfig.Clone(s.cfg)
	s.cfgMu.RUnlock()
	if err != nil {
		return result, err
	}
	cfg.Codex.StateOverride.Enabled = false
	// Probe traffic has separate usage attribution and cannot inherit payload overrides.
	cfg.Payload = internalconfig.PayloadConfig{}
	session := uuid.NewString()
	payload, _ := json.Marshal(map[string]any{"model": credential.Route, "instructions": "", "input": []map[string]string{{"role": "user", "content": policy.Prompt}}, "tools": []any{}, "stream": false, "store": false})
	opts := core.Options{SourceFormat: translator.FormatOpenAIResponse, OriginalRequest: payload, Headers: http.Header{"Session_id": {session}}, Metadata: map[string]any{core.ExecutionSessionMetadataKey: session}}
	ginCtx := &gin.Context{}
	ginCtx.Set("apiKey", "internal:codex-state-acquisition")
	ctx = helps.WithStateCapture(context.WithValue(ctx, "gin", ginCtx), &result)
	probeProxy := ""
	switch policy.ProxyMode {
	case "direct":
		probeProxy = "direct"
	case "custom":
		probeProxy, err = codexstate.ExpandProxy(policy.ProxyURL)
		if err != nil {
			return result, err
		}
	}
	if probeProxy != "" {
		ctx = coreauth.WithCredentialProbeProxy(ctx, probeProxy)
	}
	exec := executor.NewCodexExecutor(cfg)
	err = s.coreManager.ProbeCredential(ctx, a, exec, core.Request{Model: credential.Route, Payload: payload}, opts, func(execCtx context.Context, selected *coreauth.Auth, req core.Request, options core.Options) error {
		selected = selected.Clone()
		// Credential headers are copied before removing state; tokens remain unchanged.
		for name := range selected.Attributes {
			if strings.EqualFold(strings.TrimPrefix(name, "header:"), "x-codex-turn-state") || strings.EqualFold(strings.TrimPrefix(name, "header:"), "cookie") {
				delete(selected.Attributes, name)
			}
		}
		if probeProxy != "" {
			selected.ProxyURL = probeProxy
		}
		response, errExecute := exec.Execute(execCtx, selected, req, options)
		if errExecute != nil {
			if status, ok := errExecute.(interface{ StatusCode() int }); ok {
				result.Status = status.StatusCode()
			}
			return errExecute
		}
		_ = response
		return nil
	})
	if err == nil && policy.CookieOnly() && policy.CookieVerifyAfterAcquire && codexstate.ValidateAcquisition(policy, credential.Model, result, time.Now()) == "" {
		codexstate.PublishCookieCandidate(ctx, result.Cookies)
		verifyPolicy := policy
		verifyPolicy.CookieVerifyAfterAcquire = false
		verified, verifyErr := s.acquireCodexState(helps.WithCookieVerification(ctx, result.Cookies, policy), credential, verifyPolicy)
		result.Tokens += verified.Tokens
		if verifyErr != nil {
			err = verifyErr
		} else {
			// Verify the sent sample, not new Cookie values returned by the verification.
			verified.Cookies = result.Cookies
			if reason := codexstate.ValidateAcquisition(policy, credential.Model, verified, time.Now()); reason != "" {
				result.FailureReason = "cookie_verification_" + reason
				err = fmt.Errorf("%s", result.FailureReason)
			}
		}
	}
	if err != nil && result.FailureReason == "" {
		result.FailureReason = safeStateError(err)
	}
	return result, err
}

func safeStateError(err error) string {
	// Log upstream JSON errors without ever echoing transport URLs or authorization.
	root := gjson.Parse(err.Error())
	code := root.Get("error.code").String()
	if code != "" && len(code) < 128 {
		message, _ := managementdiag.ProcessText(root.Get("error.message").String(), "safe", 512)
		return code + ": " + message
	}
	return "acquisition_request_failed"
}

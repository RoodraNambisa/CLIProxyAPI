package chatgptweb

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelcompat"
)

// computeState contains exactly one challenge round. It never sends account HTTP requests.
type computeState struct {
	input       SentinelComputeInput
	ctx         context.Context
	env         ConversationTurnstileEnvironment
	p           string
	challenge   SentinelComputeChallenge
	result      SentinelComputeResult
	manager     *SentinelRuntimeManager
	goObserver  *conversationSentinelObserverVM
	sdkObserver *SentinelObserver
	generator   *SentinelGenerator
	hooks       SentinelComputeHooks
	sdkAllowed  bool
	sdkOnly     bool
}

func newComputeState(ctx context.Context, input SentinelComputeInput, policy *sentinelcompat.Policy, manager *SentinelRuntimeManager, hooks SentinelComputeHooks, sdkAllowed, sdkOnly bool) (*computeState, error) {
	var errNormalize error
	input, errNormalize = input.normalized()
	if errNormalize != nil {
		return nil, errNormalize
	}
	if err := input.validate(); err != nil {
		return nil, err
	}
	if hooks.Now == nil {
		start := time.Now()
		hooks.Now = func() time.Time { return input.Clock.Add(time.Since(start)) }
	}
	if hooks.Fetcher == nil {
		hooks.Fetcher = computeSDKFetcher
	}
	s := &computeState{input: input, ctx: ctx, env: input.Environment.native(policy), manager: manager, hooks: hooks, sdkAllowed: sdkAllowed, sdkOnly: sdkOnly, result: SentinelComputeResult{Engine: "go"}}
	if input.Format == "auth" {
		gen, err := NewSentinelGeneratorWithEnvironment(input.Environment.DeviceID, input.Environment.Persona, input.Environment.BrowserEnvironment, hooks.Reader, hooks.Now)
		if err != nil {
			return nil, err
		}
		if input.GeneratorSID != "" {
			gen.sid = input.GeneratorSID
		}
		s.generator = gen
	}
	return s, nil
}

func (s *computeState) requirements(ctx context.Context) (SentinelComputeResult, error) {
	if err := ctx.Err(); err != nil {
		return s.result, err
	}
	var err error
	if s.input.Format == "auth" {
		s.p, err = s.generator.GenerateRequirementsToken()
	} else {
		s.p, err = BuildConversationRequirementsTokenWithEnvironment(s.env, s.env.ScriptSources, s.input.DataBuild, s.hooks.Reader, s.hooks.Now)
	}
	s.result.RequirementsToken = s.p
	return s.result, err
}

func (s *computeState) sdkRequest() SentinelSDKRequest {
	return SentinelSDKRequest{BaseURL: "https://chatgpt.com", SDKURL: s.input.SDKURL, ExpectedSHA256: s.input.SDKSHA256, IntegrityRequired: true, ScriptSources: s.env.ScriptSources, TransportKey: "sentinel-compute", Challenge: s.challenge.native(), RequirementsToken: s.p, Environment: s.env, DeviceID: s.env.DeviceID, Flow: s.input.Flow, Fetcher: s.hooks.Fetcher}
}

func (s *computeState) pinSDK(ctx context.Context) error {
	if s.input.SDKURL == "" {
		resource := DefaultConversationSentinelSDKResource()
		s.input.SDKURL = resource.URL
		s.input.SDKSHA256 = resource.SHA256
	}
	hashes, errHashes := expectedSentinelHashes(SentinelSDKRequest{ExpectedSHA256: s.input.SDKSHA256, IntegrityRequired: s.input.SDKIntegrityRequired})
	if errHashes != nil {
		return computeError("invalid_input", false, errHashes)
	}
	if len(hashes) == 1 {
		s.input.SDKSHA256 = hashes[0]
		return nil
	}
	body, _, _, err := s.hooks.Fetcher(ctx, s.input.SDKURL, sentinelSDKMaxSourceBytes)
	if err != nil {
		return err
	}
	if len(body) == 0 || len(body) > sentinelSDKMaxSourceBytes {
		return computeError("safety_limit", false, nil)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(body))
	if !sentinelHashAllowed(hash, hashes) {
		return computeError("invalid_input", false, nil)
	}
	s.input.SDKSHA256 = hash
	return nil
}

func (s *computeState) startSDKObserver(ctx context.Context) error {
	if s.sdkObserver != nil {
		return nil
	}
	if !s.sdkAllowed || s.manager == nil {
		return computeError("compatibility", true, nil)
	}
	if err := s.pinSDK(ctx); err != nil {
		return err
	}
	observer, err := s.manager.beginObserver(s.ctx, s.sdkRequest(), true)
	if err != nil {
		return err
	}
	if observer == nil {
		return computeError("sdk_unavailable", true, nil)
	}
	s.sdkObserver = observer
	if err = observer.wait(ctx); err != nil {
		return err
	}
	s.result.Engine = "sdk"
	return nil
}

func (s *computeState) observer(ctx context.Context) error {
	if !s.challenge.ObserverRequired {
		return nil
	}
	if s.sdkOnly {
		return s.startSDKObserver(ctx)
	}
	observer, err := newConversationSentinelObserverVM(s.ctx, s.challenge.CollectorDX, s.challenge.SnapshotDX, s.p, s.env, s.hooks.Reader, s.hooks.Now)
	if err == nil {
		s.goObserver = observer
		return nil
	}
	var compatibility *SentinelCompatibilityError
	if !errors.As(err, &compatibility) || !s.sdkAllowed {
		return err
	}
	return s.startSDKObserver(ctx)
}

func (s *computeState) proof(ctx context.Context) error {
	if s.result.ProofToken != "" {
		return nil
	}
	var err error
	c := s.challenge
	if s.generator != nil {
		if c.ProofRequired {
			s.result.ProofToken, err = s.generator.GenerateProofContext(ctx, c.ProofSeed, c.ProofDifficulty)
		} else {
			s.result.ProofToken, err = s.generator.GenerateRequirementsToken()
		}
	} else if c.ProofRequired {
		s.result.ProofToken, err = BuildConversationProofTokenWithEnvironment(ctx, c.ProofSeed, c.ProofDifficulty, s.env, s.env.ScriptSources, s.input.DataBuild, s.hooks.Reader, s.hooks.Now)
	}
	return err
}

func (s *computeState) turnstile(ctx context.Context) error {
	if !s.challenge.TurnstileRequired || s.result.TurnstileToken != "" {
		return nil
	}
	var err error
	if !s.sdkOnly {
		s.result.TurnstileToken, err = (GoConversationTurnstileSolver{}).Solve(ctx, ConversationTurnstileSolveRequest{DX: s.challenge.TurnstileDX, RequirementsToken: s.p, Environment: s.env, Reader: s.hooks.Reader, Now: s.hooks.Now})
		if err == nil {
			return nil
		}
		var compatibility *SentinelCompatibilityError
		if !errors.As(err, &compatibility) || !s.sdkAllowed {
			return err
		}
	}
	if !s.sdkAllowed || s.manager == nil {
		return computeError("compatibility", true, err)
	}
	if err = s.pinSDK(ctx); err != nil {
		return err
	}
	if s.challenge.ObserverRequired {
		if err = s.startSDKObserver(ctx); err != nil {
			return err
		}
	}
	s.result.TurnstileToken, err = s.manager.solveTurnstileWithSDK(ctx, s.sdkRequest(), nil, "", sentinelSDKCompatibilityFallback, s.sdkObserver, s.challenge.TurnstileDX, s.p)
	if err == nil {
		s.result.Engine = "sdk"
	}
	return err
}

func (s *computeState) solve(ctx context.Context, challenge SentinelComputeChallenge) (SentinelComputeResult, error) {
	if err := challenge.validate(); err != nil {
		return s.result, err
	}
	s.challenge = challenge
	if err := s.observer(ctx); err != nil {
		return s.result, err
	}
	// Authentication and conversation flows have different established computation order.
	if s.input.Format == "auth" {
		if err := s.turnstile(ctx); err != nil {
			return s.result, err
		}
		if err := s.proof(ctx); err != nil {
			return s.result, err
		}
	} else {
		if err := s.proof(ctx); err != nil {
			return s.result, err
		}
		if err := s.turnstile(ctx); err != nil {
			return s.result, err
		}
	}
	return s.result, nil
}

func (s *computeState) snapshot(ctx context.Context) (SentinelComputeResult, error) {
	if !s.challenge.ObserverRequired {
		return s.result, nil
	}
	var err error
	if s.goObserver != nil && s.sdkObserver == nil {
		s.result.SnapshotToken, err = s.goObserver.Snapshot(ctx)
		if err == nil {
			return s.result, nil
		}
		var compatibility *SentinelCompatibilityError
		if !errors.As(err, &compatibility) || !s.sdkAllowed {
			return s.result, err
		}
	}
	if err = s.startSDKObserver(ctx); err != nil {
		return s.result, err
	}
	s.result.SnapshotToken, err = s.sdkObserver.Snapshot(ctx)
	return s.result, err
}

func (s *computeState) close() {
	if s.goObserver != nil {
		s.goObserver.Close()
		s.goObserver = nil
	}
	if s.sdkObserver != nil {
		s.sdkObserver.Close()
		s.sdkObserver = nil
	}
}

var computeSDKHTTP = &http.Client{Transport: (&http.Transport{Proxy: nil, MaxIdleConns: 16, MaxIdleConnsPerHost: 8}).Clone(), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

func computeSDKFetcher(ctx context.Context, target string, maxBytes int64) ([]byte, string, string, error) {
	u, err := url.Parse(target)
	if err != nil || validateSentinelSDKURL(u) != nil {
		return nil, "", "", computeError("invalid_input", false, nil)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, "", "", err
	}
	response, err := computeSDKHTTP.Do(req)
	if err != nil {
		return nil, "", "", computeError("sdk_unavailable", true, err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil, "", "", computeError("sdk_unavailable", true, nil)
	}
	if maxBytes < 1 || maxBytes > sentinelSDKMaxSourceBytes {
		maxBytes = sentinelSDKMaxSourceBytes
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxBytes+1))
	if err == nil && int64(len(body)) > maxBytes {
		err = computeError("safety_limit", false, nil)
	}
	return body, response.Header.Get("Content-Type"), target, err
}

package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type modelCapabilityProbeExecutor struct {
	*authFallbackExecutor
	probe func(context.Context, *Auth, core.Request) error
}

func (e *modelCapabilityProbeExecutor) Execute(ctx context.Context, auth *Auth, req core.Request, opts core.Options) (core.Response, error) {
	if err := e.probe(ctx, auth, req); err != nil {
		return core.Response{}, err
	}
	return e.authFallbackExecutor.Execute(ctx, auth, req, opts)
}

func (e *modelCapabilityProbeExecutor) CountTokens(ctx context.Context, auth *Auth, req core.Request, opts core.Options) (core.Response, error) {
	if err := e.probe(ctx, auth, req); err != nil {
		return core.Response{}, err
	}
	return e.authFallbackExecutor.CountTokens(ctx, auth, req, opts)
}

func (e *modelCapabilityProbeExecutor) ExecuteStream(ctx context.Context, auth *Auth, req core.Request, opts core.Options) (*core.StreamResult, error) {
	if err := e.probe(ctx, auth, req); err != nil {
		return nil, err
	}
	return e.authFallbackExecutor.ExecuteStream(ctx, auth, req, opts)
}

func runModelCapabilityProbe(ctx context.Context, m *Manager, provider, mode string, req core.Request, opts core.Options) error {
	switch mode {
	case "count":
		_, err := m.ExecuteCount(ctx, []string{provider}, req, opts)
		return err
	case "stream":
		stream, err := m.ExecuteStream(ctx, []string{provider}, req, opts)
		if stream != nil {
			for chunk := range stream.Chunks {
				if chunk.Err != nil {
					err = chunk.Err
				}
			}
		}
		return err
	default:
		_, err := m.Execute(ctx, []string{provider}, req, opts)
		return err
	}
}

func TestAPIKeyExecutionBindsSelectedCredentialCapabilities(t *testing.T) {
	for _, mode := range []string{"execute", "count", "stream"} {
		for _, imageOverride := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/images=%t", mode, imageOverride), func(t *testing.T) {
				configuration := func(secondLevel string) *config.Config {
					return &config.Config{NoCooldownStatusCodes: []int{500}, CodexKey: []config.CodexKey{
						{APIKey: "a", Models: []config.CodexModel{{Name: "upstream", Alias: "shared", Thinking: &registry.ThinkingSupport{Levels: []string{"low"}}}}},
						{APIKey: "b", Models: []config.CodexModel{{Name: "upstream", Alias: "shared", IsCompat: secondLevel == "max", Thinking: &registry.ThinkingSupport{Levels: []string{secondLevel}}}}},
					}}
				}
				m := NewManager(nil, &FillFirstSelector{}, nil)
				m.SetConfig(configuration("high"))
				m.SetRetryConfig(0, 0, 0)
				req := core.Request{Model: "shared", Payload: []byte(`{"input":"fixture"}`), Metadata: map[string]any{"keep": "fixture"}}
				opts := core.Options{}
				if imageOverride {
					req.Model = "gpt-image-2"
					opts.Metadata = map[string]any{core.ExecutionModelOverrideMetadataKey: "shared"}
				}
				var seen []string
				e := &modelCapabilityProbeExecutor{authFallbackExecutor: &authFallbackExecutor{id: "codex"}}
				e.probe = func(ctx context.Context, auth *Auth, attempt core.Request) error {
					info, ok := ResolvedAPIKeyModelInfo(attempt)
					if !ok || info.UserDefined || info.Thinking == nil || attempt.Model != "upstream" {
						return fmt.Errorf("selected attempt has no exact capability binding")
					}
					if enabled, ok := CodexMultiAgentV2RequestSetting(ctx); !ok || enabled {
						return fmt.Errorf("logical request policy changed between credentials")
					}
					if info.IsCompat != (auth.ID == "b") {
						return fmt.Errorf("selected attempt inherited another credential's compatibility policy")
					}
					seen = append(seen, auth.ID+":"+info.Thinking.Levels[0])
					if auth.ID == "a" {
						next := configuration("max")
						next.Codex.OptimizeMultiAgentV2 = true
						m.SetConfig(next)
						retained, _ := ResolvedAPIKeyModelInfo(attempt)
						if retained.Thinking.Levels[0] != "low" || retained.IsCompat {
							return fmt.Errorf("hot update changed an already bound attempt")
						}
						return &Error{HTTPStatus: 500, Message: "fixture failure"}
					}
					return nil
				}
				m.RegisterExecutor(e)
				for _, id := range []string{"a", "b"} {
					registerFallbackAuthForModel(t, m, &Auth{ID: id, Provider: "codex", Attributes: map[string]string{"api_key": id}}, req.Model)
				}
				if err := runModelCapabilityProbe(t.Context(), m, "codex", mode, req, opts); err != nil {
					t.Fatal(err)
				}
				if strings.Join(seen, ",") != "a:low,b:max" {
					t.Fatalf("credential bindings or existing attempt budget changed: %v", seen)
				}
				if len(req.Metadata) != 1 {
					t.Fatal("executor capability metadata leaked into the original request")
				}
			})
		}
	}
}

func TestAPIKeyExecutionModelPoolKeepsSnapshotAndStopsOnCancel(t *testing.T) {
	for _, mode := range []string{"execute", "count", "stream"} {
		for _, cancelRequest := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cancel=%t", mode, cancelRequest), func(t *testing.T) {
				cfg := configuredAttemptFixture("compat", "old", false)
				cfg.NoCooldownStatusCodes = []int{500}
				cfg.OpenAICompatibility[0].Models[0].Thinking = &registry.ThinkingSupport{Levels: []string{"low"}}
				m := NewManager(nil, nil, nil)
				m.SetConfig(cfg)
				m.SetRetryConfig(0, 0, 0)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				var seen []string
				e := &modelCapabilityProbeExecutor{authFallbackExecutor: &authFallbackExecutor{id: "compat"}}
				e.probe = func(_ context.Context, _ *Auth, req core.Request) error {
					info, ok := ResolvedAPIKeyModelInfo(req)
					if !ok || info.Thinking == nil {
						return fmt.Errorf("pooled attempt has no capability binding")
					}
					seen = append(seen, req.Model+":"+info.Thinking.Levels[0])
					if req.Model == "old-first" {
						next := configuredAttemptFixture("compat", "old", false)
						next.NoCooldownStatusCodes = []int{500}
						next.OpenAICompatibility[0].Models[1].Thinking = &registry.ThinkingSupport{Levels: []string{"max"}}
						m.SetConfig(next)
						if cancelRequest {
							cancel()
							return context.Canceled
						}
						return &Error{HTTPStatus: 500, Message: "fixture model failure"}
					}
					return nil
				}
				m.RegisterExecutor(e)
				registerFallbackAuthForModel(t, m, &Auth{ID: "one", Provider: "compat", Attributes: map[string]string{"api_key": "fixture", "compat_name": "compat"}}, "shared")
				err := runModelCapabilityProbe(ctx, m, "compat", mode, core.Request{Model: "shared", Payload: []byte(`{}`)}, core.Options{})
				want := "old-first:low,old-second:high"
				if cancelRequest {
					want = "old-first:low"
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("lost request cancellation: %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				if strings.Join(seen, ",") != want {
					t.Fatalf("pooled capabilities or attempt budget changed: %v", seen)
				}
			})
		}
	}
}

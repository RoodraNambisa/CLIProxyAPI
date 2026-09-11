package auth

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

const modelNotFoundFixture = `{"error":{"type":"invalid_request_error","code":"model_not_found","message":"The model gpt-5.5 does not exist or you do not have access to it."}}`

func TestModelNotFoundIsNotCallerFault(t *testing.T) {
	for _, status := range []int{400, 404, 422} {
		for _, body := range []string{
			modelNotFoundFixture,
			`{"response":{"error":{"type":"invalid_request_error","code":"model_not_found"}}}`,
			`{"body":{"error":{"type":"invalid_request_error","code":"model_not_found_error"}}}`,
			`{"error":{"type":"invalid_request_error","message":"Model not found gpt-5.5"}}`,
		} {
			err := fmt.Errorf("transport: %w", &Error{HTTPStatus: status, Message: body})
			if isKnownRequestFault(err) || isRequestInvalidErrorWithRules(err, nil) {
				t.Fatalf("status %d model availability failure stopped credential rotation", status)
			}
		}
	}
}

func TestModelNotFoundKeepsRequestAndCredentialBoundaries(t *testing.T) {
	for _, body := range []string{
		`{"error":{"type":"invalid_request_error","message":"The model not found in request body"}}`,
		`{"error":{"type":"invalid_request_error","code":"previous_response_not_found"}}`,
		`{"error":{"type":"misalignment_policy_violation","code":"model_not_found"}}`,
		`{"error":{"type":"invalid_request_error","code":"cyber_policy","message":"Model not found gpt-5.5"}}`,
	} {
		if !isKnownRequestFault(&Error{HTTPStatus: 404, Message: body}) {
			t.Fatal("model recognition bypassed a request fault or policy refusal")
		}
	}
	for _, status := range []int{http.StatusUnauthorized, http.StatusPaymentRequired, http.StatusTooManyRequests} {
		if isModelSupportError(&Error{HTTPStatus: status, Message: modelNotFoundFixture}) {
			t.Fatalf("model recognition overrode credential status %d", status)
		}
	}
}

func TestNotFoundUsesTenMinuteModelCooldown(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"structured-400", modelNotFoundFixture, 400},
		{"structured-404", modelNotFoundFixture, 404},
		{"plain-model-404", "Model not found gpt-5.5", 404},
		{"generic-404", "Not Found", 404},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := NewManager(nil, nil, nil)
			auth, err := manager.Register(t.Context(), &Auth{ID: "not-found-" + tc.name, Provider: "codex"})
			if err != nil {
				t.Fatal(err)
			}
			manager.MarkResult(t.Context(), Result{AuthID: auth.ID, Provider: auth.Provider, Model: "gpt-5.5(high)", Error: &Error{HTTPStatus: tc.status, Message: tc.body}})
			current, _ := manager.GetByID(auth.ID)
			state := current.ModelStates["gpt-5.5"]
			if state == nil || !state.Unavailable || state.Quota.Exceeded {
				t.Fatal("model unavailability was missing or incorrectly recorded as quota exhaustion")
			}
			remaining := time.Until(state.NextRetryAfter)
			if remaining < 9*time.Minute || remaining > 10*time.Minute {
				t.Fatalf("cooldown = %v, want approximately 10 minutes", remaining)
			}
			if blocked, _, _ := isAuthBlockedForModel(current, "gpt-5.5(low)", time.Now()); !blocked {
				t.Fatal("a reasoning suffix bypassed the model cooldown")
			}
			if blocked, _, _ := isAuthBlockedForModel(current, "gpt-5.4", time.Now()); blocked {
				t.Fatal("model failure cooled the whole credential")
			}
		})
	}
}

func TestNotFoundWithoutModelDoesNotCoolCredential(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	auth, err := manager.Register(t.Context(), &Auth{ID: "not-found-no-model", Provider: "codex"})
	if err != nil {
		t.Fatal(err)
	}
	manager.MarkResult(t.Context(), Result{AuthID: auth.ID, Provider: auth.Provider, Error: &Error{HTTPStatus: 404, Message: "Not Found"}})
	current, _ := manager.GetByID(auth.ID)
	if current.Unavailable || !current.NextRetryAfter.IsZero() || current.CooldownScope == cooldownScopeAuth {
		t.Fatal("a missing model key caused a credential-wide 404 cooldown")
	}
}

func TestNotFoundHonorsCoolingControls(t *testing.T) {
	for _, skip := range []string{"disabled", "status-exception", "custom-model-duration", "custom-auth-scope"} {
		t.Run(skip, func(t *testing.T) {
			manager := NewManager(nil, nil, nil)
			credential := &Auth{ID: "not-found-control-" + skip, Provider: "codex"}
			switch skip {
			case "disabled":
				credential.Metadata = map[string]any{"disable_cooling": true}
			case "status-exception":
				manager.SetConfig(&config.Config{NoCooldownStatusCodes: []int{404}})
			case "custom-model-duration":
				manager.SetConfig(&config.Config{FixedErrorCooldowns: []config.FixedErrorCooldownRule{{StatusCode: 404, CooldownSeconds: 120, Scope: "model"}}})
			case "custom-auth-scope":
				manager.SetConfig(&config.Config{FixedErrorCooldowns: []config.FixedErrorCooldownRule{{StatusCode: 404, CooldownSeconds: 120, Scope: "auth"}}})
			}
			if _, err := manager.Register(t.Context(), credential); err != nil {
				t.Fatal(err)
			}
			manager.MarkResult(t.Context(), Result{AuthID: credential.ID, Provider: "codex", Model: "gpt-5.5", Error: &Error{HTTPStatus: 404, Message: modelNotFoundFixture}})
			current, _ := manager.GetByID(credential.ID)
			blocked, _, _ := isAuthBlockedForModel(current, "gpt-5.5", time.Now())
			custom := skip == "custom-model-duration" || skip == "custom-auth-scope"
			if blocked != custom {
				t.Fatal("404 changed the configured cooling control")
			}
			if custom {
				remaining := time.Until(current.ModelStates["gpt-5.5"].NextRetryAfter)
				if remaining < time.Minute || remaining > 2*time.Minute {
					t.Fatal("explicit model cooldown duration was not preserved")
				}
				if blocked, _, _ := isAuthBlockedForModel(current, "gpt-5.4", time.Now()); blocked {
					t.Fatal("a model-not-found rule blocked unrelated models")
				}
			}
		})
	}
}

func TestModelNotFoundRotatesAllManagerEntrypoints(t *testing.T) {
	for _, operation := range []string{"execute", "count", "stream"} {
		for _, status := range []int{400, 404} {
			t.Run(fmt.Sprintf("%s/%d", operation, status), func(t *testing.T) {
				manager := NewManager(nil, &FillFirstSelector{}, nil)
				manager.SetRetryConfig(0, 0, 2)
				failures := map[string]error{"a-model-not-found": &Error{HTTPStatus: status, Message: modelNotFoundFixture}}
				exec := &authFallbackExecutor{id: "codex", executeErrors: failures, countErrors: failures, streamFirstErrors: failures}
				manager.RegisterExecutor(exec)
				for _, id := range []string{"a-model-not-found", "b-model-available"} {
					registerFallbackAuthForModel(t, manager, &Auth{ID: id, Provider: "codex"}, "gpt-5.5")
				}
				req := core.Request{Model: "gpt-5.5", Payload: []byte(`{"input":"local fixture"}`)}
				var err error
				switch operation {
				case "execute":
					_, err = manager.Execute(t.Context(), []string{"codex"}, req, core.Options{})
				case "count":
					_, err = manager.ExecuteCount(t.Context(), []string{"codex"}, req, core.Options{})
				case "stream":
					var stream *core.StreamResult
					stream, err = manager.ExecuteStream(t.Context(), []string{"codex"}, req, core.Options{})
					if stream != nil {
						for chunk := range stream.Chunks {
							if chunk.Err != nil {
								err = chunk.Err
							}
						}
					}
				}
				if err != nil {
					t.Fatalf("model failure prevented fallback: %v", err)
				}
				calls := len(exec.ExecuteCalls()) + len(exec.CountCalls()) + len(exec.StreamCalls())
				if calls != 2 {
					t.Fatalf("upstream calls = %d, want 2", calls)
				}
				failed, _ := manager.GetByID("a-model-not-found")
				if state := failed.ModelStates["gpt-5.5"]; state == nil || state.LastError == nil || state.LastError.Code != "model_not_found" {
					t.Fatal("fallback lost the structured model failure")
				}
			})
		}
	}
}

func TestNotFoundCooldownKeepsRouteForRecovery(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	exec := &authFallbackExecutor{id: "codex"}
	manager.RegisterExecutor(exec)
	const model = "not-found-recovery-model"
	const id = "not-found-recovery-auth"
	registerFallbackAuthForModel(t, manager, &Auth{ID: id, Provider: "codex"}, model)
	// Concurrent requests can report a different failure before this 404 arrives.
	manager.MarkResult(t.Context(), Result{AuthID: id, Provider: "codex", Model: model, Error: &Error{HTTPStatus: 401, Message: "authentication fixture"}})
	manager.MarkResult(t.Context(), Result{AuthID: id, Provider: "codex", Model: model, Error: &Error{HTTPStatus: 404, Message: modelNotFoundFixture}})
	reg := registry.GetGlobalRegistry()
	if len(reg.GetModelProviders(model)) != 1 {
		t.Fatal("temporary model cooldown hid its provider and prevented later recovery")
	}
	listed := false
	for _, item := range reg.GetAvailableModels("openai") {
		listed = listed || item["id"] == model
	}
	if !listed {
		t.Fatal("temporary model cooldown removed the model from the client catalog")
	}
	if _, err := manager.Execute(t.Context(), []string{"codex"}, core.Request{Model: model}, core.Options{}); err == nil || len(exec.ExecuteCalls()) != 0 {
		t.Fatal("keeping the route bypassed active model cooling")
	}
	manager.mu.Lock()
	current := manager.auths[id]
	current.ModelStates[model].NextRetryAfter = time.Now().Add(-time.Second)
	current.NextRetryAfter = current.ModelStates[model].NextRetryAfter
	snapshot := current.Clone()
	manager.mu.Unlock()
	manager.scheduler.upsertAuth(snapshot)
	if _, err := manager.Execute(t.Context(), []string{"codex"}, core.Request{Model: model}, core.Options{}); err != nil {
		t.Fatalf("expired model cooldown required manual recovery: %v", err)
	}
	if len(exec.ExecuteCalls()) != 1 || len(reg.GetModelProviders(model)) != 1 {
		t.Fatal("recovery did not restore normal routing")
	}
}

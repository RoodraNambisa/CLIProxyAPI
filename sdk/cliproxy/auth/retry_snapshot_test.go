package auth

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type retryReloadPreparer struct {
	*authFallbackExecutor
	manager *Manager
}

func (e *retryReloadPreparer) PrepareProviderRequest(context.Context, core.Request, core.Options, core.RequestOperation) (any, error) {
	e.manager.SetRetryConfig(0, 0, 0)
	return nil, nil
}

func TestRequestRetrySettingsCapturedBeforeProviderPreparation(t *testing.T) {
	for _, mode := range []string{"execute", "count", "stream"} {
		t.Run(mode, func(t *testing.T) {
			manager := NewManager(nil, nil, nil)
			manager.SetRetryConfig(1, 0, 0)
			manager.SetConfig(&config.Config{NoCooldownStatusCodes: []int{500}})
			errs := map[string]error{"a": &Error{HTTPStatus: 500, Message: "test retry failure"}}
			executor := &retryReloadPreparer{manager: manager, authFallbackExecutor: &authFallbackExecutor{id: "claude", executeErrors: errs, countErrors: errs, streamFirstErrors: errs}}
			manager.RegisterExecutor(executor)
			registerFallbackAuthForModel(t, manager, &Auth{ID: "a", Provider: "claude"}, "snapshot-model")
			for requestIndex, want := range []int{2, 3} {
				err := runCredentialRetryOperation(t.Context(), manager, mode, core.Request{Model: "snapshot-model"}, core.Options{})
				if err == nil {
					t.Fatal("missing synthetic failure")
				}
				if calls := len(executor.ExecuteCalls()) + len(executor.CountCalls()) + len(executor.StreamCalls()); calls != want {
					t.Fatalf("request %d: total calls=%d want=%d", requestIndex, calls, want)
				}
			}
		})
	}
}

func TestRetrySettingsDoNotNarrowGoIntegers(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	large := int64(math.MaxInt32) + 1
	if int64(int(large)) != large {
		t.Skip("Go int is 32 bits")
	}
	manager.SetRetryConfig(int(large), time.Minute, int(large+1))
	retries, credentials, wait := manager.retrySettings()
	if int64(retries) != large || int64(credentials) != large+1 || wait != time.Minute {
		t.Fatal("retry settings overflowed a narrower counter")
	}
}

func TestRetrySettingsSnapshotOwnershipAndDefaults(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	initial := manager.WithRoutingPolicySnapshot(nil)
	manager.SetRetryConfig(2, time.Second, 3)
	if retries, credentials, wait := manager.retrySettings(initial); retries != 0 || credentials != 0 || wait != 0 {
		t.Fatal("unconfigured request adopted a later retry policy")
	}
	old := manager.WithRoutingPolicySnapshot(t.Context())
	manager.SetRetryConfig(-1, -time.Second, -1)
	if manager.WithRoutingPolicySnapshot(old) != old {
		t.Fatal("repeated SDK entry replaced the logical request snapshot")
	}
	if retries, credentials, wait := manager.retrySettings(old); retries != 2 || credentials != 3 || wait != time.Second {
		t.Fatal("an in-flight retry snapshot changed")
	}
	if retries, credentials, wait := manager.retrySettings(manager.WithRoutingPolicySnapshot(t.Context())); retries != 0 || credentials != 0 || wait != 0 {
		t.Fatal("new request did not use normalized settings")
	}
	other := NewManager(nil, nil, nil)
	other.SetRetryConfig(5, 7*time.Second, 6)
	if retries, credentials, wait := other.retrySettings(other.WithRoutingPolicySnapshot(old)); retries != 5 || credentials != 6 || wait != 7*time.Second {
		t.Fatal("another manager inherited the first manager's retry policy")
	}
}

func TestRetrySettingsPublishedAsOneValue(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	manager.SetRetryConfig(1, time.Nanosecond, 1)
	var readers sync.WaitGroup
	for range 8 {
		readers.Go(func() {
			for range 1000 {
				ctx := manager.WithRoutingPolicySnapshot(t.Context())
				retries, credentials, wait := manager.retrySettings(ctx)
				if retries != credentials || int64(retries) != wait.Nanoseconds() {
					t.Error("a request observed mixed retry revisions")
					return
				}
			}
		})
	}
	for index := range 1000 {
		manager.SetRetryConfig(index, time.Duration(index), index)
	}
	readers.Wait()
}

package auth

import (
	"context"
	"errors"
	"testing"
	"time"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestCodexLiveSessionLeaseSeparatesSetupAndSessionLifetime(t *testing.T) {
	m, a, _ := newCodexLiveLeaseFixture(t)
	setup, cancelSetup := context.WithCancel(context.WithValue(t.Context(), "gin", &struct{ Body []byte }{Body: make([]byte, 1<<20)}))
	defer cancelSetup()
	lifetime, cancelLifetime := context.WithCancel(context.Background())
	defer cancelLifetime()
	lease, errAcquire := m.AcquireCodexLiveSession(setup, lifetime, "live-model", core.Options{})
	if errAcquire != nil {
		t.Fatal(errAcquire)
	}
	defer lease.Close()
	if lease.Context().Value("gin") != nil {
		t.Fatal("persistent lease retained the setup request context")
	}
	if errCommit := lease.CommitUpstream(); errCommit != nil {
		t.Fatal(errCommit)
	}
	cancelSetup()
	if lease.Context().Err() != nil || lease.CloneAuth().RuntimeInstanceID() != a.RuntimeInstanceID() {
		t.Fatal("returning the creating request invalidated the selected session")
	}
	cancelLifetime()
	select {
	case <-lease.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("session lifetime cancellation was lost")
	}
}

func TestCodexLiveSessionLeaseStillRetiresExactCredentialInstance(t *testing.T) {
	for _, remove := range []bool{false, true} {
		t.Run(map[bool]string{false: "replace", true: "delete"}[remove], func(t *testing.T) {
			m, a, _ := newCodexLiveLeaseFixture(t)
			lease, errAcquire := m.AcquireCodexLiveSession(t.Context(), t.Context(), "live-model", core.Options{})
			if errAcquire != nil {
				t.Fatal(errAcquire)
			}
			defer lease.Close()
			if remove {
				if errDelete := m.Delete(t.Context(), a.ID); errDelete != nil {
					t.Fatal(errDelete)
				}
			} else {
				a.Metadata["access_token"] = "replacement-fixture"
				if _, errUpdate := m.Update(t.Context(), a); errUpdate != nil {
					t.Fatal(errUpdate)
				}
			}
			select {
			case <-lease.Context().Done():
			case <-time.After(time.Second):
				t.Fatal("persistent lease ignored credential retirement")
			}
		})
	}
}

func TestCodexLiveSessionLeaseRejectsCancelledSetupAndLifetime(t *testing.T) {
	for _, cancelWhich := range []string{"setup", "lifetime", "missing"} {
		t.Run(cancelWhich, func(t *testing.T) {
			m, a, _ := newCodexLiveLeaseFixture(t)
			setup, cancelSetup := context.WithCancel(t.Context())
			defer cancelSetup()
			lifetime, cancelLifetime := context.WithCancel(t.Context())
			defer cancelLifetime()
			if cancelWhich == "setup" {
				cancelSetup()
			}
			if cancelWhich == "lifetime" {
				cancelLifetime()
			}
			if cancelWhich == "missing" {
				lifetime = nil
			}
			lease, errAcquire := m.AcquireCodexLiveSession(setup, lifetime, "live-model", core.Options{})
			if lease != nil {
				lease.Close()
				t.Fatal("cancelled or missing context acquired a lease")
			}
			if errAcquire == nil || (cancelWhich != "missing" && !errors.Is(errAcquire, context.Canceled)) {
				t.Fatalf("setup cancellation cause lost: %v", errAcquire)
			}
			if available, _ := m.authRequestLimiter().availableAt(a.ID, m.routingAuthRequestLimitPolicyForAuth(a), time.Now()); !available {
				t.Fatal("rejected persistent setup retained capacity")
			}
		})
	}
}

func TestCodexLiveSessionLeaseManagerShutdownCancelsPersistentProducer(t *testing.T) {
	m, _, _ := newCodexLiveLeaseFixture(t)
	setup, cancelSetup := context.WithCancel(t.Context())
	defer cancelSetup()
	lease, errAcquire := m.AcquireCodexLiveSession(setup, t.Context(), "live-model", core.Options{})
	if errAcquire != nil {
		t.Fatal(errAcquire)
	}
	defer lease.Close()
	cancelSetup()
	m.resultProducerWaitTimeout = time.Millisecond
	m.resultProducerCancelWait = time.Second
	m.stopAcceptingResultPersistenceProducers()
	m.waitForResultPersistenceProducers()
	if lease.Context().Err() == nil {
		t.Fatal("manager shutdown left the persistent lease alive")
	}
	m.resultProducerMu.Lock()
	remaining, abandoned := len(m.resultProducers), m.resultProducerAbandoned
	m.resultProducerMu.Unlock()
	if remaining != 0 || abandoned != 0 {
		t.Fatal("persistent lease required forced abandonment")
	}
}

package auth

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type spilloverExecutor struct {
	schedulerTestExecutor
	entered chan string
	release chan struct{}
}

func (e *spilloverExecutor) Execute(ctx context.Context, a *Auth, _ core.Request, _ core.Options) (core.Response, error) {
	e.entered <- a.ID
	if a.ID == "a" {
		select {
		case <-e.release:
		case <-ctx.Done():
			return core.Response{}, ctx.Err()
		}
	}
	return core.Response{}, nil
}
func spilloverFixture(t *testing.T, queue bool, timeout int) (*Manager, *SessionAffinitySelector, *spilloverExecutor, core.Options) {
	t.Helper()
	selector := NewSessionAffinitySelector(&FillFirstSelector{})
	m := NewManager(nil, selector, nil)
	e := &spilloverExecutor{entered: make(chan string, 10), release: make(chan struct{})}
	m.RegisterExecutor(e)
	m.SetConfig(&config.Config{Routing: config.RoutingConfig{Strategy: "fill-first", SessionAffinity: true, SessionAffinityCapacitySpillover: true, PerAuthConcurrency: 1, Queue: config.AdmissionQueue{Enabled: queue, MaxWaiting: 1, WaitTimeoutSeconds: timeout}}})
	for _, id := range []string{"a", "b"} {
		if _, err := m.Register(t.Context(), &Auth{ID: id, Provider: "test"}); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(selector.Stop)
	t.Cleanup(m.admission.Close)
	opts := core.Options{Headers: http.Header{"Session-Id": {"session"}}}
	selector.BindSession(t.Context(), "test", "", opts, "a")
	return m, selector, e, opts
}
func TestStickyCapacitySpilloverPreservesBinding(t *testing.T) {
	for _, queue := range []bool{false, true} {
		t.Run(map[bool]string{false: "immediate", true: "timeout"}[queue], func(t *testing.T) {
			m, s, e, opts := spilloverFixture(t, queue, 1)
			first := make(chan error, 1)
			go func() { _, err := m.Execute(t.Context(), []string{"test"}, core.Request{}, opts); first <- err }()
			if <-e.entered != "a" {
				t.Fatal("first")
			}
			if _, err := m.Execute(t.Context(), []string{"test"}, core.Request{}, opts); err != nil {
				t.Fatal(err)
			}
			if got := <-e.entered; got != "b" {
				t.Fatal(got)
			}
			if got := s.cachedAuthID("test", "", opts, t.Context()); got != "a" {
				t.Fatalf("binding moved to %s", got)
			}
			close(e.release)
			if err := <-first; err != nil {
				t.Fatal(err)
			}
			if _, err := m.Execute(t.Context(), []string{"test"}, core.Request{}, opts); err != nil {
				t.Fatal(err)
			}
			if <-e.entered != "a" {
				t.Fatal("did not return")
			}
			awaitAdmission(t, func() bool { return m.admission.Overview(time.Now(), 60, false, false).Active == 0 })
		})
	}
}
func TestStickyCapacitySpilloverQueueFirstAndFull(t *testing.T) {
	m, s, e, opts := spilloverFixture(t, true, 0)
	first := make(chan error, 1)
	go func() { _, err := m.Execute(t.Context(), []string{"test"}, core.Request{}, opts); first <- err }()
	<-e.entered
	waiting := make(chan error, 1)
	go func() { _, err := m.Execute(t.Context(), []string{"test"}, core.Request{}, opts); waiting <- err }()
	awaitAdmission(t, func() bool { n, _ := m.admission.Queue.Counts(""); return n == 1 })
	select {
	case id := <-e.entered:
		t.Fatalf("did not wait: %s", id)
	default:
	}
	if _, err := m.Execute(t.Context(), []string{"test"}, core.Request{}, opts); err != nil {
		t.Fatal(err)
	}
	if <-e.entered != "b" {
		t.Fatal("full queue did not borrow")
	}
	if s.cachedAuthID("test", "", opts, t.Context()) != "a" {
		t.Fatal("rebound")
	}
	close(e.release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-waiting; err != nil {
		t.Fatal(err)
	}
	if <-e.entered != "a" {
		t.Fatal("wait did not use original")
	}
}

func TestStickyCapacitySpilloverCancellationAndNoAlternative(t *testing.T) {
	for _, remove := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "no_alternative"}[remove], func(t *testing.T) {
			m, s, e, opts := spilloverFixture(t, !remove, 0)
			if remove {
				if err := m.Delete(t.Context(), "b"); err != nil {
					t.Fatal(err)
				}
			}
			first := make(chan error, 1)
			go func() { _, err := m.Execute(t.Context(), []string{"test"}, core.Request{}, opts); first <- err }()
			<-e.entered
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := m.Execute(ctx, []string{"test"}, core.Request{}, opts); done <- err }()
			if !remove {
				awaitAdmission(t, func() bool { n, _ := m.admission.Queue.Counts(""); return n == 1 })
				cancel()
			}
			if err := <-done; err == nil {
				t.Fatal("expected admission failure")
			}
			if s.cachedAuthID("test", "", opts, t.Context()) != "a" {
				t.Fatal("binding changed")
			}
			close(e.release)
			if err := <-first; err != nil {
				t.Fatal(err)
			}
			awaitAdmission(t, func() bool {
				n, _ := m.admission.Queue.Counts("")
				return n == 0 && m.admission.Overview(time.Now(), 60, false, false).Active == 0
			})
		})
	}
}
func TestStickyCapacitySpilloverRespectsProtocolAndPriorityOverrides(t *testing.T) {
	m, s, e, opts := spilloverFixture(t, false, 0)
	first := make(chan error, 1)
	go func() { _, err := m.Execute(t.Context(), []string{"test"}, core.Request{}, opts); first <- err }()
	<-e.entered
	defer func() {
		close(e.release)
		if err := <-first; err != nil {
			t.Error(err)
		}
	}()
	for _, body := range []string{`{"previous_response_id":"resp_a"}`, `{"input":[{"type":"reasoning","encrypted_content":"opaque"}]}`} {
		frozen := m.WithRoutingPolicySnapshot(t.Context())
		snapOpts := m.withSessionAffinityResultSnapshot(frozen, []string{"test"}, "", opts, []byte(body))
		_, _, _, handled, err := m.pickStickyCapacitySpillover(frozen, []string{"test"}, "", snapOpts, nil, nil)
		if handled || err != nil {
			t.Fatal("borrowed nonportable context", handled, err)
		}
	}
	no := false
	m.SetConfig(&config.Config{Routing: config.RoutingConfig{Strategy: "fill-first", SessionAffinity: true, SessionAffinityCapacitySpillover: true, PerAuthConcurrency: 1, PriorityOverrides: []config.RoutingPriorityOverride{{Priority: 0, SessionAffinityCapacitySpillover: &no}}}})
	frozen := m.WithRoutingPolicySnapshot(t.Context())
	snapOpts := m.withSessionAffinityResultSnapshot(frozen, []string{"test"}, "", opts)
	_, _, _, handled, err := m.pickStickyCapacitySpillover(frozen, []string{"test"}, "", snapOpts, nil, nil)
	if handled || err != nil {
		t.Fatal("ignored explicit disabled override")
	}
	if s.cachedAuthID("test", "", opts, t.Context()) != "a" {
		t.Fatal("binding changed")
	}
}

func (e *spilloverExecutor) ExecuteStream(ctx context.Context, a *Auth, _ core.Request, _ core.Options) (*core.StreamResult, error) {
	e.entered <- a.ID
	chunks := make(chan core.StreamChunk, 1)
	chunks <- core.StreamChunk{Payload: []byte("data: ready\n\n")}
	go func() {
		defer close(chunks)
		if a.ID == "a" {
			select {
			case <-e.release:
			case <-ctx.Done():
			}
		}
	}()
	return &core.StreamResult{Chunks: chunks}, nil
}
func TestStickyCapacitySpilloverStreamRetainsActualLeaseAndBinding(t *testing.T) {
	m, s, e, opts := spilloverFixture(t, false, 0)
	first, err := m.ExecuteStream(t.Context(), []string{"test"}, core.Request{}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if <-e.entered != "a" {
		t.Fatal("wrong original")
	}
	<-first.Chunks
	borrowed, err := m.ExecuteStream(t.Context(), []string{"test"}, core.Request{}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if <-e.entered != "b" {
		t.Fatal("wrong borrowed")
	}
	for range borrowed.Chunks {
	}
	if s.cachedAuthID("test", "", opts, t.Context()) != "a" {
		t.Fatal("stream rebound")
	}
	if !m.admission.Traffic.Lookup("credential", "a").Busy(1) {
		t.Fatal("released original stream early")
	}
	close(e.release)
	for range first.Chunks {
	}
	awaitAdmission(t, func() bool { return m.admission.Overview(time.Now(), 60, false, false).Active == 0 })
}

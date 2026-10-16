package proxyregion

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestInitialPreparationAndExpiryModes(t *testing.T) {
	for _, tt := range []struct {
		mode, cold      string
		initial, expiry bool
	}{{"lazy", "fallback", false, false}, {"lazy", "wait", true, false}, {"scheduled", "fallback", true, true}, {"scheduled", "wait", true, true}} {
		t.Run(tt.mode+"/"+tt.cold, func(t *testing.T) {
			s, clock := testService(t)
			interval := 86400
			if err := s.UpdateConfig(config.ProxyIPConfig{RefreshMode: tt.mode, ColdCacheMode: tt.cold, RefreshIntervalSeconds: &interval}); err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int64
			s.probe = func(context.Context, string, string) ([]byte, int, string, error) {
				calls.Add(1)
				return []byte(fixture), 200, "", nil
			}
			r := Route{ProxyURL: "http://fixture:80", OwnerID: "enabled"}
			s.Register(r)
			n := s.RefreshBatch()
			if (n > 0) != tt.initial {
				t.Fatalf("initial jobs=%d", n)
			}
			if !tt.initial {
				s.Resolve(r)
			}
			idle(t, s, r)
			clock.Add(86401)
			n = s.RefreshBatch()
			if (n > 0) != tt.expiry {
				t.Fatalf("expiry jobs=%d", n)
			}
			if tt.expiry {
				idle(t, s, r)
			} else {
				if calls.Load() != 1 {
					t.Fatal("lazy mode refreshed without demand")
				}
				s.Resolve(r)
				idle(t, s, r)
			}
			if calls.Load() != 2 {
				t.Fatalf("lookup count=%d", calls.Load())
			}
		})
	}
}
func TestPreparedMetadataFirstFailureUsesOwnRetry(t *testing.T) {
	s, clock := testService(t)
	interval := 86400
	s.UpdateConfig(config.ProxyIPConfig{ColdCacheMode: "wait", RefreshIntervalSeconds: &interval})
	var calls atomic.Int64
	s.probe = func(context.Context, string, string) ([]byte, int, string, error) {
		if calls.Add(1) == 1 {
			return nil, 0, "", errors.New("temporary")
		}
		return []byte(fixture), 200, "", nil
	}
	r := Route{ProxyURL: "http://fixture:80", OwnerID: "enabled"}
	s.Register(r)
	s.RefreshBatch()
	v := idle(t, s, r)
	if v.Timezone != "" || v.RetryAfterSeconds < 24 || v.RetryAfterSeconds > 36 {
		t.Fatalf("first failure %+v", v)
	}
	clock.Add(37)
	s.RefreshBatch()
	if v = idle(t, s, r); v.Timezone == "" {
		t.Fatalf("no recovery %+v", v)
	}
}
func TestDemandOverflowRetainedAndWaited(t *testing.T) {
	s, _ := testService(t)
	one := 1
	interval := 86400
	s.UpdateConfig(config.ProxyIPConfig{LookupConcurrency: &one, ColdCacheMode: "wait", RefreshIntervalSeconds: &interval})
	entered, release := make(chan struct{}, 1), make(chan struct{})
	s.probe = func(ctx context.Context, _, _ string) ([]byte, int, string, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, 0, "", ctx.Err()
		}
		return []byte(fixture), 200, "", nil
	}
	for i := range 600 {
		s.Resolve(Route{ProxyURL: fmt.Sprintf("http://p%d:80", i), OwnerID: fmt.Sprint(i)})
	}
	<-entered
	last := Route{ProxyURL: "http://last:80", OwnerID: "last"}
	done := make(chan Snapshot, 1)
	go func() { done <- s.ResolveForRequest(t.Context(), last) }()
	select {
	case v := <-done:
		close(release)
		t.Fatalf("overflow skipped wait %+v", v)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if v := <-done; v.Timezone == "" {
		t.Fatalf("wait missed shared result %+v", v)
	}
	for i := range 600 {
		idle(t, s, Route{ProxyURL: fmt.Sprintf("http://p%d:80", i), OwnerID: fmt.Sprint(i)})
	}
	if s.Counts()["pending"] != 0 {
		t.Fatal("demand left behind")
	}
}
func TestActiveIPProtectedThenReleased(t *testing.T) {
	s, _ := testService(t)
	size := 1
	s.UpdateConfig(config.ProxyIPConfig{IPCacheSize: &size})
	r := Route{ProxyURL: "http://active:80", OwnerID: "enabled"}
	s.Register(r)
	s.mu.Lock()
	s.ips.put("8.8.8.8", Geo{IP: "8.8.8.8", Timezone: "UTC", At: s.now()}, size)
	s.ips.put("1.1.1.1", Geo{IP: "1.1.1.1"}, size)
	s.ips.put("9.9.9.9", Geo{IP: "9.9.9.9"}, size)
	s.mu.Unlock()
	if s.Preview(r).Timezone == "" || s.Counts()["active_ips"] != 1 || s.Counts()["idle_ips"] != 1 {
		t.Fatal("active metadata evicted")
	}
	s.ForgetOwner("enabled")
	if s.Counts()["active_ips"] != 0 || s.Counts()["idle_ips"] > 1 {
		t.Fatal("retired metadata not released")
	}
}

func TestLateMetadataCannotRepinPreviousExit(t *testing.T) {
	s, _ := testService(t)
	var changed atomic.Bool
	s.SetObservationSource(func(Route) Observation {
		if changed.Load() {
			return Observation{IP: "1.1.1.1"}
		}
		return Observation{IP: "8.8.8.8"}
	}, nil)
	entered, release := make(chan struct{}), make(chan struct{})
	s.probe = func(context.Context, string, string) ([]byte, int, string, error) {
		close(entered)
		<-release
		return []byte(fixture), 200, "", nil
	}
	r := Route{ProxyURL: "http://fixture:80", OwnerID: "active"}
	s.Resolve(r)
	<-entered
	changed.Store(true)
	s.Register(r)
	close(release)
	v := idle(t, s, r)
	if v.Timezone != "" || v.IP != "1.1.1.1" {
		t.Fatalf("old metadata applied %+v", v)
	}
	s.mu.Lock()
	oldPins := s.ips.pins["8.8.8.8"]
	s.mu.Unlock()
	if oldPins != 0 {
		t.Fatal("late result restored old IP reference")
	}
}

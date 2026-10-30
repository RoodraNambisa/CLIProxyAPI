package admission

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func testController(limit, rpm int, queue config.AdmissionQueue) *Controller {
	c := NewController()
	c.Configure(&config.Config{SDKConfig: config.SDKConfig{APIKeys: []string{"test-key"}, APIKeyGroups: []config.APIKeyGroup{{APIKey: "test-key", ConcurrencyLimit: limit, RPMLimit: rpm, Queue: queue}}}})
	return c
}
func waitQueued(t *testing.T, q *Queue, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		count, _ := q.Counts("")
		if count == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("queue did not reach %d", n)
}

func TestKeyConcurrencyAndRPMRemainIndependent(t *testing.T) {
	c := testController(1, 2, config.AdmissionQueue{})
	defer c.Close()
	a, err := c.Acquire(t.Context(), "test-key", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Acquire(t.Context(), "test-key", 0, nil); err == nil {
		t.Fatal("concurrency cap ignored")
	}
	a.Close()
	a.Close()
	b, err := c.Acquire(t.Context(), "test-key", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	b.Close()
	if _, err = c.Acquire(t.Context(), "test-key", 0, nil); err == nil {
		t.Fatal("completion refunded RPM")
	}
	s := c.KeySnapshots([]string{KeyID("test-key")}, time.Now())[0]
	if s.Active != 0 || s.WindowUsed != 2 || s.RPM60 != 2 || s.CompletedRPM60 != 2 {
		t.Fatalf("unexpected snapshot %+v", s)
	}
}

func TestKeyHotReloadRetainsCountersAndRejectsDeletedWaiter(t *testing.T) {
	c := testController(1, 10, config.AdmissionQueue{Enabled: true})
	defer c.Close()
	a, err := c.Acquire(t.Context(), "test-key", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		b, e := c.Acquire(t.Context(), "test-key", 64, nil)
		if b != nil {
			b.Close()
		}
		done <- e
	}()
	waitQueued(t, c.Queue, 1)
	c.Configure(&config.Config{})
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("deleted key admitted")
		}
	case <-time.After(time.Second):
		t.Fatal("deleted waiter stuck")
	}
	a.Close()
	if c.Traffic.Overview(time.Now(), 60, false, false).Active != 0 {
		t.Fatal("retired lease leaked")
	}
}

func TestQueueFIFOAndCancellation(t *testing.T) {
	c := testController(1, 0, config.AdmissionQueue{Enabled: true, MaxWaiting: 2})
	defer c.Close()
	first, _ := c.Acquire(t.Context(), "test-key", 0, nil)
	order := make(chan int, 2)
	release := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func(i int) {
			l, e := c.Acquire(t.Context(), "test-key", 10, nil)
			if e != nil {
				return
			}
			order <- i
			<-release
			l.Close()
		}(i)
		waitQueued(t, c.Queue, i+1)
	}
	if _, err := c.Acquire(t.Context(), "test-key", 0, nil); err == nil {
		t.Fatal("queue cap ignored")
	}
	first.Close()
	if got := <-order; got != 0 {
		t.Fatalf("FIFO=%d", got)
	}
	release <- struct{}{}
	if got := <-order; got != 1 {
		t.Fatalf("FIFO=%d", got)
	}
	release <- struct{}{}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := c.Acquire(ctx, "test-key", 0, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestQueueByteBudgetAndCumulativeWait(t *testing.T) {
	c := testController(1, 0, config.AdmissionQueue{Enabled: true})
	defer c.Close()
	c.Queue.Configure(config.AdmissionConfig{MaxQueuedBytes: 8})
	a, _ := c.Acquire(t.Context(), "test-key", 0, nil)
	defer a.Close()
	if _, err := c.Acquire(t.Context(), "test-key", 9, nil); err == nil {
		t.Fatal("byte budget ignored")
	}
	spent := time.Second
	err := c.Queue.Do(t.Context(), "test", "test", config.AdmissionQueue{Enabled: true, WaitTimeoutSeconds: 1}, 0, &spent, func() error { return &Busy{Reason: "full"} })
	if err == nil {
		t.Fatal("wait budget reset")
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := c.Acquire(ctx, "test-key", 8, nil); done <- err }()
	waitQueued(t, c.Queue, 1)
	cancel()
	if !errors.Is(<-done, context.Canceled) {
		t.Fatal("cancellation lost")
	}
	count, bytes := c.Queue.Counts("")
	if count != 0 || bytes != 0 {
		t.Fatal("cancellation retained queue resources")
	}
}

func TestConcurrentKeysNeverOversubscribe(t *testing.T) {
	c := testController(3, 0, config.AdmissionQueue{})
	defer c.Close()
	var active, peak atomic.Int64
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			for range 10 {
				lease, err := c.Acquire(t.Context(), "test-key", 0, nil)
				if err != nil {
					continue
				}
				n := active.Add(1)
				for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
				}
				active.Add(-1)
				lease.Close()
			}
		})
	}
	wg.Wait()
	if peak.Load() > 3 {
		t.Fatal("oversubscribed")
	}
	if c.Traffic.Overview(time.Now(), 60, false, false).Active != 0 {
		t.Fatal("leaked logical request")
	}
}

func TestTrafficUsesRollingAndCalendarWindows(t *testing.T) {
	stats := NewTraffic()
	base := time.Date(2026, 10, 31, 12, 1, 10, 0, time.UTC)
	for _, offset := range []time.Duration{-61 * time.Second, -59 * time.Second, -11 * time.Second, -time.Second} {
		stats.ObserveLogical(true, base.Add(offset))
	}
	rolling := stats.Overview(base, 60, false, false)
	current := stats.Overview(base, 60, true, false)
	if rolling.Requests != 3 || rolling.RPM != 3 || current.Requests != 1 || current.RPM != 1 {
		t.Fatalf("rolling=%+v current=%+v", rolling, current)
	}
	if len(stats.Overview(base, 86400, false, false).Items) > 1200 {
		t.Fatal("series exceeded bound")
	}
	e, _ := stats.Acquire("credential", "one", 1)
	stats.PruneShard(int(shardFor("credential:one")), base.Add(time.Hour))
	if stats.Lookup("credential", "one") == nil {
		t.Fatal("active entry pruned")
	}
	e.Release()
}

func BenchmarkAdmissionKey(b *testing.B) {
	c := testController(0, 0, config.AdmissionQueue{})
	defer c.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l, err := c.Acquire(ctx, "test-key", 0, nil)
			if err != nil {
				b.Fatal(err)
			}
			l.Close()
		}
	})
}

func TestQueueCancellationConcurrentWithGrantDoesNotLeak(t *testing.T) {
	for range 150 {
		c := testController(1, 0, config.AdmissionQueue{Enabled: true})
		first, err := c.Acquire(t.Context(), "test-key", 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() {
			defer close(done)
			lease, _ := c.Acquire(ctx, "test-key", 16, nil)
			if lease != nil {
				lease.Close()
			}
		}()
		waitQueued(t, c.Queue, 1)
		var wg sync.WaitGroup
		wg.Go(first.Close)
		wg.Go(cancel)
		wg.Wait()
		<-done
		if s := c.Traffic.Overview(time.Now(), 60, false, false); s.Active != 0 {
			t.Fatalf("grant/cancel leaked: %+v", s)
		}
		if n, bytes := c.Queue.Counts(""); n != 0 || bytes != 0 {
			t.Fatalf("queue retained %d/%d", n, bytes)
		}
		c.Close()
	}
}

func TestRetiredEntityLateCompletionDoesNotChangeReplacement(t *testing.T) {
	c := testController(1, 0, config.AdmissionQueue{})
	defer c.Close()
	old, _ := c.Acquire(t.Context(), "test-key", 0, nil)
	c.Configure(&config.Config{})
	c.Configure(&config.Config{SDKConfig: config.SDKConfig{APIKeys: []string{"test-key"}, APIKeyGroups: []config.APIKeyGroup{{APIKey: "test-key", ConcurrencyLimit: 1}}}})
	replacement, err := c.Acquire(t.Context(), "test-key", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	old.Close()
	snapshot := c.KeySnapshots([]string{KeyID("test-key")}, time.Now())[0]
	if snapshot.Active != 1 || snapshot.CompletedRPM60 != 0 {
		t.Fatalf("old lease modified replacement: %+v", snapshot)
	}
	replacement.Close()
}

func TestKeyRPMNaturalMinuteBoundaryAndLiveSlot(t *testing.T) {
	c := testController(2, 1, config.AdmissionQueue{})
	defer c.Close()
	now := time.Date(2026, 10, 31, 12, 0, 59, 999000000, time.FixedZone("offset", 9*3600))
	c.now = func() time.Time { return now }
	first, err := c.Acquire(t.Context(), "test-key", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Acquire(t.Context(), "test-key", 0, nil); err == nil {
		t.Fatal("same minute admitted twice")
	}
	now = now.Add(time.Millisecond)
	second, err := c.Acquire(t.Context(), "test-key", 0, nil)
	if err != nil {
		t.Fatal("calendar rollover did not reset RPM", err)
	}
	if s := c.KeySnapshots([]string{KeyID("test-key")}, now)[0]; s.Active != 2 || s.WindowUsed != 1 {
		t.Fatalf("rollover reset active slots: %+v", s)
	}
	first.Close()
	second.Close()
}
func TestIdleStatisticsReclaimedWithoutRemovingActiveLeases(t *testing.T) {
	stats := NewTraffic()
	for i := 0; i < 10000; i++ {
		e, _ := stats.Acquire("credential", fmt.Sprint(i), 0)
		e.Release()
	}
	held, _ := stats.Acquire("credential", "held", 1)
	for i := range stats.shards {
		stats.PruneShard(i, time.Now().Add(3*time.Minute))
	}
	total := 0
	for i := range stats.shards {
		total += len(stats.shards[i].entities)
	}
	if total != 1 || stats.Lookup("credential", "held") != held {
		t.Fatalf("idle entries retained: %d", total)
	}
	held.Release()
}

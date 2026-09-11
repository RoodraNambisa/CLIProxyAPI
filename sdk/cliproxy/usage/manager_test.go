package usage

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
	"weak"
)

type orderedBlockingUsagePlugin struct {
	firstStarted  chan struct{}
	firstRelease  chan struct{}
	secondStarted chan struct{}
	secondRelease chan struct{}
	thirdStarted  chan struct{}
	thirdRelease  chan struct{}

	mu    sync.Mutex
	order []string
}

func TestManagerReleasesDeliveredRequestContext(t *testing.T) {
	manager := NewManager(0)
	// A burst commonly leaves spare capacity in the queue's backing array.
	manager.queue = make([]queueItem, 0, 8)
	plugin := &orderedBlockingUsagePlugin{
		firstStarted: make(chan struct{}), firstRelease: make(chan struct{}),
		secondStarted: make(chan struct{}), secondRelease: make(chan struct{}),
		thirdStarted: make(chan struct{}), thirdRelease: make(chan struct{}),
	}
	manager.Register(plugin)
	t.Cleanup(func() {
		for _, release := range []chan struct{}{plugin.firstRelease, plugin.secondRelease, plugin.thirdRelease} {
			select {
			case <-release:
			default:
				close(release)
			}
		}
		manager.Stop()
	})
	wait := func(started <-chan struct{}) {
		t.Helper()
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("usage dispatcher did not reach the expected record")
		}
	}
	manager.Publish(context.Background(), Record{Model: "first"})
	wait(plugin.firstStarted)
	// Keep the only strong reference in the queued request context. The large
	// object cannot share a tiny-allocation slot with unrelated live values.
	publishContext := func() weak.Pointer[[1 << 20]byte] {
		payload := new([1 << 20]byte)
		ref := weak.Make(payload)
		ctx := context.WithValue(context.Background(), struct{}{}, payload)
		manager.Publish(ctx, Record{Model: "payload"})
		return ref
	}
	ref := publishContext()
	manager.Publish(context.Background(), Record{Model: "second"})
	manager.Publish(context.Background(), Record{Model: "third"})
	close(plugin.firstRelease)
	wait(plugin.secondStarted)
	for range 5 {
		runtime.GC()
	}
	if ref.Value() != nil {
		t.Fatal("delivered usage retained its request context while later records remained queued")
	}
	close(plugin.secondRelease)
	wait(plugin.thirdStarted)
	close(plugin.thirdRelease)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := manager.Barrier(ctx); err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	released := manager.queue == nil
	manager.mu.Unlock()
	if !released {
		t.Fatal("drained usage queue retained its backing storage")
	}
}

func (p *orderedBlockingUsagePlugin) HandleUsage(_ context.Context, record Record) {
	switch record.Model {
	case "first":
		close(p.firstStarted)
		<-p.firstRelease
	case "second":
		close(p.secondStarted)
		<-p.secondRelease
	case "third":
		close(p.thirdStarted)
		<-p.thirdRelease
	}
	p.mu.Lock()
	p.order = append(p.order, record.Model)
	p.mu.Unlock()
}

func (p *orderedBlockingUsagePlugin) handledOrder() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.order...)
}

func TestManagerBarrierPreservesFIFOQueueBoundary(t *testing.T) {
	manager := NewManager(1)
	plugin := &orderedBlockingUsagePlugin{
		firstStarted:  make(chan struct{}),
		firstRelease:  make(chan struct{}),
		secondStarted: make(chan struct{}),
		secondRelease: make(chan struct{}),
		thirdStarted:  make(chan struct{}),
		thirdRelease:  make(chan struct{}),
	}
	manager.Register(plugin)
	manager.Publish(context.Background(), Record{Model: "first"})

	select {
	case <-plugin.firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first usage record did not start")
	}
	manager.Publish(context.Background(), Record{Model: "second"})

	barrierDone := make(chan error, 1)
	go func() {
		barrierDone <- manager.Barrier(context.Background())
	}()
	barrierSignal := waitForQueuedBarrier(t, manager)
	manager.Publish(context.Background(), Record{Model: "third"})
	close(plugin.firstRelease)
	select {
	case <-plugin.secondStarted:
	case <-time.After(time.Second):
		t.Fatal("second usage record did not start")
	}
	select {
	case <-barrierSignal:
		t.Fatal("internal barrier closed before second queued record completed")
	default:
	}
	close(plugin.secondRelease)

	select {
	case err := <-barrierDone:
		if err != nil {
			t.Fatalf("Barrier() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Barrier() did not return")
	}
	if got := plugin.handledOrder(); len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Fatalf("handled order at barrier = %v, want [first second]", got)
	}
	select {
	case <-plugin.thirdStarted:
	case <-time.After(time.Second):
		t.Fatal("record queued after barrier did not start")
	}

	close(plugin.thirdRelease)
	manager.Stop()
}

func TestManagerBarrierAndStopCanStartConcurrently(t *testing.T) {
	for i := 0; i < 100; i++ {
		manager := NewManager(1)
		start := make(chan struct{})
		barrierDone := make(chan error, 1)
		stopDone := make(chan struct{})
		go func() {
			<-start
			barrierDone <- manager.Barrier(context.Background())
		}()
		go func() {
			<-start
			manager.Stop()
			close(stopDone)
		}()
		close(start)

		select {
		case err := <-barrierDone:
			if err != nil && !errors.Is(err, ErrManagerClosed) {
				t.Fatalf("Barrier() error = %v, want nil or ErrManagerClosed", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Barrier() deadlocked with Stop()")
		}
		select {
		case <-stopDone:
		case <-time.After(time.Second):
			t.Fatal("Stop() deadlocked with Barrier()")
		}
	}
}

func TestManagerBarrierRejectsClosedManager(t *testing.T) {
	manager := NewManager(1)
	manager.Stop()
	if err := manager.Barrier(context.Background()); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("Barrier() error = %v, want ErrManagerClosed", err)
	}
}

func waitForQueuedBarrier(t *testing.T, manager *Manager) <-chan struct{} {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		manager.mu.Lock()
		found := false
		var barrier <-chan struct{}
		for _, item := range manager.queue {
			if item.barrier != nil {
				found = true
				barrier = item.barrier
				break
			}
		}
		manager.mu.Unlock()
		if found {
			return barrier
		}
		if time.Now().After(deadline) {
			t.Fatal("barrier was not queued")
		}
		runtime.Gosched()
	}
}

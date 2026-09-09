package handlers

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
)

type streamForwardLoopProbe struct {
	context.Context
	checks atomic.Int32
	spin   chan struct{}
}

func (ctx *streamForwardLoopProbe) Done() <-chan struct{} {
	if ctx.checks.Add(1) == 100 {
		close(ctx.spin)
	}
	return ctx.Context.Done()
}

func TestForwardStreamClosedErrorChannelWaitsForDataWithoutSpinning(t *testing.T) {
	h, c, flusher := newForwardStreamTestContext(t)
	ctx, cancel := context.WithCancel(t.Context())
	probe := &streamForwardLoopProbe{Context: ctx, spin: make(chan struct{})}
	c.Request = c.Request.WithContext(probe)
	data := make(chan []byte, 1)
	errs := make(chan *interfaces.ErrorMessage)
	close(errs)
	finished := make(chan struct{})
	var received string
	var completed bool
	var finalErr error
	go func() {
		defer close(finished)
		h.ForwardStream(c, flusher, func(err error) { finalErr = err }, data, errs, StreamForwardOptions{
			WriteChunk: func(chunk []byte) { received += string(chunk) },
			WriteDone:  func() { completed = true },
		})
	}()
	defer func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Error("forwarder did not stop after cancellation")
		}
	}()
	select {
	case <-probe.spin:
		t.Fatal("closed error channel kept the idle stream runnable")
	case <-time.After(30 * time.Millisecond):
	}
	data <- []byte("after error-channel close")
	close(data)
	select {
	case <-finished:
		if received != "after error-channel close" || !completed || finalErr != nil {
			t.Fatal("closing the error channel discarded later data or changed completion")
		}
	case <-time.After(time.Second):
		t.Fatal("forwarder did not finish after the data channel closed")
	}
}

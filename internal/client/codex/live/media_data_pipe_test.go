package live

import (
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

type mediaPipeSink struct {
	amount   atomic.Uint64
	observed chan struct{}
	sent     chan mediaDataMessage
	low      func()
	writeErr error
}

func (s *mediaPipeSink) Send(data []byte) error {
	if s.writeErr != nil {
		return s.writeErr
	}
	s.sent <- mediaDataMessage{data: append([]byte(nil), data...)}
	return nil
}
func (s *mediaPipeSink) SendText(data string) error {
	if s.writeErr != nil {
		return s.writeErr
	}
	s.sent <- mediaDataMessage{data: []byte(data), text: true}
	return nil
}
func (s *mediaPipeSink) BufferedAmount() uint64 {
	value := s.amount.Load()
	select {
	case s.observed <- struct{}{}:
	default:
	}
	return value
}
func (*mediaPipeSink) SetBufferedAmountLowThreshold(uint64) {}
func (s *mediaPipeSink) OnBufferedAmountLow(f func())       { s.low = f }
func (*mediaPipeSink) OnOpen(func())                        {}
func (*mediaPipeSink) ReadyState() webrtc.DataChannelState  { return webrtc.DataChannelStateOpen }

func TestMediaDataPipePreservesOrderTextAndBufferOwnership(t *testing.T) {
	done := make(chan struct{})
	p := newMediaDataPipe(done, func(error) { t.Error("unexpected channel error") })
	t.Cleanup(func() { close(done); p.wait() })
	for i, text := range []bool{true, false, true} {
		data := []byte{byte(i)}
		if !p.enqueue(data, text) {
			t.Fatal("queue rejected bounded message")
		}
		data[0] = 99
	}
	sink := &mediaPipeSink{sent: make(chan mediaDataMessage, 3)}
	if !p.setDestination(sink) || p.setDestination(sink) {
		t.Fatal("destination was not bound exactly once")
	}
	for i, text := range []bool{true, false, true} {
		select {
		case message := <-sink.sent:
			if len(message.data) != 1 || message.data[0] != byte(i) || message.text != text {
				t.Fatal("media message was reordered, rewritten or aliased")
			}
		case <-time.After(time.Second):
			t.Fatal("media message was not delivered")
		}
	}
}

func TestMediaDataPipeBackpressureResumesAndCancellationUnblocks(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		done := make(chan struct{})
		p := newMediaDataPipe(done, nil)
		sink := &mediaPipeSink{sent: make(chan mediaDataMessage, 1), observed: make(chan struct{}, 1)}
		sink.amount.Store(math.MaxUint64)
		p.setDestination(sink)
		p.enqueue([]byte("bounded"), true)
		select {
		case <-sink.observed:
		case <-time.After(time.Second):
			t.Fatal("writer did not check buffered amount")
		}
		select {
		case <-sink.sent:
			t.Fatal("writer ignored backpressure or overflowed")
		default:
		}
		if !cancel {
			sink.amount.Store(0)
			sink.low()
			select {
			case <-sink.sent:
			case <-time.After(time.Second):
				t.Fatal("drain did not resume the writer")
			}
		}
		close(done)
		select {
		case <-p.finished:
		case <-time.After(time.Second):
			t.Fatal("cancelled writer leaked")
		}
	}
}

func TestMediaDataPipeFullQueueAndPendingDestinationCancel(t *testing.T) {
	done := make(chan struct{})
	p := newMediaDataPipe(done, nil)
	for range mediaDataQueueSize {
		if !p.enqueue([]byte("queued"), false) {
			t.Fatal("bounded queue filled too soon")
		}
	}
	blocked := make(chan bool, 1)
	go func() { blocked <- p.enqueue([]byte("overflow"), false) }()
	close(done)
	select {
	case accepted := <-blocked:
		if accepted {
			t.Fatal("cancelled full queue accepted more data")
		}
	case <-time.After(time.Second):
		t.Fatal("producer remained blocked after cancellation")
	}
	p.wait()
}

func TestMediaDataPipeFailureCanJoinWriterAndDoesNotExposeMessage(t *testing.T) {
	for _, oversized := range []bool{false, true} {
		done, callback := make(chan struct{}), make(chan struct{})
		writeErr := errors.New("private-upstream-detail")
		var p *mediaDataPipe
		p = newMediaDataPipe(done, func(err error) {
			if err == nil || err.Error() == "private-upstream-detail" {
				t.Error("invalid media error")
			}
			if !oversized && !errors.Is(err, writeErr) {
				t.Error("media write error lost its cause")
			}
			close(done)
			p.wait()
			close(callback)
		})
		if oversized {
			if p.enqueue(make([]byte, mediaDataMessageMaxSize+1), false) {
				t.Fatal("oversized data accepted")
			}
		} else {
			p.setDestination(&mediaPipeSink{writeErr: writeErr})
			p.enqueue([]byte("private-payload"), true)
		}
		select {
		case <-callback:
		case <-time.After(time.Second):
			t.Fatal("failure teardown waited on its own writer")
		}
	}
}

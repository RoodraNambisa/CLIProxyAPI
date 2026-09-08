package live

import (
	"errors"
	"sync"

	"github.com/pion/webrtc/v4"
)

const (
	mediaDataQueueSize       = 64
	mediaDataMessageMaxSize  = 256 << 10
	mediaDataBufferedMaxSize = 1 << 20
)

type mediaDataMessage struct {
	data []byte
	text bool
}

type mediaDataWriteError struct{ cause error }

func (*mediaDataWriteError) Error() string   { return "realtime data channel write failed" }
func (e *mediaDataWriteError) Unwrap() error { return e.cause }

// The sink is the narrow part of a Pion DataChannel used by the ordered writer.
type mediaDataSink interface {
	Send([]byte) error
	SendText(string) error
	BufferedAmount() uint64
	SetBufferedAmountLowThreshold(uint64)
	OnBufferedAmountLow(func())
	OnOpen(func())
	ReadyState() webrtc.DataChannelState
}

type mediaDataPipe struct {
	done        <-chan struct{}
	finished    chan struct{}
	queue       chan mediaDataMessage
	ready       chan struct{}
	writable    chan struct{}
	mu          sync.Mutex
	destination mediaDataSink
	readyOnce   sync.Once
	failureOnce sync.Once
	onError     func(error)
}

func newMediaDataPipe(done <-chan struct{}, onError func(error)) *mediaDataPipe {
	p := &mediaDataPipe{done: done, finished: make(chan struct{}), queue: make(chan mediaDataMessage, mediaDataQueueSize), ready: make(chan struct{}), writable: make(chan struct{}, 1), onError: onError}
	go p.run()
	return p
}

func (p *mediaDataPipe) enqueue(data []byte, text bool) bool {
	select {
	case <-p.done:
		return false
	default:
	}
	if len(data) > mediaDataMessageMaxSize {
		p.fail(errors.New("realtime data channel message exceeds 256 KiB"))
		return false
	}
	message := mediaDataMessage{data: append([]byte(nil), data...), text: text}
	select {
	case <-p.done:
		return false
	case p.queue <- message:
		return true
	}
}

func (p *mediaDataPipe) setDestination(destination mediaDataSink) bool {
	if destination == nil {
		return false
	}
	p.mu.Lock()
	if p.destination != nil {
		p.mu.Unlock()
		return false
	}
	p.destination = destination
	p.mu.Unlock()
	destination.SetBufferedAmountLowThreshold(mediaDataBufferedMaxSize / 2)
	destination.OnBufferedAmountLow(func() {
		select {
		case p.writable <- struct{}{}:
		default:
		}
	})
	markReady := func() { p.readyOnce.Do(func() { close(p.ready) }) }
	destination.OnOpen(markReady)
	if destination.ReadyState() == webrtc.DataChannelStateOpen {
		markReady()
	}
	return true
}

func (p *mediaDataPipe) run() {
	defer close(p.finished)
	select {
	case <-p.done:
		return
	case <-p.ready:
	}
	p.mu.Lock()
	destination := p.destination
	p.mu.Unlock()
	for {
		select {
		case <-p.done:
			return
		case message := <-p.queue:
			// Subtraction avoids overflow even if a sink reports a very large backlog.
			for destination.BufferedAmount() > uint64(mediaDataBufferedMaxSize-len(message.data)) {
				select {
				case <-p.done:
					return
				case <-p.writable:
				}
			}
			select {
			case <-p.done:
				return
			default:
			}
			var errSend error
			if message.text {
				errSend = destination.SendText(string(message.data))
			} else {
				errSend = destination.Send(message.data)
			}
			if errSend != nil {
				p.fail(&mediaDataWriteError{cause: errSend})
				return
			}
		}
	}
}

// Invoke teardown outside the writer and Pion callbacks. An owner may close
// the peer connections and then wait for both pipes without waiting on itself.
func (p *mediaDataPipe) fail(err error) {
	select {
	case <-p.done:
		return
	default:
	}
	p.failureOnce.Do(func() {
		if p.onError != nil {
			go p.onError(err)
		}
	})
}

func (p *mediaDataPipe) wait() { <-p.finished }

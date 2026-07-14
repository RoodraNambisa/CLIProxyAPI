package wsrelay

import (
	"sync"
	"testing"
	"time"
)

func TestPendingRequestDeliveryAndCloseAreSerialized(t *testing.T) {
	for i := 0; i < 1000; i++ {
		request := newPendingRequest(1)
		start := make(chan struct{})
		var workers sync.WaitGroup
		workers.Add(2)
		go func() {
			defer workers.Done()
			<-start
			request.deliver(Message{ID: "request"})
		}()
		go func() {
			defer workers.Done()
			<-start
			request.close()
		}()
		close(start)
		workers.Wait()
		request.close()
		for range request.ch {
		}
	}
}

func TestPendingRequestCloseSignalsCompletion(t *testing.T) {
	request := newPendingRequest(1)
	request.close()
	select {
	case <-request.done:
	case <-time.After(time.Second):
		t.Fatal("pending request completion was not signaled")
	}
}

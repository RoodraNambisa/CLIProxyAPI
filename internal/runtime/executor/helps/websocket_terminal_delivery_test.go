package helps

import "testing"

func TestTerminalWebsocketReadImmediateAndCanceledDelivery(t *testing.T) {
	invalidations := 0
	invalidate := func() { invalidations++ }
	ch := make(chan int, 1)
	if SendTerminalWebsocketRead(ch, nil, 7, invalidate) || <-ch != 7 || invalidations != 0 {
		t.Fatal("ready queue invalidated its connection")
	}
	ch <- 1
	done := make(chan struct{})
	close(done)
	if SendTerminalWebsocketRead(ch, done, 7, invalidate) || <-ch != 1 || invalidations != 0 {
		t.Fatal("canceled turn replaced an already queued event")
	}
	if SendTerminalWebsocketRead[int](nil, nil, 7, invalidate) || invalidations != 0 {
		t.Fatal("absent reader was invalidated")
	}
}

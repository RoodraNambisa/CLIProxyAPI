package helps

// SendTerminalWebsocketRead retains a terminal event behind queued frames.
// It invalidates a full channel's connection before waiting and stops when its turn ends.
func SendTerminalWebsocketRead[T any](ch chan<- T, done <-chan struct{}, event T, invalidate func()) bool {
	if ch == nil {
		return false
	}
	select {
	case ch <- event:
		return false
	case <-done:
		return false
	default:
	}
	invalidated := invalidate != nil
	if invalidated {
		invalidate()
	}
	select {
	case ch <- event:
	case <-done:
	}
	return invalidated
}

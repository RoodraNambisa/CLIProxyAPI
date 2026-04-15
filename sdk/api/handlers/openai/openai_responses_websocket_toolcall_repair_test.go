package openai

import "testing"

func TestResponsesWebsocketToolPairStateLifecycle(t *testing.T) {
	sessionKey := "test-tool-pair-lifecycle"

	defaultWebsocketToolPairStates.delete(sessionKey)
	defaultWebsocketToolPairRefs.mu.Lock()
	delete(defaultWebsocketToolPairRefs.counts, sessionKey)
	defaultWebsocketToolPairRefs.mu.Unlock()
	t.Cleanup(func() {
		defaultWebsocketToolPairStates.delete(sessionKey)
		defaultWebsocketToolPairRefs.mu.Lock()
		delete(defaultWebsocketToolPairRefs.counts, sessionKey)
		defaultWebsocketToolPairRefs.mu.Unlock()
	})

	state1 := acquireResponsesWebsocketToolPairState(sessionKey)
	state2 := acquireResponsesWebsocketToolPairState(sessionKey)
	if state1 == nil || state2 == nil {
		t.Fatalf("expected shared websocket tool pair state to be created")
	}
	if state1 != state2 {
		t.Fatalf("expected shared state instance for session %q", sessionKey)
	}

	state1.recordCall("call-1", []byte(`{"type":"function_call","call_id":"call-1"}`))
	if got, ok := state2.getCall("call-1"); !ok || len(got) == 0 {
		t.Fatalf("expected recorded call to be visible across shared state")
	}

	releaseResponsesWebsocketToolPairState(sessionKey)

	defaultWebsocketToolPairRefs.mu.Lock()
	countAfterFirstRelease, hasCountAfterFirstRelease := defaultWebsocketToolPairRefs.counts[sessionKey]
	defaultWebsocketToolPairRefs.mu.Unlock()
	if !hasCountAfterFirstRelease || countAfterFirstRelease != 1 {
		t.Fatalf("refcount after first release = %d, want 1", countAfterFirstRelease)
	}

	defaultWebsocketToolPairStates.mu.Lock()
	_, stateStillPresent := defaultWebsocketToolPairStates.states[sessionKey]
	defaultWebsocketToolPairStates.mu.Unlock()
	if !stateStillPresent {
		t.Fatalf("expected shared state to remain until final release")
	}

	releaseResponsesWebsocketToolPairState(sessionKey)

	defaultWebsocketToolPairRefs.mu.Lock()
	_, hasCountAfterFinalRelease := defaultWebsocketToolPairRefs.counts[sessionKey]
	defaultWebsocketToolPairRefs.mu.Unlock()
	if hasCountAfterFinalRelease {
		t.Fatalf("expected refcount entry to be removed after final release")
	}

	defaultWebsocketToolPairStates.mu.Lock()
	_, statePresentAfterFinalRelease := defaultWebsocketToolPairStates.states[sessionKey]
	defaultWebsocketToolPairStates.mu.Unlock()
	if statePresentAfterFinalRelease {
		t.Fatalf("expected shared state to be deleted after final release")
	}

	state3 := acquireResponsesWebsocketToolPairState(sessionKey)
	if got, ok := state3.getCall("call-1"); ok || len(got) != 0 {
		t.Fatalf("expected state to be reset after cleanup")
	}
	releaseResponsesWebsocketToolPairState(sessionKey)
}

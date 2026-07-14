package executor

import "testing"

func TestSuccessfulStreamTerminalChunk(t *testing.T) {
	marker := SuccessfulStreamTerminalChunk()
	if !IsSuccessfulStreamTerminalChunk(marker) {
		t.Fatal("completion marker was not recognized")
	}
	if IsSuccessfulStreamTerminalChunk(StreamChunk{}) {
		t.Fatal("ordinary empty chunk was recognized as completion marker")
	}
	if IsSuccessfulStreamTerminalChunk(StreamChunk{Payload: make([]byte, 0, 1)}) {
		t.Fatal("unrelated empty slice was recognized as completion marker")
	}
}

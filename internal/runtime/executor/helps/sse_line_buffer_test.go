package helps

import (
	"bytes"
	"reflect"
	"testing"
)

func TestObserveSSELinesBuffersPartialLines(t *testing.T) {
	var pending []byte
	var lines []string
	observe := func(line []byte) { lines = append(lines, string(line)) }
	if err := ObserveSSELines(&pending, []byte("data: {\"part"), false, 64, observe); err != nil {
		t.Fatalf("first ObserveSSELines() error = %v", err)
	}
	if err := ObserveSSELines(&pending, []byte("\":1}\n\n"), false, 64, observe); err != nil {
		t.Fatalf("second ObserveSSELines() error = %v", err)
	}
	if got, want := lines, []string{`data: {"part":1}`, ""}; !reflect.DeepEqual(got, want) {
		t.Fatalf("observed lines = %#v, want %#v", got, want)
	}
	if len(pending) != 0 {
		t.Fatalf("pending = %q, want empty", pending)
	}
}

func TestObserveSSELinesRejectsOversizedPartialLine(t *testing.T) {
	pending := []byte("1234")
	if err := ObserveSSELines(&pending, []byte("5"), false, 4, nil); err == nil {
		t.Fatal("ObserveSSELines() error = nil, want line limit error")
	}
	if pending != nil {
		t.Fatalf("pending = %q, want released buffer", pending)
	}
}

func TestSSELineBufferSupportsChunkedLineEndings(t *testing.T) {
	var buffer SSELineBuffer
	var lines []string
	visit := func(line []byte) bool {
		lines = append(lines, string(line))
		return true
	}
	for _, chunk := range [][]byte{
		[]byte("one\r"),
		[]byte("\ntwo\rthree\n"),
		[]byte("four"),
	} {
		if !buffer.Feed(chunk, false, visit) {
			t.Fatal("Feed() stopped unexpectedly")
		}
	}
	if !buffer.Feed(nil, true, visit) {
		t.Fatal("EOF Feed() stopped unexpectedly")
	}
	want := []string{"one", "two", "three", "four"}
	if len(lines) != len(want) {
		t.Fatalf("lines = %#v, want %#v", lines, want)
	}
	for index := range want {
		if lines[index] != want[index] {
			t.Fatalf("lines = %#v, want %#v", lines, want)
		}
	}
}

func TestSSELineBufferReleasesLargeCompletedLine(t *testing.T) {
	var buffer SSELineBuffer
	large := bytes.Repeat([]byte("x"), retainedSSELineBufferBytes+1)
	large = append(large, '\n')
	if !buffer.Feed(large, false, func(line []byte) bool { return len(line) == len(large)-1 }) {
		t.Fatal("Feed() stopped unexpectedly")
	}
	if cap(buffer.pending) != 0 {
		t.Fatalf("large line capacity retained = %d", cap(buffer.pending))
	}
}

func TestSSELineBufferStopsWithoutRetainingPayload(t *testing.T) {
	var buffer SSELineBuffer
	if buffer.Feed([]byte("stop\nremaining"), false, func([]byte) bool { return false }) {
		t.Fatal("Feed() continued after visitor stopped")
	}
	if len(buffer.pending) != 0 || cap(buffer.pending) != 0 {
		t.Fatalf("stopped buffer retained payload: len=%d cap=%d", len(buffer.pending), cap(buffer.pending))
	}
}

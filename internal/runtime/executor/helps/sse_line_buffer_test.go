package helps

import (
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

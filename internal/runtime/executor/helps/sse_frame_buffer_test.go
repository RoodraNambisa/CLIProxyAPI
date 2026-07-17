package helps

import (
	"errors"
	"testing"
)

func TestSSEFrameBufferSupportsSplitCRLFAndCROnlyFrames(t *testing.T) {
	buffer := NewSSEFrameBuffer(128)
	frames, err := buffer.Feed([]byte("data: {\"a\":1}\r\n\r"), false)
	if err != nil || len(frames) != 0 {
		t.Fatalf("first Feed() = (%q, %v)", frames, err)
	}
	frames, err = buffer.Feed([]byte("\ndata: [DONE]\r\r"), true)
	if err != nil {
		t.Fatalf("second Feed() error = %v", err)
	}
	if len(frames) != 2 {
		t.Fatalf("frames = %q, want 2 frames", frames)
	}
	if got, want := string(frames[0]), "data: {\"a\":1}\r\n\r\n"; got != want {
		t.Fatalf("first frame = %q, want %q", got, want)
	}
	if got, want := string(frames[1]), "data: [DONE]\r\r"; got != want {
		t.Fatalf("second frame = %q, want %q", got, want)
	}
}

func TestSSEFrameBufferBoundsUnterminatedFrame(t *testing.T) {
	buffer := NewSSEFrameBuffer(4)
	if _, err := buffer.Feed([]byte("1234"), false); err != nil {
		t.Fatalf("first Feed() error = %v", err)
	}
	_, err := buffer.Feed([]byte("5"), false)
	var limitErr *SSEFrameLimitError
	if !errors.As(err, &limitErr) || limitErr.Limit != 4 {
		t.Fatalf("Feed() error = %#v, want limit 4", err)
	}
}

func TestSplitSSEFrameLinesSupportsAllLineEndings(t *testing.T) {
	lines := SplitSSEFrameLines([]byte("event: item\r\ndata: {}\rdata: [DONE]\n\n"))
	want := []string{"event: item", "data: {}", "data: [DONE]", "", ""}
	if len(lines) != len(want) {
		t.Fatalf("lines = %q, want %q", lines, want)
	}
	for index := range want {
		if string(lines[index]) != want[index] {
			t.Fatalf("line %d = %q, want %q", index, lines[index], want[index])
		}
	}
}

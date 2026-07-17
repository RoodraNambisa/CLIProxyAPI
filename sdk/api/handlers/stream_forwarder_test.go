package handlers

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type countingFlusher struct {
	count int
}

func (f *countingFlusher) Flush() {
	f.count++
}

type notifyingFlusher struct {
	flushed chan struct{}
}

func (f *notifyingFlusher) Flush() {
	select {
	case f.flushed <- struct{}{}:
	default:
	}
}

func newForwardStreamTestContext(t *testing.T) (*BaseAPIHandler, *gin.Context, *countingFlusher) {
	t.Helper()

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/stream", nil)
	return NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil), c, &countingFlusher{}
}

func TestForwardStreamBatchesFlushesByMinBytes(t *testing.T) {
	h, c, flusher := newForwardStreamTestContext(t)
	data := make(chan []byte, 4)
	errs := make(chan *interfaces.ErrorMessage)
	data <- []byte("ab")
	data <- []byte("cd")
	data <- []byte("ef")
	data <- []byte("g")
	close(data)
	close(errs)

	interval := time.Hour
	h.ForwardStream(c, flusher, func(error) {}, data, errs, StreamForwardOptions{
		FlushInterval: &interval,
		FlushMinBytes: 5,
		WriteChunk: func(chunk []byte) {
			_, _ = c.Writer.Write(chunk)
		},
	})

	if flusher.count >= 5 {
		t.Fatalf("flush count = %d, want fewer than per-chunk flushing", flusher.count)
	}
}

func TestForwardStreamFlushBudgetUsesActualWrittenBytes(t *testing.T) {
	h, c, _ := newForwardStreamTestContext(t)
	ctx, cancelContext := context.WithCancel(c.Request.Context())
	c.Request = c.Request.WithContext(ctx)
	data := make(chan []byte)
	errs := make(chan *interfaces.ErrorMessage)
	flusher := &notifyingFlusher{flushed: make(chan struct{}, 1)}
	done := make(chan struct{})
	var pending bytes.Buffer
	go func() {
		defer close(done)
		h.ForwardStream(c, flusher, func(error) {}, data, errs, StreamForwardOptions{
			FlushMinBytes: 32,
			WriteChunk: func(chunk []byte) {
				_, _ = pending.Write(chunk)
				if !bytes.Contains(pending.Bytes(), []byte("\n\n")) {
					return
				}
				_, _ = c.Writer.Write(pending.Bytes())
				pending.Reset()
			},
		})
	}()

	data <- []byte("data: \"" + strings.Repeat("x", 64))
	select {
	case <-flusher.flushed:
		t.Fatal("buffered input bytes triggered an empty flush")
	case <-time.After(20 * time.Millisecond):
	}
	data <- []byte("\"\n\n")
	select {
	case <-flusher.flushed:
	case <-time.After(time.Second):
		t.Fatal("completed frame did not flush after its bytes were written")
	}

	cancelContext()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ForwardStream did not stop after cancellation")
	}
}

func TestForwardStreamFlushChunkBypassesMinBytes(t *testing.T) {
	h, c, _ := newForwardStreamTestContext(t)
	ctx, cancelContext := context.WithCancel(c.Request.Context())
	c.Request = c.Request.WithContext(ctx)
	data := make(chan []byte, 1)
	errs := make(chan *interfaces.ErrorMessage)
	flusher := &notifyingFlusher{flushed: make(chan struct{}, 1)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ForwardStream(c, flusher, func(error) {}, data, errs, StreamForwardOptions{
			FlushMinBytes: 32768,
			WriteChunk: func(chunk []byte) {
				_, _ = c.Writer.Write(chunk)
			},
			FlushChunk: func(chunk []byte) bool {
				return string(chunk) == ": pending\n\n"
			},
		})
	}()

	data <- []byte(": pending\n\n")
	select {
	case <-flusher.flushed:
	case <-time.After(time.Second):
		t.Fatal("forced chunk was not flushed")
	}
	cancelContext()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ForwardStream did not stop after cancellation")
	}
}

func TestForwardStreamStopsOnChunkTransformationError(t *testing.T) {
	h, c, flusher := newForwardStreamTestContext(t)
	data := make(chan []byte, 2)
	errs := make(chan *interfaces.ErrorMessage)
	data <- []byte("oversized")
	data <- []byte("must-not-be-written")
	wantErr := errors.New("transform failed")
	var transformed atomic.Bool
	var canceled error
	var written bytes.Buffer
	var terminal *interfaces.ErrorMessage

	h.ForwardStream(c, flusher, func(err error) {
		canceled = err
	}, data, errs, StreamForwardOptions{
		WriteChunk: func(chunk []byte) {
			_, _ = written.Write(chunk)
			transformed.Store(true)
		},
		ChunkError: func() error {
			if transformed.Load() {
				return wantErr
			}
			return nil
		},
		WriteTerminalError: func(errMsg *interfaces.ErrorMessage) {
			terminal = errMsg
		},
	})

	if !errors.Is(canceled, wantErr) {
		t.Fatalf("cancel error = %v, want %v", canceled, wantErr)
	}
	if got := written.String(); got != "oversized" {
		t.Fatalf("response body = %q", got)
	}
	if terminal == nil || terminal.StatusCode != http.StatusBadGateway || !errors.Is(terminal.Error, wantErr) {
		t.Fatalf("terminal error = %#v, want 502 %v", terminal, wantErr)
	}
	if flusher.count == 0 {
		t.Fatal("fatal transformed chunk was not flushed")
	}
}

func TestForwardStreamSurfacesErrorCreatedByWriteDone(t *testing.T) {
	h, c, flusher := newForwardStreamTestContext(t)
	data := make(chan []byte)
	errs := make(chan *interfaces.ErrorMessage)
	close(data)
	close(errs)
	wantErr := errors.New("stream ended incomplete")
	var fatal error
	var canceled error
	var terminal *interfaces.ErrorMessage

	h.ForwardStream(c, flusher, func(err error) {
		canceled = err
	}, data, errs, StreamForwardOptions{
		WriteDone: func() {
			fatal = wantErr
		},
		ChunkError: func() error {
			return fatal
		},
		WriteTerminalError: func(errMsg *interfaces.ErrorMessage) {
			terminal = errMsg
		},
	})

	if !errors.Is(canceled, wantErr) {
		t.Fatalf("cancel error = %v, want %v", canceled, wantErr)
	}
	if terminal == nil || terminal.StatusCode != http.StatusBadGateway || !errors.Is(terminal.Error, wantErr) {
		t.Fatalf("terminal error = %#v, want 502 %v", terminal, wantErr)
	}
	if flusher.count == 0 {
		t.Fatal("terminal error created by WriteDone was not flushed")
	}
}

func TestForwardStreamUpdatesDynamicFlushPolicy(t *testing.T) {
	h, c, _ := newForwardStreamTestContext(t)
	ctx, cancelContext := context.WithCancel(c.Request.Context())
	c.Request = c.Request.WithContext(ctx)
	data := make(chan []byte)
	errs := make(chan *interfaces.ErrorMessage)
	flusher := &notifyingFlusher{flushed: make(chan struct{}, 1)}
	var immediate atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ForwardStream(c, flusher, func(error) {}, data, errs, StreamForwardOptions{
			ResolveFlushPolicy: func() (*time.Duration, int) {
				if immediate.Load() {
					return nil, 0
				}
				interval := time.Hour
				return &interval, 1 << 20
			},
			WriteChunk: func(chunk []byte) {
				_, _ = c.Writer.Write(chunk)
			},
		})
	}()

	data <- []byte("buffered")
	select {
	case <-flusher.flushed:
		t.Fatal("initial batched policy flushed immediately")
	case <-time.After(20 * time.Millisecond):
	}
	immediate.Store(true)
	data <- []byte("immediate")
	select {
	case <-flusher.flushed:
	case <-time.After(time.Second):
		t.Fatal("dynamic immediate policy did not flush pending data")
	}
	cancelContext()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ForwardStream did not stop after cancellation")
	}
}

func TestSSECommentDetectorHandlesSplitFrame(t *testing.T) {
	detector := &SSECommentDetector{}
	if detector.Feed([]byte(": pending\n")) {
		t.Fatal("partial comment frame was reported complete")
	}
	if !detector.Feed([]byte("\n")) {
		t.Fatal("split comment frame was not detected")
	}
	if detector.Feed([]byte("data: {}\n\n")) {
		t.Fatal("data frame was reported as comment-only")
	}
}

func TestSSEBootstrapDetectorTracksActualLineBoundaries(t *testing.T) {
	detector := &SSEBootstrapDetector{}
	if detector.Feed([]byte(": pending\n\n")) {
		t.Fatal("comment-only chunk committed bootstrap")
	}
	if detector.Feed([]byte(`data: {"type":"response.created"}`)) {
		t.Fatal("unterminated data frame committed bootstrap")
	}
	if !detector.Feed([]byte("\n\n")) {
		t.Fatal("semantic SSE chunk did not commit bootstrap")
	}
}

func TestSSEBootstrapDetectorIgnoresControlOnlyFrames(t *testing.T) {
	detector := &SSEBootstrapDetector{}
	for _, chunk := range [][]byte{
		[]byte("eve"),
		[]byte("nt: response.created\r"),
		[]byte("id: 1\r"),
		[]byte("retry: 1000\r\r"),
	} {
		if detector.Feed(chunk) {
			t.Fatalf("control-only chunk committed bootstrap: %q", chunk)
		}
	}
	if detector.Feed([]byte("data: {\"type\":\"response.created\"}\r")) {
		t.Fatal("unterminated CR data frame committed bootstrap")
	}
	if !detector.Feed([]byte("\r")) {
		t.Fatal("complete CR data frame did not commit bootstrap")
	}
}

func TestSSEFrameHelpersSupportAllLineEndings(t *testing.T) {
	tests := []struct {
		name  string
		frame string
	}{
		{name: "LF", frame: "event: item\ndata: {}\n\n"},
		{name: "CRLF", frame: "event: item\r\ndata: {}\r\n\r\n"},
		{name: "CR", frame: "event: item\rdata: {}\r\r"},
		{name: "mixed", frame: "event: item\r\ndata: {}\n\r"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := []byte(test.frame + "tail")
			if got := SSEFrameLen(input); got != len(test.frame) {
				t.Fatalf("SSEFrameLen() = %d, want %d", got, len(test.frame))
			}
			payload, ok := sseDataJSONPayload([]byte(test.frame))
			if !ok || string(payload) != "{}" {
				t.Fatalf("payload = %q, %t", payload, ok)
			}
		})
	}
}

func TestSSEDataJSONFramerAcceptsCROnlyFrame(t *testing.T) {
	framer := &sseDataJSONFramer{}
	frames, err := framer.Feed([]byte("event: item\rdata: {\"ok\":true}\r\r"))
	if err != nil {
		t.Fatalf("Feed() error: %v", err)
	}
	if len(frames) != 0 {
		t.Fatalf("trailing CR was committed before the next byte: %#v", frames)
	}
	frames, err = framer.Finish()
	if err != nil {
		t.Fatalf("Finish() error: %v", err)
	}
	if len(frames) != 1 || string(frames[0]) != "event: item\rdata: {\"ok\":true}\r\r" {
		t.Fatalf("frames = %#v", frames)
	}
}

func TestSSEDataJSONFramerPreservesSplitCRLFFrameBoundary(t *testing.T) {
	framer := &sseDataJSONFramer{}
	frames, err := framer.Feed([]byte("event: item\r\ndata: {\"ok\":true}\r\n\r"))
	if err != nil {
		t.Fatalf("first Feed() error: %v", err)
	}
	if len(frames) != 0 {
		t.Fatalf("split CRLF frame was committed early: %#v", frames)
	}
	frames, err = framer.Feed([]byte("\n"))
	if err != nil {
		t.Fatalf("second Feed() error: %v", err)
	}
	const want = "event: item\r\ndata: {\"ok\":true}\r\n\r\n"
	if len(frames) != 1 || string(frames[0]) != want {
		t.Fatalf("frames = %#v, want %q", frames, want)
	}
}

func TestSSEFrameLenDefersTrailingCR(t *testing.T) {
	if got := SSEFrameLen([]byte("data: {}\r\n\r")); got != 0 {
		t.Fatalf("SSEFrameLen() = %d, want 0 for a possible split CRLF", got)
	}
	const complete = "data: {}\r\n\r\n"
	if got := SSEFrameLen([]byte(complete)); got != len(complete) {
		t.Fatalf("SSEFrameLen() = %d, want %d", got, len(complete))
	}
}

func TestSSEDataJSONFramerReassemblesSplitFrameWithoutDelimiter(t *testing.T) {
	framer := &sseDataJSONFramer{}
	frames, err := framer.Feed([]byte(`data: {"type":"response.completed","response":{`))
	if err != nil || len(frames) != 0 {
		t.Fatalf("first Feed() = (%#v, %v)", frames, err)
	}
	frames, err = framer.Feed([]byte(`"output":[]}}`))
	if err != nil {
		t.Fatalf("second Feed() error: %v", err)
	}
	if len(frames) != 1 || string(frames[0]) != `data: {"type":"response.completed","response":{"output":[]}}` {
		t.Fatalf("frames = %#v", frames)
	}
}

func TestSSEDataJSONFrameSemanticClassification(t *testing.T) {
	for _, frame := range [][]byte{
		[]byte(": pending\n\n"),
		[]byte("event: response.created\nid: 1\nretry: 1000\n\n"),
		[]byte("data:\n\n"),
	} {
		if sseDataJSONFrameHasSemanticData(frame) {
			t.Fatalf("control frame was classified as semantic: %q", frame)
		}
	}
	for _, frame := range [][]byte{
		[]byte("data: {}\n\n"),
		[]byte("event: done\ndata: [DONE]\n\n"),
	} {
		if !sseDataJSONFrameHasSemanticData(frame) {
			t.Fatalf("data frame was not classified as semantic: %q", frame)
		}
	}
}

func TestSSECommentDetectorFlushesOversizedUnterminatedFrame(t *testing.T) {
	detector := &SSECommentDetector{}
	if !detector.Feed(bytes.Repeat([]byte("x"), (64<<10)+1)) {
		t.Fatal("oversized unterminated frame was not flushed")
	}
}

func TestSSECommentDetectorBoundsLargeTailAfterCompleteFrame(t *testing.T) {
	detector := &SSECommentDetector{}
	if detector.Feed([]byte("data: {}\n\n")) {
		t.Fatal("data frame was reported as comment-only")
	}
	if !detector.Feed(bytes.Repeat([]byte("x"), (64<<10)+1)) {
		t.Fatal("large tail was not flushed")
	}
	if detector.frameBytes != (64<<10)+1 || detector.lineBytes != (64<<10)+1 {
		t.Fatalf("detector state = frame:%d line:%d", detector.frameBytes, detector.lineBytes)
	}
}

func TestSSEDataJSONFramerReleasesCompleteFrameBeforePartialTail(t *testing.T) {
	framer := &sseDataJSONFramer{maxPendingBytes: 64}
	frames, err := framer.Feed([]byte("data: {\"a\":1}\n\ndata: {\"b\":"))
	if err != nil {
		t.Fatalf("Feed() error: %v", err)
	}
	if len(frames) != 1 || string(frames[0]) != "data: {\"a\":1}\n\n" {
		t.Fatalf("frames = %#v", frames)
	}
	if got := string(framer.pending); got != "data: {\"b\":" {
		t.Fatalf("pending = %q", got)
	}
}

func TestSSEDataJSONFramerDoesNotTreatChunkPrefixAsLineBoundary(t *testing.T) {
	for _, prefix := range []string{"data:", "event:", "id:", "retry:", ":"} {
		t.Run(prefix, func(t *testing.T) {
			framer := &sseDataJSONFramer{}
			if frames, err := framer.Feed([]byte(`data: {"value":"`)); err != nil || len(frames) != 0 {
				t.Fatalf("first Feed() = (%#v, %v)", frames, err)
			}
			frames, err := framer.Feed([]byte(prefix + "text\"}\n\n"))
			if err != nil {
				t.Fatalf("second Feed() error: %v", err)
			}
			if len(frames) != 1 || !bytes.Contains(frames[0], []byte(`"value":"`+prefix+`text"`)) {
				t.Fatalf("frames = %#v", frames)
			}
		})
	}
}

func TestSSEDataJSONFramerRejectsInvalidEvent(t *testing.T) {
	framer := &sseDataJSONFramer{}
	if _, err := framer.Feed([]byte("data: {invalid}\n\n")); err == nil {
		t.Fatal("invalid complete event was accepted")
	}

	framer = &sseDataJSONFramer{}
	if _, err := framer.Feed([]byte(`data: {"type"`)); err != nil {
		t.Fatalf("partial Feed() error: %v", err)
	}
	if _, err := framer.Finish(); err == nil {
		t.Fatal("invalid event at EOF was accepted")
	}
}

func TestSSEDataJSONFramerBoundsIncompleteEvent(t *testing.T) {
	framer := &sseDataJSONFramer{maxPendingBytes: 16}
	if _, err := framer.Feed([]byte("data: \"" + strings.Repeat("x", 32))); err == nil {
		t.Fatal("oversized incomplete event was accepted")
	}
}

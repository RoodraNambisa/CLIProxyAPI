package executor

import (
	"io"
	"testing"
)

func TestRequestBodyReleaseControllerReleaseCallbacks(t *testing.T) {
	ctrl := NewRequestBodyReleaseController(12, []byte("<released>"))
	var got string
	ctrl.RegisterReleaseCallback(func(placeholder []byte) {
		got = string(placeholder)
	})
	if !ctrl.Replayable() {
		t.Fatal("Replayable() = false before release")
	}
	if !ctrl.Release() {
		t.Fatal("Release() = false, want first release to win")
	}
	if ctrl.Replayable() {
		t.Fatal("Replayable() = true after release")
	}
	if got != "<released>" {
		t.Fatalf("callback placeholder = %q, want <released>", got)
	}
	if ctrl.Release() {
		t.Fatal("second Release() = true, want false")
	}
}

func TestRequestBodyReleaseControllerLogOnlyStaysReplayable(t *testing.T) {
	ctrl := NewRequestBodyReleaseControllerWithMode(12, []byte("<released>"), true)
	if !ctrl.LogOnly() {
		t.Fatal("LogOnly() = false, want true")
	}
	if !ctrl.Release() {
		t.Fatal("Release() = false, want first release to win")
	}
	if !ctrl.Replayable() {
		t.Fatal("Replayable() = false after log-only release")
	}
}

func TestReleasableBytesReleaseDropsReference(t *testing.T) {
	holder := NewReleasableBytes([]byte("large-body"))
	if got := string(holder.Bytes()); got != "large-body" {
		t.Fatalf("Bytes() = %q, want large-body", got)
	}
	holder.Release()
	if got := holder.Bytes(); got != nil {
		t.Fatalf("Bytes() after Release() = %q, want nil", got)
	}
}

func TestReleasableBytesReplaceSwapsReference(t *testing.T) {
	holder := NewReleasableBytes([]byte("large-body"))
	holder.Replace([]byte("metadata"))
	if got := string(holder.Bytes()); got != "metadata" {
		t.Fatalf("Bytes() after Replace() = %q, want metadata", got)
	}
}

func TestRegisterRequestBodyReleaseCleanupClearsLocalCopies(t *testing.T) {
	ctrl := NewRequestBodyReleaseController(1024, []byte("<released>"))
	req := Request{Payload: []byte("payload")}
	opts := Options{
		OriginalRequest: []byte("original"),
		Metadata: map[string]any{
			BodyReleaseControllerMetadataKey: ctrl,
		},
	}
	unregister := RegisterRequestBodyReleaseCleanup(nil, &req, &opts)
	defer unregister()

	ctrl.Release()

	if req.Payload != nil {
		t.Fatalf("req.Payload after release = %q, want nil", req.Payload)
	}
	if opts.OriginalRequest != nil {
		t.Fatalf("opts.OriginalRequest after release = %q, want nil", opts.OriginalRequest)
	}
}

func TestReleasableReadCloserReleasesOnEOF(t *testing.T) {
	released := false
	reader := NewReleasableReadCloser([]byte("hello"), func() {
		released = true
	})
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("data = %q, want hello", data)
	}
	if !released {
		t.Fatal("released = false after EOF")
	}
}

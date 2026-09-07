package helps

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

const bootstrapOverload = `{"type":"response.failed","response":{"error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"busy"}}}`

func TestCodexBootstrapOnlyAcceptsKnownCapacityFailures(t *testing.T) {
	for _, tc := range []struct {
		payload string
		status  int
	}{
		{bootstrapOverload, 503},
		{`{"type":"error","status":429,"error":{"type":"service_unavailable_error","code":"server_is_overloaded"}}`, 429},
		{`{"type":"response.incomplete","response":{"error":{"type":"rate_limit_error","code":"rate_limit_exceeded"}}}`, 429},
		{`{"type":"error","error":{"type":"service_unavailable_error","code":"misalignment_policy_violation"}}`, 0},
		{`{"type":"error","error":{"type":"permission_error","code":"server_is_overloaded"}}`, 0},
		{`{"type":"error","status":401,"error":{"type":"service_unavailable_error"}}`, 0},
		{`{"type":"error","error":{"code":"invalid_api_key","type":"service_unavailable_error"}}`, 0},
		{`{"type":"response.failed","response":{"error":{"message":"server_is_overloaded"}}}`, 0},
		{`{"type":"response.output_item.done","error":{"type":"service_unavailable_error"}}`, 0},
		{`{"type":"response.incomplete","response":{"incomplete_details":{"reason":"max_tokens"}}}`, 0},
	} {
		if got := CodexBootstrapOverloadStatus([]byte(tc.payload)); got != tc.status {
			t.Errorf("classification = %d, want %d", got, tc.status)
		}
	}
}

func TestCodexSSEBootstrapReplayAndBounds(t *testing.T) {
	metadata := "data: {\"type\":\"response.created\"}\n\n"
	overload := "data: " + bootstrapOverload + "\n\n"
	for _, tc := range []struct {
		name, prefix string
		wantFailure  bool
	}{
		{"empty", "", true},
		{"metadata", metadata, true},
		{"controls", ": ping\r\nevent: pending\r\nid: 4\r\nretry: 100\r\n\r\n" + metadata, true},
		{"standalone lines", strings.TrimSpace(metadata) + "\n", true},
		{"multiline", "data: {\"type\":\n" + "data: \"response.created\"}\n\n", true},
		{"content", "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n", false},
		{"unknown", "data: {\"type\":\"future.metadata\"}\n\n", false},
		{"malformed", "data: invalid\n\n", false},
		{"event cap", strings.Repeat(metadata, CodexBootstrapMaxEvents), false},
		{"byte cap", ":" + strings.Repeat("x", CodexBootstrapMaxBytes) + "\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wire := tc.prefix + overload
			body := io.NopCloser(strings.NewReader(wire))
			reader, failure, err := ProbeCodexSSEBootstrap(t.Context(), body, func() bool { return true })
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = body.Close() }()
			if tc.wantFailure {
				if reader != nil || failure == nil || failure.Status != 503 || string(failure.Payload) != bootstrapOverload {
					t.Fatal("eligible overload was not isolated")
				}
				return
			}
			if failure != nil {
				t.Fatal("committed or bounded stream remained retryable")
			}
			got, err := io.ReadAll(reader)
			if err != nil || string(got) != wire {
				t.Fatal("probe changed replay bytes")
			}
		})
	}
}

func TestCodexSSEBootstrapCancellationReleaseAndReadError(t *testing.T) {
	t.Run("cancel blocked read", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		reader, writer := io.Pipe()
		defer func() { _ = writer.Close() }()
		done := make(chan error, 1)
		go func() { _, _, err := ProbeCodexSSEBootstrap(ctx, reader, func() bool { return true }); done <- err }()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal("probe did not stop on cancellation")
		}
	})
	t.Run("release while reading", func(t *testing.T) {
		reader, writer := io.Pipe()
		var replayable atomic.Bool
		replayable.Store(true)
		go func() {
			_, _ = fmt.Fprintln(writer, `data: {"type":"response.created"}`)
			replayable.Store(false)
			_, _ = fmt.Fprintln(writer, "data: "+bootstrapOverload)
			_ = writer.Close()
		}()
		body, failure, err := ProbeCodexSSEBootstrap(t.Context(), reader, replayable.Load)
		if err != nil || failure != nil {
			t.Fatal("released body enabled overload retry")
		}
		got, err := io.ReadAll(body)
		if err != nil || !strings.Contains(string(got), bootstrapOverload) {
			t.Fatal("release dropped pending events")
		}
		_ = body.Close()
	})
	t.Run("preserve original read failure", func(t *testing.T) {
		original := errors.New("original read failure")
		body, failure, err := ProbeCodexSSEBootstrap(t.Context(), io.NopCloser(io.MultiReader(strings.NewReader("data: {\"type\":\"response.created\"}\n\n"), codexBootstrapReadError{original})), func() bool { return true })
		if err != nil || failure != nil {
			t.Fatal("read failure became an overload")
		}
		got, err := io.ReadAll(body)
		if !errors.Is(err, original) || len(got) == 0 {
			t.Fatal("probe lost prefix or read cause")
		}
		_ = body.Close()
	})
}

func TestCodexBootstrapDisconnectGateConcurrentCompletion(t *testing.T) {
	for _, discard := range []bool{false, true} {
		for range 100 {
			var calls atomic.Int64
			gate := NewCodexBootstrapDisconnectGate(func(error) { calls.Add(1) })
			var wg sync.WaitGroup
			wg.Go(func() { gate.Notify(io.EOF) })
			wg.Go(func() { gate.Finish(discard) })
			wg.Wait()
			gate.Finish(!discard)
			want := int64(1)
			if discard {
				want = 0
			}
			if calls.Load() != want {
				t.Fatal("disconnect gate lost its final decision")
			}
		}
	}
}

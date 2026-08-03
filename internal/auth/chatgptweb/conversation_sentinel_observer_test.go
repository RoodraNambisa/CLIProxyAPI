package chatgptweb

import (
	"errors"
	"testing"
	"time"
)

func TestConversationSentinelObserverVMPreservesCollectorState(t *testing.T) {
	const requirementsToken = "requirements"
	collectorDX := encodeConversationTurnstileProgram(t, requirementsToken, []any{
		[]any{2, 40, "collector-state"},
	})
	snapshotDX := encodeConversationTurnstileProgram(t, requirementsToken, []any{
		[]any{7, 3, 40},
	})
	observer, err := newConversationSentinelObserverVM(
		t.Context(),
		collectorDX,
		snapshotDX,
		requirementsToken,
		ConversationTurnstileEnvironment{Persona: DefaultPersona()},
		zeroReader{},
		func() time.Time { return time.Unix(1_700_000_000, 0) },
	)
	if err != nil {
		t.Fatalf("newConversationSentinelObserverVM() error = %v", err)
	}
	defer observer.Close()
	snapshot, err := observer.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if snapshot != "Y29sbGVjdG9yLXN0YXRl" {
		t.Fatalf("Snapshot() = %q", snapshot)
	}
}

func TestConversationSentinelObserverVMReportsCollectorCompatibility(t *testing.T) {
	const requirementsToken = "requirements"
	unsupportedDX := encodeConversationTurnstileProgram(t, requirementsToken, []any{
		[]any{36, "unsupported"},
	})
	_, err := newConversationSentinelObserverVM(
		t.Context(),
		unsupportedDX,
		unsupportedDX,
		requirementsToken,
		ConversationTurnstileEnvironment{Persona: DefaultPersona()},
		zeroReader{},
		time.Now,
	)
	var compatibility *SentinelCompatibilityError
	if !errors.As(err, &compatibility) || compatibility.ProgramKind != SentinelProgramObserverCollect {
		t.Fatalf("collector error = %#v", err)
	}
}

func TestConversationSentinelObserverVMReportsSnapshotCompatibility(t *testing.T) {
	const requirementsToken = "requirements"
	collectorDX := encodeConversationTurnstileProgram(t, requirementsToken, []any{
		[]any{2, 40, "collector-state"},
	})
	snapshotDX := encodeConversationTurnstileProgram(t, requirementsToken, []any{
		[]any{36, "unsupported"},
	})
	observer, err := newConversationSentinelObserverVM(
		t.Context(),
		collectorDX,
		snapshotDX,
		requirementsToken,
		ConversationTurnstileEnvironment{Persona: DefaultPersona()},
		zeroReader{},
		time.Now,
	)
	if err != nil {
		t.Fatalf("newConversationSentinelObserverVM() error = %v", err)
	}
	defer observer.Close()
	_, err = observer.Snapshot(t.Context())
	var compatibility *SentinelCompatibilityError
	if !errors.As(err, &compatibility) || compatibility.ProgramKind != SentinelProgramObserverSnapshot {
		t.Fatalf("snapshot error = %#v", err)
	}
}

func TestConversationSentinelObserverVMPreservesMissingValueProvenance(t *testing.T) {
	const requirementsToken = "requirements"
	collectorDX := encodeConversationTurnstileProgram(t, requirementsToken, []any{
		[]any{2, 40, "navigator"},
		[]any{6, 41, 10, 40},
		[]any{2, 42, "futureCapability"},
		[]any{6, 43, 41, 42},
	})
	snapshotDX := encodeConversationTurnstileProgram(t, requirementsToken, []any{
		[]any{7, 3, 43},
	})
	observer, err := newConversationSentinelObserverVM(
		t.Context(),
		collectorDX,
		snapshotDX,
		requirementsToken,
		ConversationTurnstileEnvironment{Persona: DefaultPersona()},
		zeroReader{},
		time.Now,
	)
	if err != nil {
		t.Fatalf("newConversationSentinelObserverVM() error = %v", err)
	}
	defer observer.Close()
	_, err = observer.Snapshot(t.Context())
	var compatibility *SentinelCompatibilityError
	if !errors.As(err, &compatibility) || compatibility.ProgramKind != SentinelProgramObserverSnapshot || compatibility.Kind != SentinelCompatibilityMissingEnvironment {
		t.Fatalf("snapshot missing-value error = %#v", err)
	}
}

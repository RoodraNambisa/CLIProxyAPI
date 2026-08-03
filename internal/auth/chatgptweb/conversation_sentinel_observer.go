package chatgptweb

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// conversationSentinelObserverVM retains the compact VM state shared by one
// Session Observer collector and snapshot pair.
type conversationSentinelObserverVM struct {
	mu       sync.Mutex
	vm       *conversationTurnstileVM
	snapshot *conversationTurnstilePreparedProgram
}

func newConversationSentinelObserverVM(
	ctx context.Context,
	collectorDX string,
	snapshotDX string,
	requirementsToken string,
	environment ConversationTurnstileEnvironment,
	reader io.Reader,
	now func() time.Time,
) (*conversationSentinelObserverVM, error) {
	memoryBudget := &conversationTurnstileMemoryBudget{}
	collector, err := prepareConversationTurnstileProgram(ctx, collectorDX, requirementsToken, memoryBudget)
	if err != nil {
		return nil, fmt.Errorf("prepare Sentinel Observer collector: %w", err)
	}
	snapshot, err := prepareConversationTurnstileProgram(ctx, snapshotDX, requirementsToken, memoryBudget)
	if err != nil {
		return nil, fmt.Errorf("prepare Sentinel Observer snapshot: %w", err)
	}
	vm, err := newConversationTurnstileVM(
		ctx,
		collector,
		environment,
		reader,
		now,
		defaultConversationTurnstileMaxSteps,
		0,
		nil,
		true,
		SentinelProgramObserverCollect,
	)
	if err != nil {
		return nil, err
	}
	if _, err = vm.runPreparedProgram(); err != nil {
		return nil, err
	}
	return &conversationSentinelObserverVM{vm: vm, snapshot: snapshot}, nil
}

func (observer *conversationSentinelObserverVM) Snapshot(ctx context.Context) (string, error) {
	if observer == nil {
		return "", fmt.Errorf("Sentinel Observer Go VM is unavailable")
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if observer.vm == nil || observer.snapshot == nil {
		return "", fmt.Errorf("Sentinel Observer Go VM is closed")
	}
	snapshot, err := observer.vm.continuePreparedProgram(ctx, observer.snapshot, SentinelProgramObserverSnapshot)
	if err != nil {
		return "", err
	}
	snapshot = strings.TrimSpace(snapshot)
	if snapshot == "" {
		return "", observer.vm.compatibilityError(
			SentinelCompatibilityUnsupportedValue,
			"observer_snapshot_empty",
			fmt.Errorf("Sentinel Observer snapshot is empty"),
		)
	}
	return snapshot, nil
}

func (observer *conversationSentinelObserverVM) Close() {
	if observer == nil {
		return
	}
	observer.mu.Lock()
	observer.vm = nil
	observer.snapshot = nil
	observer.mu.Unlock()
}

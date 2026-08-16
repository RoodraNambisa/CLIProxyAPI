package executor

import (
	"bytes"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
)

func TestChatGPTWebDownloadUsesReservedCompletionMemoryAtCapacity(t *testing.T) {
	baseline := helps.ChatGPTWebImageMemorySnapshot()
	if baseline.ProcessingTasks != 0 {
		t.Fatalf("image memory busy before test: %#v", baseline)
	}
	const reserveBytes = int64(1 << 20)
	fillBytes := baseline.CapacityBytes - reserveBytes
	if fillBytes < 1 {
		t.Skip("image memory capacity is too small for reserve test")
	}
	releaseFill, err := helps.AcquireChatGPTWebImageMemory(t.Context(), fillBytes)
	if err != nil {
		t.Fatalf("fill image memory: %v", err)
	}
	defer releaseFill()
	leases := helps.NewChatGPTWebImageMemoryLeaseSet()
	if !leases.TryReserveCompletion(reserveBytes) {
		t.Fatal("TryReserveCompletion() = false")
	}
	defer leases.Release()
	releaseTurn, err := leases.BeginFinalization(t.Context())
	if err != nil {
		t.Fatalf("BeginFinalization() error = %v", err)
	}
	defer releaseTurn()
	ctx := helps.WithChatGPTWebImageMemoryLeaseSet(t.Context(), leases)
	source := bytes.Repeat([]byte{0x7f}, 512<<10)
	type downloadResult struct {
		payload []byte
		err     error
	}
	result := make(chan downloadResult, 1)
	go func() {
		payload, errRead := readChatGPTWebImageDownloadBody(ctx, bytes.NewReader(source), int64(len(source)), len(source)+1)
		result <- downloadResult{payload: payload, err: errRead}
	}()
	deadline := time.Now().Add(time.Second)
	for helps.ChatGPTWebImageMemorySnapshot().WaitingTasks == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if snapshot := helps.ChatGPTWebImageMemorySnapshot(); snapshot.WaitingTasks != 1 {
		t.Fatalf("download did not wait for bounded completion memory: %#v", snapshot)
	}
	releaseFill()
	var download downloadResult
	select {
	case download = <-result:
	case <-time.After(time.Second):
		t.Fatal("download did not resume after completion memory became available")
	}
	if download.err != nil {
		t.Fatalf("readChatGPTWebImageDownloadBody() error = %v", download.err)
	}
	payload := download.payload
	if !bytes.Equal(payload, source) {
		t.Fatalf("download payload length = %d, want %d", len(payload), len(source))
	}
	leases.Release()
	if snapshot := helps.ChatGPTWebImageMemorySnapshot(); snapshot.ProcessingTasks != 0 || snapshot.ProcessingBytes != 0 {
		t.Fatalf("image memory leaked: %#v", snapshot)
	}
}

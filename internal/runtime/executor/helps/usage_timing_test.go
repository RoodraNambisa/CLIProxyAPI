package helps

import (
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func TestUsageTimingSeparatesPacketAndContent(t *testing.T) {
	reporter := NewUsageReporter(t.Context(), "codex", "fixture", nil)
	reporter.MarkFirstResponseByte()
	if token, packet := reporter.responseTimings(); token != 0 || packet != 0 {
		t.Fatal("timing started before an upstream attempt")
	}
	reporter.StartResponseTTFT()
	ObserveResponsesTokenEvent(reporter, []byte(`{"type":"response.created"}`))
	token, packet := reporter.responseTimings()
	if token != 0 || packet <= 0 || reporter.IsTTFTSet() {
		t.Fatal("metadata fabricated a first token")
	}
	ObserveResponsesTokenEvent(reporter, []byte(`{"type":"response.failed","response":{"error":{"message":"failed"}}}`))
	if failed := reporter.buildRecord(usage.Detail{}, true); failed.TTFT != 0 || failed.FirstPacketLatency != packet || !failed.Failed {
		t.Fatal("failed metadata-only turn used first packet as TTFT")
	}
	ObserveResponsesTokenEvent(reporter, []byte(`{"type":"response.output_text.delta","delta":"content"}`))
	token, finalPacket := reporter.responseTimings()
	if token < packet || finalPacket != packet || !reporter.IsTTFTSet() {
		t.Fatal("content timing lost the independent first packet")
	}
	reporter.StartResponseTTFT()
	reporter.MarkFirstResponseByte()
	ObserveResponsesTokenEvent(reporter, []byte(`{"type":"response.output_text.delta","delta":"later"}`))
	if got, _ := reporter.responseTimings(); got != token {
		t.Fatal("later attempts or content changed the first content timestamp")
	}
	if record := reporter.buildRecord(usage.Detail{}, false); record.TTFT != token || record.FirstPacketLatency != packet {
		t.Fatal("usage record lost timing metadata")
	}
}

func TestUsageTimingConcurrentObservationAndFastPath(t *testing.T) {
	for _, content := range []bool{false, true} {
		reporter := NewUsageReporter(t.Context(), "codex", "fixture", nil)
		reporter.StartResponseTTFT()
		var workers sync.WaitGroup
		for worker := 0; worker < 32; worker++ {
			workers.Go(func() {
				for iteration := 0; iteration < 100; iteration++ {
					reporter.StartResponseTTFT()
					reporter.ObserveTokenEvent(content)
					reporter.responseTimings()
				}
			})
		}
		workers.Wait()
		token, packet := reporter.responseTimings()
		if packet <= 0 || (content && (token < packet || !reporter.IsTTFTSet())) || (!content && token != 0) {
			t.Fatal("concurrent observation corrupted timing state")
		}
		if content {
			payload := []byte(`{"type":"response.output_text.delta","delta":"later"}`)
			if allocations := testing.AllocsPerRun(1000, func() { ObserveResponsesTokenEvent(reporter, payload) }); allocations != 0 {
				t.Fatalf("completed timing hot path allocated %g times", allocations)
			}
		}
	}
}

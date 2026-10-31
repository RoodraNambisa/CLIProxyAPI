package executor

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func BenchmarkIdentityPreparationDisabled(b *testing.B) {
	e := NewCodexExecutor(&config.Config{})
	req := core.Request{Payload: []byte(`{"model":"gpt-5.4","input":"hello"}`)}
	_, _ = e.PrepareProviderRequest(context.Background(), req, core.Options{}, core.RequestOperationExecute)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, err := e.PrepareProviderRequest(context.Background(), req, core.Options{}, core.RequestOperationExecute)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// This opt-in matrix measures active request work without preallocating identity
// histories or pools for the credential population. Bodies are shared inputs.
func TestCodexIdentityPerformanceMatrix(t *testing.T) {
	if os.Getenv("CPA_IDENTITY_PERF") != "1" {
		t.Skip("opt-in performance matrix")
	}
	enabled := true
	independent := false
	cfg := &config.Config{Routing: config.RoutingConfig{PriorityOverrides: []config.RoutingPriorityOverride{{Priority: 5, Codex: &config.CodexIdentityOverride{IdentityConfuse: &enabled, RequireSessionAffinity: &independent}}}}}
	set := config.CompileCodexIdentityPolicies(cfg)
	for _, count := range []int{10000, 100000} {
		auths := make([]coreauth.Auth, count)
		for i := range auths {
			auths[i] = coreauth.Auth{ID: fmt.Sprintf("perf-%d", i), Attributes: map[string]string{"priority": "5"}}
		}
		for _, size := range []int{4096, 1 << 20, 16 << 20} {
			raw := append([]byte(`{"input":"`), bytes.Repeat([]byte{'x'}, size)...)
			raw = append(raw, []byte(`","client_metadata":{"session_id":"s","thread_id":"s","turn_id":"t","context_window_id":"c"}}`)...)
			for _, workers := range []int{1, 100, 500} {
				runtime.GC()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				durations := make([]int64, workers)
				var wg sync.WaitGroup
				ready := make(chan struct{})
				start := time.Now()
				for i := 0; i < workers; i++ {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						<-ready
						at := time.Now()
						a := &auths[i%count]
						p := helps.ResolveCodexIdentityPolicy(set, a)
						tr := &helps.CodexIdentityTransform{Scope: a.ID, Restore: p.RestoreResponse}
						out := helps.RewriteCodexIdentityEnvelope(raw, tr.Map)
						if len(out) < size {
							t.Error("invalid projection")
						}
						durations[i] = time.Since(at).Nanoseconds()
					}(i)
				}
				close(ready)
				wg.Wait()
				elapsed := time.Since(start)
				runtime.ReadMemStats(&after)
				sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
				t.Logf("creds=%d bytes=%d workers=%d throughput=%.1f/s p95=%s p99=%s alloc/request=%d heap=%d", count, size, workers, float64(workers)/elapsed.Seconds(), time.Duration(durations[(workers-1)*95/100]), time.Duration(durations[(workers-1)*99/100]), (after.TotalAlloc-before.TotalAlloc)/uint64(workers), after.HeapAlloc)
			}
		}
	}
}

func TestCodexIdentityStreamingPerformanceMatrix(t *testing.T) {
	if os.Getenv("CPA_IDENTITY_PERF") != "1" {
		t.Skip("opt-in streaming performance matrix")
	}
	const events = 1000
	for _, framed := range []bool{false, true} {
		for _, restore := range []bool{false, true} {
			for _, workers := range []int{1, 100, 500} {
				durations := make([]int64, workers*events)
				var wg sync.WaitGroup
				ready := make(chan struct{})
				start := time.Now()
				for i := 0; i < workers; i++ {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						tr := &helps.CodexIdentityTransform{Scope: "stream-shared", Generation: "1", Restore: restore}
						id := tr.Map("session_id", "client")
						body := []byte(`{"type":"response.metadata","response":{"session_id":"` + id + `"}}`)
						if framed {
							body = append(append([]byte("event: response.metadata\r\ndata: "), body...), []byte("\r\n\r\n")...)
						}
						<-ready
						for event := 0; event < events; event++ {
							at := time.Now()
							out := tr.Response(body)
							tr.ObserveReturned(out)
							durations[i*events+event] = time.Since(at).Nanoseconds()
						}
					}(i)
				}
				close(ready)
				wg.Wait()
				elapsed := time.Since(start)
				sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
				t.Logf("SSE=%t restore=%t workers=%d events=%d throughput=%.1f/s p95=%s p99=%s", framed, restore, workers, len(durations), float64(len(durations))/elapsed.Seconds(), time.Duration(durations[(len(durations)-1)*95/100]), time.Duration(durations[(len(durations)-1)*99/100]))
			}
		}
	}
}

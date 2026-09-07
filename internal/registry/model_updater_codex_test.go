package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCodexClientCatalogFailedRefreshKeepsCurrentSnapshot(t *testing.T) {
	original, revision := GetCodexClientModelsSnapshot()
	originalURLs := codexClientModelsURLs
	t.Cleanup(func() {
		codexClientModelsURLs = originalURLs
		codexClientCatalogStore.mu.Lock()
		codexClientCatalogStore.data, codexClientCatalogStore.revision = original, revision
		codexClientCatalogStore.mu.Unlock()
	})
	var status atomic.Int64
	status.Store(500)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(int(status.Load()))
		_, _ = w.Write([]byte(`{"models":[]}`))
	}))
	defer server.Close()
	codexClientModelsURLs = []string{server.URL}
	for _, code := range []int64{500, 200} {
		status.Store(code)
		tryRefreshCodexClientModels(context.Background(), "test")
		data, current := GetCodexClientModelsSnapshot()
		if current != revision || !bytes.Equal(data, original) {
			t.Fatal("failed refresh replaced current catalog")
		}
	}
	changed := modifiedClientCatalog(t, func(p *codexClientModelsPayload) { p.Models[0]["display_name"] = "refreshed Astra" })
	valid := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(changed) }))
	defer valid.Close()
	codexClientModelsURLs = []string{server.URL, valid.URL}
	tryRefreshCodexClientModels(context.Background(), "test")
	data, updated := GetCodexClientModelsSnapshot()
	if updated != revision+1 || !bytes.Contains(data, []byte("refreshed Astra")) {
		t.Fatal("valid fallback was not published")
	}
	tryRefreshCodexClientModels(context.Background(), "test")
	_, unchanged := GetCodexClientModelsSnapshot()
	if unchanged != updated {
		t.Fatal("identical refresh changed revision")
	}
}

func TestModelCatalogDownloadsEnforceBodyLimitAndSkipErrorBody(t *testing.T) {
	clientURLs, ordinaryURLs := codexClientModelsURLs, modelsURLs
	before, revision := GetCodexClientModelsSnapshot()
	t.Cleanup(func() {
		codexClientModelsURLs, modelsURLs = clientURLs, ordinaryURLs
		codexClientCatalogStore.mu.Lock()
		codexClientCatalogStore.data, codexClientCatalogStore.revision = before, revision
		codexClientCatalogStore.mu.Unlock()
	})
	oversized := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(bytes.Repeat([]byte(" "), maxModelsCatalogBytes+1))
	}))
	defer oversized.Close()
	codexClientModelsURLs, modelsURLs = []string{oversized.URL}, []string{oversized.URL}
	tryRefreshCodexClientModels(context.Background(), "test")
	if data, current := GetCodexClientModelsSnapshot(); current != revision || !bytes.Equal(data, before) {
		t.Fatal("oversized client catalog replaced snapshot")
	}
	if catalog, _ := fetchModelsFromRemote(context.Background()); catalog != nil {
		t.Fatal("oversized ordinary catalog was accepted")
	}
	errorBody := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer errorBody.Close()
	validBody := modifiedClientCatalog(t, func(p *codexClientModelsPayload) { p.Models[0]["display_name"] = "error fallback" })
	valid := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(validBody) }))
	defer valid.Close()
	codexClientModelsURLs = []string{errorBody.URL, valid.URL}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tryRefreshCodexClientModels(ctx, "test")
	if ctx.Err() != nil {
		t.Fatal("fetch waited for an error response body")
	}
	if data, current := GetCodexClientModelsSnapshot(); current != revision+1 || !bytes.Contains(data, []byte("error fallback")) {
		t.Fatal("valid fallback after error status was not published")
	}
}

func TestCodexClientCatalogCancellationAndSnapshotIsolation(t *testing.T) {
	oldURLs := codexClientModelsURLs
	t.Cleanup(func() { codexClientModelsURLs = oldURLs })
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	codexClientModelsURLs = []string{server.URL}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); tryRefreshCodexClientModels(ctx, "test") }()
	<-started
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancellation did not release catalog fetch")
	}
	data, revision := GetCodexClientModelsSnapshot()
	data[0] = 'x'
	var readers sync.WaitGroup
	for range 8 {
		readers.Go(func() {
			for range 20 {
				snapshot, current := GetCodexClientModelsSnapshot()
				if current != revision || !json.Valid(snapshot) {
					t.Error("snapshot mutation escaped to another reader")
				}
			}
		})
	}
	readers.Wait()
}

func TestModelCatalogRefreshLoopKeepsIndependentFlightsAndCancels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ticks := make(chan time.Time)
	ordinaryCalls := make(chan string, 8)
	clientCalls := make(chan string, 8)
	releaseOrdinary := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		runModelCatalogRefreshLoop(ctx, ticks, func(ctx context.Context, label string) {
			ordinaryCalls <- label
			select {
			case <-releaseOrdinary:
			case <-ctx.Done():
			}
		}, func(ctx context.Context, label string) {
			clientCalls <- label
			<-ctx.Done()
		})
	}()
	if !strings.HasPrefix(<-ordinaryCalls, "startup") || !strings.HasPrefix(<-clientCalls, "startup") {
		t.Fatal("both catalogs were not refreshed at startup")
	}
	for range 3 {
		ticks <- time.Now()
	}
	if len(ordinaryCalls) != 0 || len(clientCalls) != 0 {
		t.Fatal("refresh overlap created duplicate downloads")
	}
	close(releaseOrdinary)
	// A slow Codex catalog cannot stop later ordinary catalog refreshes.
	deadline := time.After(time.Second)
	waiting := true
	for waiting {
		select {
		case ticks <- time.Now():
		case label := <-ordinaryCalls:
			if !strings.HasPrefix(label, "periodic") {
				t.Error("wrong refresh phase")
			}
			waiting = false
		case <-deadline:
			t.Fatal("one catalog blocked the other catalog's refresh")
		}
	}
	if len(clientCalls) != 0 {
		t.Fatal("stalled download was duplicated")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("refresh loop did not stop")
	}
}

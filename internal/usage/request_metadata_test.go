package usage_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	statistics "github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func usageRequestContext(t *testing.T) (context.Context, *gin.Context) {
	t.Helper()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/alpha/search?key=private-query-fixture", nil)
	c.Request.RemoteAddr = "203.0.113.10:1234"
	ctx, cancel := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil).GetContextWithCancel(nil, c, t.Context())
	t.Cleanup(func() { cancel() })
	return ctx, c
}

func TestUsageRequestMetadataSurvivesGinReuse(t *testing.T) {
	ctx, c := usageRequestContext(t)
	c.Writer.WriteHeader(http.StatusBadGateway)
	c.Request = httptest.NewRequest(http.MethodGet, "/another/request", nil)
	c.Request.RemoteAddr = "198.51.100.2:1234"
	stats := statistics.NewRequestStatistics()
	for _, failed := range []bool{false, true} {
		stats.Record(ctx, coreusage.Record{Provider: "codex", Model: "fixture", Failed: failed})
	}
	snapshot := stats.Snapshot()
	details := snapshot.APIs["POST /v1/alpha/search"].Models["fixture"].Details
	if len(details) != 2 || snapshot.SuccessCount != 1 || snapshot.FailureCount != 1 {
		t.Fatal("asynchronous statistics read a later request or response outcome")
	}
	for _, detail := range details {
		if detail.ClientIP != "203.0.113.10" {
			t.Fatal("usage read a reused request's address")
		}
		if detail.RequestMethod != http.MethodPost || detail.RequestPath != "/v1/alpha/search" {
			t.Fatalf("usage lost the original request route: %+v", detail)
		}
	}
}

func TestUsageRequestPathIsIndependentOfAPIKeyAndRouteTemplate(t *testing.T) {
	stats := statistics.NewRequestStatistics()
	engine := gin.New()
	engine.POST("/v1beta/models/*action", func(c *gin.Context) {
		ctx, cancel := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil).GetContextWithCancel(nil, c, c.Request.Context())
		defer cancel()
		stats.Record(ctx, coreusage.Record{APIKey: "fixture-key", Provider: "gemini", Model: "fixture"})
		c.Status(http.StatusOK)
	})
	engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1beta/models/fixture:generateContent?key=private-query-fixture", nil))
	snapshot := stats.Snapshot()
	detail := snapshot.APIs["fixture-key"].Models["fixture"].Details[0]
	if detail.RequestMethod != "POST" || detail.RequestPath != "/v1beta/models/fixture:generateContent" {
		t.Fatalf("request path was replaced by API key or route template: %+v", detail)
	}
	data, err := json.Marshal(snapshot)
	if err != nil || strings.Contains(string(data), "private-query-fixture") {
		t.Fatal("usage retained query parameters")
	}
}

type snapshotStatisticsPlugin struct {
	stats *statistics.RequestStatistics
	gate  <-chan struct{}
}

func (p snapshotStatisticsPlugin) HandleUsage(ctx context.Context, record coreusage.Record) {
	<-p.gate
	p.stats.Record(ctx, record)
}

func TestUsageRequestMetadataAsyncDispatchDoesNotReadGinWriter(t *testing.T) {
	ctx, c := usageRequestContext(t)
	stats := statistics.NewRequestStatistics()
	gate := make(chan struct{})
	manager := coreusage.NewManager(0)
	manager.Register(snapshotStatisticsPlugin{stats: stats, gate: gate})
	defer manager.Stop()
	for range 100 {
		manager.Publish(ctx, coreusage.Record{Provider: "codex", Model: "fixture"})
	}
	var wait sync.WaitGroup
	wait.Add(1)
	go func() {
		defer wait.Done()
		<-gate
		for range 1000 {
			c.Writer.WriteHeader(http.StatusAccepted)
			c.Writer.WriteHeader(http.StatusBadGateway)
			c.Request = httptest.NewRequest(http.MethodGet, "/another/request", nil)
		}
	}()
	close(gate)
	if err := manager.Barrier(t.Context()); err != nil {
		t.Fatal(err)
	}
	wait.Wait()
	if snapshot := stats.Snapshot(); snapshot.SuccessCount != 100 || snapshot.FailureCount != 0 || len(snapshot.APIs) != 1 {
		t.Fatal("queued usage depended on mutable Gin state")
	}
}

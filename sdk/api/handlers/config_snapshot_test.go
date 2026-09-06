package handlers

import (
	"context"
	"errors"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestConfigSnapshotPreservesInflightLoggingPolicy(t *testing.T) {
	base := NewBaseAPIHandlers(&config.SDKConfig{RequestLog: true}, nil)
	snapshot := base.ConfigSnapshot()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx := context.WithValue(t.Context(), "gin", c)
	var workers sync.WaitGroup
	workers.Go(func() {
		for range 100 {
			base.UpdateClients(&config.SDKConfig{})
		}
	})
	workers.Go(func() {
		for range 100 {
			_ = base.ConfigSnapshot()
			base.LoggingAPIResponseError(ctx, &interfaces.ErrorMessage{Error: errors.New("test failure")})
		}
	})
	workers.Wait()
	if !snapshot.RequestLog || base.ConfigSnapshot().RequestLog {
		t.Fatal("reload mutated the captured logging configuration")
	}
	var absent *BaseAPIHandler
	if absent.ConfigSnapshot() != nil {
		t.Fatal("nil handler returned a configuration")
	}
	base.UpdateClients(&config.SDKConfig{RequestLog: true})
	base.LoggingAPIResponseError(nil, nil)
	base.LoggingAPIResponseError(context.WithValue(t.Context(), "gin", (*gin.Context)(nil)), nil)
}

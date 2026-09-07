package handlers

import (
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestStreamingKeepAliveIntervalBoundsAndSnapshot(t *testing.T) {
	for _, seconds := range []int{-1, 0, 1, 9223372036, 9223372037, int(^uint(0) >> 1)} {
		cfg := &config.SDKConfig{Streaming: config.StreamingConfig{KeepAliveSeconds: seconds}}
		got := StreamingKeepAliveInterval(cfg)
		if seconds <= 0 || seconds > 9223372036 {
			if got != 0 {
				t.Fatal("disabled or overflow interval created a timer")
			}
		} else if got != time.Duration(seconds)*time.Second {
			t.Fatal("valid interval changed")
		}
	}
	base := NewBaseAPIHandlers(&config.SDKConfig{Streaming: config.StreamingConfig{KeepAliveSeconds: 2}}, nil)
	snapshot := base.ConfigSnapshot()
	var workers sync.WaitGroup
	workers.Go(func() {
		for range 50 {
			base.UpdateClients(&config.SDKConfig{})
		}
	})
	workers.Go(func() {
		for range 50 {
			_ = StreamingKeepAliveInterval(base.ConfigSnapshot())
		}
	})
	workers.Wait()
	if StreamingKeepAliveInterval(snapshot) != 2*time.Second || StreamingKeepAliveInterval(base.ConfigSnapshot()) != 0 {
		t.Fatal("config reload changed an in-flight interval")
	}
}

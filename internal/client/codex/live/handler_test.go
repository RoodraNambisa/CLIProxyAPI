package live

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
)

func liveHandlerRequest(ctx context.Context, principal, providers string) (*gin.Context, *httptest.ResponseRecorder) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/realtime", nil).WithContext(ctx)
	if principal != "" {
		c.Set("apiKey", principal)
	}
	if providers != "" {
		c.Set("accessMetadata", map[string]string{sdkaccess.MetadataAllowedProviders: providers})
	}
	return c, w
}

func TestLiveHandlerAdmissionRequiresCallerAndProviderAccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name, principal, providers string
		enabled                    bool
		status                     int
		code                       string
	}{
		{"disabled", "fixture-caller", "", false, 503, liveDisabledCode},
		{"anonymous", "", "", true, 401, "realtime_auth_required"},
		{"provider denied", "fixture-caller", "claude", true, 403, "provider_not_allowed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Codex.LiveEnabled = tc.enabled
			h := NewHandler(cfg, nil)
			defer h.Close()
			c, w := liveHandlerRequest(t.Context(), tc.principal, tc.providers)
			if request := h.begin(c); request != nil {
				request.finish()
				t.Fatal("denied request entered realtime setup")
			}
			if w.Code != tc.status || !strings.Contains(w.Body.String(), tc.code) {
				t.Fatal("wrong realtime admission error")
			}
		})
	}
}

func TestLiveHandlerDisableCancelsSetupButKeepsCommittedSession(t *testing.T) {
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	cfg.Streaming.KeepAliveSeconds = 2
	h := NewHandler(cfg, nil)
	defer h.Close()
	c, _ := liveHandlerRequest(t.Context(), "fixture-caller", "codex")
	pending := h.begin(c)
	if pending == nil {
		t.Fatal("enabled setup was rejected")
	}
	defer pending.finish()
	c, _ = liveHandlerRequest(t.Context(), "fixture-caller", "codex")
	established := h.begin(c)
	if established == nil {
		t.Fatal("enabled setup was rejected")
	}
	defer established.finish()
	if errCommit := established.commit(); errCommit != nil {
		t.Fatal(errCommit)
	}
	changed := &config.Config{}
	changed.Streaming.KeepAliveSeconds = 9
	h.UpdateConfig(changed)
	if !errors.Is(pending.active(), errLiveDisabled) || !errors.Is(pending.commit(), errLiveDisabled) {
		t.Fatal("disabled setup did not fail its final admission check")
	}
	select {
	case <-pending.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("disabled setup context was not cancelled")
	}
	if established.ctx.Err() != nil || established.keepalive != 2*time.Second {
		t.Fatal("config update changed an established session")
	}
	c, w := liveHandlerRequest(t.Context(), "fixture-caller", "codex")
	if request := h.begin(c); request != nil {
		request.finish()
		t.Fatal("disabled handler accepted a new connection")
	}
	if !strings.Contains(w.Body.String(), liveDisabledCode) {
		t.Fatal("disabled handler lost its error code")
	}
	h.Close()
	select {
	case <-established.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("process shutdown did not cancel an established session")
	}
	h.UpdateConfig(cfg)
	c, _ = liveHandlerRequest(t.Context(), "fixture-caller", "codex")
	if request := h.begin(c); request != nil {
		request.finish()
		t.Fatal("closed handler reopened")
	}
}

func TestLiveHandlerCancelledRequestIsNotReportedAsDisabled(t *testing.T) {
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	h := NewHandler(cfg, nil)
	defer h.Close()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	c, w := liveHandlerRequest(ctx, "fixture-caller", "codex")
	if request := h.begin(c); request != nil {
		request.finish()
		t.Fatal("cancelled caller entered setup")
	}
	if w.Code != 499 || strings.Contains(w.Body.String(), liveDisabledCode) {
		t.Fatal("caller cancellation was misclassified")
	}
}

func TestLiveHandlerConcurrentConfigAndAdmission(t *testing.T) {
	on, off := &config.Config{}, &config.Config{}
	on.Codex.LiveEnabled = true
	h := NewHandler(on, nil)
	defer h.Close()
	var workers sync.WaitGroup
	workers.Go(func() {
		for range 100 {
			h.UpdateConfig(off)
			h.UpdateConfig(on)
		}
	})
	for range 4 {
		workers.Go(func() {
			for range 100 {
				c, _ := liveHandlerRequest(t.Context(), "fixture-caller", "codex")
				if request := h.begin(c); request != nil {
					_ = request.commit()
					request.finish()
				}
			}
		})
	}
	workers.Wait()
}

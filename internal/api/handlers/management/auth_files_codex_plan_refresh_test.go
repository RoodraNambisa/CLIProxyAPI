package management

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	internalcodex "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	sdkauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestCodexPlanTypeRefreshUpdatesPlanTypeAndListAuthFiles(t *testing.T) {

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer access-1" {
			t.Fatalf("authorization = %q, want %q", got, "Bearer access-1")
		}
		if got := r.Header.Get("Chatgpt-Account-Id"); got != "acct-1" {
			t.Fatalf("account header = %q, want %q", got, "acct-1")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"plan_type":"team"}`))
	}))
	defer server.Close()

	restoreUsageURL := codexPlanTypeRefreshUsageURL
	codexPlanTypeRefreshUsageURL = server.URL
	defer func() { codexPlanTypeRefreshUsageURL = restoreUsageURL }()

	h, manager, path, authID := newCodexPlanRefreshTestHandler(t, map[string]any{
		"type":         "codex",
		"email":        "codex@example.com",
		"access_token": "access-1",
		"id_token":     testManagementCodexJWT("acct-1", "pro"),
	})

	postRec := performManagementRequest(t, http.MethodPost, "/v0/management/auth-files/codex/plan-type-refresh", "", h.StartCodexPlanTypeRefresh)
	if postRec.Code != http.StatusAccepted {
		t.Fatalf("POST status = %d, want %d; body=%s", postRec.Code, http.StatusAccepted, postRec.Body.String())
	}

	snapshot := waitForCodexPlanTypeRefreshDone(t, h)
	if snapshot.State != codexPlanTypeRefreshStateCompleted {
		t.Fatalf("state = %q, want %q", snapshot.State, codexPlanTypeRefreshStateCompleted)
	}
	if snapshot.Summary.Updated != 1 {
		t.Fatalf("updated = %d, want 1", snapshot.Summary.Updated)
	}
	if len(snapshot.Results) != 1 {
		t.Fatalf("results = %d, want 1", len(snapshot.Results))
	}
	if snapshot.Results[0].Status != codexPlanTypeRefreshStatusUpdated {
		t.Fatalf("result status = %q, want %q", snapshot.Results[0].Status, codexPlanTypeRefreshStatusUpdated)
	}
	if snapshot.Results[0].PlanTypeAfter != "team" {
		t.Fatalf("plan_type_after = %q, want %q", snapshot.Results[0].PlanTypeAfter, "team")
	}

	current, ok := manager.GetByID(authID)
	if !ok || current == nil {
		t.Fatal("expected auth to remain registered")
	}
	if got := current.Attributes["plan_type"]; got != "team" {
		t.Fatalf("runtime plan_type = %q, want %q", got, "team")
	}

	rawFile, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read auth file: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(rawFile, &payload); err != nil {
		t.Fatalf("unmarshal auth file: %v", err)
	}
	if got, _ := payload["plan_type"].(string); got != "team" {
		t.Fatalf("persisted plan_type = %q, want %q", got, "team")
	}

	listRec := performManagementRequest(t, http.MethodGet, "/v0/management/auth-files", "", h.ListAuthFiles)
	if listRec.Code != http.StatusOK {
		t.Fatalf("ListAuthFiles status = %d, want %d; body=%s", listRec.Code, http.StatusOK, listRec.Body.String())
	}
	var listPayload struct {
		Files []map[string]any `json:"files"`
	}
	if err := json.Unmarshal(listRec.Body.Bytes(), &listPayload); err != nil {
		t.Fatalf("unmarshal list response: %v", err)
	}
	if len(listPayload.Files) != 1 {
		t.Fatalf("files = %d, want 1", len(listPayload.Files))
	}
	if got, _ := listPayload.Files[0]["plan_type"].(string); got != "team" {
		t.Fatalf("top-level plan_type = %q, want %q", got, "team")
	}
}

type codexPlanRefreshProxyResolver struct {
	url   string
	calls atomic.Int32
}

func (r *codexPlanRefreshProxyResolver) Resolve(context.Context, *coreauth.Auth) (coreauth.ResolvedProxy, error) {
	r.calls.Add(1)
	return coreauth.ResolvedProxy{URL: r.url, Source: "test"}, nil
}

func (*codexPlanRefreshProxyResolver) ReportFailure(_ context.Context, _ *coreauth.Auth, err error) error {
	return err
}

func TestCodexPlanTypeRefreshUsesResolvedProxy(t *testing.T) {
	var proxyCalls atomic.Int32
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyCalls.Add(1)
		if got := r.URL.Host; got != "usage.invalid" {
			t.Errorf("proxied host = %q, want usage.invalid", got)
		}
		_, _ = w.Write([]byte(`{"plan_type":"team"}`))
	}))
	defer proxyServer.Close()

	restoreUsageURL := codexPlanTypeRefreshUsageURL
	codexPlanTypeRefreshUsageURL = "http://usage.invalid/backend-api/wham/usage"
	defer func() { codexPlanTypeRefreshUsageURL = restoreUsageURL }()

	h, manager, _, authID := newCodexPlanRefreshTestHandler(t, map[string]any{
		"type":         "codex",
		"account_id":   "acct-proxy",
		"access_token": "access-proxy",
	})
	resolver := &codexPlanRefreshProxyResolver{url: proxyServer.URL}
	manager.SetProxyResolver(resolver)
	auth, ok := manager.GetByID(authID)
	if !ok {
		t.Fatal("test auth not found")
	}

	result := h.refreshSingleCodexPlanType(manager, auth)
	if result.Status != codexPlanTypeRefreshStatusUpdated {
		t.Fatalf("result = %#v, want updated", result)
	}
	if got := resolver.calls.Load(); got != 1 {
		t.Fatalf("proxy resolutions = %d, want 1", got)
	}
	if got := proxyCalls.Load(); got != 1 {
		t.Fatalf("proxy requests = %d, want 1", got)
	}
}

func TestCodexPlanTypeRefreshRejectsRetiredBackingFileBeforeNetwork(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls.Add(1)
	}))
	defer server.Close()
	restoreUsageURL := codexPlanTypeRefreshUsageURL
	codexPlanTypeRefreshUsageURL = server.URL
	defer func() { codexPlanTypeRefreshUsageURL = restoreUsageURL }()

	h, manager, path, authID := newCodexPlanRefreshTestHandler(t, map[string]any{
		"type":               "codex",
		"account_id":         "acct-retired",
		"access_token":       "access-retired",
		"refresh_token":      "refresh-retired",
		"last_refresh":       time.Now().Format(time.RFC3339),
		"expired":            time.Now().Add(time.Hour).Format(time.RFC3339),
		"chatgpt_account_id": "acct-retired",
	})
	if errWrite := os.WriteFile(path, []byte(`{"type":"gemini","access_token":"legacy"}`), 0o600); errWrite != nil {
		t.Fatalf("replace auth with retired file: %v", errWrite)
	}
	auth, ok := manager.GetByID(authID)
	if !ok || auth == nil {
		t.Fatal("test auth not found")
	}
	result := h.refreshSingleCodexPlanType(manager, auth)
	if result.Status != codexPlanTypeRefreshStatusFailed || result.Error != errGeminiCLIAuthGone.Error() {
		t.Fatalf("refresh result = %#v", result)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("usage endpoint calls = %d, want 0", got)
	}
}

func TestCodexPlanTypeRefreshRejectsConcurrentRequests(t *testing.T) {

	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"plan_type":"pro"}`))
	}))
	defer server.Close()

	restoreUsageURL := codexPlanTypeRefreshUsageURL
	codexPlanTypeRefreshUsageURL = server.URL
	defer func() { codexPlanTypeRefreshUsageURL = restoreUsageURL }()

	h, _, _, _ := newCodexPlanRefreshTestHandler(t, map[string]any{
		"type":         "codex",
		"email":        "codex@example.com",
		"access_token": "access-1",
		"id_token":     testManagementCodexJWT("acct-1", "pro"),
	})

	firstRec := performManagementRequest(t, http.MethodPost, "/v0/management/auth-files/codex/plan-type-refresh", "", h.StartCodexPlanTypeRefresh)
	if firstRec.Code != http.StatusAccepted {
		t.Fatalf("first POST status = %d, want %d; body=%s", firstRec.Code, http.StatusAccepted, firstRec.Body.String())
	}

	waitForCodexPlanTypeRefreshRunning(t, h)

	getRec := performManagementRequest(t, http.MethodGet, "/v0/management/auth-files/codex/plan-type-refresh", "", h.GetCodexPlanTypeRefreshStatus)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want %d; body=%s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
	var statusPayload codexPlanTypeRefreshTask
	if err := json.Unmarshal(getRec.Body.Bytes(), &statusPayload); err != nil {
		t.Fatalf("unmarshal GET response: %v", err)
	}
	if !statusPayload.Running {
		t.Fatalf("running = %v, want true", statusPayload.Running)
	}

	secondRec := performManagementRequest(t, http.MethodPost, "/v0/management/auth-files/codex/plan-type-refresh", "", h.StartCodexPlanTypeRefresh)
	if secondRec.Code != http.StatusConflict {
		t.Fatalf("second POST status = %d, want %d; body=%s", secondRec.Code, http.StatusConflict, secondRec.Body.String())
	}

	close(release)
	waitForCodexPlanTypeRefreshDone(t, h)
}

func TestCodexPlanTypeRefreshClearCompletedTask(t *testing.T) {

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"plan_type":"team"}`))
	}))
	defer server.Close()

	restoreUsageURL := codexPlanTypeRefreshUsageURL
	codexPlanTypeRefreshUsageURL = server.URL
	defer func() { codexPlanTypeRefreshUsageURL = restoreUsageURL }()

	h, _, _, _ := newCodexPlanRefreshTestHandler(t, map[string]any{
		"type":         "codex",
		"email":        "codex@example.com",
		"access_token": "access-1",
		"id_token":     testManagementCodexJWT("acct-1", "pro"),
	})

	postRec := performManagementRequest(t, http.MethodPost, "/v0/management/auth-files/codex/plan-type-refresh", "", h.StartCodexPlanTypeRefresh)
	if postRec.Code != http.StatusAccepted {
		t.Fatalf("POST status = %d, want %d; body=%s", postRec.Code, http.StatusAccepted, postRec.Body.String())
	}
	done := waitForCodexPlanTypeRefreshDone(t, h)
	if done.State != codexPlanTypeRefreshStateCompleted {
		t.Fatalf("state = %q, want %q", done.State, codexPlanTypeRefreshStateCompleted)
	}

	deleteRec := performManagementRequest(t, http.MethodDelete, "/v0/management/auth-files/codex/plan-type-refresh", "", h.ClearCodexPlanTypeRefresh)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("DELETE status = %d, want %d; body=%s", deleteRec.Code, http.StatusOK, deleteRec.Body.String())
	}

	var cleared codexPlanTypeRefreshTask
	if err := json.Unmarshal(deleteRec.Body.Bytes(), &cleared); err != nil {
		t.Fatalf("unmarshal DELETE response: %v", err)
	}
	if cleared.State != codexPlanTypeRefreshStateIdle {
		t.Fatalf("state = %q, want %q", cleared.State, codexPlanTypeRefreshStateIdle)
	}
	if cleared.Running {
		t.Fatal("running = true, want false")
	}
	if len(cleared.Results) != 0 {
		t.Fatalf("results = %d, want 0", len(cleared.Results))
	}
}

func TestCodexPlanTypeRefreshRetriesAfterUnauthorized(t *testing.T) {

	var requests int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch got := r.Header.Get("Authorization"); got {
		case "Bearer access-old":
			atomic.AddInt32(&requests, 1)
			w.WriteHeader(http.StatusUnauthorized)
		case "Bearer access-new":
			atomic.AddInt32(&requests, 1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"plan_type":"team"}`))
		default:
			t.Fatalf("unexpected authorization header %q", got)
		}
	}))
	defer server.Close()

	restoreUsageURL := codexPlanTypeRefreshUsageURL
	codexPlanTypeRefreshUsageURL = server.URL
	defer func() { codexPlanTypeRefreshUsageURL = restoreUsageURL }()

	h, manager, path, authID := newCodexPlanRefreshTestHandler(t, map[string]any{
		"type":          "codex",
		"email":         "codex@example.com",
		"access_token":  "access-old",
		"refresh_token": "refresh-old",
		"id_token":      testManagementCodexJWT("acct-1", "pro"),
	})
	manager.RegisterExecutor(codexPlanRefreshTestExecutor{
		refreshFn: func(auth *coreauth.Auth) (*coreauth.Auth, error) {
			if auth.Metadata == nil {
				auth.Metadata = make(map[string]any)
			}
			auth.Metadata["access_token"] = "access-new"
			auth.Metadata["refresh_token"] = "refresh-new"
			auth.Metadata["account_id"] = "acct-1"
			return auth, nil
		},
	})

	postRec := performManagementRequest(t, http.MethodPost, "/v0/management/auth-files/codex/plan-type-refresh", "", h.StartCodexPlanTypeRefresh)
	if postRec.Code != http.StatusAccepted {
		t.Fatalf("POST status = %d, want %d; body=%s", postRec.Code, http.StatusAccepted, postRec.Body.String())
	}

	snapshot := waitForCodexPlanTypeRefreshDone(t, h)
	if snapshot.State != codexPlanTypeRefreshStateCompleted {
		t.Fatalf("state = %q, want %q", snapshot.State, codexPlanTypeRefreshStateCompleted)
	}
	if atomic.LoadInt32(&requests) != 2 {
		t.Fatalf("requests = %d, want 2", atomic.LoadInt32(&requests))
	}

	current, ok := manager.GetByID(authID)
	if !ok || current == nil {
		t.Fatal("expected auth to remain registered")
	}
	if got := current.Attributes["plan_type"]; got != "team" {
		t.Fatalf("runtime plan_type = %q, want %q", got, "team")
	}
	if got := strings.TrimSpace(stringValue(current.Metadata, "access_token")); got != "access-new" {
		t.Fatalf("persisted access_token = %q, want %q", got, "access-new")
	}
	if got := strings.TrimSpace(stringValue(current.Metadata, "refresh_token")); got != "refresh-new" {
		t.Fatalf("persisted refresh_token = %q, want %q", got, "refresh-new")
	}

	rawFile, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read auth file: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(rawFile, &payload); err != nil {
		t.Fatalf("unmarshal auth file: %v", err)
	}
	if got, _ := payload["plan_type"].(string); got != "team" {
		t.Fatalf("persisted plan_type = %q, want %q", got, "team")
	}
	if got, _ := payload["access_token"].(string); got != "access-new" {
		t.Fatalf("persisted access_token = %q, want %q", got, "access-new")
	}
}

func TestCodexPlanTypeRefreshRetryFailedOnly(t *testing.T) {

	var mu sync.Mutex
	requestsByAccount := make(map[string]int)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		accountID := r.Header.Get("Chatgpt-Account-Id")
		mu.Lock()
		requestsByAccount[accountID]++
		count := requestsByAccount[accountID]
		mu.Unlock()

		if accountID == "acct-fail" && count == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"temporary failure"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"plan_type":"team"}`))
	}))
	defer server.Close()

	restoreUsageURL := codexPlanTypeRefreshUsageURL
	codexPlanTypeRefreshUsageURL = server.URL
	defer func() { codexPlanTypeRefreshUsageURL = restoreUsageURL }()

	h, manager, _, _ := newCodexPlanRefreshTestHandler(t, map[string]any{
		"type":         "codex",
		"email":        "fail@example.com",
		"access_token": "access-fail",
		"id_token":     testManagementCodexJWT("acct-fail", "pro"),
	})
	addCodexPlanRefreshTestAuth(t, h, manager, "success.json", map[string]any{
		"type":         "codex",
		"email":        "success@example.com",
		"access_token": "access-success",
		"id_token":     testManagementCodexJWT("acct-success", "pro"),
	})

	postRec := performManagementRequest(t, http.MethodPost, "/v0/management/auth-files/codex/plan-type-refresh", "", h.StartCodexPlanTypeRefresh)
	if postRec.Code != http.StatusAccepted {
		t.Fatalf("POST status = %d, want %d; body=%s", postRec.Code, http.StatusAccepted, postRec.Body.String())
	}
	first := waitForCodexPlanTypeRefreshDone(t, h)
	if first.State != codexPlanTypeRefreshStateCompletedWithErrors {
		t.Fatalf("first state = %q, want %q", first.State, codexPlanTypeRefreshStateCompletedWithErrors)
	}
	if !first.CanRetryFailed {
		t.Fatal("can_retry_failed = false, want true")
	}
	if first.Summary.Failed != 1 {
		t.Fatalf("failed = %d, want 1", first.Summary.Failed)
	}

	retryRec := performManagementRequest(t, http.MethodPost, "/v0/management/auth-files/codex/plan-type-refresh", `{"mode":"failed"}`, h.StartCodexPlanTypeRefresh)
	if retryRec.Code != http.StatusAccepted {
		t.Fatalf("retry POST status = %d, want %d; body=%s", retryRec.Code, http.StatusAccepted, retryRec.Body.String())
	}
	second := waitForCodexPlanTypeRefreshDone(t, h)
	if second.State != codexPlanTypeRefreshStateCompleted {
		t.Fatalf("second state = %q, want %q", second.State, codexPlanTypeRefreshStateCompleted)
	}
	if second.Summary.Processed != 1 {
		t.Fatalf("retry processed = %d, want 1", second.Summary.Processed)
	}

	mu.Lock()
	failRequests := requestsByAccount["acct-fail"]
	successRequests := requestsByAccount["acct-success"]
	mu.Unlock()
	if failRequests != 2 {
		t.Fatalf("acct-fail requests = %d, want 2", failRequests)
	}
	if successRequests != 1 {
		t.Fatalf("acct-success requests = %d, want 1", successRequests)
	}
}

func TestCodexPlanTypeRefreshPauseAndResume(t *testing.T) {

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var firstStartedClosed int32
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&requestCount, 1)
		if count == 1 {
			if atomic.CompareAndSwapInt32(&firstStartedClosed, 0, 1) {
				close(firstStarted)
			}
			<-releaseFirst
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"plan_type":"team"}`))
	}))
	defer server.Close()

	restoreUsageURL := codexPlanTypeRefreshUsageURL
	codexPlanTypeRefreshUsageURL = server.URL
	defer func() { codexPlanTypeRefreshUsageURL = restoreUsageURL }()

	h, manager, _, _ := newCodexPlanRefreshTestHandler(t, map[string]any{
		"type":         "codex",
		"email":        "first@example.com",
		"access_token": "access-first",
		"id_token":     testManagementCodexJWT("acct-first", "pro"),
	})
	addCodexPlanRefreshTestAuth(t, h, manager, "second.json", map[string]any{
		"type":         "codex",
		"email":        "second@example.com",
		"access_token": "access-second",
		"id_token":     testManagementCodexJWT("acct-second", "pro"),
	})

	postRec := performManagementRequest(t, http.MethodPost, "/v0/management/auth-files/codex/plan-type-refresh", "", h.StartCodexPlanTypeRefresh)
	if postRec.Code != http.StatusAccepted {
		t.Fatalf("POST status = %d, want %d; body=%s", postRec.Code, http.StatusAccepted, postRec.Body.String())
	}
	select {
	case <-firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first request")
	}

	pauseRec := performManagementRequest(t, http.MethodPatch, "/v0/management/auth-files/codex/plan-type-refresh", `{"action":"pause"}`, h.ControlCodexPlanTypeRefresh)
	if pauseRec.Code != http.StatusOK {
		t.Fatalf("pause status = %d, want %d; body=%s", pauseRec.Code, http.StatusOK, pauseRec.Body.String())
	}

	deleteRec := performManagementRequest(t, http.MethodDelete, "/v0/management/auth-files/codex/plan-type-refresh", "", h.ClearCodexPlanTypeRefresh)
	if deleteRec.Code != http.StatusConflict {
		t.Fatalf("DELETE running status = %d, want %d; body=%s", deleteRec.Code, http.StatusConflict, deleteRec.Body.String())
	}

	close(releaseFirst)
	paused := waitForCodexPlanTypeRefreshPaused(t, h)
	if paused.Summary.Processed != 1 {
		t.Fatalf("processed while paused = %d, want 1", paused.Summary.Processed)
	}
	if !paused.Running || !paused.Paused {
		t.Fatalf("running/paused = %v/%v, want true/true", paused.Running, paused.Paused)
	}
	if got := atomic.LoadInt32(&requestCount); got != 1 {
		t.Fatalf("request count while paused = %d, want 1", got)
	}

	resumeRec := performManagementRequest(t, http.MethodPatch, "/v0/management/auth-files/codex/plan-type-refresh", `{"action":"resume"}`, h.ControlCodexPlanTypeRefresh)
	if resumeRec.Code != http.StatusOK {
		t.Fatalf("resume status = %d, want %d; body=%s", resumeRec.Code, http.StatusOK, resumeRec.Body.String())
	}

	done := waitForCodexPlanTypeRefreshDone(t, h)
	if done.State != codexPlanTypeRefreshStateCompleted {
		t.Fatalf("state = %q, want %q", done.State, codexPlanTypeRefreshStateCompleted)
	}
	if done.Summary.Processed != 2 {
		t.Fatalf("processed = %d, want 2", done.Summary.Processed)
	}
	if got := atomic.LoadInt32(&requestCount); got != 2 {
		t.Fatalf("request count = %d, want 2", got)
	}
}

func TestCodexPlanTypeRefreshClearPausedTask(t *testing.T) {
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var firstStartedClosed int32
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&requestCount, 1)
		if count == 1 {
			if atomic.CompareAndSwapInt32(&firstStartedClosed, 0, 1) {
				close(firstStarted)
			}
			<-releaseFirst
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"plan_type":"team"}`))
	}))
	defer server.Close()

	restoreUsageURL := codexPlanTypeRefreshUsageURL
	codexPlanTypeRefreshUsageURL = server.URL
	defer func() { codexPlanTypeRefreshUsageURL = restoreUsageURL }()

	h, manager, _, _ := newCodexPlanRefreshTestHandler(t, map[string]any{
		"type":         "codex",
		"email":        "first@example.com",
		"access_token": "access-first",
		"id_token":     testManagementCodexJWT("acct-first", "pro"),
	})
	addCodexPlanRefreshTestAuth(t, h, manager, "second.json", map[string]any{
		"type":         "codex",
		"email":        "second@example.com",
		"access_token": "access-second",
		"id_token":     testManagementCodexJWT("acct-second", "pro"),
	})

	postRec := performManagementRequest(t, http.MethodPost, "/v0/management/auth-files/codex/plan-type-refresh", "", h.StartCodexPlanTypeRefresh)
	if postRec.Code != http.StatusAccepted {
		t.Fatalf("POST status = %d, want %d; body=%s", postRec.Code, http.StatusAccepted, postRec.Body.String())
	}
	select {
	case <-firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first request")
	}

	pauseRec := performManagementRequest(t, http.MethodPatch, "/v0/management/auth-files/codex/plan-type-refresh", `{"action":"pause"}`, h.ControlCodexPlanTypeRefresh)
	if pauseRec.Code != http.StatusOK {
		t.Fatalf("pause status = %d, want %d; body=%s", pauseRec.Code, http.StatusOK, pauseRec.Body.String())
	}
	close(releaseFirst)
	paused := waitForCodexPlanTypeRefreshPaused(t, h)
	if paused.Summary.Processed != 1 {
		t.Fatalf("processed while paused = %d, want 1", paused.Summary.Processed)
	}

	deleteRec := performManagementRequest(t, http.MethodDelete, "/v0/management/auth-files/codex/plan-type-refresh", "", h.ClearCodexPlanTypeRefresh)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("DELETE paused status = %d, want %d; body=%s", deleteRec.Code, http.StatusOK, deleteRec.Body.String())
	}
	var cleared codexPlanTypeRefreshTask
	if err := json.Unmarshal(deleteRec.Body.Bytes(), &cleared); err != nil {
		t.Fatalf("unmarshal DELETE response: %v", err)
	}
	if cleared.State != codexPlanTypeRefreshStateIdle || cleared.Running || cleared.Paused {
		t.Fatalf("cleared state/running/paused = %q/%v/%v, want idle/false/false", cleared.State, cleared.Running, cleared.Paused)
	}
	if cleared.Summary.Processed != 0 || len(cleared.Results) != 0 {
		t.Fatalf("cleared processed/results = %d/%d, want 0/0", cleared.Summary.Processed, len(cleared.Results))
	}
	if got := atomic.LoadInt32(&requestCount); got != 1 {
		t.Fatalf("request count after closing paused task = %d, want 1", got)
	}

	updatedCredentials := 0
	for _, auth := range manager.List() {
		if effectiveCodexPlanType(auth) == "team" {
			updatedCredentials++
		}
	}
	if updatedCredentials != 1 {
		t.Fatalf("updated credentials after closing paused task = %d, want 1", updatedCredentials)
	}

	restartRec := performManagementRequest(t, http.MethodPost, "/v0/management/auth-files/codex/plan-type-refresh", "", h.StartCodexPlanTypeRefresh)
	if restartRec.Code != http.StatusAccepted {
		t.Fatalf("restart POST status = %d, want %d; body=%s", restartRec.Code, http.StatusAccepted, restartRec.Body.String())
	}
	done := waitForCodexPlanTypeRefreshDone(t, h)
	if done.State != codexPlanTypeRefreshStateCompleted || done.Summary.Processed != 2 {
		t.Fatalf("restart state/processed = %q/%d, want completed/2", done.State, done.Summary.Processed)
	}
	if got := atomic.LoadInt32(&requestCount); got != 3 {
		t.Fatalf("request count after restart = %d, want 3", got)
	}
}

func TestCodexPlanTypeRefreshDoesNotOverwriteConcurrentRetention(t *testing.T) {
	h, manager, path, authID := newCodexPlanRefreshTestHandler(t, map[string]any{
		"type":           "codex",
		"credential_uid": "uid-a",
		"access_token":   "access-before",
	})
	expected, ok := manager.GetByID(authID)
	if !ok || expected == nil {
		t.Fatal("expected registered Codex credential")
	}
	refreshed := expected.Clone()
	refreshed.Metadata["access_token"] = "access-after"

	retained := expected.Clone()
	retained.Disabled = true
	retained.Status = coreauth.StatusDisabled
	retained.StatusMessage = coreauth.ChatGPTWebDeletionStateRetained
	retained.Metadata["disabled"] = true
	retained.Metadata["deletion_state"] = coreauth.ChatGPTWebDeletionStateRetained
	retained.Metadata["deletion_requested_at"] = "2026-07-19T00:00:00Z"
	if _, errUpdate := manager.Update(coreauth.WithSkipStateCarryForward(t.Context()), retained); errUpdate != nil {
		t.Fatalf("retain credential: %v", errUpdate)
	}

	errPersist := h.persistCodexPlanTypeRefreshAuth(t.Context(), manager, expected, refreshed, "team", "acct-new", true)
	if !errors.Is(errPersist, errCodexPlanTypeRefreshCredentialChanged) {
		t.Fatalf("persist refresh error = %v, want credential changed", errPersist)
	}
	current, ok := manager.GetByID(authID)
	if !ok || current == nil || !coreauth.ChatGPTWebAuthRetainedForDependents(current) || !current.Disabled {
		t.Fatalf("current credential = %#v, want retained", current)
	}
	if got := stringValue(current.Metadata, "access_token"); got != "access-before" {
		t.Fatalf("access_token = %q, want original token", got)
	}
	if got := stringValue(current.Metadata, "plan_type"); got != "" {
		t.Fatalf("plan_type = %q, want unchanged", got)
	}
	data, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatal(errRead)
	}
	if !strings.Contains(string(data), coreauth.ChatGPTWebDeletionStateRetained) || strings.Contains(string(data), "access-after") {
		t.Fatalf("persisted credential was overwritten: %s", data)
	}
}

func TestCodexPlanTypeRefreshMergesConcurrentUnrelatedMetadata(t *testing.T) {
	h, manager, _, authID := newCodexPlanRefreshTestHandler(t, map[string]any{
		"type":           "codex",
		"credential_uid": "uid-a",
		"access_token":   "access-before",
		"refresh_token":  "refresh-before",
	})
	expected, ok := manager.GetByID(authID)
	if !ok || expected == nil {
		t.Fatal("expected registered Codex credential")
	}
	refreshed := expected.Clone()
	refreshed.Metadata["access_token"] = "access-after"
	refreshed.Metadata["refresh_token"] = "refresh-after"

	concurrent := expected.Clone()
	concurrent.Metadata["note"] = "keep this edit"
	if _, errUpdate := manager.Update(t.Context(), concurrent); errUpdate != nil {
		t.Fatalf("update concurrent metadata: %v", errUpdate)
	}
	if errPersist := h.persistCodexPlanTypeRefreshAuth(t.Context(), manager, expected, refreshed, "team", "acct-a", true); errPersist != nil {
		t.Fatalf("persist refresh: %v", errPersist)
	}
	current, ok := manager.GetByID(authID)
	if !ok || current == nil {
		t.Fatal("refreshed credential disappeared")
	}
	for key, want := range map[string]string{
		"note":          "keep this edit",
		"access_token":  "access-after",
		"refresh_token": "refresh-after",
		"plan_type":     "team",
		"account_id":    "acct-a",
	} {
		if got := stringValue(current.Metadata, key); got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}
}

func TestCodexPlanTypeRefreshDoesNotOverwritePlanOrAccountChangedDuringRequest(t *testing.T) {
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(requestStarted)
		<-releaseRequest
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"plan_type":"team"}`))
	}))
	defer server.Close()

	restoreUsageURL := codexPlanTypeRefreshUsageURL
	codexPlanTypeRefreshUsageURL = server.URL
	defer func() { codexPlanTypeRefreshUsageURL = restoreUsageURL }()

	h, manager, _, authID := newCodexPlanRefreshTestHandler(t, map[string]any{
		"type":           "codex",
		"credential_uid": "uid-concurrent",
		"access_token":   "access-before",
		"account_id":     "account-before",
		"plan_type":      "pro",
	})
	auth, ok := manager.GetByID(authID)
	if !ok || auth == nil {
		t.Fatal("expected registered Codex credential")
	}
	resultCh := make(chan codexPlanTypeRefreshResult, 1)
	go func() {
		resultCh <- h.refreshSingleCodexPlanType(manager, auth)
	}()
	select {
	case <-requestStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("usage request did not start")
	}
	current, _ := manager.GetByID(authID)
	concurrent := current.Clone()
	concurrent.Metadata = cloneStringAnyMap(current.Metadata)
	concurrent.Attributes = make(map[string]string, len(current.Attributes))
	for key, value := range current.Attributes {
		concurrent.Attributes[key] = value
	}
	concurrent.Metadata["plan_type"] = "enterprise"
	concurrent.Metadata["account_id"] = "account-after"
	concurrent.Attributes["plan_type"] = "enterprise"
	if _, currentMatch, errUpdate := manager.UpdateIfCurrentSourceHash(t.Context(), current, concurrent); errUpdate != nil || !currentMatch {
		t.Fatalf("install concurrent edit: current=%v err=%v", currentMatch, errUpdate)
	}
	close(releaseRequest)

	result := <-resultCh
	if result.Status != codexPlanTypeRefreshStatusFailed || !strings.Contains(result.Error, errCodexPlanTypeRefreshCredentialChanged.Error()) {
		t.Fatalf("refresh result = %+v", result)
	}
	latest, _ := manager.GetByID(authID)
	if got := effectiveCodexPlanType(latest); got != "enterprise" {
		t.Fatalf("plan_type = %q, want enterprise", got)
	}
	if got := internalcodex.EffectiveAccountID(latest.Metadata); got != "account-after" {
		t.Fatalf("account_id = %q, want account-after", got)
	}
}

func newCodexPlanRefreshTestHandler(t *testing.T, metadata map[string]any) (*Handler, *coreauth.Manager, string, string) {
	t.Helper()

	authDir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	manager := coreauth.NewManager(store, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	path, authID := addCodexPlanRefreshTestAuth(t, h, manager, "codex-auth.json", metadata)
	return h, manager, path, authID
}

func addCodexPlanRefreshTestAuth(t *testing.T, h *Handler, manager *coreauth.Manager, fileName string, metadata map[string]any) (string, string) {
	t.Helper()

	authDir := h.cfg.AuthDir
	path := filepath.Join(authDir, fileName)
	data, err := json.Marshal(metadata)
	if err != nil {
		t.Fatalf("marshal auth metadata: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}

	auth, err := h.buildAuthFromFileData(path, data)
	if err != nil {
		t.Fatalf("buildAuthFromFileData: %v", err)
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}
	return path, auth.ID
}

func performManagementRequest(t *testing.T, method string, target string, body string, handler func(*gin.Context)) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	ctx.Request = req
	handler(ctx)
	return rec
}

func waitForCodexPlanTypeRefreshRunning(t *testing.T, h *Handler) codexPlanTypeRefreshTask {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snapshot := h.codexPlanTypeRefreshSnapshot()
		if snapshot.Running {
			return snapshot
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for refresh task to start")
	return codexPlanTypeRefreshTask{}
}

func waitForCodexPlanTypeRefreshDone(t *testing.T, h *Handler) codexPlanTypeRefreshTask {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		snapshot := h.codexPlanTypeRefreshSnapshot()
		if !snapshot.Running {
			return snapshot
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for refresh task to finish")
	return codexPlanTypeRefreshTask{}
}

func waitForCodexPlanTypeRefreshPaused(t *testing.T, h *Handler) codexPlanTypeRefreshTask {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		snapshot := h.codexPlanTypeRefreshSnapshot()
		if snapshot.State == codexPlanTypeRefreshStatePaused && snapshot.Paused {
			return snapshot
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for refresh task to pause")
	return codexPlanTypeRefreshTask{}
}

func testManagementCodexJWT(accountID string, planType string) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	payload, _ := json.Marshal(map[string]any{
		"https://api.openai.com/auth": map[string]any{
			"chatgpt_account_id": accountID,
			"chatgpt_plan_type":  planType,
		},
	})
	return header + "." + base64.RawURLEncoding.EncodeToString(payload) + ".sig"
}

type codexPlanRefreshTestExecutor struct {
	refreshFn func(auth *coreauth.Auth) (*coreauth.Auth, error)
}

func (e codexPlanRefreshTestExecutor) Identifier() string { return "codex" }

func (e codexPlanRefreshTestExecutor) Execute(_ context.Context, _ *coreauth.Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, fmt.Errorf("not implemented")
}

func (e codexPlanRefreshTestExecutor) ExecuteStream(_ context.Context, _ *coreauth.Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, fmt.Errorf("not implemented")
}

func (e codexPlanRefreshTestExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	if e.refreshFn == nil {
		return auth, nil
	}
	return e.refreshFn(auth)
}

func (e codexPlanRefreshTestExecutor) CountTokens(_ context.Context, _ *coreauth.Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, fmt.Errorf("not implemented")
}

func (e codexPlanRefreshTestExecutor) HttpRequest(_ context.Context, _ *coreauth.Auth, _ *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("not implemented")
}

package management

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	codexauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	sdkauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestCodexAgentIdentityConversionPersistsDownloadableCredential(t *testing.T) {
	handler, manager, authDir := newCodexAgentIdentityManagementFixture(t)
	registrationServer := newCodexAgentIdentityRegistrationServer(t)
	defer registrationServer.Close()
	handler.agentIdentityBaseURL = registrationServer.URL

	token := codexAgentIdentityTestAccessToken(t, "account-1", "team-1", "user-1", "user@example.com", "free")
	task := startCodexAgentIdentityConversion(t, handler, map[string]any{"access_tokens": []string{token}})
	task = waitForCodexAgentIdentityTask(t, handler, task.ID)
	if task.Status != agentIdentityTaskCompleted || task.Succeeded != 1 || len(task.Results) != 1 {
		t.Fatalf("task = %#v, want one completed conversion", task)
	}
	targetName := task.Results[0].TargetName
	if targetName == "" || task.Results[0].Status != agentIdentityItemCreated {
		t.Fatalf("result = %#v, want created target", task.Results[0])
	}
	if payload, errMarshal := json.Marshal(task); errMarshal != nil {
		t.Fatalf("marshal task: %v", errMarshal)
	} else if bytes.Contains(payload, []byte(token)) || bytes.Contains(payload, []byte("agent_private_key")) {
		t.Fatalf("task snapshot leaked credential material: %s", payload)
	}

	auth := findCodexAgentIdentityByName(t, manager, targetName)
	if got := stringValue(auth.Metadata, "auth_mode"); got != codexauth.AgentIdentityAuthMode {
		t.Fatalf("auth_mode = %q, want %q", got, codexauth.AgentIdentityAuthMode)
	}
	if got := strings.TrimSpace(stringValue(auth.Metadata, "access_token")); got != token {
		t.Fatalf("persisted access_token was not retained")
	}
	privateKey := strings.TrimSpace(stringValue(auth.Metadata, "agent_private_key"))
	if privateKey == "" || strings.TrimSpace(stringValue(auth.Metadata, "agent_runtime_id")) == "" || strings.TrimSpace(stringValue(auth.Metadata, "task_id")) == "" {
		t.Fatalf("persisted Agent Identity is incomplete: %#v", auth.Metadata)
	}

	filePath := filepath.Join(authDir, targetName)
	info, errStat := os.Stat(filePath)
	if errStat != nil {
		t.Fatalf("stat Agent Identity file: %v", errStat)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("Agent Identity file mode = %o, want 600", got)
	}

	listRecorder := httptest.NewRecorder()
	listContext, _ := gin.CreateTestContext(listRecorder)
	listContext.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files", nil)
	handler.ListAuthFiles(listContext)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("ListAuthFiles() status = %d, body = %s", listRecorder.Code, listRecorder.Body.String())
	}
	if !strings.Contains(listRecorder.Body.String(), `"auth_mode":"agentIdentity"`) ||
		!strings.Contains(listRecorder.Body.String(), `"auth_mode_label":"Agent Identity"`) ||
		!strings.Contains(listRecorder.Body.String(), `"can_convert_to_oauth":true`) {
		t.Fatalf("auth list omitted Agent Identity marker: %s", listRecorder.Body.String())
	}
	if strings.Contains(listRecorder.Body.String(), privateKey) || strings.Contains(listRecorder.Body.String(), "agent_private_key") || strings.Contains(listRecorder.Body.String(), token) {
		t.Fatalf("auth list leaked private key: %s", listRecorder.Body.String())
	}

	downloadRecorder := httptest.NewRecorder()
	downloadContext, _ := gin.CreateTestContext(downloadRecorder)
	downloadContext.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files/download?name="+url.QueryEscape(targetName), nil)
	handler.DownloadAuthFile(downloadContext)
	if downloadRecorder.Code != http.StatusOK {
		t.Fatalf("DownloadAuthFile() status = %d, body = %s", downloadRecorder.Code, downloadRecorder.Body.String())
	}
	if !bytes.Contains(downloadRecorder.Body.Bytes(), []byte(privateKey)) || !bytes.Contains(downloadRecorder.Body.Bytes(), []byte(`"task_id"`)) || !bytes.Contains(downloadRecorder.Body.Bytes(), []byte(token)) {
		t.Fatalf("downloaded Agent Identity omitted credential material: %s", downloadRecorder.Body.String())
	}
}

func TestCodexAgentIdentityDirectTokenUpdatesExistingAccountInPlace(t *testing.T) {
	handler, manager, _ := newCodexAgentIdentityManagementFixture(t)
	registrationServer := newCodexAgentIdentityRegistrationServer(t)
	defer registrationServer.Close()
	handler.agentIdentityBaseURL = registrationServer.URL

	existingToken := codexAgentIdentityTestAccessToken(t, "root-existing", "team-shared", "user-existing", "old@example.com", "plus")
	existingPayload, errMarshal := json.Marshal(map[string]any{
		"type":          "codex",
		"access_token":  existingToken,
		"refresh_token": "refresh-existing",
		"account_id":    "root-existing",
		"email":         "old@example.com",
		"plan_type":     "plus",
	})
	if errMarshal != nil {
		t.Fatalf("marshal existing auth: %v", errMarshal)
	}
	if errWrite := handler.writeAuthFile(t.Context(), "existing.json", existingPayload); errWrite != nil {
		t.Fatalf("write existing auth: %v", errWrite)
	}

	newToken := codexAgentIdentityTestAccessToken(t, "root-new", "team-shared", "user-new", "new@example.com", "pro")
	task := startCodexAgentIdentityConversion(t, handler, map[string]any{"access_tokens": []string{newToken}})
	task = waitForCodexAgentIdentityTask(t, handler, task.ID)
	if task.Status != agentIdentityTaskCompleted || task.Results[0].Status != agentIdentityItemUpdated || task.Results[0].TargetName != "existing.json" {
		t.Fatalf("task = %#v, want direct token to update existing account in place", task)
	}
	if got := len(manager.List()); got != 1 {
		t.Fatalf("manager auth count = %d, want one credential", got)
	}
	updated := findCodexAgentIdentityByName(t, manager, "existing.json")
	if got := stringValue(updated.Metadata, "access_token"); got != newToken {
		t.Fatal("new access token was not retained on the existing credential")
	}
	if got := stringValue(updated.Metadata, "refresh_token"); got != "refresh-existing" {
		t.Fatalf("refresh_token = %q, want existing refresh token", got)
	}
}

func TestCodexAgentIdentityDirectTokensForSameAccountShareCredential(t *testing.T) {
	handler, manager, _ := newCodexAgentIdentityManagementFixture(t)
	registrationServer := newCodexAgentIdentityRegistrationServer(t)
	defer registrationServer.Close()
	handler.agentIdentityBaseURL = registrationServer.URL

	firstToken := codexAgentIdentityTestAccessToken(t, "root-first", "team-shared-batch", "user-first", "first@example.com", "plus")
	secondToken := codexAgentIdentityTestAccessToken(t, "root-second", "team-shared-batch", "user-second", "second@example.com", "pro")
	task := startCodexAgentIdentityConversion(t, handler, map[string]any{"access_tokens": []string{firstToken, secondToken}})
	task = waitForCodexAgentIdentityTask(t, handler, task.ID)
	if task.Status != agentIdentityTaskCompleted || task.Succeeded != 2 || len(task.Results) != 2 {
		t.Fatalf("task = %#v, want both conversions to share one credential", task)
	}
	if task.Results[0].TargetName == "" || task.Results[0].TargetName != task.Results[1].TargetName {
		t.Fatalf("target names = %q and %q, want the same stable name", task.Results[0].TargetName, task.Results[1].TargetName)
	}
	if got := len(manager.List()); got != 1 {
		t.Fatalf("manager auth count = %d, want one credential for the shared account", got)
	}
	created, updated := 0, 0
	for _, result := range task.Results {
		switch result.Status {
		case agentIdentityItemCreated:
			created++
		case agentIdentityItemUpdated:
			updated++
		}
	}
	if created != 1 || updated != 1 {
		t.Fatalf("result statuses = %#v, want one created and one updated", task.Results)
	}
}

func TestCodexAPIKeyAuthDoesNotExposeOAuthMode(t *testing.T) {
	handler, _, _ := newCodexAgentIdentityManagementFixture(t)
	entry := handler.buildAuthFileEntry(&coreauth.Auth{
		ID:       "codex-api-key",
		Provider: "codex",
		FileName: "codex-api-key",
		Attributes: map[string]string{
			"runtime_only": "true",
			"api_key":      "secret",
			"auth_kind":    "apikey",
		},
	})
	if entry == nil {
		t.Fatal("buildAuthFileEntry() returned nil")
	}
	for _, key := range []string{"auth_mode", "auth_mode_label", "can_convert_to_agent_identity", "can_convert_to_oauth"} {
		if _, exists := entry[key]; exists {
			t.Fatalf("API-key entry unexpectedly exposes %s: %#v", key, entry)
		}
	}
}

func TestCodexAgentIdentityConversionAcceptsUnchangedNonCanonicalSource(t *testing.T) {
	handler, manager, _ := newCodexAgentIdentityManagementFixture(t)
	registrationServer := newCodexAgentIdentityRegistrationServer(t)
	defer registrationServer.Close()
	handler.agentIdentityBaseURL = registrationServer.URL

	token := codexAgentIdentityTestAccessToken(t, "account-2", "team-2", "user-2", "source@example.com", "plus")
	sourcePayload := fmt.Sprintf("{\n  \"type\": \"codex\",\n  \"access_token\": %q,\n  \"refresh_token\": \"refresh-token\",\n  \"email\": \"source@example.com\",\n  \"plan_type\": \"plus\"\n}\n", token)
	if errWrite := handler.writeAuthFile(t.Context(), "source.json", []byte(sourcePayload)); errWrite != nil {
		t.Fatalf("write source auth: %v", errWrite)
	}
	if source := handler.findManagedFileAuth("source.json"); source == nil || source.Attributes[coreauth.SourceHashAttributeKey] == "" {
		t.Fatalf("source auth was not registered with a source hash: %#v", source)
	}

	task := startCodexAgentIdentityConversion(t, handler, map[string]any{"names": []string{"source.json"}})
	task = waitForCodexAgentIdentityTask(t, handler, task.ID)
	if task.Status != agentIdentityTaskCompleted || task.Succeeded != 1 {
		t.Fatalf("task = %#v, want unchanged source to convert", task)
	}
	if _, ok := manager.GetByID(handler.authIDForPath(filepath.Join(handler.cfg.AuthDir, "source.json"))); !ok {
		t.Fatal("source OAuth credential was removed during conversion")
	}
	converted := findCodexAgentIdentityByName(t, manager, "source.json")
	if got := stringValue(converted.Metadata, "access_token"); got != token {
		t.Fatal("source access token was not retained after in-place conversion")
	}
	if got := stringValue(converted.Metadata, "refresh_token"); got != "refresh-token" {
		t.Fatal("source refresh token was not retained after in-place conversion")
	}
	if task.Results[0].TargetName != "source.json" || task.Results[0].SourceMode != codexauth.OAuthAuthMode || task.Results[0].TargetMode != codexauth.AgentIdentityAuthMode {
		t.Fatalf("conversion result = %#v, want in-place OAuth to Agent Identity", task.Results[0])
	}
	if got := len(manager.List()); got != 1 {
		t.Fatalf("manager auth count = %d, want one in-place credential", got)
	}
}

func TestCodexAgentIdentityConversionAcceptsMultipartTokenAndCodexJSON(t *testing.T) {
	handler, manager, _ := newCodexAgentIdentityManagementFixture(t)
	registrationServer := newCodexAgentIdentityRegistrationServer(t)
	defer registrationServer.Close()
	handler.agentIdentityBaseURL = registrationServer.URL

	rawToken := codexAgentIdentityTestAccessToken(t, "account-multipart-1", "team-multipart-1", "user-multipart-1", "multipart-1@example.com", "plus")
	jsonToken := codexAgentIdentityTestAccessToken(t, "account-multipart-2", "team-multipart-2", "user-multipart-2", "multipart-2@example.com", "pro")
	jsonPayload, errMarshal := json.Marshal(map[string]any{"type": "codex", "access_token": jsonToken, "refresh_token": "multipart-refresh", "label": "multipart-label"})
	if errMarshal != nil {
		t.Fatalf("marshal Codex upload: %v", errMarshal)
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for _, file := range []struct {
		name    string
		payload []byte
	}{
		{name: "raw-token.txt", payload: []byte(rawToken)},
		{name: "codex.json", payload: jsonPayload},
	} {
		part, errPart := writer.CreateFormFile("files", file.name)
		if errPart != nil {
			t.Fatalf("create multipart file %s: %v", file.name, errPart)
		}
		if _, errWrite := part.Write(file.payload); errWrite != nil {
			t.Fatalf("write multipart file %s: %v", file.name, errWrite)
		}
	}
	if errClose := writer.Close(); errClose != nil {
		t.Fatalf("close multipart writer: %v", errClose)
	}

	recorder := httptest.NewRecorder()
	requestContext, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodPost, "/v0/management/codex/agent-identity/conversion-tasks", &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	requestContext.Request = request
	handler.StartCodexAgentIdentityConversionTask(requestContext)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("StartCodexAgentIdentityConversionTask() status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var task codexAgentIdentityTask
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &task); errDecode != nil {
		t.Fatalf("decode conversion task: %v", errDecode)
	}
	completed := waitForCodexAgentIdentityTask(t, handler, task.ID)
	if completed.Status != agentIdentityTaskCompleted || completed.Total != 2 || completed.Succeeded != 2 {
		t.Fatalf("multipart conversion task = %#v", completed)
	}
	for _, result := range completed.Results {
		if result.SourceName != "codex.json" {
			continue
		}
		auth := findCodexAgentIdentityByName(t, manager, result.TargetName)
		if stringValue(auth.Metadata, "refresh_token") != "multipart-refresh" || stringValue(auth.Metadata, "label") != "multipart-label" {
			t.Fatalf("multipart Codex metadata was not retained: %#v", auth.Metadata)
		}
		return
	}
	t.Fatal("multipart Codex conversion result was not found")
}

func TestCodexAgentIdentityConversionRefreshesExistingSourceAtMostOnce(t *testing.T) {
	handler, manager, _ := newCodexAgentIdentityManagementFixture(t)
	registrationServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusUnauthorized)
		_, _ = writer.Write([]byte(`{"error":{"code":"invalid_access_token"}}`))
	}))
	defer registrationServer.Close()
	handler.agentIdentityBaseURL = registrationServer.URL

	expiredToken := codexAgentIdentityTestAccessTokenAt(t, "account-refresh-once", "team-refresh-once", "user-refresh-once", "refresh-once@example.com", "plus", time.Now().Add(-time.Hour))
	freshToken := codexAgentIdentityTestAccessToken(t, "account-refresh-once", "team-refresh-once", "user-refresh-once", "refresh-once@example.com", "plus")
	sourcePayload, errMarshal := json.Marshal(map[string]any{
		"type":          "codex",
		"access_token":  expiredToken,
		"refresh_token": "refresh-token",
		"email":         "refresh-once@example.com",
		"plan_type":     "plus",
	})
	if errMarshal != nil {
		t.Fatalf("marshal source credential: %v", errMarshal)
	}
	if errWrite := handler.writeAuthFile(t.Context(), "refresh-once.json", sourcePayload); errWrite != nil {
		t.Fatalf("write source credential: %v", errWrite)
	}
	var refreshCalls atomic.Int32
	manager.RegisterExecutor(codexPlanRefreshTestExecutor{refreshFn: func(auth *coreauth.Auth) (*coreauth.Auth, error) {
		refreshCalls.Add(1)
		updated := auth.Clone()
		updated.Metadata["access_token"] = freshToken
		return updated, nil
	}})

	task := startCodexAgentIdentityConversion(t, handler, map[string]any{"names": []string{"refresh-once.json"}})
	task = waitForCodexAgentIdentityTask(t, handler, task.ID)
	if task.Status != agentIdentityTaskCompletedWithErrors || task.Failed != 1 || task.Results[0].ErrorCategory != "identity_registration_failed" {
		t.Fatalf("conversion task = %#v", task)
	}
	if got := refreshCalls.Load(); got != 1 {
		t.Fatalf("Codex source refresh calls = %d, want 1", got)
	}
}

func TestCodexAgentIdentityConversionUpdatesSourceInPlaceAndPreservesOperations(t *testing.T) {
	handler, manager, authDir := newCodexAgentIdentityManagementFixture(t)
	registrationServer := newCodexAgentIdentityRegistrationServer(t)
	defer registrationServer.Close()
	handler.agentIdentityBaseURL = registrationServer.URL

	token := codexAgentIdentityTestAccessToken(t, "account-existing", "team-existing", "user-existing", "existing@example.com", "pro")
	metadata := map[string]any{
		"type":          "codex",
		"access_token":  token,
		"refresh_token": "refresh-existing",
		"account_id":    "account-existing",
		"email":         "existing@example.com",
		"plan_type":     "pro",
	}
	metadata["disabled"] = true
	metadata["priority"] = float64(17)
	metadata["proxy_url"] = "http://proxy.example"
	metadata["custom_operation_field"] = "preserve-me"
	payload, _ := json.Marshal(metadata)
	const targetName = "existing-oauth.json"
	if errWrite := handler.writeAuthFile(t.Context(), targetName, payload); errWrite != nil {
		t.Fatalf("write existing Agent Identity: %v", errWrite)
	}
	existing := handler.findManagedFileAuth(targetName)
	if existing == nil {
		t.Fatal("existing OAuth credential was not registered")
	}
	nextRetry := time.Now().Add(time.Hour).Round(time.Second)
	failed := existing.Clone()
	failed.Unavailable = true
	failed.LastError = &coreauth.Error{HTTPStatus: http.StatusTooManyRequests, Message: "old failure"}
	failed.NextRetryAfter = nextRetry
	failed.Quota = coreauth.QuotaState{Exceeded: true, Reason: "old quota", NextRecoverAt: nextRetry}
	failed.ModelStates = map[string]*coreauth.ModelState{
		"gpt-5.4": {Unavailable: true, LastError: failed.LastError, NextRetryAfter: nextRetry, Quota: failed.Quota},
	}
	if _, errUpdate := manager.Update(coreauth.WithSkipStateCarryForward(t.Context()), failed); errUpdate != nil {
		t.Fatalf("install old runtime state: %v", errUpdate)
	}
	beforeConversion, ok := manager.GetByID(existing.ID)
	if !ok {
		t.Fatal("existing OAuth credential disappeared before conversion")
	}
	oldRuntimeInstanceID := beforeConversion.RuntimeInstanceID()

	task := startCodexAgentIdentityConversion(t, handler, map[string]any{"names": []string{targetName}, "target_mode": codexauth.AgentIdentityAuthMode})
	task = waitForCodexAgentIdentityTask(t, handler, task.ID)
	if task.Status != agentIdentityTaskCompleted || task.Results[0].Status != agentIdentityItemUpdated || task.Results[0].TargetName != targetName {
		t.Fatalf("task = %#v, want in-place identity update", task)
	}

	updated := findCodexAgentIdentityByName(t, manager, targetName)
	if !updated.Disabled || updated.Status != coreauth.StatusDisabled {
		t.Fatalf("updated disabled state = (%v, %q)", updated.Disabled, updated.Status)
	}
	if updated.Unavailable || updated.LastError != nil || !updated.NextRetryAfter.IsZero() || updated.Quota.Exceeded || len(updated.ModelStates) != 0 {
		t.Fatalf("old runtime failure state was retained: %#v", updated)
	}
	if got := stringValue(updated.Metadata, "custom_operation_field"); got != "preserve-me" {
		t.Fatalf("custom operation field = %q", got)
	}
	if got := stringValue(updated.Metadata, "agent_runtime_id"); got == "" {
		t.Fatalf("agent_runtime_id = %q, want generated identity", got)
	}
	if got := stringValue(updated.Metadata, "agent_private_key"); got == "" {
		t.Fatal("agent_private_key was not generated")
	}
	if got := stringValue(updated.Metadata, "access_token"); got != token {
		t.Fatal("OAuth access token was not retained")
	}
	if got := stringValue(updated.Metadata, "refresh_token"); got != "refresh-existing" {
		t.Fatal("OAuth refresh token was not retained")
	}
	if updated.RuntimeInstanceID() == oldRuntimeInstanceID {
		t.Fatal("Agent Identity conversion reused the old runtime instance")
	}
	if _, errStat := os.Stat(filepath.Join(authDir, targetName)); errStat != nil {
		t.Fatalf("updated Agent Identity file: %v", errStat)
	}
	if got := len(manager.List()); got != 1 {
		t.Fatalf("manager auth count = %d, want one in-place credential", got)
	}
}

func TestCodexAgentIdentityConversionSwitchesBackToOAuthInPlace(t *testing.T) {
	handler, manager, _ := newCodexAgentIdentityManagementFixture(t)
	registrationServer := newCodexAgentIdentityRegistrationServer(t)
	defer registrationServer.Close()
	handler.agentIdentityBaseURL = registrationServer.URL

	token := codexAgentIdentityTestAccessToken(t, "account-switch", "team-switch", "user-switch", "switch@example.com", "plus")
	metadata := map[string]any{
		"type":          "codex",
		"access_token":  token,
		"refresh_token": "refresh-switch",
		"email":         "switch@example.com",
		"plan_type":     "plus",
	}
	payload, _ := json.Marshal(metadata)
	if errWrite := handler.writeAuthFile(t.Context(), "switch.json", payload); errWrite != nil {
		t.Fatalf("write OAuth credential: %v", errWrite)
	}

	agentTask := startCodexAgentIdentityConversion(t, handler, map[string]any{
		"names":       []string{"switch.json"},
		"target_mode": codexauth.AgentIdentityAuthMode,
	})
	agentTask = waitForCodexAgentIdentityTask(t, handler, agentTask.ID)
	if agentTask.Status != agentIdentityTaskCompleted || agentTask.Results[0].TargetName != "switch.json" {
		t.Fatalf("Agent Identity conversion task = %#v", agentTask)
	}

	oauthTask := startCodexAgentIdentityConversion(t, handler, map[string]any{
		"names":       []string{"switch.json"},
		"target_mode": codexauth.OAuthAuthMode,
	})
	oauthTask = waitForCodexAgentIdentityTask(t, handler, oauthTask.ID)
	if oauthTask.Status != agentIdentityTaskCompleted || oauthTask.Results[0].Status != agentIdentityItemUpdated {
		t.Fatalf("OAuth conversion task = %#v", oauthTask)
	}
	if oauthTask.Results[0].SourceMode != codexauth.AgentIdentityAuthMode || oauthTask.Results[0].TargetMode != codexauth.OAuthAuthMode {
		t.Fatalf("OAuth conversion result = %#v", oauthTask.Results[0])
	}

	converted := handler.findManagedFileAuth("switch.json")
	if converted == nil {
		t.Fatal("converted credential disappeared")
	}
	if got := codexauth.EffectiveAuthMode(converted.Metadata); got != codexauth.OAuthAuthMode {
		t.Fatalf("auth mode = %q, want OAuth", got)
	}
	if !codexauth.HasStoredAgentIdentity(converted.Metadata) {
		t.Fatal("switching to OAuth discarded reusable Agent Identity material")
	}
	if got := stringValue(converted.Metadata, "access_token"); got != token {
		t.Fatal("switching to OAuth discarded the access token")
	}
	authorization, errAuthorization := codexauth.AuthorizationHeader(converted.Metadata, token, time.Now())
	if errAuthorization != nil || authorization != "Bearer "+token {
		t.Fatalf("OAuth authorization = %q, error = %v", authorization, errAuthorization)
	}
	if got := len(manager.List()); got != 1 {
		t.Fatalf("manager auth count = %d, want one credential", got)
	}

	entry := handler.buildAuthFileEntry(converted)
	if entry["auth_mode_label"] != "OAuth" || entry["can_convert_to_oauth"] != false || entry["can_convert_to_agent_identity"] != true {
		t.Fatalf("OAuth list capabilities = %#v", entry)
	}
}

func TestCodexAgentIdentityConversionRefreshesBeforeSwitchingBackToOAuth(t *testing.T) {
	handler, manager, _ := newCodexAgentIdentityManagementFixture(t)
	keyMaterial, errKey := codexauth.GenerateAgentIdentityKeyMaterial()
	if errKey != nil {
		t.Fatalf("GenerateAgentIdentityKeyMaterial(): %v", errKey)
	}
	expiredToken := codexAgentIdentityTestAccessTokenAt(t, "account-reverse-refresh", "team-reverse-refresh", "user-reverse-refresh", "reverse-refresh@example.com", "plus", time.Now().Add(-time.Hour))
	freshToken := codexAgentIdentityTestAccessToken(t, "account-reverse-refresh", "team-reverse-refresh", "user-reverse-refresh", "reverse-refresh@example.com", "plus")
	metadata := codexauth.AgentIdentityMetadata(codexauth.AgentIdentityCredential{
		AgentRuntimeID:        "runtime-reverse-refresh",
		PrivateKeyPKCS8Base64: keyMaterial.PrivateKeyPKCS8Base64,
		TaskID:                "task-reverse-refresh",
		AccountID:             "account-reverse-refresh",
		ChatGPTAccountID:      "team-reverse-refresh",
		ChatGPTUserID:         "user-reverse-refresh",
		Email:                 "reverse-refresh@example.com",
		PlanType:              "plus",
	})
	metadata["access_token"] = expiredToken
	metadata["refresh_token"] = "refresh-reverse"
	metadata["expired"] = time.Now().Add(-time.Hour).Format(time.RFC3339)
	payload, _ := json.Marshal(metadata)
	if errWrite := handler.writeAuthFile(t.Context(), "reverse-refresh.json", payload); errWrite != nil {
		t.Fatalf("write Agent Identity credential: %v", errWrite)
	}
	var refreshCalls atomic.Int32
	manager.RegisterExecutor(codexPlanRefreshTestExecutor{refreshFn: func(auth *coreauth.Auth) (*coreauth.Auth, error) {
		refreshCalls.Add(1)
		updated := auth.Clone()
		updated.Metadata["access_token"] = freshToken
		updated.Metadata["expired"] = time.Now().Add(time.Hour).Format(time.RFC3339)
		return updated, nil
	}})

	task := startCodexAgentIdentityConversion(t, handler, map[string]any{
		"names":       []string{"reverse-refresh.json"},
		"target_mode": codexauth.OAuthAuthMode,
	})
	task = waitForCodexAgentIdentityTask(t, handler, task.ID)
	if task.Status != agentIdentityTaskCompleted || task.Results[0].Status != agentIdentityItemUpdated {
		t.Fatalf("conversion task = %#v", task)
	}
	if got := refreshCalls.Load(); got != 1 {
		t.Fatalf("OAuth refresh calls = %d, want 1", got)
	}
	converted := handler.findManagedFileAuth("reverse-refresh.json")
	if converted == nil || codexauth.EffectiveAuthMode(converted.Metadata) != codexauth.OAuthAuthMode || stringValue(converted.Metadata, "access_token") != freshToken {
		t.Fatalf("converted credential = %#v", converted)
	}
	if !codexauth.HasStoredAgentIdentity(converted.Metadata) {
		t.Fatal("OAuth refresh discarded stored Agent Identity material")
	}
}

func TestCodexAgentIdentityConversionRejectsOAuthWithoutRetainedToken(t *testing.T) {
	handler, _, _ := newCodexAgentIdentityManagementFixture(t)
	keyMaterial, errKey := codexauth.GenerateAgentIdentityKeyMaterial()
	if errKey != nil {
		t.Fatalf("GenerateAgentIdentityKeyMaterial(): %v", errKey)
	}
	metadata := codexauth.AgentIdentityMetadata(codexauth.AgentIdentityCredential{
		AgentRuntimeID:        "runtime-agent-only",
		PrivateKeyPKCS8Base64: keyMaterial.PrivateKeyPKCS8Base64,
		TaskID:                "task-agent-only",
		AccountID:             "account-agent-only",
		ChatGPTAccountID:      "team-agent-only",
		ChatGPTUserID:         "user-agent-only",
		Email:                 "agent-only@example.com",
		PlanType:              "plus",
	})
	payload, _ := json.Marshal(metadata)
	if errWrite := handler.writeAuthFile(t.Context(), "agent-only.json", payload); errWrite != nil {
		t.Fatalf("write Agent Identity credential: %v", errWrite)
	}

	task := startCodexAgentIdentityConversion(t, handler, map[string]any{
		"names":       []string{"agent-only.json"},
		"target_mode": codexauth.OAuthAuthMode,
	})
	task = waitForCodexAgentIdentityTask(t, handler, task.ID)
	if task.Status != agentIdentityTaskCompletedWithErrors || task.Results[0].ErrorCategory != "oauth_material_missing" {
		t.Fatalf("conversion task = %#v, want OAuth material error", task)
	}
	entry := handler.buildAuthFileEntry(handler.findManagedFileAuth("agent-only.json"))
	if entry["can_convert_to_oauth"] != false {
		t.Fatalf("Agent-only list capabilities = %#v", entry)
	}
}

func TestCodexAgentIdentityConversionRejectsOAuthTargetForRawTokens(t *testing.T) {
	handler, _, _ := newCodexAgentIdentityManagementFixture(t)
	token := codexAgentIdentityTestAccessToken(t, "account-invalid-target", "team-invalid-target", "user-invalid-target", "invalid-target@example.com", "plus")
	body, _ := json.Marshal(map[string]any{
		"access_tokens": []string{token},
		"target_mode":   codexauth.OAuthAuthMode,
	})
	recorder := httptest.NewRecorder()
	requestContext, _ := gin.CreateTestContext(recorder)
	requestContext.Request = httptest.NewRequest(http.MethodPost, "/v0/management/codex/agent-identity/conversion-tasks", bytes.NewReader(body))
	requestContext.Request.Header.Set("Content-Type", "application/json")
	handler.StartCodexAgentIdentityConversionTask(requestContext)
	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "access_tokens only supports") {
		t.Fatalf("response = %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestCodexAgentIdentityConversionRejectsChangedSourceAtCommit(t *testing.T) {
	handler, manager, _ := newCodexAgentIdentityManagementFixture(t)
	identityRegistered := make(chan struct{})
	releaseTaskRegistration := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.URL.Path == "/v1/agent/register":
			close(identityRegistered)
			_, _ = writer.Write([]byte(`{"agent_runtime_id":"runtime-source-check"}`))
		case strings.HasSuffix(request.URL.Path, "/task/register"):
			<-releaseTaskRegistration
			_, _ = writer.Write([]byte(`{"task_id":"task-source-check"}`))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	handler.agentIdentityBaseURL = server.URL

	token := codexAgentIdentityTestAccessToken(t, "account-source", "team-source", "user-source", "source-check@example.com", "plus")
	source := map[string]any{"type": "codex", "access_token": token, "email": "source-check@example.com", "plan_type": "plus"}
	sourcePayload, _ := json.Marshal(source)
	if errWrite := handler.writeAuthFile(t.Context(), "source-check.json", sourcePayload); errWrite != nil {
		t.Fatalf("write source auth: %v", errWrite)
	}

	task := startCodexAgentIdentityConversion(t, handler, map[string]any{"names": []string{"source-check.json"}})
	select {
	case <-identityRegistered:
	case <-time.After(2 * time.Second):
		t.Fatal("identity registration did not start")
	}
	source["label"] = "changed while converting"
	changedPayload, _ := json.Marshal(source)
	if errWrite := handler.writeAuthFile(t.Context(), "source-check.json", changedPayload); errWrite != nil {
		close(releaseTaskRegistration)
		t.Fatalf("change source auth: %v", errWrite)
	}
	close(releaseTaskRegistration)

	task = waitForCodexAgentIdentityTask(t, handler, task.ID)
	if task.Status != agentIdentityTaskCompletedWithErrors || task.Failed != 1 || task.Results[0].ErrorCategory != "source_changed" {
		t.Fatalf("task = %#v, want source_changed failure", task)
	}
	for _, auth := range manager.List() {
		if auth != nil && codexauth.IsAgentIdentityMetadata(auth.Metadata) {
			t.Fatalf("Agent Identity persisted after source changed: %#v", auth)
		}
	}
}

func TestDirectAgentIdentityUploadValidatesAndRemainsDownloadable(t *testing.T) {
	handler, _, _ := newCodexAgentIdentityManagementFixture(t)
	keyMaterial, errKey := codexauth.GenerateAgentIdentityKeyMaterial()
	if errKey != nil {
		t.Fatalf("GenerateAgentIdentityKeyMaterial(): %v", errKey)
	}
	metadata := codexauth.AgentIdentityMetadata(codexauth.AgentIdentityCredential{
		AgentRuntimeID:        "runtime-upload",
		PrivateKeyPKCS8Base64: keyMaterial.PrivateKeyPKCS8Base64,
		TaskID:                "task-upload",
		AccountID:             "account-upload",
		ChatGPTAccountID:      "team-upload",
		ChatGPTUserID:         "user-upload",
		Email:                 "upload@example.com",
		PlanType:              "pro",
	})
	metadata["access_token"] = "retained-upload-access"
	metadata["refresh_token"] = "retained-upload-refresh"
	payload, errMarshal := json.Marshal(metadata)
	if errMarshal != nil {
		t.Fatalf("marshal Agent Identity: %v", errMarshal)
	}
	if errWrite := handler.writeAuthFile(t.Context(), "uploaded-agent.json", payload); errWrite != nil {
		t.Fatalf("write complete Agent Identity: %v", errWrite)
	}
	uploaded := handler.findManagedFileAuth("uploaded-agent.json")
	if uploaded == nil || stringValue(uploaded.Metadata, "access_token") != "retained-upload-access" || stringValue(uploaded.Metadata, "refresh_token") != "retained-upload-refresh" {
		t.Fatalf("direct Agent Identity upload did not retain OAuth material: %#v", uploaded)
	}

	delete(metadata, "task_id")
	invalidPayload, _ := json.Marshal(metadata)
	if errWrite := handler.writeAuthFile(t.Context(), "invalid-agent.json", invalidPayload); !errorsIsInvalidAuthFileData(errWrite) {
		t.Fatalf("missing task_id error = %v, want invalid auth data", errWrite)
	}
	metadata["task_id"] = "task-upload"
	delete(metadata, "type")
	invalidPayload, _ = json.Marshal(metadata)
	if errWrite := handler.writeAuthFile(t.Context(), "invalid-agent-type.json", invalidPayload); !errorsIsInvalidAuthFileData(errWrite) {
		t.Fatalf("missing type error = %v, want invalid auth data", errWrite)
	}
}

func TestFailedAgentIdentityResultRedactsLongSecretBeforeTruncation(t *testing.T) {
	secret := strings.Repeat("sensitive-token-", 64)
	result := failedAgentIdentityResultWithSecret(
		codexAgentIdentityTaskResult{SourceName: "token"},
		"registration_failed",
		fmt.Errorf("upstream echoed %s", secret),
		secret,
	)
	if strings.Contains(result.Error, "sensitive-token-") || !strings.Contains(result.Error, "[redacted]") {
		t.Fatalf("conversion error was not redacted: %q", result.Error)
	}
}

func newCodexAgentIdentityManagementFixture(t *testing.T) (*Handler, *coreauth.Manager, string) {
	t.Helper()
	authDir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	manager := coreauth.NewManager(store, nil, nil)
	handler := NewHandler(&config.Config{AuthDir: authDir}, "", manager)
	handler.tokenStore = store
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if errShutdown := handler.Shutdown(ctx); errShutdown != nil {
			t.Errorf("handler shutdown: %v", errShutdown)
		}
	})
	return handler, manager, authDir
}

func newCodexAgentIdentityRegistrationServer(t *testing.T) *httptest.Server {
	t.Helper()
	var identityRegistrations atomic.Int32
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.URL.Path == "/v1/agent/register":
			if !strings.HasPrefix(request.Header.Get("Authorization"), "Bearer ") {
				t.Errorf("agent registration Authorization = %q", request.Header.Get("Authorization"))
				writer.WriteHeader(http.StatusUnauthorized)
				_, _ = writer.Write([]byte(`{"error":"missing bearer"}`))
				return
			}
			registration := identityRegistrations.Add(1)
			_, _ = fmt.Fprintf(writer, `{"agent_runtime_id":"runtime-%d"}`, registration)
		case strings.HasPrefix(request.URL.Path, "/v1/agent/runtime-") && strings.HasSuffix(request.URL.Path, "/task/register"):
			_, _ = writer.Write([]byte(`{"task_id":"task-created"}`))
		default:
			http.NotFound(writer, request)
		}
	}))
}

func codexAgentIdentityTestAccessToken(t *testing.T, accountID, chatGPTAccountID, userID, email, planType string) string {
	return codexAgentIdentityTestAccessTokenAt(t, accountID, chatGPTAccountID, userID, email, planType, time.Now().Add(time.Hour))
}

func codexAgentIdentityTestAccessTokenAt(t *testing.T, accountID, chatGPTAccountID, userID, email, planType string, expiresAt time.Time) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`))
	claims := map[string]any{
		"sub":        userID,
		"email":      email,
		"account_id": accountID,
		"exp":        expiresAt.Unix(),
		"https://api.openai.com/auth": map[string]any{
			"chatgpt_account_id": chatGPTAccountID,
			"chatgpt_user_id":    userID,
			"chatgpt_plan_type":  planType,
		},
	}
	payload, errMarshal := json.Marshal(claims)
	if errMarshal != nil {
		t.Fatalf("marshal access token claims: %v", errMarshal)
	}
	return header + "." + base64.RawURLEncoding.EncodeToString(payload) + ".signature"
}

func startCodexAgentIdentityConversion(t *testing.T, handler *Handler, payload any) *codexAgentIdentityTask {
	t.Helper()
	body, errMarshal := json.Marshal(payload)
	if errMarshal != nil {
		t.Fatalf("marshal conversion request: %v", errMarshal)
	}
	recorder := httptest.NewRecorder()
	requestContext, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodPost, "/v0/management/codex/agent-identity/conversion-tasks", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	requestContext.Request = request
	handler.StartCodexAgentIdentityConversionTask(requestContext)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("StartCodexAgentIdentityConversionTask() status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var task codexAgentIdentityTask
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &task); errDecode != nil {
		t.Fatalf("decode conversion task: %v", errDecode)
	}
	return &task
}

func waitForCodexAgentIdentityTask(t *testing.T, handler *Handler, taskID string) *codexAgentIdentityTask {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		task, ok := handler.codexAgentIdentityTaskManager().get(taskID)
		if !ok {
			t.Fatalf("conversion task %q disappeared", taskID)
		}
		if isTerminalAgentIdentityTaskStatus(task.Status) {
			return task
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("conversion task %q did not finish", taskID)
	return nil
}

func findCodexAgentIdentityByName(t *testing.T, manager *coreauth.Manager, name string) *coreauth.Auth {
	t.Helper()
	for _, auth := range manager.List() {
		if auth != nil && auth.FileName == name && codexauth.IsAgentIdentityMetadata(auth.Metadata) {
			return auth
		}
	}
	t.Fatalf("Agent Identity %q was not registered", name)
	return nil
}

func errorsIsInvalidAuthFileData(err error) bool {
	return errors.Is(err, errInvalidAuthFileData)
}

package executor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	codexauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestCodexAgentIdentityAuthorizationOverridesCustomHeaders(t *testing.T) {
	auth := codexAgentIdentityTestAuth(t, "task-http")
	auth.Attributes = map[string]string{"header:Authorization": "Bearer attacker-controlled"}
	executor := NewCodexExecutor(&config.Config{})

	request := httptest.NewRequest(http.MethodPost, "https://chatgpt.com/backend-api/codex/responses", nil)
	if errPrepare := executor.PrepareRequest(request, auth); errPrepare != nil {
		t.Fatalf("PrepareRequest() error = %v", errPrepare)
	}
	assertAgentIdentityAuthorization(t, request.Header.Get("Authorization"), "task-http")

	directRequest := httptest.NewRequest(http.MethodPost, "https://chatgpt.com/backend-api/codex/responses", nil)
	if errHeaders := applyCodexHeaders(directRequest, auth, "ignored-bearer", true, &config.Config{}); errHeaders != nil {
		t.Fatalf("applyCodexHeaders() error = %v", errHeaders)
	}
	assertAgentIdentityAuthorization(t, directRequest.Header.Get("Authorization"), "task-http")
	assertAgentIdentityAccountHeaders(t, directRequest.Header)

	imageRequest := httptest.NewRequest(http.MethodPost, "https://chatgpt.com/backend-api/codex/images/generations", nil)
	if errHeaders := applyCodexDirectImageHeaders(imageRequest, auth, "ignored-bearer", false, &config.Config{}); errHeaders != nil {
		t.Fatalf("applyCodexDirectImageHeaders() error = %v", errHeaders)
	}
	assertAgentIdentityAuthorization(t, imageRequest.Header.Get("Authorization"), "task-http")
	assertAgentIdentityAccountHeaders(t, imageRequest.Header)

	websocketHeaders, errWebsocket := prepareCodexWebsocketHeadersForURL(context.Background(), http.Header{}, auth, "ignored-bearer", &config.Config{}, nil)
	if errWebsocket != nil {
		t.Fatalf("prepareCodexWebsocketHeadersForURL() error = %v", errWebsocket)
	}
	assertAgentIdentityAuthorization(t, websocketHeaders.Get("Authorization"), "task-http")
	assertAgentIdentityAccountHeaders(t, websocketHeaders)
}

func TestCodexOAuthRequestAccountHeaderKeepsAccountIDPriority(t *testing.T) {
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Metadata: map[string]any{
			"account_id":         "oauth-account",
			"chatgpt_account_id": "secondary-account",
		},
	}
	request := httptest.NewRequest(http.MethodPost, "https://chatgpt.com/backend-api/codex/responses", nil)
	if errHeaders := applyCodexHeaders(request, auth, "oauth-token", false, &config.Config{}); errHeaders != nil {
		t.Fatalf("applyCodexHeaders() error = %v", errHeaders)
	}
	if got := request.Header.Get("Chatgpt-Account-Id"); got != "oauth-account" {
		t.Fatalf("Chatgpt-Account-Id = %q, want oauth-account", got)
	}
}

func TestCodexAgentIdentityRegistersMissingTaskAndClassifiesRecovery(t *testing.T) {
	auth := codexAgentIdentityTestAuth(t, "")
	registrationServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !strings.HasSuffix(request.URL.Path, "/task/register") {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"task_id":"replacement-task"}`))
	}))
	defer registrationServer.Close()

	executor := NewCodexExecutor(&config.Config{})
	executor.agentIdentityBaseURL = registrationServer.URL
	executor.agentIdentityHTTPClient = func(context.Context, *cliproxyauth.Auth) *http.Client {
		return registrationServer.Client()
	}
	if !executor.ShouldPrepareRequestAuth(auth) {
		t.Fatal("Agent Identity without task_id was not marked for preparation")
	}
	prepared, errPrepare := executor.PrepareRequestAuth(t.Context(), auth)
	if errPrepare != nil {
		t.Fatalf("PrepareRequestAuth() error = %v", errPrepare)
	}
	if got := strings.TrimSpace(prepared.Metadata["task_id"].(string)); got != "replacement-task" {
		t.Fatalf("prepared task_id = %q, want replacement-task", got)
	}
	if strings.TrimSpace(auth.Metadata["task_id"].(string)) != "" {
		t.Fatal("PrepareRequestAuth() mutated the caller auth")
	}

	knownTaskError := statusErr{code: http.StatusUnauthorized, msg: `{"error":{"code":"task_expired"}}`}
	if !executor.ShouldRecoverUnauthorized(prepared, knownTaskError) {
		t.Fatal("task_expired 401 was not classified for task recovery")
	}
	ordinaryUnauthorized := statusErr{code: http.StatusUnauthorized, msg: `{"error":{"code":"invalid_access_token"}}`}
	if executor.ShouldRecoverUnauthorized(prepared, ordinaryUnauthorized) {
		t.Fatal("ordinary 401 was incorrectly classified for task recovery")
	}
}

func TestCodexAgentIdentityWebsocketDialRebuildsAuthorization(t *testing.T) {
	auth := codexAgentIdentityTestAuth(t, "task-dial")
	var receivedAuthorization string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		receivedAuthorization = request.Header.Get("Authorization")
		writer.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	executor := NewCodexWebsocketsExecutor(&config.Config{})
	_, response, errDial := executor.dialCodexWebsocket(t.Context(), auth, "ws"+strings.TrimPrefix(server.URL, "http"), http.Header{"Authorization": []string{"AgentAssertion stale"}})
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if errDial == nil {
		t.Fatal("dial unexpectedly succeeded")
	}
	if receivedAuthorization == "AgentAssertion stale" {
		t.Fatal("websocket dial reused the caller-provided assertion")
	}
	assertAgentIdentityAuthorization(t, receivedAuthorization, "task-dial")
}

func codexAgentIdentityTestAuth(t *testing.T, taskID string) *cliproxyauth.Auth {
	t.Helper()
	keyMaterial, errKey := codexauth.GenerateAgentIdentityKeyMaterial()
	if errKey != nil {
		t.Fatalf("GenerateAgentIdentityKeyMaterial() error = %v", errKey)
	}
	metadata := codexauth.AgentIdentityMetadata(codexauth.AgentIdentityCredential{
		AgentRuntimeID:          "runtime-test",
		PrivateKeyPKCS8Base64:   keyMaterial.PrivateKeyPKCS8Base64,
		TaskID:                  taskID,
		AccountID:               "root-account-test",
		ChatGPTAccountID:        "team-test",
		ChatGPTUserID:           "user-test",
		Email:                   "agent@example.com",
		PlanType:                "plus",
		ChatGPTAccountIsFedRAMP: true,
	})
	if taskID == "" {
		metadata["task_id"] = ""
	}
	return &cliproxyauth.Auth{ID: "agent-auth", Provider: "codex", Metadata: metadata}
}

func assertAgentIdentityAccountHeaders(t *testing.T, headers http.Header) {
	t.Helper()
	if got := headerValueCaseInsensitive(headers, "ChatGPT-Account-ID"); got != "team-test" {
		t.Fatalf("Chatgpt-Account-Id = %q, want team-test", got)
	}
	if got := headers.Get("X-OpenAI-Fedramp"); got != "true" {
		t.Fatalf("X-OpenAI-Fedramp = %q, want true", got)
	}
}

func assertAgentIdentityAuthorization(t *testing.T, authorization, wantTaskID string) {
	t.Helper()
	if !strings.HasPrefix(authorization, "AgentAssertion ") {
		t.Fatalf("Authorization = %q, want AgentAssertion", authorization)
	}
	payload, errDecode := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(authorization, "AgentAssertion "))
	if errDecode != nil {
		t.Fatalf("decode AgentAssertion: %v", errDecode)
	}
	var envelope struct {
		AgentRuntimeID string `json:"agent_runtime_id"`
		TaskID         string `json:"task_id"`
		Timestamp      string `json:"timestamp"`
		Signature      string `json:"signature"`
	}
	if errJSON := json.Unmarshal(payload, &envelope); errJSON != nil {
		t.Fatalf("decode AgentAssertion JSON: %v", errJSON)
	}
	if envelope.AgentRuntimeID != "runtime-test" || envelope.TaskID != wantTaskID || envelope.Timestamp == "" || envelope.Signature == "" {
		t.Fatalf("AgentAssertion envelope = %#v", envelope)
	}
}

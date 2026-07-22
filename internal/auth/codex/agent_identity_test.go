package codex

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/nacl/box"
)

func TestAgentIdentityKeyAndAssertion(t *testing.T) {
	keyMaterial, err := GenerateAgentIdentityKeyMaterial()
	if err != nil {
		t.Fatalf("GenerateAgentIdentityKeyMaterial() error = %v", err)
	}
	if !strings.HasPrefix(keyMaterial.PublicKeySSH, "ssh-ed25519 ") {
		t.Fatalf("public key = %q", keyMaterial.PublicKeySSH)
	}
	credential := AgentIdentityCredential{
		AgentRuntimeID:        "runtime-a",
		PrivateKeyPKCS8Base64: keyMaterial.PrivateKeyPKCS8Base64,
		TaskID:                "task-a",
	}
	now := time.Date(2026, 7, 22, 8, 9, 10, 999, time.UTC)
	header, err := BuildAgentAssertion(credential, now)
	if err != nil {
		t.Fatalf("BuildAgentAssertion() error = %v", err)
	}
	encoded := strings.TrimPrefix(header, "AgentAssertion ")
	payload, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("decode assertion: %v", err)
	}
	var envelope struct {
		AgentRuntimeID string `json:"agent_runtime_id"`
		Signature      string `json:"signature"`
		TaskID         string `json:"task_id"`
		Timestamp      string `json:"timestamp"`
	}
	if err = json.Unmarshal(payload, &envelope); err != nil {
		t.Fatalf("decode assertion JSON: %v", err)
	}
	if envelope.AgentRuntimeID != "runtime-a" || envelope.TaskID != "task-a" || envelope.Timestamp != "2026-07-22T08:09:10Z" {
		t.Fatalf("assertion envelope = %+v", envelope)
	}
	privateKey, err := signingKeyFromPKCS8Base64(keyMaterial.PrivateKeyPKCS8Base64)
	if err != nil {
		t.Fatalf("parse private key: %v", err)
	}
	signature, err := base64.StdEncoding.DecodeString(envelope.Signature)
	if err != nil {
		t.Fatalf("decode signature: %v", err)
	}
	signed := strings.Join([]string{envelope.AgentRuntimeID, envelope.TaskID, envelope.Timestamp}, ":")
	if !ed25519.Verify(privateKey.Public().(ed25519.PublicKey), []byte(signed), signature) {
		t.Fatal("assertion signature did not verify")
	}
}

func TestStoredAgentIdentityCanRemainInactiveWithOAuthMaterial(t *testing.T) {
	keyMaterial, errKey := GenerateAgentIdentityKeyMaterial()
	if errKey != nil {
		t.Fatalf("GenerateAgentIdentityKeyMaterial() error = %v", errKey)
	}
	metadata := AgentIdentityMetadata(AgentIdentityCredential{
		AgentRuntimeID:        "runtime-stored",
		PrivateKeyPKCS8Base64: keyMaterial.PrivateKeyPKCS8Base64,
		TaskID:                "task-stored",
		AccountID:             "account-stored",
		ChatGPTAccountID:      "team-stored",
		ChatGPTUserID:         "user-stored",
		Email:                 "stored@example.com",
		PlanType:              "plus",
	})
	metadata["auth_mode"] = OAuthAuthMode
	metadata["access_token"] = "oauth-access"
	metadata["refresh_token"] = "oauth-refresh"

	if !HasStoredAgentIdentity(metadata) {
		t.Fatal("inactive Agent Identity material was not recognized")
	}
	if got := EffectiveAuthMode(metadata); got != OAuthAuthMode {
		t.Fatalf("EffectiveAuthMode() = %q, want OAuth", got)
	}
	if got := AuthModeLabel(metadata); got != "OAuth" {
		t.Fatalf("AuthModeLabel() = %q, want OAuth", got)
	}
	if errValidate := ValidateCompleteAgentIdentityMetadata(metadata); errValidate == nil {
		t.Fatal("inactive Agent Identity unexpectedly passed active upload validation")
	}
	metadata["auth_mode"] = AgentIdentityAuthMode
	if errValidate := ValidateCompleteAgentIdentityMetadata(metadata); errValidate != nil {
		t.Fatalf("active Agent Identity with retained OAuth material failed validation: %v", errValidate)
	}
}

func TestOAuthModeAvailableUsesAccessExpiryOrRefreshToken(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	if OAuthModeAvailable(map[string]any{"access_token": "access", "expired": now.Add(-time.Minute).Format(time.RFC3339)}, now) {
		t.Fatal("expired access token without refresh token was reported available")
	}
	if !OAuthModeAvailable(map[string]any{"access_token": "access", "expired": now.Add(time.Minute).Format(time.RFC3339)}, now) {
		t.Fatal("unexpired access token was reported unavailable")
	}
	if !OAuthModeAvailable(map[string]any{"refresh_token": "refresh"}, now) {
		t.Fatal("refresh token was reported unavailable")
	}
}

func TestRegisterAgentIdentityAndEncryptedTask(t *testing.T) {
	keyMaterial, err := GenerateAgentIdentityKeyMaterial()
	if err != nil {
		t.Fatalf("GenerateAgentIdentityKeyMaterial() error = %v", err)
	}
	privateKey, err := signingKeyFromPKCS8Base64(keyMaterial.PrivateKeyPKCS8Base64)
	if err != nil {
		t.Fatalf("parse private key: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/agent/register":
			if got := r.Header.Get("Authorization"); got != "Bearer access-token" {
				t.Errorf("Authorization = %q", got)
			}
			var request registerAgentRequest
			if errDecode := json.NewDecoder(r.Body).Decode(&request); errDecode != nil {
				t.Errorf("decode agent registration: %v", errDecode)
			}
			if request.AgentPublicKey != keyMaterial.PublicKeySSH || len(request.Capabilities) != 1 || request.Capabilities[0] != "responsesapi" {
				t.Errorf("agent registration request = %+v", request)
			}
			_ = json.NewEncoder(w).Encode(registerAgentResponse{AgentRuntimeID: "runtime-a"})
		case "/v1/agent/runtime-a/task/register":
			var request registerTaskRequest
			if errDecode := json.NewDecoder(r.Body).Decode(&request); errDecode != nil {
				t.Errorf("decode task registration: %v", errDecode)
			}
			signature, errDecode := base64.StdEncoding.DecodeString(request.Signature)
			if errDecode != nil || !ed25519.Verify(privateKey.Public().(ed25519.PublicKey), []byte("runtime-a:"+request.Timestamp), signature) {
				t.Errorf("task registration signature is invalid: %v", errDecode)
			}
			encrypted, errEncrypt := encryptAgentTaskIDForTest(privateKey, "task-encrypted")
			if errEncrypt != nil {
				t.Errorf("encrypt task ID: %v", errEncrypt)
			}
			_ = json.NewEncoder(w).Encode(registerTaskResponse{EncryptedTaskIDCamel: encrypted})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	runtimeID, err := RegisterAgentIdentity(context.Background(), server.Client(), server.URL, "access-token", false, keyMaterial)
	if err != nil {
		t.Fatalf("RegisterAgentIdentity() error = %v", err)
	}
	credential := AgentIdentityCredential{AgentRuntimeID: runtimeID, PrivateKeyPKCS8Base64: keyMaterial.PrivateKeyPKCS8Base64}
	taskID, err := RegisterAgentTask(context.Background(), server.Client(), server.URL, credential)
	if err != nil {
		t.Fatalf("RegisterAgentTask() error = %v", err)
	}
	if taskID != "task-encrypted" {
		t.Fatalf("task ID = %q", taskID)
	}
}

func TestParseAccessTokenIdentity(t *testing.T) {
	claims := map[string]any{
		"sub": "user-a",
		"exp": float64(1_900_000_000),
		"https://api.openai.com/profile": map[string]any{
			"email": "codex@example.com",
		},
		"https://api.openai.com/auth": map[string]any{
			"chatgpt_account_id":         "account-a",
			"chatgpt_plan_type":          "plus",
			"chatgpt_account_is_fedramp": true,
		},
	}
	payload, _ := json.Marshal(claims)
	token := "header." + base64.RawURLEncoding.EncodeToString(payload) + ".signature"
	identity, err := ParseAccessTokenIdentity(token)
	if err != nil {
		t.Fatalf("ParseAccessTokenIdentity() error = %v", err)
	}
	if identity.AccountID != "account-a" || identity.ChatGPTUserID != "user-a" || identity.Email != "codex@example.com" || identity.PlanType != "plus" || !identity.ChatGPTAccountIsFedRAMP {
		t.Fatalf("identity = %+v", identity)
	}
}

func TestSanitizeAgentIdentityErrorBody(t *testing.T) {
	metadata := map[string]any{
		"type":              "codex",
		"auth_mode":         AgentIdentityAuthMode,
		"agent_private_key": "private-secret",
		"agent_runtime_id":  "runtime-secret",
		"task_id":           "task-secret",
		"access_token":      "access-secret",
		"refresh_token":     "refresh-secret",
		"id_token":          "id-secret",
	}
	body := []byte(`{"error":{"code":"invalid_task_id","message":"private-secret runtime-secret task-secret access-secret refresh-secret id-secret AgentAssertion assertion-secret"}}`)
	sanitized := SanitizeAgentIdentityErrorBody(metadata, body)
	for _, secret := range []string{"private-secret", "runtime-secret", "task-secret", "access-secret", "refresh-secret", "id-secret", "assertion-secret"} {
		if strings.Contains(string(sanitized), secret) {
			t.Fatalf("sanitized body contains %q: %s", secret, sanitized)
		}
	}
	if !strings.Contains(string(sanitized), "invalid_task_id") || !strings.Contains(string(sanitized), "AgentAssertion [redacted]") {
		t.Fatalf("sanitized body = %s", sanitized)
	}

	ordinary := []byte("ordinary response")
	if got := SanitizeAgentIdentityErrorBody(map[string]any{"type": "codex"}, ordinary); string(got) != string(ordinary) {
		t.Fatalf("ordinary body changed to %q", got)
	}
}

func TestAgentIdentityRegistrationErrorsAreRedactedAndTyped(t *testing.T) {
	keyMaterial, err := GenerateAgentIdentityKeyMaterial()
	if err != nil {
		t.Fatalf("GenerateAgentIdentityKeyMaterial() error = %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusUnauthorized)
		switch request.URL.Path {
		case "/v1/agent/register":
			_, _ = writer.Write([]byte(`{"error":"Bearer access-secret"}`))
		default:
			var payload registerTaskRequest
			_ = json.NewDecoder(request.Body).Decode(&payload)
			_, _ = writer.Write([]byte(`{"error":"runtime-secret private=` + keyMaterial.PrivateKeyPKCS8Base64 + ` signature=` + payload.Signature + `"}`))
		}
	}))
	defer server.Close()

	_, err = RegisterAgentIdentity(t.Context(), server.Client(), server.URL, "access-secret", false, keyMaterial)
	assertRedactedRegistrationError(t, err, "access-secret")
	_, err = RegisterAgentTask(t.Context(), server.Client(), server.URL, AgentIdentityCredential{
		AgentRuntimeID:        "runtime-secret",
		PrivateKeyPKCS8Base64: keyMaterial.PrivateKeyPKCS8Base64,
		TaskID:                "task-secret",
	})
	assertRedactedRegistrationError(t, err, "runtime-secret", keyMaterial.PrivateKeyPKCS8Base64)
}

func TestRetryAgentIdentityRegistrationRetriesAttemptTimeout(t *testing.T) {
	attempts := 0
	err := RetryAgentIdentityRegistration(t.Context(), 2, func(context.Context) error {
		attempts++
		if attempts == 1 {
			return fmt.Errorf("registration timed out: %w", context.DeadlineExceeded)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RetryAgentIdentityRegistration() error = %v", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}

func TestRetryAgentIdentityRegistrationStopsOnParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	err := RetryAgentIdentityRegistration(ctx, 2, func(context.Context) error {
		attempts++
		cancel()
		return fmt.Errorf("registration timed out: %w", context.DeadlineExceeded)
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RetryAgentIdentityRegistration() error = %v, want context canceled", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func assertRedactedRegistrationError(t *testing.T, err error, secrets ...string) {
	t.Helper()
	var responseErr *AgentIdentityRegistrationHTTPError
	if !errors.As(err, &responseErr) || responseErr.StatusCode != http.StatusUnauthorized {
		t.Fatalf("registration error = %T %v", err, err)
	}
	for _, secret := range secrets {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("registration error contains secret %q: %v", secret, err)
		}
	}
}

func encryptAgentTaskIDForTest(privateKey ed25519.PrivateKey, taskID string) (string, error) {
	digest := sha512.Sum512(privateKey.Seed())
	var curvePrivate [32]byte
	copy(curvePrivate[:], digest[:32])
	curvePrivate[0] &= 248
	curvePrivate[31] &= 127
	curvePrivate[31] |= 64
	curvePublicBytes, err := curve25519.X25519(curvePrivate[:], curve25519.Basepoint)
	if err != nil {
		return "", err
	}
	var curvePublic [32]byte
	copy(curvePublic[:], curvePublicBytes)
	ciphertext, err := box.SealAnonymous(nil, []byte(taskID), &curvePublic, rand.Reader)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

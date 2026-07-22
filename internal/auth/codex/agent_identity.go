package codex

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha512"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	neturl "net/url"
	"runtime"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/buildinfo"
	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/nacl/box"
)

const (
	AgentIdentityAuthMode       = "agentIdentity"
	OAuthAuthMode               = "oauth"
	AgentIdentityAuthAPIBaseURL = "https://auth.openai.com/api/accounts"

	agentIdentityRegistrationTimeout = 15 * time.Second
	agentTaskRegistrationTimeout     = 30 * time.Second
	agentIdentityMaxResponseBytes    = 1 << 20
	agentIdentityMaxErrorBodyBytes   = 64 << 10
	agentIdentityKeySeedBytes        = 64
)

var agentIdentityKeyDerivationContext = []byte("codex-agent-identity-ed25519-v1")

// AgentIdentityCredential contains the durable signing material used by Codex.
type AgentIdentityCredential struct {
	AgentRuntimeID          string
	PrivateKeyPKCS8Base64   string
	TaskID                  string
	AccountID               string
	ChatGPTAccountID        string
	ChatGPTUserID           string
	Email                   string
	PlanType                string
	WorkspaceID             string
	ChatGPTAccountIsFedRAMP bool
}

// AgentIdentityKeyMaterial contains a newly generated private key and its SSH public key.
type AgentIdentityKeyMaterial struct {
	PrivateKeyPKCS8Base64 string
	PublicKeySSH          string
}

// AccessTokenIdentity contains account metadata read from a Codex access token.
type AccessTokenIdentity struct {
	AccountID               string
	ChatGPTAccountID        string
	ChatGPTUserID           string
	Email                   string
	PlanType                string
	WorkspaceID             string
	ChatGPTAccountIsFedRAMP bool
	ExpiresAt               time.Time
}

// AgentIdentityRegistrationHTTPError reports a non-successful registration response.
type AgentIdentityRegistrationHTTPError struct {
	Operation  string
	StatusCode int
	Body       string
}

func (e *AgentIdentityRegistrationHTTPError) Error() string {
	if e == nil {
		return "agent identity registration failed"
	}
	if e.Body == "" {
		return fmt.Sprintf("%s failed with status %d", e.Operation, e.StatusCode)
	}
	return fmt.Sprintf("%s failed with status %d: %s", e.Operation, e.StatusCode, e.Body)
}

// HTTPStatus returns the upstream HTTP status.
func (e *AgentIdentityRegistrationHTTPError) HTTPStatus() int {
	if e == nil {
		return 0
	}
	return e.StatusCode
}

// Retryable reports whether the registration failure is safe to retry.
func (e *AgentIdentityRegistrationHTTPError) Retryable() bool {
	if e == nil {
		return false
	}
	return e.StatusCode == http.StatusTooManyRequests || e.StatusCode >= http.StatusInternalServerError
}

// IsAgentIdentityMetadata reports whether metadata declares Codex Agent Identity auth.
func IsAgentIdentityMetadata(metadata map[string]any) bool {
	return strings.EqualFold(strings.TrimSpace(metadataString(metadata, "auth_mode", "authMode")), AgentIdentityAuthMode)
}

// EffectiveAuthMode returns the active Codex authentication mode. Existing
// credentials without auth_mode remain OAuth credentials.
func EffectiveAuthMode(metadata map[string]any) string {
	if IsAgentIdentityMetadata(metadata) {
		return AgentIdentityAuthMode
	}
	return OAuthAuthMode
}

// AuthModeLabel returns the frontend-facing name used for a Codex auth mode.
func AuthModeLabel(metadata map[string]any) string {
	if IsAgentIdentityMetadata(metadata) {
		return "Agent Identity"
	}
	return "OAuth"
}

// HasStoredAgentIdentity reports whether reusable signing material exists,
// regardless of which authentication mode is currently active.
func HasStoredAgentIdentity(metadata map[string]any) bool {
	_, err := ParseAgentIdentityCredential(metadata)
	return err == nil
}

// HasOAuthCredentialMaterial reports whether the credential retains enough
// OAuth material to attempt or refresh Bearer authentication.
func HasOAuthCredentialMaterial(metadata map[string]any) bool {
	return strings.TrimSpace(metadataString(metadata, "access_token", "accessToken")) != "" ||
		strings.TrimSpace(metadataString(metadata, "refresh_token", "refreshToken")) != ""
}

// OAuthModeAvailable reports whether switching to OAuth can produce an
// immediately usable token or refresh one.
func OAuthModeAvailable(metadata map[string]any, now time.Time) bool {
	if strings.TrimSpace(metadataString(metadata, "refresh_token", "refreshToken")) != "" {
		return true
	}
	accessToken := strings.TrimSpace(metadataString(metadata, "access_token", "accessToken"))
	if accessToken == "" {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	if identity, err := ParseAccessTokenIdentity(accessToken); err == nil && !identity.ExpiresAt.IsZero() {
		return identity.ExpiresAt.After(now)
	}
	if expired := strings.TrimSpace(metadataString(metadata, "expired", "expires_at", "expiresAt")); expired != "" {
		if expiresAt, err := time.Parse(time.RFC3339, expired); err == nil {
			return expiresAt.After(now)
		}
	}
	return true
}

// ParseAgentIdentityCredential validates reusable Agent Identity signing material.
// TaskID may be empty so callers can register a new task before execution.
func ParseAgentIdentityCredential(metadata map[string]any) (AgentIdentityCredential, error) {
	if provider := strings.TrimSpace(metadataString(metadata, "type")); provider != "" && !strings.EqualFold(provider, "codex") {
		return AgentIdentityCredential{}, errors.New("Agent Identity is only supported for codex credentials")
	}
	credential := AgentIdentityCredential{
		AgentRuntimeID:          strings.TrimSpace(metadataString(metadata, "agent_runtime_id", "agentRuntimeId")),
		PrivateKeyPKCS8Base64:   strings.TrimSpace(metadataString(metadata, "agent_private_key", "agentPrivateKey")),
		TaskID:                  strings.TrimSpace(metadataString(metadata, "task_id", "taskId")),
		AccountID:               strings.TrimSpace(metadataString(metadata, "account_id", "accountId")),
		ChatGPTAccountID:        strings.TrimSpace(metadataString(metadata, "chatgpt_account_id", "chatgptAccountId")),
		ChatGPTUserID:           strings.TrimSpace(metadataString(metadata, "chatgpt_user_id", "chatgptUserId")),
		Email:                   strings.TrimSpace(metadataString(metadata, "email")),
		PlanType:                strings.TrimSpace(metadataString(metadata, "plan_type", "planType")),
		WorkspaceID:             strings.TrimSpace(metadataString(metadata, "workspace_id", "workspaceId")),
		ChatGPTAccountIsFedRAMP: metadataBool(metadata, "chatgpt_account_is_fedramp", "chatgptAccountIsFedramp"),
	}
	if credential.AgentRuntimeID == "" {
		return AgentIdentityCredential{}, errors.New("agent_runtime_id is required")
	}
	if credential.PrivateKeyPKCS8Base64 == "" {
		return AgentIdentityCredential{}, errors.New("agent_private_key is required")
	}
	if _, err := signingKeyFromPKCS8Base64(credential.PrivateKeyPKCS8Base64); err != nil {
		return AgentIdentityCredential{}, fmt.Errorf("invalid agent_private_key: %w", err)
	}
	if credential.ChatGPTAccountID == "" {
		credential.ChatGPTAccountID = credential.AccountID
	}
	if credential.AccountID == "" {
		credential.AccountID = credential.ChatGPTAccountID
	}
	return credential, nil
}

// ValidateCompleteAgentIdentityMetadata validates a directly uploadable credential.
func ValidateCompleteAgentIdentityMetadata(metadata map[string]any) error {
	if !strings.EqualFold(strings.TrimSpace(metadataString(metadata, "type")), "codex") {
		return errors.New("type must be codex")
	}
	if !IsAgentIdentityMetadata(metadata) {
		return errors.New("auth_mode must be agentIdentity")
	}
	credential, err := ParseAgentIdentityCredential(metadata)
	if err != nil {
		return err
	}
	if credential.TaskID == "" {
		return errors.New("task_id is required")
	}
	for _, required := range []string{"account_id", "chatgpt_account_id", "chatgpt_user_id", "email", "plan_type"} {
		if strings.TrimSpace(metadataString(metadata, required)) == "" {
			return fmt.Errorf("%s is required", required)
		}
	}
	return nil
}

// AgentIdentityMetadata builds the canonical persisted credential payload.
func AgentIdentityMetadata(credential AgentIdentityCredential) map[string]any {
	metadata := map[string]any{
		"type":                       "codex",
		"auth_mode":                  AgentIdentityAuthMode,
		"agent_runtime_id":           strings.TrimSpace(credential.AgentRuntimeID),
		"agent_private_key":          strings.TrimSpace(credential.PrivateKeyPKCS8Base64),
		"task_id":                    strings.TrimSpace(credential.TaskID),
		"chatgpt_account_is_fedramp": credential.ChatGPTAccountIsFedRAMP,
	}
	setAgentIdentityMetadataString(metadata, "account_id", credential.AccountID)
	setAgentIdentityMetadataString(metadata, "chatgpt_account_id", credential.ChatGPTAccountID)
	setAgentIdentityMetadataString(metadata, "chatgpt_user_id", credential.ChatGPTUserID)
	setAgentIdentityMetadataString(metadata, "email", credential.Email)
	setAgentIdentityMetadataString(metadata, "plan_type", credential.PlanType)
	setAgentIdentityMetadataString(metadata, "workspace_id", credential.WorkspaceID)
	return metadata
}

func setAgentIdentityMetadataString(metadata map[string]any, key, value string) {
	if value = strings.TrimSpace(value); value != "" {
		metadata[key] = value
	}
}

// ParseAccessTokenIdentity reads Codex account metadata from an access-token JWT.
// The upstream registration endpoint remains responsible for authenticating the token.
func ParseAccessTokenIdentity(accessToken string) (AccessTokenIdentity, error) {
	parts := strings.Split(strings.TrimSpace(accessToken), ".")
	if len(parts) != 3 || parts[1] == "" {
		return AccessTokenIdentity{}, errors.New("access token is not a JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		payload, err = base64.URLEncoding.DecodeString(parts[1])
	}
	if err != nil {
		return AccessTokenIdentity{}, fmt.Errorf("decode access token claims: %w", err)
	}
	var claims map[string]any
	if err = json.Unmarshal(payload, &claims); err != nil {
		return AccessTokenIdentity{}, fmt.Errorf("decode access token claims: %w", err)
	}
	authClaims, _ := claims["https://api.openai.com/auth"].(map[string]any)
	profileClaims, _ := claims["https://api.openai.com/profile"].(map[string]any)
	email := firstMetadataString(claims, "email")
	if email == "" {
		email = firstMetadataString(profileClaims, "email")
	}
	identity := AccessTokenIdentity{
		ChatGPTAccountID:        firstMetadataString(authClaims, "chatgpt_account_id", "account_id"),
		ChatGPTUserID:           firstMetadataString(authClaims, "chatgpt_user_id", "user_id"),
		Email:                   email,
		PlanType:                firstMetadataString(authClaims, "chatgpt_plan_type", "plan_type"),
		WorkspaceID:             firstMetadataString(authClaims, "workspace_id"),
		ChatGPTAccountIsFedRAMP: metadataBool(authClaims, "chatgpt_account_is_fedramp", "is_fedramp"),
	}
	identity.AccountID = firstMetadataString(claims, "account_id")
	if identity.AccountID == "" {
		identity.AccountID = identity.ChatGPTAccountID
	}
	if identity.ChatGPTAccountID == "" {
		identity.ChatGPTAccountID = identity.AccountID
	}
	if identity.ChatGPTUserID == "" {
		identity.ChatGPTUserID = firstMetadataString(claims, "sub")
	}
	if expiry, ok := numericMetadataValue(claims["exp"]); ok && expiry > 0 {
		identity.ExpiresAt = time.Unix(expiry, 0).UTC()
	}
	if identity.AccountID == "" {
		return AccessTokenIdentity{}, errors.New("access token does not contain a ChatGPT account ID")
	}
	return identity, nil
}

func firstMetadataString(metadata map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := metadata[key].(string); ok {
			if value = strings.TrimSpace(value); value != "" {
				return value
			}
		}
	}
	return ""
}

func metadataBool(metadata map[string]any, keys ...string) bool {
	for _, key := range keys {
		switch value := metadata[key].(type) {
		case bool:
			return value
		case string:
			return strings.EqualFold(strings.TrimSpace(value), "true")
		}
	}
	return false
}

func numericMetadataValue(value any) (int64, bool) {
	switch typed := value.(type) {
	case float64:
		return int64(typed), true
	case json.Number:
		parsed, err := typed.Int64()
		return parsed, err == nil
	case int64:
		return typed, true
	case int:
		return int64(typed), true
	default:
		return 0, false
	}
}

// GenerateAgentIdentityKeyMaterial creates the Ed25519 material expected by Codex.
func GenerateAgentIdentityKeyMaterial() (AgentIdentityKeyMaterial, error) {
	seedMaterial := make([]byte, agentIdentityKeySeedBytes)
	if _, err := io.ReadFull(rand.Reader, seedMaterial); err != nil {
		return AgentIdentityKeyMaterial{}, fmt.Errorf("generate agent identity seed: %w", err)
	}
	digestInput := make([]byte, 0, len(agentIdentityKeyDerivationContext)+len(seedMaterial))
	digestInput = append(digestInput, agentIdentityKeyDerivationContext...)
	digestInput = append(digestInput, seedMaterial...)
	digest := sha512.Sum512(digestInput)
	privateKey := ed25519.NewKeyFromSeed(digest[:ed25519.SeedSize])
	der, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return AgentIdentityKeyMaterial{}, fmt.Errorf("encode agent identity private key: %w", err)
	}
	publicKey := privateKey.Public().(ed25519.PublicKey)
	return AgentIdentityKeyMaterial{
		PrivateKeyPKCS8Base64: base64.StdEncoding.EncodeToString(der),
		PublicKeySSH:          encodeSSHEd25519PublicKey(publicKey),
	}, nil
}

func encodeSSHEd25519PublicKey(publicKey ed25519.PublicKey) string {
	keyType := []byte("ssh-ed25519")
	blob := make([]byte, 0, 4+len(keyType)+4+len(publicKey))
	blob = appendSSHString(blob, keyType)
	blob = appendSSHString(blob, publicKey)
	return "ssh-ed25519 " + base64.StdEncoding.EncodeToString(blob)
}

func appendSSHString(dst, value []byte) []byte {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	dst = append(dst, length[:]...)
	return append(dst, value...)
}

func signingKeyFromPKCS8Base64(encoded string) (ed25519.PrivateKey, error) {
	der, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
	if err != nil {
		return nil, fmt.Errorf("decode PKCS#8 base64: %w", err)
	}
	key, err := x509.ParsePKCS8PrivateKey(der)
	if err != nil {
		return nil, fmt.Errorf("parse PKCS#8 private key: %w", err)
	}
	privateKey, ok := key.(ed25519.PrivateKey)
	if !ok || len(privateKey) != ed25519.PrivateKeySize {
		return nil, errors.New("PKCS#8 key is not Ed25519")
	}
	return privateKey, nil
}

// BuildAgentAssertion returns the Authorization value for one Codex request.
func BuildAgentAssertion(credential AgentIdentityCredential, now time.Time) (string, error) {
	if strings.TrimSpace(credential.TaskID) == "" {
		return "", errors.New("task_id is required")
	}
	timestamp := agentIdentityTimestamp(now)
	signature, err := signAgentIdentityPayload(credential.PrivateKeyPKCS8Base64, strings.Join([]string{credential.AgentRuntimeID, credential.TaskID, timestamp}, ":"))
	if err != nil {
		return "", err
	}
	envelope := struct {
		AgentRuntimeID string `json:"agent_runtime_id"`
		Signature      string `json:"signature"`
		TaskID         string `json:"task_id"`
		Timestamp      string `json:"timestamp"`
	}{credential.AgentRuntimeID, signature, credential.TaskID, timestamp}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return "", fmt.Errorf("serialize agent assertion: %w", err)
	}
	return "AgentAssertion " + base64.RawURLEncoding.EncodeToString(payload), nil
}

// AuthorizationHeader returns Bearer auth for OAuth credentials or AgentAssertion.
func AuthorizationHeader(metadata map[string]any, bearerToken string, now time.Time) (string, error) {
	if !IsAgentIdentityMetadata(metadata) {
		if bearerToken = strings.TrimSpace(bearerToken); bearerToken == "" {
			return "", nil
		}
		return "Bearer " + bearerToken, nil
	}
	credential, err := ParseAgentIdentityCredential(metadata)
	if err != nil {
		return "", err
	}
	return BuildAgentAssertion(credential, now)
}

func signAgentIdentityPayload(encodedPrivateKey, payload string) (string, error) {
	privateKey, err := signingKeyFromPKCS8Base64(encodedPrivateKey)
	if err != nil {
		return "", err
	}
	signature := ed25519.Sign(privateKey, []byte(payload))
	return base64.StdEncoding.EncodeToString(signature), nil
}

func agentIdentityTimestamp(now time.Time) string {
	if now.IsZero() {
		now = time.Now()
	}
	return now.UTC().Truncate(time.Second).Format(time.RFC3339)
}

type registerAgentRequest struct {
	ABOM           agentBillOfMaterials `json:"abom"`
	AgentPublicKey string               `json:"agent_public_key"`
	Capabilities   []string             `json:"capabilities"`
	TTL            *uint64              `json:"ttl"`
}

type agentBillOfMaterials struct {
	AgentVersion    string `json:"agent_version"`
	AgentHarnessID  string `json:"agent_harness_id"`
	RunningLocation string `json:"running_location"`
}

type registerAgentResponse struct {
	AgentRuntimeID string `json:"agent_runtime_id"`
}

type registerTaskRequest struct {
	Timestamp string `json:"timestamp"`
	Signature string `json:"signature"`
}

type registerTaskResponse struct {
	TaskID               string `json:"task_id"`
	TaskIDCamel          string `json:"taskId"`
	EncryptedTaskID      string `json:"encrypted_task_id"`
	EncryptedTaskIDCamel string `json:"encryptedTaskId"`
}

// RegisterAgentIdentity registers durable key material using a one-time access token.
func RegisterAgentIdentity(ctx context.Context, client *http.Client, baseURL, accessToken string, fedramp bool, keyMaterial AgentIdentityKeyMaterial) (string, error) {
	if client == nil {
		client = http.DefaultClient
	}
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		baseURL = AgentIdentityAuthAPIBaseURL
	}
	if strings.TrimSpace(accessToken) == "" {
		return "", errors.New("access token is required")
	}
	if strings.TrimSpace(keyMaterial.PublicKeySSH) == "" {
		return "", errors.New("agent public key is required")
	}
	version := strings.TrimSpace(buildinfo.Version)
	if version == "" {
		version = "dev"
	}
	payload := registerAgentRequest{
		ABOM: agentBillOfMaterials{
			AgentVersion:    version,
			AgentHarnessID:  "codex-cli",
			RunningLocation: "cli-" + runtime.GOOS,
		},
		AgentPublicKey: keyMaterial.PublicKeySSH,
		Capabilities:   []string{"responsesapi"},
		TTL:            nil,
	}
	var response registerAgentResponse
	if err := doAgentIdentityJSONRequest(ctx, client, agentIdentityRegistrationTimeout, http.MethodPost, baseURL+"/v1/agent/register", "Bearer "+strings.TrimSpace(accessToken), fedramp, payload, &response, "agent identity registration"); err != nil {
		return "", redactAgentIdentityRegistrationError(err, accessToken)
	}
	if response.AgentRuntimeID = strings.TrimSpace(response.AgentRuntimeID); response.AgentRuntimeID == "" {
		return "", errors.New("agent identity registration response omitted agent_runtime_id")
	}
	return response.AgentRuntimeID, nil
}

// RegisterAgentTask registers a task for existing durable Agent Identity material.
func RegisterAgentTask(ctx context.Context, client *http.Client, baseURL string, credential AgentIdentityCredential) (string, error) {
	if client == nil {
		client = http.DefaultClient
	}
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		baseURL = AgentIdentityAuthAPIBaseURL
	}
	if credential.AgentRuntimeID = strings.TrimSpace(credential.AgentRuntimeID); credential.AgentRuntimeID == "" {
		return "", errors.New("agent_runtime_id is required")
	}
	timestamp := agentIdentityTimestamp(time.Now())
	signature, err := signAgentIdentityPayload(credential.PrivateKeyPKCS8Base64, credential.AgentRuntimeID+":"+timestamp)
	if err != nil {
		return "", fmt.Errorf("sign task registration: %w", err)
	}
	payload := registerTaskRequest{Timestamp: timestamp, Signature: signature}
	var response registerTaskResponse
	endpoint := fmt.Sprintf("%s/v1/agent/%s/task/register", baseURL, neturl.PathEscape(credential.AgentRuntimeID))
	if err = doAgentIdentityJSONRequest(ctx, client, agentTaskRegistrationTimeout, http.MethodPost, endpoint, "", false, payload, &response, "agent task registration"); err != nil {
		return "", redactAgentIdentityRegistrationError(err, credential.AgentRuntimeID, credential.PrivateKeyPKCS8Base64, credential.TaskID, signature)
	}
	if taskID := strings.TrimSpace(firstNonEmptyAgentTaskID(response.TaskID, response.TaskIDCamel)); taskID != "" {
		return taskID, nil
	}
	encrypted := strings.TrimSpace(firstNonEmptyAgentTaskID(response.EncryptedTaskID, response.EncryptedTaskIDCamel))
	if encrypted == "" {
		return "", errors.New("agent task registration response omitted task_id")
	}
	return decryptAgentTaskID(credential.PrivateKeyPKCS8Base64, encrypted)
}

func firstNonEmptyAgentTaskID(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func decryptAgentTaskID(encodedPrivateKey, encrypted string) (string, error) {
	privateKey, err := signingKeyFromPKCS8Base64(encodedPrivateKey)
	if err != nil {
		return "", err
	}
	ciphertext, err := base64.StdEncoding.DecodeString(encrypted)
	if err != nil {
		return "", fmt.Errorf("decode encrypted task_id: %w", err)
	}
	digest := sha512.Sum512(privateKey.Seed())
	var curvePrivate [32]byte
	copy(curvePrivate[:], digest[:32])
	curvePrivate[0] &= 248
	curvePrivate[31] &= 127
	curvePrivate[31] |= 64
	curvePublicBytes, err := curve25519.X25519(curvePrivate[:], curve25519.Basepoint)
	if err != nil {
		return "", fmt.Errorf("derive Curve25519 public key: %w", err)
	}
	var curvePublic [32]byte
	copy(curvePublic[:], curvePublicBytes)
	plaintext, ok := box.OpenAnonymous(nil, ciphertext, &curvePublic, &curvePrivate)
	if !ok {
		return "", errors.New("decrypt encrypted task_id")
	}
	taskID := strings.TrimSpace(string(plaintext))
	if taskID == "" {
		return "", errors.New("decrypted task_id is empty")
	}
	return taskID, nil
}

func doAgentIdentityJSONRequest(ctx context.Context, client *http.Client, timeout time.Duration, method, endpoint, authorization string, fedramp bool, payload, output any, operation string) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode %s request: %w", operation, err)
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build %s request: %w", operation, err)
	}
	request.Header.Set("Content-Type", "application/json")
	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}
	if fedramp {
		request.Header.Set("X-OpenAI-Fedramp", "true")
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("%s request failed: %w", operation, err)
	}
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, agentIdentityMaxResponseBytes+1))
	closeErr := response.Body.Close()
	if readErr != nil {
		return fmt.Errorf("read %s response: %w", operation, readErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close %s response: %w", operation, closeErr)
	}
	if len(responseBody) > agentIdentityMaxResponseBytes {
		return fmt.Errorf("%s response is too large", operation)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return &AgentIdentityRegistrationHTTPError{
			Operation:  operation,
			StatusCode: response.StatusCode,
			Body:       truncateAgentIdentityErrorBody(responseBody),
		}
	}
	if err = json.Unmarshal(responseBody, output); err != nil {
		return fmt.Errorf("decode %s response: %w", operation, err)
	}
	return nil
}

func truncateAgentIdentityErrorBody(body []byte) string {
	body = []byte(strings.TrimSpace(string(body)))
	if len(body) <= 512 {
		return string(body)
	}
	return string(body[:512]) + "..."
}

// SanitizeAgentIdentityErrorBody removes durable credentials and assertions from
// an Agent Identity upstream error before it reaches logs or clients.
func SanitizeAgentIdentityErrorBody(metadata map[string]any, body []byte) []byte {
	if !IsAgentIdentityMetadata(metadata) || len(body) == 0 {
		return body
	}
	redacted := redactAgentIdentitySecrets(body,
		metadataString(metadata, "agent_private_key", "agentPrivateKey"),
		metadataString(metadata, "agent_runtime_id", "agentRuntimeId"),
		metadataString(metadata, "task_id", "taskId"),
		metadataString(metadata, "access_token", "accessToken"),
		metadataString(metadata, "refresh_token", "refreshToken"),
		metadataString(metadata, "id_token", "idToken"),
	)
	if len(redacted) <= agentIdentityMaxErrorBodyBytes {
		return redacted
	}
	truncated := make([]byte, 0, agentIdentityMaxErrorBodyBytes+3)
	truncated = append(truncated, redacted[:agentIdentityMaxErrorBodyBytes]...)
	return append(truncated, '.', '.', '.')
}

func redactAgentIdentityRegistrationError(err error, secrets ...string) error {
	var responseErr *AgentIdentityRegistrationHTTPError
	if !errors.As(err, &responseErr) || responseErr == nil {
		return err
	}
	redacted := redactAgentIdentitySecrets([]byte(responseErr.Body), secrets...)
	clone := *responseErr
	clone.Body = truncateAgentIdentityErrorBody(redacted)
	return &clone
}

func redactAgentIdentitySecrets(body []byte, secrets ...string) []byte {
	redacted := string(body)
	for _, secret := range secrets {
		if secret = strings.TrimSpace(secret); secret != "" {
			redacted = strings.ReplaceAll(redacted, secret, "[redacted]")
		}
	}
	const assertionPrefix = "AgentAssertion "
	for offset := 0; offset < len(redacted); {
		relativeStart := strings.Index(redacted[offset:], assertionPrefix)
		if relativeStart < 0 {
			break
		}
		valueStart := offset + relativeStart + len(assertionPrefix)
		end := valueStart
		for end < len(redacted) && !strings.ContainsRune(" \t\r\n\"',}", rune(redacted[end])) {
			end++
		}
		redacted = redacted[:valueStart] + "[redacted]" + redacted[end:]
		offset = valueStart + len("[redacted]")
	}
	return []byte(redacted)
}

// RetryAgentIdentityRegistration retries network, 429 and 5xx failures.
func RetryAgentIdentityRegistration(ctx context.Context, attempts int, operation func(context.Context) error) error {
	if attempts < 1 {
		attempts = 1
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		lastErr = operation(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}
		if lastErr == nil || !agentIdentityRegistrationErrorRetryable(lastErr) || attempt+1 >= attempts {
			return lastErr
		}
		delay := time.Duration(250*(1<<attempt)) * time.Millisecond
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

func agentIdentityRegistrationErrorRetryable(err error) bool {
	var responseErr *AgentIdentityRegistrationHTTPError
	if errors.As(err, &responseErr) {
		return responseErr.Retryable()
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var networkErr net.Error
	if errors.As(err, &networkErr) {
		return true
	}
	var urlErr *neturl.Error
	return errors.As(err, &urlErr)
}

// IsAgentTaskUnauthorizedError reports whether a failed request needs a new task ID.
func IsAgentTaskUnauthorizedError(err error) bool {
	if err == nil {
		return false
	}
	type statusError interface{ StatusCode() int }
	var status statusError
	if !errors.As(err, &status) || status.StatusCode() != http.StatusUnauthorized {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "invalid_task_id") || strings.Contains(message, "task_not_found") || strings.Contains(message, "task_expired")
}

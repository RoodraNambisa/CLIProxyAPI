package auth

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
)

type chatGPTWebCredentialReplacementContextKey struct{}
type chatGPTWebCredentialRefreshContextKey struct{}

func withChatGPTWebCredentialReplacement(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, chatGPTWebCredentialReplacementContextKey{}, true)
}

// ChatGPTWebCredentialReplaced reports whether an auth update installed a
// different ChatGPT Web account under the same auth ID.
func ChatGPTWebCredentialReplaced(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	replaced, _ := ctx.Value(chatGPTWebCredentialReplacementContextKey{}).(bool)
	return replaced
}

func withChatGPTWebCredentialRefresh(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, chatGPTWebCredentialRefreshContextKey{}, true)
}

func withoutChatGPTWebCredentialUpdateMarkers(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = context.WithValue(ctx, chatGPTWebDependencyMutationContextKey{}, (*chatGPTWebDependencyMutationToken)(nil))
	ctx = context.WithValue(ctx, chatGPTWebCredentialReplacementContextKey{}, false)
	return context.WithValue(ctx, chatGPTWebCredentialRefreshContextKey{}, false)
}

// ChatGPTWebCredentialRefreshed reports whether an auth update was installed
// by the controlled provider refresh path without replacing the account.
func ChatGPTWebCredentialRefreshed(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	refreshed, _ := ctx.Value(chatGPTWebCredentialRefreshContextKey{}).(bool)
	return refreshed
}

// ChatGPTWebCredentialIdentity returns a stable, non-secret identity for a
// native ChatGPT Web credential. Access token rotations for the same account
// produce the same value.
func ChatGPTWebCredentialIdentity(auth *Auth) string {
	if !isNativeChatGPTWebCredentialAuth(auth) {
		return ""
	}
	identity := chatGPTWebStableCredentialIdentity(auth)
	if identity == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(identity))
	return fmt.Sprintf("%x", sum[:])
}

// ChatGPTWebCatalogCredentialKey returns a non-secret key for model catalog
// cache ownership. Opaque credentials fall back to a token-generation hash so
// replacing them cannot reuse another account's catalog.
func ChatGPTWebCatalogCredentialKey(auth *Auth) string {
	if identity := ChatGPTWebCredentialIdentity(auth); identity != "" {
		return "identity:" + identity
	}
	if !isNativeChatGPTWebCredentialAuth(auth) {
		return ""
	}
	parts := []string{
		chatGPTWebAccessCredentialToken(auth),
		chatGPTWebRefreshCredentialToken(auth),
		chatGPTWebIDCredentialToken(auth),
	}
	if parts[0] == "" && parts[1] == "" && parts[2] == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return fmt.Sprintf("credential:%x", sum[:])
}

// ChatGPTWebCredentialIdentityChanged reports whether an update replaces the
// account behind the same native ChatGPT Web auth ID.
func ChatGPTWebCredentialIdentityChanged(existing, next *Auth) bool {
	if !isNativeChatGPTWebCredentialAuth(existing) || !isNativeChatGPTWebCredentialAuth(next) {
		return false
	}
	existingEvidence := collectChatGPTWebCredentialEvidence(existing)
	nextEvidence := collectChatGPTWebCredentialEvidence(next)
	if existingEvidence.conflicting() || nextEvidence.conflicting() {
		return existingEvidence.fingerprint() != nextEvidence.fingerprint()
	}
	existingClaims := chatGPTWebCredentialIdentityClaims(existing)
	nextClaims := chatGPTWebCredentialIdentityClaims(next)
	if existingClaims.accountID != "" && nextClaims.accountID != "" {
		if existingClaims.accountID != nextClaims.accountID {
			return true
		}
		if existingClaims.userID != "" && nextClaims.userID != "" {
			return existingClaims.userID != nextClaims.userID
		}
		if existingClaims.subject != "" && nextClaims.subject != "" {
			return existingClaims.subject != nextClaims.subject
		}
		return false
	}
	existingFallback := chatGPTWebComparableFallbackClaims(existing)
	nextFallback := chatGPTWebComparableFallbackClaims(next)
	if chatGPTWebFallbackClaimsConflict(existingFallback, nextFallback) {
		return true
	}
	existingRefreshToken := chatGPTWebRefreshCredentialToken(existing)
	nextRefreshToken := chatGPTWebRefreshCredentialToken(next)
	if existingRefreshToken != "" && nextRefreshToken != "" {
		if existingRefreshToken != nextRefreshToken {
			return true
		}
	}
	existingIDToken := chatGPTWebIDCredentialToken(existing)
	nextIDToken := chatGPTWebIDCredentialToken(next)
	if chatGPTWebOpaqueIdentityToken(existingIDToken) && chatGPTWebOpaqueIdentityToken(nextIDToken) {
		if existingIDToken != nextIDToken {
			return true
		}
	}
	existingIdentity := ChatGPTWebCredentialIdentity(existing)
	nextIdentity := ChatGPTWebCredentialIdentity(next)
	if existingIdentity != "" && nextIdentity != "" && existingIdentity != nextIdentity {
		return true
	}
	if (existingRefreshToken != "" && existingRefreshToken == nextRefreshToken) ||
		(existingIDToken != "" && existingIDToken == nextIDToken) {
		return false
	}
	if existingIdentity == "" && nextIdentity == "" {
		if existingIDToken != "" && nextIDToken != "" {
			return existingIDToken != nextIDToken
		}
		return ChatGPTWebCatalogCredentialKey(existing) != ChatGPTWebCatalogCredentialKey(next)
	}
	return existingIdentity != nextIdentity
}

// ChatGPTWebCredentialRefreshIdentityChanged reports whether a controlled
// refresh produced explicit account evidence that conflicts with the existing
// credential. Rotated opaque refresh or ID tokens are not account changes.
func ChatGPTWebCredentialRefreshIdentityChanged(existing, next *Auth) bool {
	if !ChatGPTWebCredentialIdentityChanged(existing, next) {
		return false
	}
	existingEvidence := collectChatGPTWebCredentialEvidence(existing)
	nextEvidence := collectChatGPTWebCredentialEvidence(next)
	if existingEvidence.conflicting() || nextEvidence.conflicting() {
		return true
	}
	strongMatch := false
	for _, pair := range [][2][]string{
		{existingEvidence.accountIDs, nextEvidence.accountIDs},
		{existingEvidence.userIDs, nextEvidence.userIDs},
		{existingEvidence.subjects, nextEvidence.subjects},
	} {
		if len(pair[0]) == 0 || len(pair[1]) == 0 {
			continue
		}
		if pair[0][0] != pair[1][0] {
			return true
		}
		strongMatch = true
	}
	if !strongMatch && len(existingEvidence.emails) > 0 && len(nextEvidence.emails) > 0 {
		return existingEvidence.emails[0] != nextEvidence.emails[0]
	}
	return false
}

// ChatGPTWebCredentialHasStrongIdentity reports whether account, user, or
// subject evidence is available. Email alone is insufficient for a linked
// Codex refresh relationship.
func ChatGPTWebCredentialHasStrongIdentity(auth *Auth) bool {
	claims := chatGPTWebCredentialIdentityClaims(auth)
	return claims.accountID != "" || claims.userID != "" || claims.subject != ""
}

func resetChatGPTWebCredentialReplacementState(auth *Auth, now time.Time) {
	if auth == nil {
		return
	}
	auth.Runtime = nil
	auth.Unavailable = false
	auth.Quota = QuotaState{}
	auth.LastError = nil
	auth.NextRefreshAfter = time.Time{}
	auth.NextRetryAfter = time.Time{}
	auth.CooldownScope = ""
	auth.ModelStates = nil
	auth.Status = StatusActive
	auth.StatusMessage = ""
	auth.UpdatedAt = now
	applyLifecycleRuntimeState(auth)
	// Disabled is persisted administrative state, not credential-scoped state.
	if auth.Disabled {
		auth.Status = StatusDisabled
	}
}

func prepareChatGPTWebCredentialReplacement(existing, next *Auth, now time.Time) bool {
	if !ChatGPTWebCredentialIdentityChanged(existing, next) {
		stampChatGPTWebCredentialGeneration(next)
		return false
	}
	next.Index = existing.Index
	next.indexAssigned = existing.indexAssigned
	next.CreatedAt = existing.CreatedAt
	if existing.Disabled || existing.Status == StatusDisabled {
		next.Disabled = true
		if next.Metadata == nil {
			next.Metadata = make(map[string]any)
		}
		next.Metadata["disabled"] = true
	}
	if inheritedChatGPTWebCredentialGeneration(existing, next) {
		clearCarriedChatGPTWebCredentialMetadata(existing, next, now)
	}
	resetChatGPTWebCredentialReplacementState(next, now)
	stampChatGPTWebCredentialGeneration(next)
	return true
}

func inheritedChatGPTWebCredentialGeneration(existing, next *Auth) bool {
	return existing != nil && next != nil &&
		existing.chatGPTWebCredentialGeneration != "" &&
		next.chatGPTWebCredentialGeneration == existing.chatGPTWebCredentialGeneration
}

func stampChatGPTWebCredentialGeneration(auth *Auth) {
	if auth == nil {
		return
	}
	auth.chatGPTWebCredentialGeneration = ChatGPTWebCatalogCredentialKey(auth)
}

func clearCarriedChatGPTWebCredentialMetadata(existing, next *Auth, now time.Time) {
	if existing == nil || next == nil || existing.Metadata == nil || next.Metadata == nil {
		return
	}
	for _, key := range []string{
		"refresh_token",
		"refreshToken",
		"id_token",
		"idToken",
		"expired",
		"cookies",
		"session_id",
		"account_id",
		"chatgpt_account_id",
		"user_id",
		"chatgpt_user_id",
		"sub",
		"last_login_at",
		"last_refresh_at",
		"last_relogin_at",
	} {
		existingValue, existingOK := existing.Metadata[key]
		nextValue, nextOK := next.Metadata[key]
		if existingOK && nextOK && reflect.DeepEqual(existingValue, nextValue) {
			delete(next.Metadata, key)
		}
	}

	existingState, existingStateOK := existing.Metadata["lifecycle_state"]
	nextState, nextStateOK := next.Metadata["lifecycle_state"]
	if !nextStateOK || (existingStateOK && reflect.DeepEqual(existingState, nextState)) {
		next.Metadata["lifecycle_state"] = LifecycleStateActive
		delete(next.Metadata, "lifecycle_reason")
		next.Metadata["lifecycle_updated_at"] = now.UTC().Format(time.RFC3339Nano)
		return
	}
	if existingReason, ok := existing.Metadata["lifecycle_reason"]; ok &&
		reflect.DeepEqual(existingReason, next.Metadata["lifecycle_reason"]) {
		delete(next.Metadata, "lifecycle_reason")
	}
	if existingUpdatedAt, ok := existing.Metadata["lifecycle_updated_at"]; ok &&
		reflect.DeepEqual(existingUpdatedAt, next.Metadata["lifecycle_updated_at"]) {
		delete(next.Metadata, "lifecycle_updated_at")
	}
}

func prepareRefreshedChatGPTWebCredentialReplacement(existing, next *Auth, now time.Time) bool {
	if !ChatGPTWebCredentialRefreshIdentityChanged(existing, next) {
		stampChatGPTWebCredentialGeneration(next)
		return false
	}
	return prepareChatGPTWebCredentialReplacement(existing, next, now)
}

func isNativeChatGPTWebCredentialAuth(auth *Auth) bool {
	if auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), "chatgpt-web") {
		return false
	}
	return auth.Attributes == nil || strings.TrimSpace(auth.Attributes["compat_name"]) == ""
}

func chatGPTWebOpaqueIdentityToken(token string) bool {
	token = strings.TrimSpace(token)
	return token != "" && len(chatGPTWebJWTClaims(token)) == 0
}

func chatGPTWebStableCredentialIdentity(auth *Auth) string {
	if auth == nil {
		return ""
	}
	for _, token := range []string{chatGPTWebAccessCredentialToken(auth), chatGPTWebIDCredentialToken(auth)} {
		if identity := chatGPTWebJWTIdentity(token); identity != "" {
			return identity
		}
	}
	accountID := chatGPTWebIdentityMetadataString(auth.Metadata, "account_id", "chatgpt_account_id")
	userID := chatGPTWebIdentityMetadataString(auth.Metadata, "user_id", "chatgpt_user_id")
	subject := chatGPTWebIdentityMetadataString(auth.Metadata, "sub")
	email := chatGPTWebIdentityMetadataString(auth.Metadata, "email")
	if accountID != "" {
		return chatGPTWebIdentity(accountID, userID, subject, email)
	}
	for _, token := range []string{chatGPTWebAccessCredentialToken(auth), chatGPTWebIDCredentialToken(auth)} {
		if identity := chatGPTWebJWTFallbackIdentity(token); identity != "" {
			return identity
		}
	}
	return chatGPTWebIdentity("", userID, subject, email)
}

type chatGPTWebCredentialClaims struct {
	accountID string
	userID    string
	subject   string
}

type chatGPTWebCredentialEvidence struct {
	accountIDs []string
	userIDs    []string
	subjects   []string
	emails     []string
}

type chatGPTWebFallbackClaims struct {
	userID  string
	subject string
	email   string
}

// ChatGPTWebCredentialReference is a non-secret snapshot used to compare
// credential ownership after optional JWT claims change.
type ChatGPTWebCredentialReference struct {
	accountHash  string
	userHash     string
	subjectHash  string
	identityHash string
	conflicting  bool
}

const chatGPTWebCredentialReferenceVersion = "v1"

// NewChatGPTWebCredentialReference creates a hashed credential reference.
func NewChatGPTWebCredentialReference(auth *Auth) ChatGPTWebCredentialReference {
	claims := chatGPTWebCredentialIdentityClaims(auth)
	evidence := collectChatGPTWebCredentialEvidence(auth)
	return ChatGPTWebCredentialReference{
		accountHash:  chatGPTWebIdentityPartHash("account", claims.accountID),
		userHash:     chatGPTWebIdentityPartHash("user", claims.userID),
		subjectHash:  chatGPTWebIdentityPartHash("subject", claims.subject),
		identityHash: ChatGPTWebCredentialIdentity(auth),
		conflicting:  evidence.conflicting(),
	}
}

// ChatGPTWebCredentialReferenceValue serializes a non-secret identity
// reference that tolerates optional JWT claims appearing or disappearing.
func ChatGPTWebCredentialReferenceValue(auth *Auth) string {
	reference := NewChatGPTWebCredentialReference(auth)
	if reference.Empty() || reference.conflicting {
		return ""
	}
	return strings.Join([]string{
		chatGPTWebCredentialReferenceVersion,
		reference.accountHash,
		reference.userHash,
		reference.subjectHash,
		reference.identityHash,
	}, ":")
}

// ChatGPTWebCredentialReferenceMatches validates a serialized reference.
// Legacy identity hashes remain readable for credentials created by earlier
// builds of this feature.
func ChatGPTWebCredentialReferenceMatches(value string, auth *Auth) bool {
	value = strings.TrimSpace(value)
	if value == "" || auth == nil {
		return false
	}
	parts := strings.Split(value, ":")
	if len(parts) != 5 || parts[0] != chatGPTWebCredentialReferenceVersion {
		return ChatGPTWebCredentialIdentity(auth) == value
	}
	reference := ChatGPTWebCredentialReference{
		accountHash:  parts[1],
		userHash:     parts[2],
		subjectHash:  parts[3],
		identityHash: parts[4],
	}
	return reference.Matches(auth)
}

// MergeChatGPTWebCredentialReferenceValues keeps previously observed identity
// dimensions when a refreshed token omits optional claims.
func MergeChatGPTWebCredentialReferenceValues(existing, incoming string) string {
	existing = strings.TrimSpace(existing)
	incoming = strings.TrimSpace(incoming)
	if existing == "" {
		return incoming
	}
	if incoming == "" {
		return existing
	}
	currentParts := strings.Split(incoming, ":")
	previousParts := strings.Split(existing, ":")
	if len(currentParts) != 5 || currentParts[0] != chatGPTWebCredentialReferenceVersion ||
		len(previousParts) != 5 || previousParts[0] != chatGPTWebCredentialReferenceVersion {
		return incoming
	}
	for index := 1; index < len(currentParts); index++ {
		if currentParts[index] == "" {
			currentParts[index] = previousParts[index]
		}
	}
	return strings.Join(currentParts, ":")
}

// Empty reports whether the reference contains no comparable identity.
func (reference ChatGPTWebCredentialReference) Empty() bool {
	return reference.accountHash == "" && reference.userHash == "" &&
		reference.subjectHash == "" && reference.identityHash == ""
}

// Matches reports whether auth can safely reuse state owned by the reference.
func (reference ChatGPTWebCredentialReference) Matches(auth *Auth) bool {
	current := NewChatGPTWebCredentialReference(auth)
	if reference.conflicting || current.conflicting {
		return false
	}
	if reference.accountHash != "" && current.accountHash != "" {
		if reference.accountHash != current.accountHash {
			return false
		}
		if reference.userHash != "" && current.userHash != "" {
			return reference.userHash == current.userHash
		}
		if reference.subjectHash != "" && current.subjectHash != "" {
			return reference.subjectHash == current.subjectHash
		}
		return true
	}
	if reference.userHash != "" && current.userHash != "" {
		return reference.userHash == current.userHash
	}
	if reference.subjectHash != "" && current.subjectHash != "" {
		return reference.subjectHash == current.subjectHash
	}
	return reference.identityHash != "" && reference.identityHash == current.identityHash
}

func chatGPTWebIdentityPartHash(kind, value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(kind + "\x00" + value))
	return fmt.Sprintf("%x", sum[:])
}

func chatGPTWebCredentialIdentityClaims(auth *Auth) chatGPTWebCredentialClaims {
	if auth == nil {
		return chatGPTWebCredentialClaims{}
	}
	for _, token := range []string{chatGPTWebAccessCredentialToken(auth), chatGPTWebIDCredentialToken(auth)} {
		claims := chatGPTWebJWTClaims(token)
		if len(claims) == 0 {
			continue
		}
		authClaims, _ := claims["https://api.openai.com/auth"].(map[string]any)
		accountID := chatGPTWebIdentityMetadataString(authClaims, "chatgpt_account_id", "account_id")
		if accountID == "" {
			accountID = chatGPTWebIdentityMetadataString(claims, "chatgpt_account_id", "account_id")
		}
		if accountID == "" {
			continue
		}
		userID := chatGPTWebIdentityMetadataString(authClaims, "chatgpt_user_id", "user_id")
		if userID == "" {
			userID = chatGPTWebIdentityMetadataString(claims, "chatgpt_user_id", "user_id")
		}
		return chatGPTWebCredentialClaims{
			accountID: accountID,
			userID:    userID,
			subject:   chatGPTWebIdentityMetadataString(claims, "sub"),
		}
	}
	return chatGPTWebCredentialClaims{
		accountID: chatGPTWebIdentityMetadataString(auth.Metadata, "account_id", "chatgpt_account_id"),
		userID:    chatGPTWebIdentityMetadataString(auth.Metadata, "user_id", "chatgpt_user_id"),
		subject:   chatGPTWebIdentityMetadataString(auth.Metadata, "sub"),
	}
}

func collectChatGPTWebCredentialEvidence(auth *Auth) chatGPTWebCredentialEvidence {
	if auth == nil {
		return chatGPTWebCredentialEvidence{}
	}
	evidence := chatGPTWebCredentialEvidence{}
	evidence.add(
		chatGPTWebIdentityMetadataString(auth.Metadata, "account_id"),
		chatGPTWebIdentityMetadataString(auth.Metadata, "user_id"),
		chatGPTWebIdentityMetadataString(auth.Metadata, "sub"),
		chatGPTWebIdentityMetadataString(auth.Metadata, "email"),
	)
	evidence.add(
		chatGPTWebIdentityMetadataString(auth.Metadata, "chatgpt_account_id"),
		chatGPTWebIdentityMetadataString(auth.Metadata, "chatgpt_user_id"),
		"",
		"",
	)
	for _, tokenKey := range []string{"access_token", "accessToken", "id_token", "idToken"} {
		claims := chatGPTWebJWTClaims(chatGPTWebIdentityMetadataString(auth.Metadata, tokenKey))
		if len(claims) == 0 {
			continue
		}
		authClaims, _ := claims["https://api.openai.com/auth"].(map[string]any)
		evidence.add(
			chatGPTWebIdentityMetadataString(authClaims, "chatgpt_account_id"),
			chatGPTWebIdentityMetadataString(authClaims, "chatgpt_user_id"),
			chatGPTWebIdentityMetadataString(claims, "sub"),
			chatGPTWebIdentityMetadataString(claims, "email"),
		)
		evidence.add(
			chatGPTWebIdentityMetadataString(authClaims, "account_id"),
			chatGPTWebIdentityMetadataString(authClaims, "user_id"),
			"",
			"",
		)
		evidence.add(
			chatGPTWebIdentityMetadataString(claims, "chatgpt_account_id"),
			chatGPTWebIdentityMetadataString(claims, "chatgpt_user_id"),
			"",
			"",
		)
		evidence.add(
			chatGPTWebIdentityMetadataString(claims, "account_id"),
			chatGPTWebIdentityMetadataString(claims, "user_id"),
			"",
			"",
		)
	}
	sort.Strings(evidence.accountIDs)
	sort.Strings(evidence.userIDs)
	sort.Strings(evidence.subjects)
	sort.Strings(evidence.emails)
	return evidence
}

func (evidence *chatGPTWebCredentialEvidence) add(accountID, userID, subject, email string) {
	evidence.accountIDs = appendChatGPTWebIdentityEvidence(evidence.accountIDs, accountID, false)
	evidence.userIDs = appendChatGPTWebIdentityEvidence(evidence.userIDs, userID, false)
	evidence.subjects = appendChatGPTWebIdentityEvidence(evidence.subjects, subject, false)
	evidence.emails = appendChatGPTWebIdentityEvidence(evidence.emails, email, true)
}

func appendChatGPTWebIdentityEvidence(values []string, value string, foldCase bool) []string {
	value = strings.TrimSpace(value)
	if foldCase {
		value = strings.ToLower(value)
	}
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func (evidence chatGPTWebCredentialEvidence) conflicting() bool {
	if len(evidence.accountIDs) > 1 || len(evidence.userIDs) > 1 || len(evidence.subjects) > 1 {
		return true
	}
	return len(evidence.accountIDs) == 0 && len(evidence.emails) > 1
}

func (evidence chatGPTWebCredentialEvidence) fingerprint() string {
	return strings.Join([]string{
		"account:" + strings.Join(evidence.accountIDs, "\x00"),
		"user:" + strings.Join(evidence.userIDs, "\x00"),
		"subject:" + strings.Join(evidence.subjects, "\x00"),
		"email:" + strings.Join(evidence.emails, "\x00"),
	}, "\x01")
}

func chatGPTWebComparableFallbackClaims(auth *Auth) chatGPTWebFallbackClaims {
	if auth == nil {
		return chatGPTWebFallbackClaims{}
	}
	result := chatGPTWebFallbackClaims{
		userID:  chatGPTWebIdentityMetadataString(auth.Metadata, "user_id", "chatgpt_user_id"),
		subject: chatGPTWebIdentityMetadataString(auth.Metadata, "sub"),
		email:   strings.ToLower(chatGPTWebIdentityMetadataString(auth.Metadata, "email")),
	}
	for _, token := range []string{chatGPTWebAccessCredentialToken(auth), chatGPTWebIDCredentialToken(auth)} {
		claims := chatGPTWebJWTClaims(token)
		if len(claims) == 0 {
			continue
		}
		authClaims, _ := claims["https://api.openai.com/auth"].(map[string]any)
		if result.userID == "" {
			result.userID = chatGPTWebIdentityMetadataString(authClaims, "chatgpt_user_id", "user_id")
			if result.userID == "" {
				result.userID = chatGPTWebIdentityMetadataString(claims, "chatgpt_user_id", "user_id")
			}
		}
		if result.subject == "" {
			result.subject = chatGPTWebIdentityMetadataString(claims, "sub")
		}
		if result.email == "" {
			result.email = strings.ToLower(chatGPTWebIdentityMetadataString(claims, "email"))
		}
	}
	return result
}

func chatGPTWebFallbackClaimsConflict(existing, next chatGPTWebFallbackClaims) bool {
	return (existing.userID != "" && next.userID != "" && existing.userID != next.userID) ||
		(existing.subject != "" && next.subject != "" && existing.subject != next.subject) ||
		(existing.email != "" && next.email != "" && existing.email != next.email)
}

func chatGPTWebJWTIdentity(token string) string {
	claims := chatGPTWebJWTClaims(token)
	if len(claims) == 0 {
		return ""
	}
	authClaims, _ := claims["https://api.openai.com/auth"].(map[string]any)
	accountID := chatGPTWebIdentityMetadataString(authClaims, "chatgpt_account_id", "account_id")
	if accountID == "" {
		accountID = chatGPTWebIdentityMetadataString(claims, "chatgpt_account_id", "account_id")
	}
	userID := chatGPTWebIdentityMetadataString(authClaims, "chatgpt_user_id", "user_id")
	if userID == "" {
		userID = chatGPTWebIdentityMetadataString(claims, "chatgpt_user_id", "user_id")
	}
	if accountID == "" {
		return ""
	}
	return chatGPTWebIdentity(
		accountID,
		userID,
		chatGPTWebIdentityMetadataString(claims, "sub"),
		chatGPTWebIdentityMetadataString(claims, "email"),
	)
}

func chatGPTWebJWTFallbackIdentity(token string) string {
	claims := chatGPTWebJWTClaims(token)
	if len(claims) == 0 {
		return ""
	}
	authClaims, _ := claims["https://api.openai.com/auth"].(map[string]any)
	userID := chatGPTWebIdentityMetadataString(authClaims, "chatgpt_user_id", "user_id")
	if userID == "" {
		userID = chatGPTWebIdentityMetadataString(claims, "chatgpt_user_id", "user_id")
	}
	return chatGPTWebIdentity(
		"",
		userID,
		chatGPTWebIdentityMetadataString(claims, "sub"),
		chatGPTWebIdentityMetadataString(claims, "email"),
	)
}

func chatGPTWebIdentity(accountID, userID, subject, email string) string {
	switch {
	case accountID != "" && userID != "":
		return "account:" + accountID + "\x00user:" + userID
	case accountID != "" && subject != "":
		return "account:" + accountID + "\x00subject:" + subject
	case accountID != "" && email != "":
		return "account:" + accountID + "\x00email:" + strings.ToLower(email)
	case accountID != "":
		return "account:" + accountID
	case userID != "":
		return "user:" + userID
	case subject != "":
		return "subject:" + subject
	case email != "":
		return "email:" + strings.ToLower(email)
	default:
		return ""
	}
}

func chatGPTWebIdentityMetadataString(metadata map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := metadata[key].(string); ok {
			if value = strings.TrimSpace(value); value != "" {
				return value
			}
		}
	}
	return ""
}

func chatGPTWebAccessCredentialToken(auth *Auth) string {
	if auth == nil {
		return ""
	}
	return chatGPTWebIdentityMetadataString(auth.Metadata, "access_token", "accessToken")
}

func chatGPTWebRefreshCredentialToken(auth *Auth) string {
	if auth == nil {
		return ""
	}
	return chatGPTWebIdentityMetadataString(auth.Metadata, "refresh_token", "refreshToken")
}

func chatGPTWebIDCredentialToken(auth *Auth) string {
	if auth == nil {
		return ""
	}
	return chatGPTWebIdentityMetadataString(auth.Metadata, "id_token", "idToken")
}

func chatGPTWebJWTClaims(token string) map[string]any {
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) != 3 {
		return nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil
	}
	var claims map[string]any
	if err = json.Unmarshal(payload, &claims); err != nil {
		return nil
	}
	return claims
}

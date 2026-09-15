package management

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	xaiauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/xai"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

const xaiPKCETTL = 10 * time.Minute

type xaiPKCEPending struct {
	flow     *xaiauth.PKCEFlow
	callback chan oauthCallbackFilePayload
	consumed bool
}

func (h *Handler) requestXAIPKCE(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Grok callback listener unavailable"})
		return
	}
	owned := false
	defer func() {
		if !owned {
			if errClose := listener.Close(); errClose != nil {
				log.WithError(errClose).Warn("close Grok callback listener")
			}
		}
	}()
	authSvc := xaiauth.NewXAIAuth(cfg)
	flow, err := authSvc.StartPKCE(c.Request.Context(), "http://"+listener.Addr().String()+"/callback")
	if err != nil {
		log.WithError(err).Warn("start Grok PKCE login")
		c.JSON(http.StatusBadGateway, gin.H{"error": "failed to start Grok PKCE login"})
		return
	}
	pending := &xaiPKCEPending{flow: flow, callback: make(chan oauthCallbackFilePayload, 1)}
	h.xaiPKCEMu.Lock()
	if len(h.xaiPKCE) >= 32 {
		h.xaiPKCEMu.Unlock()
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "too many pending Grok logins"})
		return
	}
	if h.xaiPKCE == nil {
		h.xaiPKCE = make(map[string]*xaiPKCEPending)
	}
	h.xaiPKCE[flow.State] = pending
	h.xaiPKCEMu.Unlock()
	RegisterOAuthSession(flow.State, "xai")
	ctx, cancel := context.WithTimeout(PopulateAuthContext(context.Background(), c), xaiPKCETTL)
	mux := http.NewServeMux()
	mux.HandleFunc("/callback", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if r.Method != http.MethodGet || r.URL.Query().Get("state") != flow.State {
			http.Error(w, "Invalid OAuth callback", http.StatusBadRequest)
			return
		}
		errSubmit := h.submitXAIPKCECallback(flow.State, r.URL.Query().Get("code"), r.URL.Query().Get("error"), "")
		if errSubmit != nil {
			http.Error(w, errSubmit.Error(), http.StatusConflict)
			return
		}
		_, _ = w.Write([]byte("Callback received. Return to the management page to check login status."))
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	owned = true
	go func() {
		if errServe := server.Serve(listener); errServe != nil && !errors.Is(errServe, http.ErrServerClosed) {
			SetOAuthSessionError(flow.State, "Grok callback listener stopped")
			cancel()
		}
	}()
	go func() {
		defer cancel()
		defer func() {
			if errClose := server.Close(); errClose != nil {
				log.WithError(errClose).Warn("close Grok callback server")
			}
		}()
		defer func() { h.xaiPKCEMu.Lock(); delete(h.xaiPKCE, flow.State); h.xaiPKCEMu.Unlock() }()
		go watchOAuthSessionCancel(ctx, cancel, flow.State, "xai")
		select {
		case <-ctx.Done():
			SetOAuthSessionError(flow.State, "Grok login expired or cancelled")
			return
		case callback := <-pending.callback:
			if callback.Error != "" {
				SetOAuthSessionError(flow.State, "Grok authorization was denied")
				return
			}
			bundle, errExchange := authSvc.ExchangePKCE(ctx, flow, callback.Code)
			if errExchange != nil {
				log.WithError(errExchange).Warn("Grok PKCE exchange failed")
				SetOAuthSessionError(flow.State, "Grok token exchange failed")
				return
			}
			if IsOAuthSessionPending(flow.State, "xai") {
				h.saveXAIAuthBundle(ctx, flow.State, authSvc, bundle)
			}
		}
	}()
	c.JSON(http.StatusOK, gin.H{"status": "ok", "url": flow.URL, "state": flow.State, "flow": "pkce", "redirect_uri": flow.RedirectURI, "expires_in": int(xaiPKCETTL / time.Second)})
}

// submitXAIPKCECallback binds manual and loopback callbacks to the same flow.
func (h *Handler) submitXAIPKCECallback(state, code, errorMessage, rawRedirect string) error {
	h.xaiPKCEMu.Lock()
	defer h.xaiPKCEMu.Unlock()
	pending := h.xaiPKCE[state]
	if pending == nil || pending.consumed || !IsOAuthSessionPending(state, "xai") {
		return fmt.Errorf("Grok PKCE callback is not pending")
	}
	if code == "" && errorMessage == "" {
		return fmt.Errorf("code or error is required")
	}
	if rawRedirect != "" {
		u, err := url.Parse(rawRedirect)
		expected, errExpected := url.Parse(pending.flow.RedirectURI)
		if err != nil || errExpected != nil || u.Scheme != expected.Scheme || u.Host != expected.Host || u.Path != expected.Path || u.User != nil || u.Fragment != "" || u.Query().Get("state") != state || u.Query().Get("code") != code {
			return fmt.Errorf("callback URL does not match the Grok login session")
		}
	}
	pending.consumed = true
	pending.callback <- oauthCallbackFilePayload{State: state, Code: code, Error: errorMessage}
	return nil
}

func (h *Handler) saveXAIAuthBundle(ctx context.Context, state string, authSvc *xaiauth.XAIAuth, bundle *xaiauth.AuthBundle) {
	tokenStorage := authSvc.CreateTokenStorage(bundle)
	if tokenStorage == nil || strings.TrimSpace(tokenStorage.AccessToken) == "" {
		log.Error("xAI token exchange returned empty access token")
		SetOAuthSessionError(state, "Failed to exchange token")
		return
	}

	fileName := xaiauth.CredentialFileName(tokenStorage.Email, tokenStorage.Subject)
	label := strings.TrimSpace(tokenStorage.Email)
	if label == "" {
		label = "xAI"
	}

	metadata := map[string]any{
		"type":           "xai",
		"access_token":   tokenStorage.AccessToken,
		"refresh_token":  tokenStorage.RefreshToken,
		"id_token":       tokenStorage.IDToken,
		"token_type":     tokenStorage.TokenType,
		"expires_in":     tokenStorage.ExpiresIn,
		"expired":        tokenStorage.Expire,
		"last_refresh":   tokenStorage.LastRefresh,
		"base_url":       tokenStorage.BaseURL,
		"token_endpoint": tokenStorage.TokenEndpoint,
		"auth_kind":      "oauth",
		"using_api":      false,
		"websockets":     false,
	}
	if tokenStorage.Email != "" {
		metadata["email"] = tokenStorage.Email
	}
	if tokenStorage.Subject != "" {
		metadata["sub"] = tokenStorage.Subject
	}

	record := &coreauth.Auth{
		ID:       fileName,
		Provider: "xai",
		FileName: fileName,
		Label:    label,
		Storage:  tokenStorage,
		Metadata: metadata,
		Attributes: map[string]string{
			"auth_kind":  "oauth",
			"base_url":   tokenStorage.BaseURL,
			"using_api":  "false",
			"websockets": "false",
		},
	}
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	existing := h.findManagedAuthWithManager(fileName, manager)
	replacesAccount := false
	if existing != nil && !strings.EqualFold(existing.Provider, "xai") {
		SetOAuthSessionError(state, "Credential filename belongs to another provider")
		return
	}
	if existing != nil && strings.EqualFold(existing.Provider, "xai") {
		merged := existing.Clone()
		if merged.Metadata == nil {
			merged.Metadata = make(map[string]any)
		}
		for key, value := range metadata {
			if key == "base_url" || key == "using_api" || key == "websockets" {
				if _, ok := merged.Metadata[key]; ok {
					continue
				}
			}
			merged.Metadata[key] = value
		}
		merged.Metadata["redirect_uri"] = tokenStorage.RedirectURI
		delete(merged.Metadata, "api_key")
		delete(merged.Metadata, "api-key")
		previousSubject, _ := existing.Metadata["sub"].(string)
		if previousSubject != "" && tokenStorage.Subject != "" && previousSubject != tokenStorage.Subject {
			replacesAccount = true
			delete(merged.Metadata, helps.XAIIdentitySeedKey)
			delete(merged.Metadata, helps.XAIModelCatalogKey)
			delete(merged.Metadata, helps.XAIModelCatalogsKey)
		}
		merged.Storage = tokenStorage
		if merged.Attributes == nil {
			merged.Attributes = make(map[string]string)
		}
		merged.Attributes["auth_kind"] = "oauth"
		delete(merged.Attributes, "api_key")
		record = merged
	}
	if errGuard := beginOAuthSessionSave(state, "xai"); errGuard != nil {
		return
	}
	if existing != nil {
		if replacesAccount {
			ctx = coreauth.WithForceRuntimeReplacement(ctx)
		}
		_, installed, errUpdate := manager.UpdateIfCurrent(ctx, existing, record)
		if errUpdate != nil || !installed {
			SetOAuthSessionError(state, "Credential changed during login or could not be saved; retry login")
			return
		}
		CompleteOAuthSession(state)
		log.Info("Grok login updated the existing credential")
		return
	}
	savedPath, errSave := h.saveTokenRecord(ctx, record)
	if errSave != nil {
		log.Errorf("Failed to save xAI token to file: %v", errSave)
		SetOAuthSessionError(state, "Failed to save token to file")
		return
	}

	CompleteOAuthSession(state)
	fmt.Printf("Authentication successful! Token saved to %s\n", savedPath)
	fmt.Println("You can now use xAI services through this CLI")
}

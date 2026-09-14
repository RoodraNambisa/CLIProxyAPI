package xai

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// PKCEFlow is request-owned login state. Never serialize the verifier to clients.
type PKCEFlow struct {
	State         string
	Verifier      string
	RedirectURI   string
	TokenEndpoint string
	URL           string
}

func (a *XAIAuth) StartPKCE(ctx context.Context, redirectURI string) (*PKCEFlow, error) {
	u, err := url.Parse(redirectURI)
	if err != nil || u.Scheme != "http" || u.Hostname() != "127.0.0.1" || u.Port() == "" || u.Path != "/callback" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return nil, fmt.Errorf("Grok PKCE requires a loopback /callback redirect")
	}
	discovery, err := a.Discover(ctx)
	if err != nil {
		return nil, err
	}
	validator := a.validateOAuthEndpoint
	if validator == nil {
		validator = ValidateOAuthEndpoint
	}
	authorization, err := validator(discovery.AuthorizationEndpoint, "authorization_endpoint")
	if err != nil {
		return nil, err
	}
	state := make([]byte, 32)
	verifier := make([]byte, 32)
	if _, err = rand.Read(state); err != nil {
		return nil, err
	}
	if _, err = rand.Read(verifier); err != nil {
		return nil, err
	}
	flow := &PKCEFlow{State: base64.RawURLEncoding.EncodeToString(state), Verifier: base64.RawURLEncoding.EncodeToString(verifier), RedirectURI: redirectURI, TokenEndpoint: discovery.TokenEndpoint}
	challenge := sha256.Sum256([]byte(flow.Verifier))
	endpoint, err := url.Parse(authorization)
	if err != nil {
		return nil, err
	}
	q := endpoint.Query()
	q.Set("client_id", ClientID)
	q.Set("scope", Scope)
	q.Set("response_type", "code")
	q.Set("redirect_uri", redirectURI)
	q.Set("state", flow.State)
	q.Set("code_challenge_method", "S256")
	q.Set("code_challenge", base64.RawURLEncoding.EncodeToString(challenge[:]))
	endpoint.RawQuery = q.Encode()
	flow.URL = endpoint.String()
	return flow, nil
}

func (a *XAIAuth) ExchangePKCE(ctx context.Context, flow *PKCEFlow, code string) (*AuthBundle, error) {
	if flow == nil || flow.Verifier == "" || strings.TrimSpace(code) == "" {
		return nil, fmt.Errorf("invalid Grok PKCE exchange")
	}
	validator := a.validateOAuthEndpoint
	if validator == nil {
		validator = ValidateOAuthEndpoint
	}
	endpoint, err := validator(flow.TokenEndpoint, "token_endpoint")
	if err != nil {
		return nil, err
	}
	token, err := a.postTokenForm(ctx, endpoint, url.Values{"grant_type": {"authorization_code"}, "client_id": {ClientID}, "code": {code}, "code_verifier": {flow.Verifier}, "redirect_uri": {flow.RedirectURI}})
	if err != nil {
		return nil, err
	}
	return &AuthBundle{TokenData: *token, LastRefresh: time.Now().UTC().Format(time.RFC3339), BaseURL: "", RedirectURI: flow.RedirectURI, TokenEndpoint: endpoint}, nil
}

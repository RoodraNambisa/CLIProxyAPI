package chatgptweb

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestAccessTokenExpiryGuardBlocksBeforeHTTPAndAccounting(t *testing.T) {
	var requests, accounting atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client, err := NewAccessTokenClient(Persona{}, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	now := time.Now()
	client.SetAccessTokenExpiry("expired", now, func() time.Time { return now })
	client.SetBeforeRequestHook(func() { accounting.Add(1) })
	cloned, err := client.CloneWithProxy("")
	if err != nil {
		t.Fatal(err)
	}
	defer cloned.CloseIdleConnections()
	attempt, err := client.NewBootstrapAttempt(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer attempt.CloseIdleConnections()
	for _, candidate := range []*Client{client, cloned, attempt} {
		candidate.SetBeforeRequestHook(func() { accounting.Add(1) })
		_, _, err = candidate.DoNoRedirect(t.Context(), http.MethodGet, server.URL, map[string]string{"authorization": "Bearer expired"}, nil)
		var expired *AccessTokenExpiredError
		if !errors.As(err, &expired) || !expired.SkipAuthResult() || !expired.RetryOtherAuth() || expired.StatusCode() == 401 {
			t.Fatalf("not a local retryable rejection: %v", err)
		}
	}
	if requests.Load() != 0 || accounting.Load() != 0 {
		t.Fatal("expired token reached HTTP or charged a request")
	}
	// Anonymous/Session requests and a different bearer are not this token.
	for _, headers := range []map[string]string{nil, {"Authorization": "Bearer another-token"}} {
		if _, _, err = client.DoNoRedirect(t.Context(), http.MethodGet, server.URL, headers, nil); err != nil {
			t.Fatal(err)
		}
	}
	if requests.Load() != 2 || accounting.Load() != 1 {
		t.Fatal("guard interfered with unrelated authentication or first-request accounting")
	}
}

func TestAccessTokenExpiryGuardChecksRedirectAndLaterRequests(t *testing.T) {
	var elapsed atomic.Bool
	var requests atomic.Int32
	now := time.Now()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		elapsed.Store(true)
		http.Redirect(w, r, "/next", http.StatusFound)
	}))
	defer server.Close()
	client, err := NewAccessTokenClient(Persona{}, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	client.SetAccessTokenExpiry("token", now.Add(time.Second), func() time.Time {
		if elapsed.Load() {
			return now.Add(time.Second)
		}
		return now
	})
	headers := map[string]string{"authorization": "Bearer token"}
	_, _, err = client.DoFollow(t.Context(), http.MethodGet, server.URL, headers, nil)
	var expired *AccessTokenExpiredError
	if !errors.As(err, &expired) || requests.Load() != 1 {
		t.Fatalf("redirect sent an expired token: count=%d err=%v", requests.Load(), err)
	}
	_, _, err = client.DoNoRedirect(t.Context(), http.MethodGet, server.URL, headers, nil)
	if !errors.As(err, &expired) || requests.Load() != 1 {
		t.Fatalf("later request sent an expired token: count=%d err=%v", requests.Load(), err)
	}
}

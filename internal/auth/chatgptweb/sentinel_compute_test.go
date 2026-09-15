package chatgptweb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelcompat"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelconfig"
)

func computeTestInput() SentinelComputeInput {
	persona := canonicalPersona(DefaultPersona())
	return SentinelComputeInput{Format: "conversation", Environment: SentinelComputeEnvironment{Persona: persona, BrowserEnvironment: browserEnvironmentIdentityForSeed(persona, "compute-test"), DeviceID: "compute-test", PageStartedAt: time.Now(), Location: "https://chatgpt.com/", ScriptSources: []string{sentinelSDKURL}}, Flow: "conversation", Clock: time.Now(), SDKURL: sentinelSDKURL, SDKSHA256: sentinelSDKSHA256}
}
func computeTestServer(t testing.TB) (*SentinelComputeServer, *httptest.Server) {
	t.Helper()
	server, err := NewSentinelComputeServer(sentinelconfig.Server{Enabled: true, APIKeys: []string{"key-a", "key-b"}})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() { server.Close(); httpServer.Close() })
	return server, httpServer
}
func computeTestPool(t testing.TB, nodes ...sentinelconfig.Node) *SentinelComputePool {
	t.Helper()
	pool := NewSentinelComputePool()
	if err := pool.UpdateConfig("remote", sentinelconfig.Remote{Nodes: nodes}, SentinelRuntimeConfig{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

type computeDeadlineRecorder struct {
	*httptest.ResponseRecorder
	deadlines []time.Time
}

func (w *computeDeadlineRecorder) SetReadDeadline(deadline time.Time) error {
	w.deadlines = append(w.deadlines, deadline)
	return nil
}

func TestSentinelComputeClearsRPCReadDeadline(t *testing.T) {
	node, err := NewSentinelComputeServer(sentinelconfig.Server{Enabled: true, APIKeys: []string{"key"}})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	w := &computeDeadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	r := httptest.NewRequest("POST", "/v1/sentinel/sessions", bytes.NewBufferString(`{}`))
	r.Header.Set("Authorization", "Bearer key")
	node.ServeHTTP(w, r)
	if len(w.deadlines) != 2 || w.deadlines[0].IsZero() || !w.deadlines[1].IsZero() {
		t.Fatalf("RPC leaked read deadline: %v", w.deadlines)
	}
}

func TestSentinelComputeRemoteRoundAndScope(t *testing.T) {
	server, endpoint := computeTestServer(t)
	pool := computeTestPool(t, sentinelconfig.Node{Name: "one", URL: endpoint.URL, APIKey: "key-a"})
	if !pool.Enabled("images") || pool.Enabled("chat") || pool.Enabled("login") {
		t.Fatal("default scopes changed")
	}
	session, err := pool.Begin(context.Background(), "images", computeTestInput(), SentinelComputeHooks{})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	p := session.RequirementsToken()
	if p == "" {
		t.Fatal("missing p")
	}
	result, err := session.Solve(context.Background(), SentinelComputeChallenge{})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequirementsToken != p || result.Engine != "go" {
		t.Fatal("result changed identity")
	}
	if _, err = session.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if server.Snapshot().GoSuccess != 3 || len(pool.Snapshot()) != 1 {
		t.Fatal("missing separate metrics")
	}
}

func TestSentinelComputeObserverStateAcrossRPCs(t *testing.T) {
	_, endpoint := computeTestServer(t)
	pool := computeTestPool(t, sentinelconfig.Node{Name: "one", URL: endpoint.URL, APIKey: "key-a"})
	session, err := pool.Begin(t.Context(), "images", computeTestInput(), SentinelComputeHooks{})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	p := session.RequirementsToken()
	challenge := SentinelComputeChallenge{ObserverRequired: true, CollectorDX: encodeConversationTurnstileProgram(t, p, []any{[]any{2, 40, "collector-state"}}), SnapshotDX: encodeConversationTurnstileProgram(t, p, []any{[]any{7, 3, 40}})}
	if _, err = session.Solve(t.Context(), challenge); err != nil {
		t.Fatal(err)
	}
	result, err := session.Snapshot(t.Context())
	if err != nil || result != "Y29sbGVjdG9yLXN0YXRl" {
		t.Fatalf("observer state lost: %q %v", result, err)
	}
}

func TestSentinelComputeAuthFlow(t *testing.T) {
	_, endpoint := computeTestServer(t)
	pool := computeTestPool(t, sentinelconfig.Node{Name: "one", URL: endpoint.URL, APIKey: "key-a"})
	scopes := []string{"login"}
	if err := pool.UpdateConfig("remote", sentinelconfig.Remote{Scopes: &scopes, Nodes: []sentinelconfig.Node{{Name: "one", URL: endpoint.URL, APIKey: "key-a"}}}, SentinelRuntimeConfig{}); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	official := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.URL.Path != "/backend-api/sentinel/req" {
			t.Error("unexpected official request")
		}
		calls.Add(1)
		_, _ = io.WriteString(w, `{"token":"challenge","proofofwork":{"required":true,"seed":"fixture","difficulty":"ffffffff"}}`)
	}))
	defer official.Close()
	client, err := NewClient(DefaultPersona(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	sentinel, err := NewSentinel(client, official.URL, official.URL, "test-device", zeroReader{}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithSentinelComputePool(t.Context(), pool)
	for _, flow := range []string{"authorize_continue", "password_verify", "email_otp_validate"} {
		token, err := sentinel.Token(ctx, flow)
		if err != nil {
			t.Fatal(err)
		}
		var decoded map[string]string
		if json.Unmarshal([]byte(token), &decoded) != nil || decoded["flow"] != flow || decoded["c"] != "challenge" {
			t.Fatal("auth envelope changed")
		}
	}
	if calls.Load() != 3 {
		t.Fatal("authentication replayed")
	}
}

func TestSentinelComputeSnapshotFallbackPreservesCompletedProofs(t *testing.T) {
	_, endpoint := computeTestServer(t)
	pool := computeTestPool(t, sentinelconfig.Node{Name: "one", URL: endpoint.URL, APIKey: "key-a"})
	pool.mu.Lock()
	pool.local.manager.Close()
	pool.local.sdk = true
	pool.local.manager = newSentinelRuntimeTestManager()
	pool.mu.Unlock()
	input := computeTestInput()
	input.SDKURL = "https://sentinel.openai.com/sentinel/20260721/sdk.js"
	input.SDKSHA256 = fmt.Sprintf("%x", sha256.Sum256([]byte(sentinelRuntimeFailingTurnstileSDK)))
	fetcher := func(context.Context, string, int64) ([]byte, string, string, error) {
		return []byte(sentinelRuntimeFailingTurnstileSDK), "application/javascript", input.SDKURL, nil
	}
	session, err := pool.Begin(t.Context(), "images", input, SentinelComputeHooks{Fetcher: fetcher})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	p := session.RequirementsToken()
	challenge := SentinelComputeChallenge{ObserverRequired: true, CollectorDX: encodeConversationTurnstileProgram(t, p, []any{[]any{2, 40, "state"}}), SnapshotDX: encodeConversationTurnstileProgram(t, p, []any{[]any{36, "unsupported"}}), TurnstileRequired: true, TurnstileDX: encodeConversationTurnstileProgram(t, p, []any{[]any{2, 40, "go-turnstile"}, []any{7, 3, 40}})}
	result, err := session.Solve(t.Context(), challenge)
	if err != nil || result.TurnstileToken == "" {
		t.Fatal(err)
	}
	snapshot, err := session.Snapshot(t.Context())
	if err != nil || snapshot != "sdk-snapshot-token" {
		t.Fatalf("snapshot fallback: %q %v", snapshot, err)
	}
	if session.result.TurnstileToken != result.TurnstileToken || session.RequirementsToken() != p {
		t.Fatal("completed result replaced")
	}
}

func TestSentinelComputeExhaustedRPCBudgetFallsBackWithLiveParent(t *testing.T) {
	_, endpoint := computeTestServer(t)
	pool := computeTestPool(t, sentinelconfig.Node{Name: "one", URL: endpoint.URL, APIKey: "key-a"})
	pool.mu.Lock()
	pool.local.sdk = true
	pool.mu.Unlock()
	session, err := pool.Begin(t.Context(), "images", computeTestInput(), SentinelComputeHooks{})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	session.remaining = 0
	p := session.RequirementsToken()
	result, err := session.Solve(t.Context(), SentinelComputeChallenge{})
	if err != nil || session.local == nil || result.RequirementsToken != p {
		t.Fatalf("budget fallback: %v", err)
	}
}

func TestSentinelComputeResourceDeclarations(t *testing.T) {
	input := computeTestInput()
	input.SDKURL = "/backend-api/sentinel/20260219f9f6/sdk.js"
	input.SDKSHA256 = "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
	normalized, err := input.normalized()
	if err != nil || normalized.SDKURL != "https://chatgpt.com/backend-api/sentinel/20260219f9f6/sdk.js" || len(normalized.SDKSHA256) != 64 {
		t.Fatalf("SRI/relative source rejected: %v", err)
	}
}

func TestSentinelComputeSDKFallbackAtCallerAndNode(t *testing.T) {
	for _, nodeFallback := range []bool{false, true} {
		t.Run(fmt.Sprint(nodeFallback), func(t *testing.T) {
			server, endpoint := computeTestServer(t)
			server.mu.Lock()
			server.current.manager.Close()
			server.current.sdk = nodeFallback
			server.current.manager = newSentinelRuntimeTestManager()
			server.mu.Unlock()
			pool := computeTestPool(t, sentinelconfig.Node{Name: "one", URL: endpoint.URL, APIKey: "key-a"})
			pool.mu.Lock()
			pool.local.manager.Close()
			pool.local.sdk = true
			pool.local.manager = newSentinelRuntimeTestManager()
			pool.mu.Unlock()
			input := computeTestInput()
			input.SDKURL = "https://sentinel.openai.com/sentinel/20260721/sdk.js"
			input.SDKSHA256 = fmt.Sprintf("%x", sha256.Sum256([]byte(sentinelRuntimeTestSDK)))
			input.Environment.ScriptSources = []string{input.SDKURL}
			fetcher := func(context.Context, string, int64) ([]byte, string, string, error) {
				return []byte(sentinelRuntimeTestSDK), "application/javascript", input.SDKURL, nil
			}
			session, err := pool.Begin(t.Context(), "images", input, SentinelComputeHooks{Fetcher: fetcher})
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			server.mu.Lock()
			serverSession := server.sessions[session.id]
			server.mu.Unlock()
			serverSession.lock <- struct{}{}
			serverSession.state.hooks.Fetcher = fetcher
			<-serverSession.lock
			p := session.RequirementsToken()
			dx := encodeConversationTurnstileProgram(t, p, []any{[]any{36, "unsupported"}})
			result, err := session.Solve(t.Context(), SentinelComputeChallenge{ObserverRequired: true, CollectorDX: dx, SnapshotDX: dx, TurnstileRequired: true, TurnstileDX: dx})
			if err != nil || result.TurnstileToken != "sdk-turnstile-token" {
				t.Fatalf("SDK fallback: %q %v", result.TurnstileToken, err)
			}
			snapshot, err := session.Snapshot(t.Context())
			if err != nil || snapshot != "sdk-snapshot-token" {
				t.Fatalf("SDK observer: %q %v", snapshot, err)
			}
			if (session.local == nil) != nodeFallback {
				t.Fatal("fallback ran on wrong side")
			}
			if session.RequirementsToken() != p {
				t.Fatal("fallback replaced p")
			}
		})
	}
}

func TestSentinelComputeAffinityAndRoundRobin(t *testing.T) {
	one, httpOne := computeTestServer(t)
	two, httpTwo := computeTestServer(t)
	pool := computeTestPool(t, sentinelconfig.Node{Name: "one", URL: httpOne.URL, APIKey: "key-a"}, sentinelconfig.Node{Name: "two", URL: httpTwo.URL, APIKey: "key-a"})
	for i := 0; i < 2; i++ {
		session, err := pool.Begin(context.Background(), "images", computeTestInput(), SentinelComputeHooks{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = session.Solve(context.Background(), SentinelComputeChallenge{}); err != nil {
			t.Fatal(err)
		}
		if _, err = session.Snapshot(context.Background()); err != nil {
			t.Fatal(err)
		}
		session.Close()
	}
	if one.Snapshot().GoSuccess != 3 || two.Snapshot().GoSuccess != 3 {
		t.Fatal("phases must remain on the selected node")
	}
}

func TestSentinelComputeConcurrentAndGeneration(t *testing.T) {
	server, endpoint := computeTestServer(t)
	pool := computeTestPool(t, sentinelconfig.Node{Name: "one", URL: endpoint.URL, APIKey: "key-a"})
	session, err := pool.Begin(context.Background(), "images", computeTestInput(), SentinelComputeHooks{})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	old := session.policy.Version()
	if err = server.UpdateConfig(sentinelconfig.Server{Enabled: true, APIKeys: []string{"key-a"}}); err != nil {
		t.Fatal(err)
	}
	if _, err = session.Solve(context.Background(), SentinelComputeChallenge{}); err != nil {
		t.Fatal(err)
	}
	if session.policy.Version() != old {
		t.Fatal("generation changed")
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = server.Snapshot(); _ = pool.Snapshot() }()
	}
	wg.Wait()
}

func TestSentinelComputeAuthenticationIdempotenceAndIsolation(t *testing.T) {
	server, _ := computeTestServer(t)
	input := computeTestInput()
	id := uuid.NewString()
	op := uuid.NewString()
	body, _ := json.Marshal(sentinelComputeRequest{Version: 1, SessionID: id, OperationID: op, BudgetMillis: 30000, Input: &input})
	send := func(key string, data []byte, path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(data))
		req.Header.Set("Authorization", "Bearer "+key)
		out := httptest.NewRecorder()
		server.ServeHTTP(out, req)
		return out
	}
	if out := send("wrong", body, "/v1/sentinel/sessions"); out.Code != 401 {
		t.Fatal(out.Code)
	}
	one := send("key-a", body, "/v1/sentinel/sessions")
	if one.Code != 200 {
		t.Fatal(one.Body.String())
	}
	two := send("key-a", body, "/v1/sentinel/sessions")
	if !bytes.Equal(one.Body.Bytes(), two.Body.Bytes()) {
		t.Fatal("duplicate operation reran computation")
	}
	if out := send("key-b", body, "/v1/sentinel/sessions"); out.Code != 404 {
		t.Fatal("cross-key session access")
	}
	input.DataBuild = "different"
	body, _ = json.Marshal(sentinelComputeRequest{Version: 1, SessionID: id, OperationID: op, BudgetMillis: 30000, Input: &input})
	if out := send("key-a", body, "/v1/sentinel/sessions"); out.Code != 409 {
		t.Fatal("mismatched retry accepted")
	}
	if err := server.UpdateConfig(sentinelconfig.Server{Enabled: true, APIKeys: []string{"key-b"}}); err != nil {
		t.Fatal(err)
	}
	if out := send("key-a", body, "/v1/sentinel/sessions"); out.Code != 401 {
		t.Fatal("revoked key accepted")
	}
}

func TestSentinelComputeBudgetAndCancellation(t *testing.T) {
	var requests atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusOK)
			return
		}
		requests.Add(1)
		<-r.Context().Done()
	}))
	defer endpoint.Close()
	pool := computeTestPool(t, sentinelconfig.Node{Name: "slow", URL: endpoint.URL, APIKey: "key"})
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	_, err := pool.Begin(ctx, "images", computeTestInput(), SentinelComputeHooks{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("parent cancellation lost: %v", err)
	}
	if requests.Load() != 1 {
		t.Fatal("cancellation retried")
	}
}

func TestSentinelComputeFallbackDoesNotRequestOfficialEndpoints(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(sentinelComputeResponse{Version: 1, Instance: "down", Error: computeError("unavailable", true, nil)})
	}))
	defer endpoint.Close()
	pool := NewSentinelComputePool()
	defer pool.Close()
	policy, _ := sentinelcompat.Compile(sentinelcompat.Config{})
	if err := pool.UpdateConfig("remote", sentinelconfig.Remote{Nodes: []sentinelconfig.Node{{Name: "down", URL: endpoint.URL, APIKey: "key"}}}, SentinelRuntimeConfig{Enabled: true, Compatibility: policy}); err != nil {
		t.Fatal(err)
	}
	session, err := pool.Begin(context.Background(), "images", computeTestInput(), SentinelComputeHooks{Fetcher: func(context.Context, string, int64) ([]byte, string, string, error) {
		t.Fatal("unexpected SDK network request")
		return nil, "", "", nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if session.local == nil || session.RequirementsToken() == "" {
		t.Fatal("missing local fallback")
	}
	p := session.RequirementsToken()
	result, err := session.Solve(context.Background(), SentinelComputeChallenge{})
	if err != nil || result.RequirementsToken != p {
		t.Fatal("fallback did not preserve p")
	}
}

func TestSentinelComputeRejectsInvalidAndUnsafeInput(t *testing.T) {
	for _, change := range []func(*SentinelComputeInput){func(in *SentinelComputeInput) { in.SDKURL = "https://127.0.0.1/sdk.js" }, func(in *SentinelComputeInput) { in.Environment.Persona.CatalogVersion = "unknown" }, func(in *SentinelComputeInput) { in.Environment.BrowserEnvironment.CatalogVersion = "unknown" }} {
		input := computeTestInput()
		change(&input)
		if input.validate() == nil {
			t.Fatal("invalid input accepted")
		}
	}
	if (&SentinelComputeError{}).RetryOtherAuth() || !(&SentinelComputeError{}).SkipAuthResult() {
		t.Fatal("compute errors must not change account scheduling")
	}
}

func BenchmarkSentinelComputeRemote(b *testing.B) {
	server, endpoint := computeTestServer(b)
	pool := computeTestPool(b, sentinelconfig.Node{Name: "one", URL: endpoint.URL, APIKey: "key-a"})
	input := computeTestInput()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := pool.Begin(context.Background(), "images", input, SentinelComputeHooks{})
		if err != nil {
			b.Fatal(err)
		}
		s.Close()
		b.StopTimer()
		for {
			server.mu.Lock()
			empty := len(server.sessions) == 0
			changed := server.changed
			server.mu.Unlock()
			if empty {
				break
			}
			<-changed
		}
		b.StartTimer()
	}
}

func BenchmarkSentinelComputeLocal(b *testing.B) {
	input := computeTestInput()
	env := input.Environment.native(nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := BuildConversationRequirementsTokenWithEnvironment(env, env.ScriptSources, "", nil, time.Now); err != nil {
			b.Fatal(err)
		}
	}
}

func TestSentinelComputeWireOmitsSecrets(t *testing.T) {
	challenge := ComputeChallenge(map[string]any{"access_token": "secret-access", "cookie": "secret-cookie", "prompt": "secret-prompt", "token": "challenge-token"}, true)
	body, _ := json.Marshal(challenge)
	for _, secret := range [][]byte{[]byte("secret-access"), []byte("secret-cookie"), []byte("secret-prompt")} {
		if bytes.Contains(body, secret) {
			t.Fatal("unexpected field forwarded")
		}
	}
}

func TestSentinelComputeResponseAllowsAdditiveMetadata(t *testing.T) {
	var response sentinelComputeResponse
	if err := decodeComputeResponse([]byte(`{"version":1,"new_optional_metadata":{"value":"ignored"}}`), &response); err != nil {
		t.Fatal(err)
	}
	if err := decodeComputeResponse([]byte(`{"version":2}`), &response); err == nil {
		t.Fatal("incompatible protocol accepted")
	}
}

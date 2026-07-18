package chatgptweb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type zeroReader struct{}

func (zeroReader) Read(buffer []byte) (int, error) {
	clear(buffer)
	return len(buffer), nil
}

func TestSentinelRequirementsAndPoW(t *testing.T) {
	t.Parallel()
	fixedNow := time.Date(2026, time.July, 16, 12, 30, 0, 0, time.UTC)
	generator, err := NewSentinelGenerator("00000000-0000-4000-8000-000000000001", DefaultPersona(), zeroReader{}, func() time.Time { return fixedNow })
	if err != nil {
		t.Fatal(err)
	}
	requirements, err := generator.GenerateRequirementsToken()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(requirements, "gAAAAAC") {
		t.Fatalf("requirements token = %q", requirements)
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(requirements, "gAAAAAC"))
	if err != nil {
		t.Fatal(err)
	}
	var configuration []any
	if err := json.Unmarshal(decoded, &configuration); err != nil {
		t.Fatal(err)
	}
	if len(configuration) != 18 || configuration[0] != "1920x1080" || configuration[3] != float64(1) || configuration[5] != sentinelSDKURL {
		t.Fatalf("requirements configuration = %#v", configuration)
	}
	if got := fnv1a32("hello"); got != "888d766e" {
		t.Fatalf("fnv1a32(hello) = %q, want 888d766e", got)
	}

	proof, err := generator.GenerateProof("fixture-seed", "ffffffff")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(proof, "gAAAAAB") || !strings.HasSuffix(proof, "~S") {
		t.Fatalf("proof token = %q", proof)
	}
	payload := strings.TrimSuffix(strings.TrimPrefix(proof, "gAAAAAB"), "~S")
	if hash := fnv1a32("fixture-seed" + payload); hash > "ffffffff" {
		t.Fatalf("proof hash %q does not satisfy difficulty", hash)
	}
}

func TestSentinelTokenRequest(t *testing.T) {
	t.Parallel()
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/backend-api/sentinel/req" {
			http.NotFound(response, request)
			return
		}
		if request.Header.Get("Content-Type") != "text/plain;charset=UTF-8" {
			t.Errorf("Content-Type = %q", request.Header.Get("Content-Type"))
		}
		body, _ := io.ReadAll(request.Body)
		var input map[string]any
		if err := json.Unmarshal(body, &input); err != nil {
			t.Errorf("decode sentinel request: %v", err)
		}
		if input["flow"] != "password_verify" || !strings.HasPrefix(stringValue(input["p"]), "gAAAAAC") {
			t.Errorf("sentinel request = %#v", input)
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(response, `{"token":"challenge-token","proofofwork":{"required":true,"seed":"fixture","difficulty":"ffffffff"}}`)
	}))
	defer server.Close()

	client, err := NewClient(DefaultPersona(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	sentinel, err := NewSentinel(client, server.URL, server.URL, "00000000-0000-4000-8000-000000000001", zeroReader{}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	token, err := sentinel.Token(context.Background(), "password_verify")
	if err != nil {
		t.Fatal(err)
	}
	var header map[string]any
	if err := json.Unmarshal([]byte(token), &header); err != nil {
		t.Fatal(err)
	}
	if header["c"] != "challenge-token" || !strings.HasPrefix(stringValue(header["p"]), "gAAAAAB") {
		t.Fatalf("sentinel header = %#v", header)
	}
	foundCookie := false
	for _, cookie := range client.ExportCookies() {
		if cookie.Name == "oai-sc" && cookie.Value == "0challenge-token" {
			foundCookie = true
		}
	}
	if !foundCookie {
		t.Fatal("oai-sc cookie was not persisted")
	}
}

func TestSentinelTokenStopsUnsolvablePoWWhenContextCanceled(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(response, `{"token":"challenge-token","proofofwork":{"required":true,"seed":"fixture","difficulty":"!"}}`)
	}))
	defer server.Close()

	client, err := NewClient(DefaultPersona(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	powStarted := make(chan struct{})
	nowCalls := 0
	now := func() time.Time {
		nowCalls++
		if nowCalls == 4 {
			close(powStarted)
		}
		return time.Date(2026, time.July, 16, 12, 30, 0, 0, time.UTC)
	}
	sentinel, err := NewSentinel(client, server.URL, server.URL, "00000000-0000-4000-8000-000000000001", zeroReader{}, now)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, tokenErr := sentinel.Token(ctx, "password_verify")
		result <- tokenErr
	}()
	select {
	case <-powStarted:
	case <-time.After(time.Second):
		t.Fatal("Sentinel.Token() did not start proof-of-work")
	}
	cancel()
	select {
	case tokenErr := <-result:
		if !errors.Is(tokenErr, context.Canceled) {
			t.Fatalf("Sentinel.Token() error = %v, want context.Canceled", tokenErr)
		}
	case <-time.After(time.Second):
		t.Fatal("Sentinel.Token() did not stop after context cancellation")
	}
}

func TestSentinelInteractiveChallenges(t *testing.T) {
	t.Parallel()
	for _, challenge := range []struct {
		name string
		body string
		code string
	}{
		{name: "turnstile", body: `{"token":"challenge","turnstile":{"required":true,"sitekey":"site"}}`, code: "turnstile_required"},
		{name: "arkose", body: `{"token":"challenge","arkose":{"required":true}}`, code: "arkose_required"},
	} {
		t.Run(challenge.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				response.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(response, challenge.body)
			}))
			defer server.Close()
			client, err := NewClient(DefaultPersona(), "", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer client.CloseIdleConnections()
			sentinel, err := NewSentinel(client, server.URL, server.URL, "00000000-0000-4000-8000-000000000001", zeroReader{}, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := sentinel.Token(context.Background(), "password_verify"); err == nil {
				t.Fatal("Token() succeeded for an interactive challenge")
			} else if authError, ok := AsAuthError(err); !ok || authError.Code != challenge.code || authError.State != LifecycleInteractionRequired {
				t.Fatalf("Token() error = %#v", err)
			}
		})
	}
}

func TestSentinelIgnoresDisabledInteractiveChallenges(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(response, `{
			"token":"challenge",
			"turnstile":{"required":false,"sitekey":"unused"},
			"arkose":{"required":false,"metadata":"unused"}
		}`)
	}))
	defer server.Close()
	client, err := NewClient(DefaultPersona(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	sentinel, err := NewSentinel(client, server.URL, server.URL, "00000000-0000-4000-8000-000000000001", zeroReader{}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sentinel.Token(context.Background(), "password_verify"); err != nil {
		t.Fatalf("Token() error = %v", err)
	}
}

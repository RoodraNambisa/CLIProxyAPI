package chatgptweb

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelconfig"
)

func TestBrowserRegionSnapshotConsumersAndPersistence(t *testing.T) {
	client, err := NewClient(DefaultPersona(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	before := client.Persona()
	snapshot := BrowserRegionSnapshot{Timezone: "Asia/Tokyo", OffsetMinutes: -540, Language: "ja-JP", OverrideLanguage: true}
	if err = client.SetBrowserRegion(snapshot); err != nil {
		t.Fatal(err)
	}
	req, _ := fhttp.NewRequest(http.MethodGet, "https://chatgpt.com/", nil)
	client.applyHeaders(req, map[string]string{"oai-language": "en-US", "accept-language": "en-US,en;q=0.9"})
	if req.Header.Get("Accept-Language") != "ja-JP,ja;q=0.9" || req.Header.Get("Oai-Language") != "ja-JP" {
		t.Fatal("headers ignored snapshot")
	}
	if client.Persona() != before || len(client.ExportCookies()) != 0 {
		t.Fatal("region overlay mutated persisted identity")
	}
	clone, err := client.CloneWithProxy("")
	if err != nil {
		t.Fatal(err)
	}
	defer clone.CloseIdleConnections()
	attempt, err := client.NewBootstrapAttempt(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer attempt.CloseIdleConnections()
	for _, copy := range []*Client{clone, attempt} {
		if locale := copy.BrowserLocale(); locale == nil || locale.Language != "ja-JP" {
			t.Fatal("clone lost locale")
		}
	}
	if err = client.SetBrowserRegion(BrowserRegionSnapshot{Timezone: "UTC", Language: "en-US", OverrideLanguage: true}); err == nil {
		t.Fatal("in-flight snapshot drifted")
	}
	env := ConversationTurnstileEnvironment{Persona: before, Locale: client.BrowserLocale(), Timezone: "Asia/Tokyo", TimezoneOffsetMinutes: -540}
	fields, err := buildConversationPoWConfig(env, nil, "", zeroReader{}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if fields[7] != "ja-JP" || fields[8] != "ja-JP,ja" || !strings.Contains(fields[1].(string), "GMT+0900") {
		t.Fatalf("proof locale mismatch: %v", fields)
	}
	vm, _, _ := normalizeConversationTurnstileEnvironment(env, time.Now())
	if vm["window.navigator.language"] != "ja-JP" {
		t.Fatal("VM locale mismatch")
	}
	sentinel, err := NewSentinel(client, "", "", "fixture", zeroReader{}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	authFields, err := sentinel.generator.configuration()
	if err != nil {
		t.Fatal(err)
	}
	if authFields[8] != "ja-JP" || !strings.Contains(authFields[1].(string), "GMT+0900") {
		t.Fatal("login proof did not inherit snapshot")
	}
	remote := ComputeEnvironment(env).native(nil)
	if remote.Locale == nil || remote.Locale.Language != "ja-JP" || remote.Timezone != "Asia/Tokyo" {
		t.Fatal("RPC lost snapshot")
	}
}
func TestBrowserRegionLoginFlowFrozen(t *testing.T) {
	calls := 0
	service := NewService(Options{ResolveBrowserRegion: func(string, Persona) BrowserRegionSnapshot {
		calls++
		return BrowserRegionSnapshot{Timezone: "Asia/Tokyo", OffsetMinutes: -540, Language: "ja-JP", OverrideLanguage: true}
	}})
	ctx := withBrowserRegionFlow(t.Context())
	for range 3 {
		client := &Client{persona: DefaultPersona()}
		if err := service.prepareBrowserRegionClient(ctx, client); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatal("login retries re-resolved environment")
	}
}

func TestBrowserRegionFallbackLanguageCannotChangeMidFlow(t *testing.T) {
	client := &Client{persona: DefaultPersona()}
	if err := client.SetBrowserRegion(BrowserRegionSnapshot{Timezone: "UTC"}); err != nil {
		t.Fatal(err)
	}
	if client.BrowserLocale() != nil {
		t.Fatal("fallback changed legacy locale encoding")
	}
	if err := client.SetBrowserRegion(BrowserRegionSnapshot{Timezone: "UTC", OverrideLanguage: true, Language: "ja-JP"}); err == nil {
		t.Fatal("late locale result changed a frozen fallback")
	}
	if client.BrowserLocale() != nil {
		t.Fatal("failed repin changed locale")
	}
}
func TestBrowserRegionSDKLanguage(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	request, sdk := sentinelRuntimeTestRequests(t, false)
	request.Environment.Locale = &BrowserLocale{Language: "ja-JP"}
	sdk.Environment = request.Environment
	source := strings.Replace(sentinelRuntimeTestSDK, `return "sdk-turnstile-token"`, `return JSON.stringify({language:navigator.language,languages:navigator.languages})`, 1)
	sdk.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		return []byte(source), "application/javascript", target, nil
	}
	result, err := manager.SolveTurnstile(t.Context(), request, sdk, nil)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Language  string   `json:"language"`
		Languages []string `json:"languages"`
	}
	if json.Unmarshal([]byte(result), &got) != nil || got.Language != "ja-JP" || strings.Join(got.Languages, ",") != "ja-JP,ja" {
		t.Fatalf("SDK lost locale: %s", result)
	}
}
func TestBrowserRegionRemoteCapability(t *testing.T) {
	server, _ := computeTestServer(t)
	for _, legacy := range []bool{false, true} {
		endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			recorder := httptest.NewRecorder()
			server.ServeHTTP(recorder, r)
			var body map[string]any
			_ = json.Unmarshal(recorder.Body.Bytes(), &body)
			if legacy {
				delete(body, "capabilities")
			}
			w.WriteHeader(recorder.Code)
			_ = json.NewEncoder(w).Encode(body)
		}))
		pool := computeTestPool(t, sentinelconfig.Node{Name: "fixture", URL: endpoint.URL, APIKey: "key-a"})
		input := computeTestInput()
		input.Environment.Locale = &BrowserLocale{Language: "ja-JP"}
		session, err := pool.Begin(t.Context(), "images", input, SentinelComputeHooks{})
		if legacy {
			if err == nil || classifyComputeError(err).Code != "unsupported_browser_region" {
				t.Fatalf("old node silently ignored locale: %v", err)
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			session.Close()
		}
		pool.Close()
		endpoint.Close()
	}
}

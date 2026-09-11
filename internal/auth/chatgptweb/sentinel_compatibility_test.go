package chatgptweb

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelcompat"
)

func TestSentinelCompatibilityCachesAreRuleScoped(t *testing.T) {
	m := newSentinelRuntimeTestManager()
	defer m.Close()
	for _, version := range []string{"old", "new"} {
		m.markPreferredForChallenge(m.cacheGeneration, "sdk", SentinelProgramTurnstile, "program", "dx", "token", version)
	}
	if m.isPreferred("sdk", SentinelProgramTurnstile, "program") || m.isPreferred("sdk", SentinelProgramTurnstile, "program", "unseen") {
		t.Fatal("preferred cache leaked to another revision")
	}
	if !m.isPreferred("sdk", SentinelProgramTurnstile, "program", "new") {
		t.Fatal("new revision cache missing")
	}
	if _, _, hit := m.preferredChallengeSignature(SentinelProgramTurnstile, "dx", "token", "unseen"); hit {
		t.Fatal("signature hints leaked")
	}
	// A late old request must not cause a new revision to prefer SDK execution.
	m.cacheChallengeSignature(m.cacheGeneration, SentinelProgramTurnstile, "other", "late-dx", "token", false, "new")
	m.markPreferredForChallenge(m.cacheGeneration, "sdk", SentinelProgramTurnstile, "other", "late-dx", "token", "old")
	if _, candidate, _ := m.preferredChallengeSignature(SentinelProgramTurnstile, "late-dx", "token", "new"); candidate {
		t.Fatal("late completion polluted new rules")
	}
	recordSentinelCircuitForError(m, t.Context(), m.cacheGeneration, "sdk", errors.New("old rules failed"), "old")
	if _, open := m.circuitRetryAfter("sdk", "new"); open {
		t.Fatal("old execution circuit blocked new rules")
	}
	if _, open := m.circuitRetryAfter("sdk", "old"); !open {
		t.Fatal("old request lost its execution circuit")
	}
}

func TestSentinelCompatibilityObserverKeepsSnapshotDuringReload(t *testing.T) {
	p := compileCompatibility(t, sentinelcompat.Config{Enabled: true})
	m := newSentinelRuntimeTestManager()
	defer m.Close()
	collector, snapshot := compatibilityPrograms("__oai_so_future", "owner-value")
	_, request := sentinelRuntimeGoObserverRequests(t, collector, snapshot)
	request.Environment.Compatibility = p
	var fetches atomic.Int64
	request.Fetcher = sentinelRuntimeTestFetcher(&fetches)
	observer, err := m.BeginObserver(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 20 {
			m.UpdateConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 4, CacheVersions: 3})
		}
	}()
	token, err := observer.Snapshot(t.Context())
	observer.Close()
	wg.Wait()
	if err != nil || !strings.Contains(token, "b3duZXItdmFsdWU=") {
		t.Fatalf("in-flight rules changed: %v", err)
	}
	state := m.Snapshot()
	if state.Initialized || fetches.Load() != 0 || state.GoVMExtensionUses != 1 {
		t.Fatalf("not pure Go: %+v", state)
	}
}

func TestSentinelCompatibilitySDKUsesSameConstants(t *testing.T) {
	p := compileCompatibility(t, sentinelcompat.Config{Enabled: true, EnvironmentProperties: []sentinelcompat.Property{{Path: "window.__fixture", Type: "string", Value: "safe-constant"}}})
	code := strings.Replace(sentinelRuntimeTestSDK, `return "sdk-turnstile-token"`, `return window.__fixture`, 1)
	m := newSentinelRuntimeTestManager()
	defer m.Close()
	const sourceURL = "https://sentinel.openai.com/sentinel/20260724/sdk.js"
	request := SentinelSDKRequest{BaseURL: "https://chatgpt.com", SDKURL: sourceURL, Environment: ConversationTurnstileEnvironment{Compatibility: p}}
	request.Fetcher = func(_ context.Context, _ string, _ int64) ([]byte, string, string, error) {
		return []byte(code), "application/javascript", sourceURL, nil
	}
	goRequest := ConversationTurnstileSolveRequest{DX: encodeConversationTurnstileProgram(t, "requirements", []any{[]any{36, "unsupported"}}), RequirementsToken: "requirements", Environment: request.Environment}
	request.RequirementsToken = goRequest.RequirementsToken
	request.Challenge = map[string]any{"turnstile": map[string]any{"required": true, "dx": goRequest.DX}}
	token, err := m.SolveTurnstile(t.Context(), goRequest, request, nil)
	if err != nil || token != "safe-constant" {
		t.Fatalf("SDK constant mismatch: %q %v", token, err)
	}
}

func solveCompatibilitySDKFixture(t *testing.T, policy *sentinelcompat.Policy, expression string) string {
	t.Helper()
	code := strings.Replace(sentinelRuntimeTestSDK, `return "sdk-turnstile-token"`, `return JSON.stringify(`+expression+`)`, 1)
	m := newSentinelRuntimeTestManager()
	defer m.Close()
	const sourceURL = "https://sentinel.openai.com/sentinel/20260724/sdk.js"
	environment := ConversationTurnstileEnvironment{Compatibility: policy}
	goRequest := ConversationTurnstileSolveRequest{DX: encodeConversationTurnstileProgram(t, "requirements", []any{[]any{36, "unsupported"}}), RequirementsToken: "requirements", Environment: environment}
	request := SentinelSDKRequest{BaseURL: "https://chatgpt.com", SDKURL: sourceURL, RequirementsToken: "requirements", Environment: environment,
		Challenge: map[string]any{"turnstile": map[string]any{"required": true, "dx": goRequest.DX}},
		Fetcher: func(_ context.Context, _ string, _ int64) ([]byte, string, string, error) {
			return []byte(code), "application/javascript", sourceURL, nil
		},
	}
	token, err := m.SolveTurnstile(t.Context(), goRequest, request, nil)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func TestSentinelCompatibilitySDKPrimitiveAndReadonlyParity(t *testing.T) {
	policy := compileCompatibility(t, sentinelcompat.Config{Enabled: true, EnvironmentProperties: []sentinelcompat.Property{
		{Path: "window.__empty", Type: "string", Value: ""},
		{Path: "window.__flag", Type: "boolean", Value: false},
		{Path: "window.document.__zero", Type: "number", Value: 0},
		{Path: "window.navigator.__nil", Type: "null"},
		{Path: "window.screen.__missing", Type: "undefined", Enumerable: true},
	}})
	result := solveCompatibilitySDKFixture(t, policy, `[window.__empty, window.__flag, document.__zero, navigator.__nil, typeof screen.__missing, Object.hasOwn(screen,"__missing"), Object.keys(screen).includes("__missing"), Object.keys(window).includes("__flag"), Reflect.set(window,"__flag",true), window.__flag]`)
	if result != `["",false,0,null,"undefined",true,true,false,false,false]` {
		t.Fatalf("SDK property semantics differ: %s", result)
	}
}

func TestSentinelCompatibilityProtectsSDKHostProperties(t *testing.T) {
	result := solveCompatibilitySDKFixture(t, nil, `Object.entries({window, "window.document":document, "window.navigator":navigator, "window.screen":screen}).flatMap(([path,value])=>{const keys=new Set();for(let v=value;v;v=Object.getPrototypeOf(v))for(const k of Object.getOwnPropertyNames(v))keys.add(path+"."+k);return [...keys]})`)
	var paths []string
	if err := json.Unmarshal([]byte(result), &paths); err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		if strings.Contains(path, ".SentinelSDK") || strings.Contains(path, ".__bound_") {
			continue
		}
		if sentinelcompat.ValidatePath(path) == nil {
			t.Errorf("SDK built-in can be overridden: %s", path)
		}
	}
}

func TestSentinelCompatibilityCurrentSDKDifferential(t *testing.T) {
	path := os.Getenv("CPA_SENTINEL_SDK_FIXTURE")
	if path == "" {
		t.Skip("set CPA_SENTINEL_SDK_FIXTURE to a verified current SDK source file")
	}
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	resource := DefaultConversationSentinelSDKResource()
	for _, key := range []string{"__oai_so_future_owner", "__fixture_owner"} {
		t.Run(key, func(t *testing.T) {
			policy := compileCompatibility(t, sentinelcompat.Config{Enabled: true, WritableWindowProperties: []string{"__fixture_owner"}, EnvironmentProperties: []sentinelcompat.Property{{Path: "window.__fixture_seed", Type: "string", Value: "11111111-2222-4333-8444-555555555555"}}})
			collector, snapshot := compatibilityPrograms(key, "unused")
			collector[5] = []any{2, 45, "__fixture_seed"}
			collector = append(collector[:6], []any{6, 45, 10, 45}, collector[6])
			collector = append(collector, []any{7, 3, 45})
			_, request := sentinelRuntimeGoObserverRequests(t, collector, snapshot)
			request.SDKURL = resource.URL
			request.ExpectedSHA256 = resource.SHA256
			request.IntegrityRequired = true
			request.Environment.Compatibility = policy
			request.Fetcher = func(_ context.Context, _ string, _ int64) ([]byte, string, string, error) {
				return source, "application/javascript", resource.URL, nil
			}
			goVM, err := newConversationSentinelObserverVM(t.Context(), encodeConversationTurnstileProgram(t, request.RequirementsToken, collector), encodeConversationTurnstileProgram(t, request.RequirementsToken, snapshot), request.RequirementsToken, request.Environment, zeroReader{}, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			defer goVM.Close()
			goToken, err := goVM.Snapshot(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			m := newSentinelRuntimeTestManager()
			defer m.Close()
			finish, err := m.beginSDKTask()
			if err != nil {
				t.Fatal(err)
			}
			defer finish()
			lease, err := m.acquire(t.Context(), sentinelPriorityObserverCollector)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.release()
			observer := &SentinelObserver{manager: m, request: request, ctx: t.Context()}
			instance, _, err := observer.startSDK(t.Context(), lease)
			if err != nil {
				t.Fatal(err)
			}
			defer instance.close()
			sdkToken, err := instance.call(t.Context(), "snapshotObserver", map[string]any{"challenge": request.Challenge})
			if err != nil || goToken != sdkToken {
				t.Fatalf("Go/SDK differential mismatch: Go=%q SDK=%q err=%v", goToken, sdkToken, err)
			}
		})
	}
}

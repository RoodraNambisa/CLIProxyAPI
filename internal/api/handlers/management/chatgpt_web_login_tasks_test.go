package management

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	sdkAuth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type chatGPTWebManagementCodedError string

func (err chatGPTWebManagementCodedError) Error() string { return "coded chatgpt web error" }

func (err chatGPTWebManagementCodedError) ChatGPTWebErrorCode() string { return string(err) }

type chatGPTWebManagementTestExecutor struct {
	mu          sync.Mutex
	loginFn     func(context.Context, chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error)
	reloginFn   func(context.Context, *coreauth.Auth) (*coreauth.Auth, bool, error)
	normalizeFn func(context.Context, *chatgptwebauth.Credential, string) (*chatgptwebauth.Credential, error)
	fetchFn     func(context.Context, *coreauth.Auth) ([]chatgptwebauth.CatalogModel, error)
	beginFn     func(context.Context, string) (context.Context, func(), error)
}

type countingChatGPTWebAuthStore struct {
	*sdkAuth.FileTokenStore
	saves            atomic.Int32
	block            atomic.Bool
	respectContext   atomic.Bool
	rollbackOnCancel atomic.Bool
	saveIfAbsentErr  error
	entered          chan struct{}
	release          chan struct{}
}

type failingListChatGPTWebAuthStore struct {
	*sdkAuth.FileTokenStore
	fail atomic.Bool
}

func (store *failingListChatGPTWebAuthStore) List(ctx context.Context) ([]*coreauth.Auth, error) {
	if store.fail.Load() {
		return nil, errors.New("auth snapshot unavailable")
	}
	return store.FileTokenStore.List(ctx)
}

type chatGPTWebManagementLeaseResolver struct {
	active atomic.Int32
	heldID chan string
}

func (*chatGPTWebManagementLeaseResolver) Resolve(context.Context, *coreauth.Auth) (coreauth.ResolvedProxy, error) {
	return coreauth.ResolvedProxy{}, nil
}

func (*chatGPTWebManagementLeaseResolver) ReportFailure(_ context.Context, _ *coreauth.Auth, err error) error {
	return err
}

func (resolver *chatGPTWebManagementLeaseResolver) HoldBinding(authID string) func() {
	resolver.active.Add(1)
	resolver.heldID <- authID
	var once sync.Once
	return func() {
		once.Do(func() { resolver.active.Add(-1) })
	}
}

func (store *countingChatGPTWebAuthStore) Save(ctx context.Context, auth *coreauth.Auth) (string, error) {
	if errWait := store.beforeSave(ctx); errWait != nil {
		return "", errWait
	}
	return store.FileTokenStore.Save(ctx, auth)
}

func (store *countingChatGPTWebAuthStore) SaveIfAbsent(ctx context.Context, auth *coreauth.Auth) (string, error) {
	if errWait := store.beforeSave(ctx); errWait != nil {
		return "", errWait
	}
	if store.saveIfAbsentErr != nil {
		return "", store.saveIfAbsentErr
	}
	return store.FileTokenStore.SaveIfAbsent(ctx, auth)
}

func (store *countingChatGPTWebAuthStore) beforeSave(ctx context.Context) error {
	store.saves.Add(1)
	if store.block.Load() {
		select {
		case store.entered <- struct{}{}:
		default:
		}
		if store.respectContext.Load() {
			select {
			case <-store.release:
			case <-ctx.Done():
				if store.rollbackOnCancel.Load() {
					return coreauth.NewSaveOutcomeError(coreauth.SaveOutcomeRolledBack, ctx.Err())
				}
				return ctx.Err()
			}
		} else {
			<-store.release
		}
	}
	return nil
}

func (*chatGPTWebManagementTestExecutor) Identifier() string { return chatgptwebauth.Provider }

func (*chatGPTWebManagementTestExecutor) Execute(context.Context, *coreauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (*chatGPTWebManagementTestExecutor) ExecuteStream(context.Context, *coreauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, nil
}

func (*chatGPTWebManagementTestExecutor) Refresh(context.Context, *coreauth.Auth) (*coreauth.Auth, error) {
	return nil, nil
}

func (*chatGPTWebManagementTestExecutor) CountTokens(context.Context, *coreauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (*chatGPTWebManagementTestExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func (executor *chatGPTWebManagementTestExecutor) Login(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
	executor.mu.Lock()
	loginFn := executor.loginFn
	executor.mu.Unlock()
	if loginFn == nil {
		return nil, errors.New("unexpected login")
	}
	return loginFn(ctx, input)
}

func (executor *chatGPTWebManagementTestExecutor) BeginLoginOperation(ctx context.Context, key string) (context.Context, func(), error) {
	executor.mu.Lock()
	beginFn := executor.beginFn
	executor.mu.Unlock()
	if beginFn != nil {
		return beginFn(ctx, key)
	}
	return ctx, func() {}, nil
}

func (executor *chatGPTWebManagementTestExecutor) ReloginCurrent(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, bool, error) {
	executor.mu.Lock()
	reloginFn := executor.reloginFn
	executor.mu.Unlock()
	if reloginFn == nil {
		return nil, false, errors.New("unexpected re-login")
	}
	return reloginFn(ctx, auth)
}

func (executor *chatGPTWebManagementTestExecutor) NormalizeImportedCredential(ctx context.Context, credential *chatgptwebauth.Credential, proxyURL string) (*chatgptwebauth.Credential, error) {
	executor.mu.Lock()
	normalizeFn := executor.normalizeFn
	executor.mu.Unlock()
	if normalizeFn != nil {
		return normalizeFn(ctx, credential, proxyURL)
	}
	return credential, nil
}

func (executor *chatGPTWebManagementTestExecutor) FetchModels(ctx context.Context, auth *coreauth.Auth) ([]chatgptwebauth.CatalogModel, error) {
	executor.mu.Lock()
	fetchFn := executor.fetchFn
	executor.mu.Unlock()
	if fetchFn != nil {
		return fetchFn(ctx, auth)
	}
	return nil, nil
}

func TestParseChatGPTWebLoginInputs(t *testing.T) {
	inputs, errParse := parseChatGPTWebLoginInputs([]byte("\r\n person@example.com---pass---segment---JBSWY3DPEHPK3PXP\r\n"))
	if errParse != nil {
		t.Fatalf("parseChatGPTWebLoginInputs() error = %v", errParse)
	}
	if len(inputs) != 1 || inputs[0].Line != 2 || inputs[0].Email != "person@example.com" || inputs[0].Password != "pass---segment" {
		t.Fatalf("inputs = %+v", inputs)
	}
	withoutTOTP, errWithoutTOTP := parseChatGPTWebLoginInputs([]byte("no-2fa@example.com---password---"))
	if errWithoutTOTP != nil {
		t.Fatalf("parse account without TOTP error = %v", errWithoutTOTP)
	}
	if len(withoutTOTP) != 1 || withoutTOTP[0].TOTPSecret != "" {
		t.Fatalf("account without TOTP = %+v", withoutTOTP)
	}

	for _, test := range []struct {
		name string
		data string
	}{
		{name: "empty", data: "\n"},
		{name: "missing delimiter", data: "person@example.com-password-secret"},
		{name: "empty password", data: "person@example.com------JBSWY3DPEHPK3PXP"},
		{name: "duplicate", data: "Person@example.com---a---JBSWY3DPEHPK3PXP\nperson@example.com---b---JBSWY3DPEHPK3PXP"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parseChatGPTWebLoginInputs([]byte(test.data)); err == nil {
				t.Fatal("parseChatGPTWebLoginInputs() error = nil")
			}
		})
	}

	_, errDuplicate := parseChatGPTWebLoginInputs([]byte("\r\nPerson@example.com---a---JBSWY3DPEHPK3PXP\r\n\r\nperson@example.com---b---JBSWY3DPEHPK3PXP"))
	if got, want := fmt.Sprint(errDuplicate), "line 4 duplicates email from line 2"; got != want {
		t.Fatalf("parseChatGPTWebLoginInputs() error = %q, want %q", got, want)
	}
}

func TestHandlerShutdownCancelsLoginAndMutationManagersBeforeWaiting(t *testing.T) {
	loginTasks := newChatGPTWebLoginTaskManager()
	_, loginCtx, errLogin := loginTasks.create([]chatGPTWebLoginInput{{Line: 1, Email: "login@example.com"}})
	if errLogin != nil {
		t.Fatal(errLogin)
	}
	mutationTasks := newChatGPTWebMutationTaskManager()
	_, mutationCtx, errMutation := mutationTasks.create(chatGPTWebMutationTaskImport, []chatGPTWebMutationTaskResult{{File: "import.json", Status: chatGPTWebLoginResultQueued}})
	if errMutation != nil {
		t.Fatal(errMutation)
	}
	h := &Handler{chatGPTWebTasks: loginTasks, chatGPTWebMutationTasks: mutationTasks}
	shutdownCtx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if errShutdown := h.Shutdown(shutdownCtx); !errors.Is(errShutdown, context.DeadlineExceeded) {
		t.Fatalf("Shutdown() error = %v, want deadline", errShutdown)
	}
	select {
	case <-loginCtx.Done():
	default:
		t.Fatal("login task manager was not canceled")
	}
	select {
	case <-mutationCtx.Done():
	default:
		t.Fatal("mutation task manager was not canceled")
	}
	loginTasks.taskDone()
	mutationTasks.workers.Done()
}

func TestParseChatGPTWebLoginInputsManyBlankLines(t *testing.T) {
	t.Run("only newlines", func(t *testing.T) {
		data := bytes.Repeat([]byte{'\n'}, chatGPTWebLoginTaskMaxBodyBytes)
		_, errParse := parseChatGPTWebLoginInputs(data)
		if got, want := fmt.Sprint(errParse), "login file is empty"; got != want {
			t.Fatalf("parseChatGPTWebLoginInputs() error = %q, want %q", got, want)
		}
	})

	t.Run("account at body limit", func(t *testing.T) {
		const record = "person@example.com---password---JBSWY3DPEHPK3PXP"
		blankLines := chatGPTWebLoginTaskMaxBodyBytes - len(record)
		data := make([]byte, chatGPTWebLoginTaskMaxBodyBytes)
		for index := range data[:blankLines] {
			data[index] = '\n'
		}
		copy(data[blankLines:], record)

		inputs, errParse := parseChatGPTWebLoginInputs(data)
		if errParse != nil {
			t.Fatalf("parseChatGPTWebLoginInputs() error = %v", errParse)
		}
		if len(inputs) != 1 || inputs[0].Line != blankLines+1 || inputs[0].Email != "person@example.com" {
			t.Fatalf("inputs = %+v", inputs)
		}
		if got := cap(inputs); got > chatGPTWebLoginTaskMaxAccounts {
			t.Fatalf("cap(inputs) = %d, want at most %d", got, chatGPTWebLoginTaskMaxAccounts)
		}
	})
}

func TestParseChatGPTWebLoginInputsLineLengthBoundaries(t *testing.T) {
	const (
		prefix = "person@example.com---"
		suffix = "---JBSWY3DPEHPK3PXP"
	)
	password := strings.Repeat("p", chatGPTWebLoginTaskMaxLineBytes-len(prefix)-len(suffix))
	maxLine := prefix + password + suffix
	if got := len(maxLine); got != chatGPTWebLoginTaskMaxLineBytes {
		t.Fatalf("test line length = %d, want %d", got, chatGPTWebLoginTaskMaxLineBytes)
	}

	t.Run("maximum with CRLF", func(t *testing.T) {
		inputs, errParse := parseChatGPTWebLoginInputs([]byte("\r\n" + maxLine + "\r\n"))
		if errParse != nil {
			t.Fatalf("parseChatGPTWebLoginInputs() error = %v", errParse)
		}
		if len(inputs) != 1 || inputs[0].Line != 2 || inputs[0].Password != password {
			t.Fatalf("inputs = %+v", inputs)
		}
	})

	t.Run("one byte over maximum", func(t *testing.T) {
		_, errParse := parseChatGPTWebLoginInputs([]byte("\n\r\n" + maxLine + "x\r\n"))
		if got, want := fmt.Sprint(errParse), "line 3 is too long"; got != want {
			t.Fatalf("parseChatGPTWebLoginInputs() error = %q, want %q", got, want)
		}
	})

	t.Run("oversized blank line", func(t *testing.T) {
		const record = "person@example.com---password---JBSWY3DPEHPK3PXP"
		data := strings.Repeat(" ", chatGPTWebLoginTaskMaxLineBytes+1) + "\r\n" + record
		inputs, errParse := parseChatGPTWebLoginInputs([]byte(data))
		if errParse != nil {
			t.Fatalf("parseChatGPTWebLoginInputs() error = %v", errParse)
		}
		if len(inputs) != 1 || inputs[0].Line != 2 {
			t.Fatalf("inputs = %+v", inputs)
		}
	})
}

func TestParseChatGPTWebLoginInputsAccountLimitCountsNonEmptyLines(t *testing.T) {
	for _, test := range []struct {
		name      string
		accounts  int
		wantError string
	}{
		{name: "at limit", accounts: chatGPTWebLoginTaskMaxAccounts},
		{name: "over limit", accounts: chatGPTWebLoginTaskMaxAccounts + 1, wantError: "login file exceeds 500 accounts"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var data strings.Builder
			for index := 0; index < test.accounts; index++ {
				if index > 0 {
					data.WriteString("\r\n\r\n")
				}
				fmt.Fprintf(&data, "person-%03d@example.com---password---JBSWY3DPEHPK3PXP", index)
			}

			inputs, errParse := parseChatGPTWebLoginInputs([]byte(data.String()))
			if test.wantError != "" {
				if got := fmt.Sprint(errParse); got != test.wantError {
					t.Fatalf("parseChatGPTWebLoginInputs() error = %q, want %q", got, test.wantError)
				}
				return
			}
			if errParse != nil {
				t.Fatalf("parseChatGPTWebLoginInputs() error = %v", errParse)
			}
			if len(inputs) != chatGPTWebLoginTaskMaxAccounts || inputs[len(inputs)-1].Line != chatGPTWebLoginTaskMaxAccounts*2-1 {
				t.Fatalf("last input = %+v, total = %d", inputs[len(inputs)-1], len(inputs))
			}
		})
	}
}

func TestFindExistingChatGPTWebAuthRejectsDeterministicNameOwnedByAnotherAccount(t *testing.T) {
	_, manager, _ := newChatGPTWebManagementTestHandler(t, &chatGPTWebManagementTestExecutor{})
	const targetEmail = "target@example.com"
	fileName := chatGPTWebCredentialFileName(targetEmail)
	existing := &coreauth.Auth{
		ID:       fileName,
		FileName: fileName,
		Provider: chatgptwebauth.Provider,
		Metadata: map[string]any{
			"type":  chatgptwebauth.Provider,
			"email": "other@example.com",
		},
	}
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), existing); errRegister != nil {
		t.Fatal(errRegister)
	}

	if _, errFind := findExistingChatGPTWebAuth(t.Context(), manager, fileName, targetEmail); !errors.Is(errFind, errChatGPTWebCredentialIDOwned) {
		t.Fatalf("findExistingChatGPTWebAuth() error = %v, want credential ID conflict", errFind)
	}
}

func TestChatGPTWebLoginTaskPersistsSafeSuccessAndTerminalFailure(t *testing.T) {
	const (
		passwordSecret = "password-secret"
		totpSecret     = "JBSWY3DPEHPK3PXP"
		tokenSecret    = "access-token-secret"
		refreshSecret  = "refresh-token-secret"
		idTokenSecret  = "id-token-secret"
		cookieSecret   = "cookie-secret"
	)
	executor := &chatGPTWebManagementTestExecutor{}
	executor.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		state := chatgptwebauth.LifecycleActive
		reason := ""
		var errLogin error
		if strings.HasPrefix(input.Email, "dead") {
			state = chatgptwebauth.LifecycleDead
			reason = "account_deleted"
			errLogin = &chatgptwebauth.AuthError{
				Code:           reason,
				State:          state,
				LifecycleState: state,
				StatusCode:     http.StatusForbidden,
				Terminal:       true,
				Message:        "account is deleted",
			}
		}
		return &chatgptwebauth.Credential{
			Type:               chatgptwebauth.Provider,
			Email:              input.Email,
			Password:           input.Password,
			TOTPSecret:         input.TOTPSecret,
			AccessToken:        tokenSecret,
			RefreshToken:       refreshSecret,
			IDToken:            idTokenSecret,
			Cookies:            []chatgptwebauth.Cookie{{Name: "session", Value: cookieSecret}},
			Expired:            time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
			LifecycleState:     state,
			LifecycleReason:    reason,
			LifecycleUpdatedAt: time.Now().UTC().Format(time.RFC3339),
			LastLoginAt:        time.Now().UTC().Format(time.RFC3339),
		}, errLogin
	}
	h, manager, authDir := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	body := "good@example.com---" + passwordSecret + "---" + totpSecret + "\n" +
		"dead@example.com---" + passwordSecret + "---" + totpSecret + "\n"

	create := performChatGPTWebManagementRequest(t, router, http.MethodPost, "/chatgpt-web/login-tasks", body)
	if create.Code != http.StatusAccepted {
		t.Fatalf("create status = %d, body=%s", create.Code, create.Body.String())
	}
	assertChatGPTWebManagementSecretsAbsent(t, create.Body.String(), passwordSecret, totpSecret, tokenSecret, refreshSecret, idTokenSecret, cookieSecret)
	var created chatGPTWebLoginTask
	decodeChatGPTWebManagementResponse(t, create, &created)
	task := waitForChatGPTWebLoginTask(t, router, created.ID)
	if task.State != chatGPTWebLoginTaskCompletedWithErrors || task.Succeeded != 1 || task.Failed != 1 || task.Processed != 2 {
		t.Fatalf("task = %+v", task)
	}
	if len(task.Results) != 2 || task.Results[0].Status != chatGPTWebLoginResultSuccess || task.Results[1].ErrorCategory != "account_deleted" {
		t.Fatalf("results = %+v", task.Results)
	}
	if task.Results[0].AuthIndex == "" || task.Results[1].AuthIndex == "" || task.Results[1].LifecycleState != string(chatgptwebauth.LifecycleDead) {
		t.Fatalf("result identities = %+v", task.Results)
	}

	finalRecorder := performChatGPTWebManagementRequest(t, router, http.MethodGet, "/chatgpt-web/login-tasks/"+created.ID, "")
	assertChatGPTWebManagementSecretsAbsent(t, finalRecorder.Body.String(), passwordSecret, totpSecret, tokenSecret, refreshSecret, idTokenSecret, cookieSecret)
	auths := manager.List()
	if len(auths) != 2 {
		t.Fatalf("registered auth count = %d, want 2", len(auths))
	}
	for _, auth := range auths {
		if uid := coreauth.ChatGPTWebCredentialUID(auth); uid == "" {
			t.Fatalf("registered auth %q has no credential UID", auth.ID)
		}
	}
	for _, result := range task.Results {
		path := filepath.Join(authDir, result.Name)
		info, errStat := os.Stat(path)
		if errStat != nil {
			t.Fatalf("stat %s: %v", path, errStat)
		}
		if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
			t.Fatalf("%s mode = %o, want 600", path, info.Mode().Perm())
		}
	}
}

func TestChatGPTWebLoginTaskPropagatesRequestInfoToPostAuthHook(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	executor.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		return activeChatGPTWebManagementTestCredential(input), nil
	}
	h, _, _ := newChatGPTWebManagementTestHandler(t, executor)
	hookInfo := make(chan *coreauth.RequestInfo, 1)
	h.SetPostAuthHook(func(ctx context.Context, _ *coreauth.Auth) error {
		hookInfo <- coreauth.GetRequestInfo(ctx)
		return nil
	})
	router := chatGPTWebManagementTestRouter(h)
	request := httptest.NewRequest(
		http.MethodPost,
		"/chatgpt-web/login-tasks?tenant=test-tenant",
		bytes.NewBufferString("request-info@example.com---password---JBSWY3DPEHPK3PXP"),
	)
	request.Header.Set("Content-Type", "text/plain")
	request.Header.Set("X-Tenant", "test-header")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("start task status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	var task chatGPTWebLoginTask
	decodeChatGPTWebManagementResponse(t, recorder, &task)
	completed := waitForChatGPTWebLoginTask(t, router, task.ID)
	if completed.State != chatGPTWebLoginTaskCompleted {
		t.Fatalf("task state = %q, want completed", completed.State)
	}
	select {
	case info := <-hookInfo:
		if info == nil || info.Query.Get("tenant") != "test-tenant" || info.Headers.Get("X-Tenant") != "test-header" {
			t.Fatalf("post-auth request info = %+v", info)
		}
	case <-time.After(time.Second):
		t.Fatal("post-auth hook did not receive request info")
	}
}

func TestChatGPTWebLoginTaskCancellationAndActiveEmailConflict(t *testing.T) {
	started := make(chan struct{}, 1)
	executor := &chatGPTWebManagementTestExecutor{}
	executor.loginFn = func(ctx context.Context, _ chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	h, _, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	body := "busy@example.com---password---JBSWY3DPEHPK3PXP"
	create := performChatGPTWebManagementRequest(t, router, http.MethodPost, "/chatgpt-web/login-tasks", body)
	if create.Code != http.StatusAccepted {
		t.Fatalf("create status = %d, body=%s", create.Code, create.Body.String())
	}
	var task chatGPTWebLoginTask
	decodeChatGPTWebManagementResponse(t, create, &task)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("login did not start")
	}

	conflict := performChatGPTWebManagementRequest(t, router, http.MethodPost, "/chatgpt-web/login-tasks", body)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d, body=%s", conflict.Code, conflict.Body.String())
	}
	cancel := performChatGPTWebManagementRequest(t, router, http.MethodDelete, "/chatgpt-web/login-tasks/"+task.ID, "")
	if cancel.Code != http.StatusOK {
		t.Fatalf("cancel status = %d, body=%s", cancel.Code, cancel.Body.String())
	}
	completed := waitForChatGPTWebLoginTask(t, router, task.ID)
	if completed.State != chatGPTWebLoginTaskCanceled || completed.Canceled != 1 || completed.Results[0].Status != chatGPTWebLoginResultCanceled {
		t.Fatalf("canceled task = %+v", completed)
	}
}

func TestChatGPTWebLoginTaskCancellationWinsOverTransportError(t *testing.T) {
	started := make(chan struct{}, 1)
	executor := &chatGPTWebManagementTestExecutor{}
	executor.loginFn = func(ctx context.Context, _ chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		started <- struct{}{}
		<-ctx.Done()
		return nil, errors.New("transport closed after cancellation")
	}
	h, _, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	create := performChatGPTWebManagementRequest(t, router, http.MethodPost, "/chatgpt-web/login-tasks", "canceled-error@example.com---password---JBSWY3DPEHPK3PXP")
	if create.Code != http.StatusAccepted {
		t.Fatalf("create status = %d, body=%s", create.Code, create.Body.String())
	}
	var task chatGPTWebLoginTask
	decodeChatGPTWebManagementResponse(t, create, &task)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("login did not start")
	}
	cancel := performChatGPTWebManagementRequest(t, router, http.MethodDelete, "/chatgpt-web/login-tasks/"+task.ID, "")
	if cancel.Code != http.StatusOK {
		t.Fatalf("cancel status = %d, body=%s", cancel.Code, cancel.Body.String())
	}
	completed := waitForChatGPTWebLoginTask(t, router, task.ID)
	if completed.State != chatGPTWebLoginTaskCanceled || completed.Canceled != 1 || completed.Failed != 0 {
		t.Fatalf("canceled task = %+v", completed)
	}
	if completed.Results[0].Status != chatGPTWebLoginResultCanceled || completed.Results[0].ErrorCategory != "canceled" {
		t.Fatalf("canceled result = %+v", completed.Results[0])
	}
}

func TestChatGPTWebLoginTasksShareGlobalConcurrencyLimit(t *testing.T) {
	var running atomic.Int32
	var maximum atomic.Int32
	release := make(chan struct{})
	executor := &chatGPTWebManagementTestExecutor{}
	executor.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		current := running.Add(1)
		for {
			previous := maximum.Load()
			if current <= previous || maximum.CompareAndSwap(previous, current) {
				break
			}
		}
		<-release
		running.Add(-1)
		return activeChatGPTWebManagementTestCredential(input), nil
	}
	h, _, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	taskIDs := make([]string, 0, 2)
	for taskIndex := range 2 {
		var lines strings.Builder
		for accountIndex := range 4 {
			fmt.Fprintf(&lines, "user-%d-%d@example.com---password---JBSWY3DPEHPK3PXP\n", taskIndex, accountIndex)
		}
		recorder := performChatGPTWebManagementRequest(t, router, http.MethodPost, "/chatgpt-web/login-tasks", lines.String())
		if recorder.Code != http.StatusAccepted {
			t.Fatalf("create status = %d, body=%s", recorder.Code, recorder.Body.String())
		}
		var task chatGPTWebLoginTask
		decodeChatGPTWebManagementResponse(t, recorder, &task)
		taskIDs = append(taskIDs, task.ID)
	}
	deadline := time.Now().Add(2 * time.Second)
	for maximum.Load() < chatGPTWebLoginTaskWorkers && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := maximum.Load(); got != chatGPTWebLoginTaskWorkers {
		t.Fatalf("maximum concurrent logins = %d, want %d", got, chatGPTWebLoginTaskWorkers)
	}
	close(release)
	for _, taskID := range taskIDs {
		task := waitForChatGPTWebLoginTask(t, router, taskID)
		if task.State != chatGPTWebLoginTaskCompleted || task.Succeeded != 4 {
			t.Fatalf("task = %+v", task)
		}
	}
}

func TestChatGPTWebLoginTaskAcceptsMultipartFile(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	executor.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		return activeChatGPTWebManagementTestCredential(input), nil
	}
	h, _, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, errPart := writer.CreateFormFile("file", "accounts.txt")
	if errPart != nil {
		t.Fatal(errPart)
	}
	if _, errWrite := part.Write([]byte("multipart@example.com---password---JBSWY3DPEHPK3PXP")); errWrite != nil {
		t.Fatal(errWrite)
	}
	if errClose := writer.Close(); errClose != nil {
		t.Fatal(errClose)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/chatgpt-web/login-tasks", &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("create status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	var task chatGPTWebLoginTask
	decodeChatGPTWebManagementResponse(t, recorder, &task)
	task = waitForChatGPTWebLoginTask(t, router, task.ID)
	if task.Succeeded != 1 {
		t.Fatalf("task = %+v", task)
	}
}

func TestChatGPTWebLoginTaskRejectsOversizedMultipartFile(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, _, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, errPart := writer.CreateFormFile("file", "accounts.txt")
	if errPart != nil {
		t.Fatal(errPart)
	}
	if _, errWrite := part.Write(bytes.Repeat([]byte("x"), chatGPTWebLoginTaskMaxBodyBytes)); errWrite != nil {
		t.Fatal(errWrite)
	}
	if errClose := writer.Close(); errClose != nil {
		t.Fatal(errClose)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/chatgpt-web/login-tasks", &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusRequestEntityTooLarge, recorder.Body.String())
	}
}

func TestChatGPTWebLoginTaskPreservesExistingSettingsAndWritesOnce(t *testing.T) {
	const explicitProxy = "socks5h://user:proxy-secret@127.0.0.1:1080"
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, store := newChatGPTWebManagementCountingTestHandler(t, executor)
	fileName := chatGPTWebCredentialFileName("existing@example.com")
	initialCredential := activeChatGPTWebManagementTestCredential(chatgptwebauth.LoginInput{
		Email:      "existing@example.com",
		Password:   "old-password",
		TOTPSecret: "OLDTOTPSECRET",
	})
	initialCredential.AccessToken = "old-access-token"
	initialCredential.DeviceID = "stable-device"
	metadata := map[string]any{
		"priority":       -1,
		"proxy_url":      explicitProxy,
		"custom_setting": "keep-me",
		"headers":        map[string]any{"X-Custom": "value"},
	}
	initialCredential.ApplyToMetadata(metadata)
	initial := &coreauth.Auth{
		ID:         fileName,
		Provider:   chatgptwebauth.Provider,
		FileName:   fileName,
		Label:      initialCredential.Email,
		ProxyURL:   explicitProxy,
		Disabled:   true,
		Attributes: map[string]string{"priority": "-1", "custom": "keep-me"},
		Metadata:   metadata,
		Status:     coreauth.StatusDisabled,
	}
	if _, errRegister := manager.Register(t.Context(), initial); errRegister != nil {
		t.Fatal(errRegister)
	}
	store.saves.Store(0)

	var received chatgptwebauth.LoginInput
	executor.mu.Lock()
	executor.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		received = input
		credential := activeChatGPTWebManagementTestCredential(input)
		credential.AccessToken = "new-access-token"
		return credential, nil
	}
	executor.mu.Unlock()
	router := chatGPTWebManagementTestRouter(h)
	body := "existing@example.com---new-password---JBSWY3DPEHPK3PXP"
	create := performChatGPTWebManagementRequest(t, router, http.MethodPost, "/chatgpt-web/login-tasks", body)
	if create.Code != http.StatusAccepted {
		t.Fatalf("create status = %d, body=%s", create.Code, create.Body.String())
	}
	var task chatGPTWebLoginTask
	decodeChatGPTWebManagementResponse(t, create, &task)
	task = waitForChatGPTWebLoginTask(t, router, task.ID)
	if task.State != chatGPTWebLoginTaskCompleted || store.saves.Load() != 1 {
		t.Fatalf("task = %+v, saves = %d", task, store.saves.Load())
	}
	if received.ProxyURL != explicitProxy || received.Credential == nil || received.Credential.DeviceID != "stable-device" {
		t.Fatalf("login input proxy/persona source = %q/%+v", received.ProxyURL, received.Credential)
	}
	updated, ok := manager.GetByID(fileName)
	if !ok {
		t.Fatal("updated credential not installed")
	}
	if updated.ProxyURL != explicitProxy || updated.Attributes["priority"] != "-1" || updated.Attributes["custom"] != "keep-me" {
		t.Fatalf("updated settings = proxy %q attributes %+v", updated.ProxyURL, updated.Attributes)
	}
	if !updated.Disabled || updated.Status != coreauth.StatusDisabled {
		t.Fatalf("updated disabled status = %t/%q", updated.Disabled, updated.Status)
	}
	if updated.Metadata["custom_setting"] != "keep-me" || updated.Metadata["priority"] != -1 || updated.Metadata["access_token"] != "new-access-token" || updated.Metadata["password"] != "new-password" {
		t.Fatalf("updated metadata = %+v", updated.Metadata)
	}

	store.saves.Store(0)
	const unsafeLifecycleReason = "tokenLikeABC123"
	executor.mu.Lock()
	executor.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		credential := *input.Credential
		credential.Password = input.Password
		credential.TOTPSecret = input.TOTPSecret
		credential.AccessToken = "should-not-replace-token"
		credential.LifecycleState = chatgptwebauth.LifecycleReauthRequired
		credential.LifecycleReason = unsafeLifecycleReason
		credential.LifecycleUpdatedAt = time.Now().UTC().Format(time.RFC3339)
		return &credential, &chatgptwebauth.AuthError{
			Code:           unsafeLifecycleReason,
			State:          chatgptwebauth.LifecycleReauthRequired,
			LifecycleState: chatgptwebauth.LifecycleReauthRequired,
			StatusCode:     http.StatusUnauthorized,
			Terminal:       true,
			Message:        "upstream echoed bad-password and should-not-replace-token",
		}
	}
	executor.mu.Unlock()
	create = performChatGPTWebManagementRequest(t, router, http.MethodPost, "/chatgpt-web/login-tasks", "existing@example.com---bad-password---BADTOTPSECRET")
	var failedTask chatGPTWebLoginTask
	decodeChatGPTWebManagementResponse(t, create, &failedTask)
	failedTask = waitForChatGPTWebLoginTask(t, router, failedTask.ID)
	if failedTask.State != chatGPTWebLoginTaskCompletedWithErrors || store.saves.Load() != 1 {
		t.Fatalf("failed task = %+v, saves = %d", failedTask, store.saves.Load())
	}
	assertChatGPTWebManagementSecretsAbsent(t, mustMarshalChatGPTWebTask(t, failedTask), "bad-password", "BADTOTPSECRET", "should-not-replace-token", unsafeLifecycleReason)
	updated, _ = manager.GetByID(fileName)
	if updated.Metadata["password"] != "new-password" || updated.Metadata["access_token"] != "new-access-token" || updated.LifecycleState() != coreauth.LifecycleStateReauthRequired ||
		updated.Metadata["lifecycle_reason"] != "authentication_failed" || updated.StatusMessage != "authentication_failed" ||
		!updated.Disabled || updated.Status != coreauth.StatusDisabled {
		t.Fatalf("terminal failure overwrote credential = %+v", updated.Metadata)
	}
}

func TestChatGPTWebLoginTaskRecoversInvalidExistingCredential(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, _ := newChatGPTWebManagementCountingTestHandler(t, executor)
	fileName := chatGPTWebCredentialFileName("recover@example.com")
	existing := &coreauth.Auth{
		ID:       fileName,
		Provider: chatgptwebauth.Provider,
		FileName: fileName,
		Label:    "recover@example.com",
		Metadata: map[string]any{
			"type":    chatgptwebauth.Provider,
			"email":   "recover@example.com",
			"cookies": "invalid-cookie-shape",
		},
	}
	if _, errRegister := manager.Register(t.Context(), existing); errRegister != nil {
		t.Fatal(errRegister)
	}

	executor.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		if input.Credential != nil {
			t.Fatalf("invalid existing credential was passed to Login: %+v", input.Credential)
		}
		return activeChatGPTWebManagementTestCredential(input), nil
	}
	router := chatGPTWebManagementTestRouter(h)
	create := performChatGPTWebManagementRequest(
		t,
		router,
		http.MethodPost,
		"/chatgpt-web/login-tasks",
		"recover@example.com---new-password---JBSWY3DPEHPK3PXP",
	)
	if create.Code != http.StatusAccepted {
		t.Fatalf("create status = %d, body=%s", create.Code, create.Body.String())
	}
	var task chatGPTWebLoginTask
	decodeChatGPTWebManagementResponse(t, create, &task)
	task = waitForChatGPTWebLoginTask(t, router, task.ID)
	if task.Succeeded != 1 || len(task.Results) != 1 || task.Results[0].Status != chatGPTWebLoginResultSuccess {
		t.Fatalf("task = %+v", task)
	}
	current, ok := manager.GetByID(fileName)
	if !ok || current == nil {
		t.Fatal("recovered credential is missing")
	}
	credential, errCredential := chatgptwebauth.ParseCredential(current.Metadata)
	if errCredential != nil {
		t.Fatalf("recovered credential is invalid: %v", errCredential)
	}
	if credential.Password != "new-password" || credential.AccessToken == "" {
		t.Fatalf("recovered credential = %+v", credential)
	}
}

func TestChatGPTWebLoginTaskDoesNotOverwriteCredentialCreatedDuringLogin(t *testing.T) {
	loginStarted := make(chan struct{})
	continueLogin := make(chan struct{})
	executor := &chatGPTWebManagementTestExecutor{}
	executor.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(loginStarted)
		<-continueLogin
		return activeChatGPTWebManagementTestCredential(input), nil
	}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	const email = "race@example.com"
	create := performChatGPTWebManagementRequest(t, router, http.MethodPost, "/chatgpt-web/login-tasks", email+"---password---JBSWY3DPEHPK3PXP")
	var task chatGPTWebLoginTask
	decodeChatGPTWebManagementResponse(t, create, &task)
	select {
	case <-loginStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("login did not start")
	}

	fileName := chatGPTWebCredentialFileName(email)
	inserted := &coreauth.Auth{
		ID:       fileName,
		Provider: "codex",
		FileName: fileName,
		Label:    "concurrent credential",
		Metadata: map[string]any{"type": "codex"},
	}
	if _, errRegister := manager.Register(t.Context(), inserted); errRegister != nil {
		t.Fatalf("register concurrent credential: %v", errRegister)
	}
	close(continueLogin)

	task = waitForChatGPTWebLoginTask(t, router, task.ID)
	if task.State != chatGPTWebLoginTaskCompletedWithErrors || task.Results[0].ErrorCategory != "credential_changed" || task.Results[0].HTTPStatus != http.StatusConflict {
		t.Fatalf("task = %+v", task)
	}
	current, ok := manager.GetByID(fileName)
	if !ok || current.Provider != "codex" || current.Label != inserted.Label {
		t.Fatalf("concurrent credential was replaced: %+v", current)
	}
}

func TestChatGPTWebLoginTaskReportsUncertainPersistenceWithoutLeakingCause(t *testing.T) {
	const secretCause = "storage-token-secret"
	executor := &chatGPTWebManagementTestExecutor{}
	executor.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		return activeChatGPTWebManagementTestCredential(input), nil
	}
	h, manager, store := newChatGPTWebManagementCountingTestHandler(t, executor)
	store.saveIfAbsentErr = coreauth.NewSaveOutcomeError(
		coreauth.SaveOutcomeUncertain,
		errors.Join(context.Canceled, errors.New(secretCause)),
	)
	router := chatGPTWebManagementTestRouter(h)
	create := performChatGPTWebManagementRequest(t, router, http.MethodPost, "/chatgpt-web/login-tasks", "uncertain@example.com---password---JBSWY3DPEHPK3PXP")
	var task chatGPTWebLoginTask
	decodeChatGPTWebManagementResponse(t, create, &task)
	completed := waitForChatGPTWebLoginTask(t, router, task.ID)
	if completed.State != chatGPTWebLoginTaskCompletedWithErrors || len(completed.Results) != 1 {
		t.Fatalf("task = %+v", completed)
	}
	result := completed.Results[0]
	if result.Status != chatGPTWebLoginResultFailed || result.ErrorCategory != "persist_uncertain" || result.HTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("result = %+v", result)
	}
	if strings.Contains(mustMarshalChatGPTWebTask(t, completed), secretCause) {
		t.Fatalf("task leaked persistence error: %+v", completed)
	}
	if _, exists := manager.GetByID(chatGPTWebCredentialFileName("uncertain@example.com")); exists {
		t.Fatal("uncertain credential was installed")
	}
}

func TestChatGPTWebLoginTaskDoesNotTreatCommittedPostAuthHookAsInstalled(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	executor.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		return activeChatGPTWebManagementTestCredential(input), nil
	}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	h.SetPostAuthHook(func(context.Context, *coreauth.Auth) error {
		return coreauth.NewSaveOutcomeError(coreauth.SaveOutcomeCommitted, errors.New("hook cleanup warning"))
	})
	router := chatGPTWebManagementTestRouter(h)
	create := performChatGPTWebManagementRequest(t, router, http.MethodPost, "/chatgpt-web/login-tasks", "hook-commit@example.com---password---JBSWY3DPEHPK3PXP")
	var task chatGPTWebLoginTask
	decodeChatGPTWebManagementResponse(t, create, &task)
	completed := waitForChatGPTWebLoginTask(t, router, task.ID)
	if completed.State != chatGPTWebLoginTaskCompletedWithErrors || len(completed.Results) != 1 {
		t.Fatalf("task = %+v", completed)
	}
	result := completed.Results[0]
	if result.Status != chatGPTWebLoginResultFailed || result.ErrorCategory != "persist_uncertain" || result.HTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("result = %+v", result)
	}
	if _, exists := manager.GetByID(chatGPTWebCredentialFileName("hook-commit@example.com")); exists {
		t.Fatal("credential was installed after the post-auth hook stopped registration")
	}
}

func TestChatGPTWebLoginTaskDoesNotOverwriteUnloadedCredentialFile(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	executor.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		return activeChatGPTWebManagementTestCredential(input), nil
	}
	h, manager, authDir := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	const (
		email            = "unloaded@example.com"
		existingContents = `{"type":"codex","access_token":"existing-token"}`
	)
	fileName := chatGPTWebCredentialFileName(email)
	path := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(path, []byte(existingContents), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}

	create := performChatGPTWebManagementRequest(t, router, http.MethodPost, "/chatgpt-web/login-tasks", email+"---password---JBSWY3DPEHPK3PXP")
	var task chatGPTWebLoginTask
	decodeChatGPTWebManagementResponse(t, create, &task)
	task = waitForChatGPTWebLoginTask(t, router, task.ID)
	if task.State != chatGPTWebLoginTaskCompletedWithErrors || task.Results[0].ErrorCategory != "credential_id_conflict" || task.Results[0].HTTPStatus != http.StatusConflict {
		t.Fatalf("task = %+v", task)
	}
	if _, exists := manager.GetByID(fileName); exists {
		t.Fatal("unloaded credential was installed into the manager")
	}
	data, errRead := os.ReadFile(path)
	if errRead != nil || string(data) != existingContents {
		t.Fatalf("unloaded credential changed: content=%q error=%v", data, errRead)
	}
}

func TestChatGPTWebLoginTaskLateCancelCompletesCommittedCredential(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, store := newChatGPTWebManagementCountingTestHandler(t, executor)
	executor.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		return activeChatGPTWebManagementTestCredential(input), nil
	}
	store.block.Store(true)
	router := chatGPTWebManagementTestRouter(h)
	create := performChatGPTWebManagementRequest(t, router, http.MethodPost, "/chatgpt-web/login-tasks", "commit@example.com---password---JBSWY3DPEHPK3PXP")
	var task chatGPTWebLoginTask
	decodeChatGPTWebManagementResponse(t, create, &task)
	select {
	case <-store.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("credential did not enter persistence")
	}
	cancel := performChatGPTWebManagementRequest(t, router, http.MethodDelete, "/chatgpt-web/login-tasks/"+task.ID, "")
	if cancel.Code != http.StatusOK {
		t.Fatalf("cancel status = %d, body=%s", cancel.Code, cancel.Body.String())
	}
	close(store.release)
	completed := waitForChatGPTWebLoginTask(t, router, task.ID)
	if completed.State != chatGPTWebLoginTaskCompleted || completed.Succeeded != 1 || completed.Canceled != 0 {
		t.Fatalf("late-canceled task = %+v", completed)
	}
	if _, ok := manager.GetByID(chatGPTWebCredentialFileName("commit@example.com")); !ok {
		t.Fatal("committed credential was not installed")
	}
}

func TestChatGPTWebLoginTaskShutdownClassifiesPersistenceOutcome(t *testing.T) {
	tests := []struct {
		name             string
		email            string
		existing         bool
		rollbackOnCancel bool
		wantState        string
		wantCanceled     int
		wantFailed       int
		wantCategory     string
	}{
		{
			name:         "unknown cancellation is uncertain",
			email:        "commit-shutdown-uncertain@example.com",
			wantState:    chatGPTWebLoginTaskCompletedWithErrors,
			wantFailed:   1,
			wantCategory: "persist_uncertain",
		},
		{
			name:         "existing update cancellation is uncertain",
			email:        "commit-shutdown-existing@example.com",
			existing:     true,
			wantState:    chatGPTWebLoginTaskCompletedWithErrors,
			wantFailed:   1,
			wantCategory: "persist_uncertain",
		},
		{
			name:             "explicit rollback is canceled",
			email:            "commit-shutdown-rolled-back@example.com",
			rollbackOnCancel: true,
			wantState:        chatGPTWebLoginTaskCanceled,
			wantCanceled:     1,
			wantCategory:     "canceled",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := &chatGPTWebManagementTestExecutor{}
			executor.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
				return activeChatGPTWebManagementTestCredential(input), nil
			}
			h, manager, store := newChatGPTWebManagementCountingTestHandler(t, executor)
			if test.existing {
				credential := activeChatGPTWebManagementTestCredential(chatgptwebauth.LoginInput{
					Email: test.email, Password: "old-password", TOTPSecret: "JBSWY3DPEHPK3PXP",
				})
				if _, errPersist := h.persistChatGPTWebLoginCredential(t.Context(), manager, chatGPTWebCredentialFileName(test.email), credential, nil, nil); errPersist != nil {
					t.Fatalf("persist existing credential: %v", errPersist)
				}
			}
			store.block.Store(true)
			store.respectContext.Store(true)
			store.rollbackOnCancel.Store(test.rollbackOnCancel)
			router := chatGPTWebManagementTestRouter(h)
			create := performChatGPTWebManagementRequest(
				t,
				router,
				http.MethodPost,
				"/chatgpt-web/login-tasks",
				test.email+"---password---JBSWY3DPEHPK3PXP",
			)
			var task chatGPTWebLoginTask
			decodeChatGPTWebManagementResponse(t, create, &task)
			select {
			case <-store.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("credential did not enter persistence")
			}

			shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if errShutdown := h.Shutdown(shutdownCtx); errShutdown != nil {
				t.Fatalf("Shutdown() error = %v", errShutdown)
			}
			completed, ok := h.chatGPTWebTaskManager().get(task.ID)
			if !ok || completed.State != test.wantState || completed.Canceled != test.wantCanceled || completed.Failed != test.wantFailed {
				t.Fatalf("shutdown task = %+v, exists=%v", completed, ok)
			}
			if len(completed.Results) != 1 || completed.Results[0].ErrorCategory != test.wantCategory {
				t.Fatalf("shutdown result = %+v", completed.Results)
			}
			_, exists := manager.GetByID(chatGPTWebCredentialFileName(test.email))
			if exists != test.existing {
				t.Fatalf("credential existence after persistence failure = %v, want %v", exists, test.existing)
			}
		})
	}
}

func TestChatGPTWebLoginTaskShutdownRejectsCommitAfterLoginReturns(t *testing.T) {
	started := make(chan struct{})
	releaseLogin := make(chan struct{})
	executor := &chatGPTWebManagementTestExecutor{}
	executor.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(started)
		<-releaseLogin
		return activeChatGPTWebManagementTestCredential(input), nil
	}
	h, manager, store := newChatGPTWebManagementCountingTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	create := performChatGPTWebManagementRequest(t, router, http.MethodPost, "/chatgpt-web/login-tasks", "shutdown-before-commit@example.com---password---JBSWY3DPEHPK3PXP")
	var task chatGPTWebLoginTask
	decodeChatGPTWebManagementResponse(t, create, &task)
	<-started

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		shutdownDone <- h.Shutdown(shutdownCtx)
	}()
	waitForChatGPTWebManagementCondition(t, time.Second, func() bool {
		taskManager := h.chatGPTWebTaskManager()
		taskManager.mu.Lock()
		defer taskManager.mu.Unlock()
		return taskManager.closed
	})
	close(releaseLogin)
	if errShutdown := <-shutdownDone; errShutdown != nil {
		t.Fatalf("Shutdown() error = %v", errShutdown)
	}
	completed, ok := h.chatGPTWebTaskManager().get(task.ID)
	if !ok || completed.State != chatGPTWebLoginTaskCanceled || completed.Canceled != 1 {
		t.Fatalf("shutdown task = %+v, exists=%v", completed, ok)
	}
	if got := store.saves.Load(); got != 0 {
		t.Fatalf("store saves = %d, want 0", got)
	}
	if _, exists := manager.GetByID(chatGPTWebCredentialFileName("shutdown-before-commit@example.com")); exists {
		t.Fatal("credential was installed after shutdown")
	}
}

func TestStartChatGPTWebLoginTaskReturnsServiceUnavailableAfterShutdown(t *testing.T) {
	h, _, _ := newChatGPTWebManagementTestHandler(t, &chatGPTWebManagementTestExecutor{})
	router := chatGPTWebManagementTestRouter(h)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if errShutdown := h.Shutdown(shutdownCtx); errShutdown != nil {
		t.Fatalf("Shutdown() error = %v", errShutdown)
	}

	recorder := performChatGPTWebManagementRequest(
		t,
		router,
		http.MethodPost,
		"/chatgpt-web/login-tasks",
		"closed@example.com---password---JBSWY3DPEHPK3PXP",
	)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusServiceUnavailable, recorder.Body.String())
	}
}

func TestChatGPTWebTaskLookupAndCancellationRejectNilHandler(t *testing.T) {
	router := chatGPTWebManagementTestRouter((*Handler)(nil))
	paths := []string{
		"/chatgpt-web/login-tasks/missing",
		"/chatgpt-web/import-tasks/missing",
		"/chatgpt-web/conversion-tasks/missing",
	}
	for _, path := range paths {
		for _, method := range []string{http.MethodGet, http.MethodDelete} {
			recorder := performChatGPTWebManagementRequest(t, router, method, path, "")
			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("%s %s status = %d, want %d; body=%s", method, path, recorder.Code, http.StatusServiceUnavailable, recorder.Body.String())
			}
		}
	}
}

func TestChatGPTWebLoginTaskManagerShutdownCancelsActiveTask(t *testing.T) {
	started := make(chan struct{}, 1)
	executor := &chatGPTWebManagementTestExecutor{}
	executor.loginFn = func(ctx context.Context, _ chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		started <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	h, _, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	create := performChatGPTWebManagementRequest(t, router, http.MethodPost, "/chatgpt-web/login-tasks", "shutdown@example.com---password---JBSWY3DPEHPK3PXP")
	var task chatGPTWebLoginTask
	decodeChatGPTWebManagementResponse(t, create, &task)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("login did not start")
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if errShutdown := h.Shutdown(shutdownCtx); errShutdown != nil {
		t.Fatalf("Shutdown() error = %v", errShutdown)
	}
	completed, ok := h.chatGPTWebTaskManager().get(task.ID)
	if !ok || completed.State != chatGPTWebLoginTaskCanceled {
		t.Fatalf("shutdown task = %+v, exists=%v", completed, ok)
	}
}

func TestChatGPTWebLoginTaskFinishCancelsTaskContext(t *testing.T) {
	manager := newChatGPTWebLoginTaskManager()
	task, taskCtx, errCreate := manager.create([]chatGPTWebLoginInput{{
		Line:       1,
		Email:      "finished@example.com",
		Password:   "password",
		TOTPSecret: "JBSWY3DPEHPK3PXP",
	}})
	if errCreate != nil {
		t.Fatal(errCreate)
	}
	defer manager.taskDone()
	if !manager.start(task.ID) || !manager.markRunning(task.ID, 0) {
		t.Fatal("task did not enter running state")
	}
	manager.setResult(task.ID, 0, chatGPTWebLoginTaskResult{
		Line:   1,
		Email:  "finished@example.com",
		Status: chatGPTWebLoginResultSuccess,
	})
	manager.finish(task.ID, false)
	select {
	case <-taskCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("completed task context was not canceled")
	}
}

func TestClassifyChatGPTWebManagementErrorUsesFixedMessage(t *testing.T) {
	category, message, status, lifecycle := classifyChatGPTWebManagementError(&chatgptwebauth.AuthError{
		Code:           "invalid_password",
		State:          chatgptwebauth.LifecycleReauthRequired,
		LifecycleState: chatgptwebauth.LifecycleReauthRequired,
		StatusCode:     http.StatusUnauthorized,
		Message:        "password-secret token-secret proxy-secret",
	})
	if category != "invalid_password" || message != "password was rejected" || status != http.StatusUnprocessableEntity || lifecycle != string(chatgptwebauth.LifecycleReauthRequired) {
		t.Fatalf("classification = %q/%q/%d/%q", category, message, status, lifecycle)
	}

	category, message, _, _ = classifyChatGPTWebManagementError(&chatgptwebauth.AuthError{
		Code:    "tokenLikeABC123",
		Message: "upstream token-shaped error",
	})
	if category != "authentication_failed" || message != "chatgpt web authentication failed" {
		t.Fatalf("unknown classification = %q/%q", category, message)
	}

	category, message, status, lifecycle = classifyChatGPTWebManagementError(&chatgptwebauth.AuthError{
		Code:           "access_denied",
		State:          chatgptwebauth.LifecycleInteractionRequired,
		LifecycleState: chatgptwebauth.LifecycleInteractionRequired,
		StatusCode:     http.StatusForbidden,
	})
	if category != "access_denied" || message != "additional user verification is required" || status != http.StatusConflict || lifecycle != string(chatgptwebauth.LifecycleInteractionRequired) {
		t.Fatalf("access denied classification = %q/%q/%d/%q", category, message, status, lifecycle)
	}

	for _, upstreamStatus := range []int{http.StatusNotFound, http.StatusInternalServerError, http.StatusServiceUnavailable} {
		_, _, status, _ = classifyChatGPTWebManagementError(&chatgptwebauth.AuthError{
			Code:       "authentication_failed",
			StatusCode: upstreamStatus,
		})
		if status != http.StatusBadGateway {
			t.Fatalf("upstream status %d mapped to %d, want %d", upstreamStatus, status, http.StatusBadGateway)
		}
	}

	for _, test := range []struct {
		name    string
		code    string
		message string
		status  int
	}{
		{name: "rate limited", code: "rate_limited", message: "credential was rate limited", status: http.StatusTooManyRequests},
		{name: "credential cooldown", code: "credential_cooldown", message: "credential is cooling down", status: http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			category, message, status, _ := classifyChatGPTWebManagementError(&chatgptwebauth.AuthError{
				Code:       test.code,
				StatusCode: test.status,
				Retryable:  true,
			})
			if category != test.code || message != test.message || status != test.status {
				t.Fatalf("classification = %q/%q/%d, want %q/%q/%d", category, message, status, test.code, test.message, test.status)
			}
		})
	}

	for _, test := range []struct {
		code       string
		wantStatus int
	}{
		{code: "token_only_expired", wantStatus: http.StatusUnprocessableEntity},
		{code: "access_token_missing", wantStatus: http.StatusUnprocessableEntity},
		{code: "source_auth_missing", wantStatus: http.StatusConflict},
		{code: "source_identity_changed", wantStatus: http.StatusConflict},
		{code: "source_refresh_unavailable", wantStatus: http.StatusServiceUnavailable},
	} {
		category, message, status, lifecycle := classifyChatGPTWebManagementError(chatGPTWebManagementCodedError(test.code))
		if category != test.code || message == "" || status != test.wantStatus || lifecycle != string(chatgptwebauth.LifecycleReauthRequired) {
			t.Fatalf("coded %s classification = %q/%q/%d/%q", test.code, category, message, status, lifecycle)
		}
	}
}

func TestExecuteChatGPTWebLoginPersistsTerminalCredentialAfterDeadline(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	executor.loginFn = func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		<-ctx.Done()
		credential := activeChatGPTWebManagementTestCredential(input)
		credential.LifecycleState = chatgptwebauth.LifecycleDead
		credential.LifecycleReason = "account_deleted"
		credential.LifecycleUpdatedAt = time.Now().UTC().Format(time.RFC3339)
		return credential, &chatgptwebauth.AuthError{
			Code:           "account_deleted",
			State:          chatgptwebauth.LifecycleDead,
			LifecycleState: chatgptwebauth.LifecycleDead,
			StatusCode:     http.StatusGone,
			Terminal:       true,
		}
	}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	input := chatGPTWebLoginInput{
		Line:       1,
		Email:      "deadline@example.com",
		Password:   "password",
		TOTPSecret: "JBSWY3DPEHPK3PXP",
	}

	result := h.executeChatGPTWebLogin(ctx, input, executor, manager, func() (context.Context, bool) {
		return context.Background(), true
	})

	if result.Status != chatGPTWebLoginResultFailed || result.ErrorCategory != "timeout" || result.HTTPStatus != http.StatusGatewayTimeout || result.LifecycleState != string(chatgptwebauth.LifecycleDead) {
		t.Fatalf("result = %+v", result)
	}
	installed, ok := manager.GetByID(chatGPTWebCredentialFileName(input.Email))
	if !ok || installed.LifecycleState() != string(chatgptwebauth.LifecycleDead) || installed.Metadata["lifecycle_reason"] != "account_deleted" {
		t.Fatalf("terminal credential = %+v, exists=%v", installed, ok)
	}
}

func TestReloginChatGPTWebAuthUsesExecutorAndReturnsSafeLifecycle(t *testing.T) {
	const (
		passwordSecret = "manual-password-secret"
		tokenSecret    = "manual-token-secret"
	)
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	initial := &chatgptwebauth.Credential{
		Type:               chatgptwebauth.Provider,
		Email:              "manual@example.com",
		Password:           passwordSecret,
		TOTPSecret:         "JBSWY3DPEHPK3PXP",
		AccessToken:        "old-token",
		RefreshToken:       "refresh-token",
		LifecycleState:     chatgptwebauth.LifecycleReauthRequired,
		LifecycleReason:    "invalid_grant",
		LifecycleUpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	installed, errPersist := h.persistChatGPTWebLoginCredential(t.Context(), manager, chatGPTWebCredentialFileName(initial.Email), initial, nil, nil)
	if errPersist != nil {
		t.Fatalf("persist initial credential: %v", errPersist)
	}
	executor.mu.Lock()
	executor.reloginFn = func(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, bool, error) {
		credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
		if errCredential != nil {
			return nil, false, errCredential
		}
		credential.AccessToken = tokenSecret
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		credential.LifecycleReason = ""
		credential.LifecycleUpdatedAt = time.Now().UTC().Format(time.RFC3339)
		credential.LastReloginAt = time.Now().UTC().Format(time.RFC3339)
		updated := auth.Clone()
		credential.ApplyToMetadata(updated.Metadata)
		current, errUpdate := manager.Update(ctx, updated)
		return current, errUpdate == nil, errUpdate
	}
	executor.mu.Unlock()
	router := chatGPTWebManagementTestRouter(h)
	path := "/chatgpt-web/auth-files/" + url.PathEscape(installed.FileName) + "/relogin"
	recorder := performChatGPTWebManagementRequest(t, router, http.MethodPost, path, "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("relogin status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	assertChatGPTWebManagementSecretsAbsent(t, recorder.Body.String(), passwordSecret, tokenSecret, initial.TOTPSecret)
	var response struct {
		Status string `json:"status"`
		Auth   gin.H  `json:"auth"`
	}
	decodeChatGPTWebManagementResponse(t, recorder, &response)
	if response.Status != "ok" || response.Auth["lifecycle_state"] != string(chatgptwebauth.LifecycleActive) {
		t.Fatalf("response = %+v", response)
	}

	executor.mu.Lock()
	executor.reloginFn = func(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, bool, error) {
		credential, _ := chatgptwebauth.ParseCredential(auth.Metadata)
		credential.LifecycleState = chatgptwebauth.LifecycleDead
		credential.LifecycleReason = "account_deactivated"
		updated := auth.Clone()
		credential.ApplyToMetadata(updated.Metadata)
		current, errUpdate := manager.Update(ctx, updated)
		if errUpdate != nil {
			return nil, false, errUpdate
		}
		return current, true, &chatgptwebauth.AuthError{
			Code:           "account_deactivated",
			State:          chatgptwebauth.LifecycleDead,
			LifecycleState: chatgptwebauth.LifecycleDead,
			StatusCode:     http.StatusForbidden,
			Terminal:       true,
			Message:        "account is deactivated",
		}
	}
	executor.mu.Unlock()
	recorder = performChatGPTWebManagementRequest(t, router, http.MethodPost, path, "")
	if recorder.Code != http.StatusGone {
		t.Fatalf("dead relogin status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"error_category":"account_deactivated"`) || strings.Contains(recorder.Body.String(), tokenSecret) {
		t.Fatalf("dead response = %s", recorder.Body.String())
	}

	executor.mu.Lock()
	executor.reloginFn = func(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, bool, error) {
		return auth.Clone(), false, chatgptwebauth.ErrCredentialSuperseded
	}
	executor.mu.Unlock()
	recorder = performChatGPTWebManagementRequest(t, router, http.MethodPost, path, "")
	if recorder.Code != http.StatusConflict ||
		!strings.Contains(recorder.Body.String(), `"error_category":"credential_changed"`) {
		t.Fatalf("superseded relogin response = %d %s", recorder.Code, recorder.Body.String())
	}

	executor.mu.Lock()
	executor.reloginFn = func(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, bool, error) {
		return auth.Clone(), false, coreauth.NewSaveOutcomeError(coreauth.SaveOutcomeUncertain, context.Canceled)
	}
	executor.mu.Unlock()
	recorder = performChatGPTWebManagementRequest(t, router, http.MethodPost, path, "")
	if recorder.Code != http.StatusServiceUnavailable ||
		!strings.Contains(recorder.Body.String(), `"error_category":"persist_uncertain"`) {
		t.Fatalf("uncertain relogin response = %d %s", recorder.Code, recorder.Body.String())
	}

	executor.mu.Lock()
	executor.reloginFn = func(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, bool, error) {
		return auth.Clone(), false, coreauth.NewSaveOutcomeError(coreauth.SaveOutcomeRolledBack, errors.New("disk write failed"))
	}
	executor.mu.Unlock()
	recorder = performChatGPTWebManagementRequest(t, router, http.MethodPost, path, "")
	if recorder.Code != http.StatusInternalServerError ||
		!strings.Contains(recorder.Body.String(), `"error_category":"persist_failed"`) {
		t.Fatalf("rolled-back relogin response = %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestReloginChatGPTWebAuthHoldsProxyBindingLease(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	credential := activeChatGPTWebManagementTestCredential(chatgptwebauth.LoginInput{
		Email:      "manual-lease@example.com",
		Password:   "password",
		TOTPSecret: "JBSWY3DPEHPK3PXP",
	})
	installed, errPersist := h.persistChatGPTWebLoginCredential(t.Context(), manager, chatGPTWebCredentialFileName(credential.Email), credential, nil, nil)
	if errPersist != nil {
		t.Fatalf("persist initial credential: %v", errPersist)
	}
	resolver := &chatGPTWebManagementLeaseResolver{heldID: make(chan string, 1)}
	manager.SetProxyResolver(resolver)
	executor.reloginFn = func(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, bool, error) {
		if resolver.active.Load() != 1 {
			return nil, false, errors.New("proxy binding lease is not active")
		}
		return auth.Clone(), true, nil
	}

	router := chatGPTWebManagementTestRouter(h)
	path := "/chatgpt-web/auth-files/" + url.PathEscape(installed.FileName) + "/relogin"
	recorder := performChatGPTWebManagementRequest(t, router, http.MethodPost, path, "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("relogin status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	select {
	case heldID := <-resolver.heldID:
		if heldID != installed.ID {
			t.Fatalf("held binding ID = %q, want %q", heldID, installed.ID)
		}
	default:
		t.Fatal("manual re-login did not acquire a proxy binding lease")
	}
	if resolver.active.Load() != 0 {
		t.Fatal("manual re-login did not release its proxy binding lease")
	}
}

func TestReloginChatGPTWebAuthStopsWithHandlerShutdown(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	credential := activeChatGPTWebManagementTestCredential(chatgptwebauth.LoginInput{
		Email:      "manual-shutdown@example.com",
		Password:   "password",
		TOTPSecret: "JBSWY3DPEHPK3PXP",
	})
	installed, errPersist := h.persistChatGPTWebLoginCredential(t.Context(), manager, chatGPTWebCredentialFileName(credential.Email), credential, nil, nil)
	if errPersist != nil {
		t.Fatalf("persist initial credential: %v", errPersist)
	}
	started := make(chan struct{})
	executor.reloginFn = func(ctx context.Context, _ *coreauth.Auth) (*coreauth.Auth, bool, error) {
		close(started)
		<-ctx.Done()
		return nil, false, ctx.Err()
	}
	router := chatGPTWebManagementTestRouter(h)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/chatgpt-web/auth-files/"+url.PathEscape(installed.FileName)+"/relogin", nil)
	requestDone := make(chan struct{})
	go func() {
		router.ServeHTTP(recorder, request)
		close(requestDone)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("manual re-login did not start")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if errShutdown := h.Shutdown(shutdownCtx); errShutdown != nil {
		t.Fatalf("Shutdown() error = %v", errShutdown)
	}
	select {
	case <-requestDone:
	case <-time.After(2 * time.Second):
		t.Fatal("manual re-login request did not stop")
	}
	if recorder.Code != http.StatusRequestTimeout {
		t.Fatalf("re-login status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestChatGPTWebMutationClassifiesCredentialSnapshotFailure(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, store := newChatGPTWebManagementFailingListTestHandler(t, executor)
	store.fail.Store(true)

	login := h.executeChatGPTWebLogin(t.Context(), chatGPTWebLoginInput{
		Line: 1, Email: "login@example.com", Password: "password",
	}, executor, manager, nil)
	if login.ErrorCategory != "credential_lookup_failed" || login.HTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("login result = %+v", login)
	}

	imported := h.executeChatGPTWebImport(t.Context(), chatGPTWebImportInput{
		file: "import.json", data: []byte(`{"email":"import@example.com","access_token":"access"}`),
	}, executor, manager, nil)
	if imported.ErrorCategory != "credential_lookup_failed" || imported.HTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("import result = %+v", imported)
	}
}

func activeChatGPTWebManagementTestCredential(input chatgptwebauth.LoginInput) *chatgptwebauth.Credential {
	now := time.Now().UTC()
	return &chatgptwebauth.Credential{
		Type:               chatgptwebauth.Provider,
		Email:              input.Email,
		Password:           input.Password,
		TOTPSecret:         input.TOTPSecret,
		AccessToken:        "access-token",
		RefreshToken:       "refresh-token",
		Expired:            now.Add(time.Hour).Format(time.RFC3339),
		LifecycleState:     chatgptwebauth.LifecycleActive,
		LifecycleUpdatedAt: now.Format(time.RFC3339),
		LastLoginAt:        now.Format(time.RFC3339),
	}
}

func newChatGPTWebManagementTestHandler(t *testing.T, executor *chatGPTWebManagementTestExecutor) (*Handler, *coreauth.Manager, string) {
	t.Helper()
	authDir := t.TempDir()
	store := sdkAuth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	manager := coreauth.NewManager(store, nil, nil)
	manager.RegisterExecutor(executor)
	h := NewHandler(&config.Config{AuthDir: authDir}, "", manager)
	h.tokenStore = store
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if errShutdown := h.Shutdown(ctx); errShutdown != nil {
			t.Errorf("shutdown management handler: %v", errShutdown)
		}
	})
	return h, manager, authDir
}

func newChatGPTWebManagementCountingTestHandler(t *testing.T, executor *chatGPTWebManagementTestExecutor) (*Handler, *coreauth.Manager, *countingChatGPTWebAuthStore) {
	t.Helper()
	authDir := t.TempDir()
	fileStore := sdkAuth.NewFileTokenStore()
	fileStore.SetBaseDir(authDir)
	store := &countingChatGPTWebAuthStore{
		FileTokenStore: fileStore,
		entered:        make(chan struct{}, 1),
		release:        make(chan struct{}),
	}
	manager := coreauth.NewManager(store, nil, nil)
	manager.RegisterExecutor(executor)
	h := NewHandler(&config.Config{AuthDir: authDir}, "", manager)
	h.tokenStore = store
	t.Cleanup(func() {
		if store.block.Load() {
			select {
			case <-store.release:
			default:
				close(store.release)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if errShutdown := h.Shutdown(ctx); errShutdown != nil {
			t.Errorf("shutdown management handler: %v", errShutdown)
		}
	})
	return h, manager, store
}

func newChatGPTWebManagementFailingListTestHandler(t *testing.T, executor *chatGPTWebManagementTestExecutor) (*Handler, *coreauth.Manager, *failingListChatGPTWebAuthStore) {
	t.Helper()
	authDir := t.TempDir()
	fileStore := sdkAuth.NewFileTokenStore()
	fileStore.SetBaseDir(authDir)
	store := &failingListChatGPTWebAuthStore{FileTokenStore: fileStore}
	manager := coreauth.NewManager(store, nil, nil)
	manager.RegisterExecutor(executor)
	h := NewHandler(&config.Config{AuthDir: authDir}, "", manager)
	h.tokenStore = store
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if errShutdown := h.Shutdown(ctx); errShutdown != nil {
			t.Errorf("shutdown management handler: %v", errShutdown)
		}
	})
	return h, manager, store
}

func chatGPTWebManagementTestRouter(h *Handler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/chatgpt-web/login-tasks", h.StartChatGPTWebLoginTask)
	router.GET("/chatgpt-web/login-tasks/:id", h.GetChatGPTWebLoginTask)
	router.DELETE("/chatgpt-web/login-tasks/:id", h.CancelChatGPTWebLoginTask)
	router.POST("/chatgpt-web/import-tasks", h.StartChatGPTWebImportTask)
	router.GET("/chatgpt-web/import-tasks/:id", h.GetChatGPTWebImportTask)
	router.DELETE("/chatgpt-web/import-tasks/:id", h.CancelChatGPTWebImportTask)
	router.POST("/chatgpt-web/conversion-tasks", h.StartChatGPTWebConversionTask)
	router.GET("/chatgpt-web/conversion-tasks/:id", h.GetChatGPTWebConversionTask)
	router.DELETE("/chatgpt-web/conversion-tasks/:id", h.CancelChatGPTWebConversionTask)
	router.POST("/chatgpt-web/auth-files/:name/relogin", h.ReloginChatGPTWebAuth)
	return router
}

func performChatGPTWebManagementRequest(t *testing.T, router http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	if body != "" {
		request.Header.Set("Content-Type", "text/plain")
	}
	router.ServeHTTP(recorder, request)
	return recorder
}

func waitForChatGPTWebLoginTask(t *testing.T, router http.Handler, id string) chatGPTWebLoginTask {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		recorder := performChatGPTWebManagementRequest(t, router, http.MethodGet, "/chatgpt-web/login-tasks/"+id, "")
		if recorder.Code != http.StatusOK {
			t.Fatalf("get task status = %d, body=%s", recorder.Code, recorder.Body.String())
		}
		var task chatGPTWebLoginTask
		decodeChatGPTWebManagementResponse(t, recorder, &task)
		if isTerminalChatGPTWebLoginTaskState(task.State) {
			return task
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("task %s did not complete", id)
	return chatGPTWebLoginTask{}
}

func waitForChatGPTWebManagementCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if !condition() {
		t.Fatal("condition was not satisfied before timeout")
	}
}

func decodeChatGPTWebManagementResponse(t *testing.T, recorder *httptest.ResponseRecorder, target any) {
	t.Helper()
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), target); errDecode != nil {
		t.Fatalf("decode response: %v; body=%s", errDecode, recorder.Body.String())
	}
}

func assertChatGPTWebManagementSecretsAbsent(t *testing.T, body string, secrets ...string) {
	t.Helper()
	for _, secret := range secrets {
		if secret != "" && strings.Contains(body, secret) {
			t.Fatalf("response leaked secret %q: %s", secret, body)
		}
	}
}

func mustMarshalChatGPTWebTask(t *testing.T, task chatGPTWebLoginTask) string {
	t.Helper()
	data, errMarshal := json.Marshal(task)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	return string(data)
}

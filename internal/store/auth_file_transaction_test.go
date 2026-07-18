package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	internalauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func assertPersistentAuthTargetHeldDuringRemoteOperation(t *testing.T, rootDir, relativePath string, remoteStarted <-chan struct{}, releaseRemote chan struct{}, operationDone <-chan error) {
	t.Helper()
	released := false
	defer func() {
		if !released {
			close(releaseRemote)
		}
	}()

	select {
	case <-remoteStarted:
	case errOperation := <-operationDone:
		t.Fatalf("persistence completed before the remote operation blocked: %v", errOperation)
	case <-time.After(5 * time.Second):
		t.Fatal("persistence did not reach the blocked remote operation")
	}

	contenderRoot, errRoot := os.OpenRoot(rootDir)
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer contenderRoot.Close()

	waitCtx, cancelWait := context.WithTimeout(t.Context(), 100*time.Millisecond)
	unlockWhileBlocked, errLock := authfileguard.LockRootTargetContext(waitCtx, contenderRoot, relativePath)
	cancelWait()
	if errLock == nil {
		_ = unlockWhileBlocked()
		close(releaseRemote)
		released = true
		<-operationDone
		t.Fatal("independent root acquired the persistent target during the remote operation")
	}
	if !errors.Is(errLock, context.DeadlineExceeded) {
		t.Fatalf("independent root target lock error = %v, want deadline exceeded", errLock)
	}

	close(releaseRemote)
	released = true
	select {
	case errOperation := <-operationDone:
		if errOperation != nil {
			t.Fatalf("persistence failed after releasing the remote operation: %v", errOperation)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("persistence did not finish after releasing the remote operation")
	}

	acquireCtx, cancelAcquire := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelAcquire()
	unlockAfter, errAfter := authfileguard.LockRootTargetContext(acquireCtx, contenderRoot, relativePath)
	if errAfter != nil {
		t.Fatalf("independent root could not acquire the persistent target after persistence: %v", errAfter)
	}
	if errUnlock := unlockAfter(); errUnlock != nil {
		t.Fatalf("unlock independent persistent target: %v", errUnlock)
	}
}

func TestProbeAuthDeleteResultIgnoresCancellation(t *testing.T) {
	type contextKey string
	const key contextKey = "delete-probe"
	ctx, cancel := context.WithCancel(t.Context())
	ctx = context.WithValue(ctx, key, "value")
	cancel()
	called := false
	state, errProbe := probeAuthDeleteResult(ctx, func(probeCtx context.Context) (authDeleteProbeState, error) {
		called = true
		if errContext := probeCtx.Err(); errContext != nil {
			t.Fatalf("probe context error = %v, want nil", errContext)
		}
		if got := probeCtx.Value(key); got != "value" {
			t.Fatalf("probe context value = %v, want value", got)
		}
		return authDeleteProbeAbsent, nil
	})
	if errProbe != nil || state != authDeleteProbeAbsent {
		t.Fatalf("probeAuthDeleteResult() = %v, %v; want absent, nil", state, errProbe)
	}
	if !called {
		t.Fatal("probe did not run after context cancellation")
	}
}

func TestWriteAuthFileAtomicallyWaitsForRootRebuild(t *testing.T) {
	dir := t.TempDir()
	rebuildRoot, errRebuildRoot := os.OpenRoot(dir)
	if errRebuildRoot != nil {
		t.Fatal(errRebuildRoot)
	}
	defer rebuildRoot.Close()
	writeRoot, errWriteRoot := os.OpenRoot(dir)
	if errWriteRoot != nil {
		t.Fatal(errWriteRoot)
	}
	defer writeRoot.Close()

	unlockRebuild, errRebuild := authfileguard.LockRootRebuild(rebuildRoot)
	if errRebuild != nil {
		t.Fatal(errRebuild)
	}
	defer unlockRebuild()

	written := make(chan error, 1)
	go func() {
		written <- writeAuthFileAtomicallyAtRoot(writeRoot, "auth.json", []byte(`{"type":"codex"}`), &authFileSnapshot{})
	}()
	select {
	case errWrite := <-written:
		t.Fatalf("write completed during root rebuild lock: %v", errWrite)
	case <-time.After(50 * time.Millisecond):
	}
	if errUnlock := unlockRebuild(); errUnlock != nil {
		t.Fatal(errUnlock)
	}
	select {
	case errWrite := <-written:
		if errWrite != nil {
			t.Fatalf("write error: %v", errWrite)
		}
	case <-time.After(time.Second):
		t.Fatal("write remained blocked after root rebuild lock was released")
	}
}

func TestWriteAuthFileAtomicallyContextCancelsLockWaits(t *testing.T) {
	tests := []struct {
		name string
		lock func(*testing.T, *os.Root, string) func()
	}{
		{
			name: "root rebuild",
			lock: func(t *testing.T, root *os.Root, _ string) func() {
				t.Helper()
				unlock, errLock := authfileguard.LockRootRebuild(root)
				if errLock != nil {
					t.Fatal(errLock)
				}
				return func() {
					if errUnlock := unlock(); errUnlock != nil {
						t.Fatal(errUnlock)
					}
				}
			},
		},
		{
			name: "path",
			lock: func(_ *testing.T, root *os.Root, name string) func() {
				return authfileguard.Lock(filepath.Join(root.Name(), name))
			},
		},
		{
			name: "persistent target",
			lock: func(t *testing.T, root *os.Root, name string) func() {
				t.Helper()
				unlock, errLock := authfileguard.LockRootTarget(root, name)
				if errLock != nil {
					t.Fatal(errLock)
				}
				return func() {
					if errUnlock := unlock(); errUnlock != nil {
						t.Fatal(errUnlock)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			lockRoot, errLockRoot := os.OpenRoot(dir)
			if errLockRoot != nil {
				t.Fatal(errLockRoot)
			}
			defer lockRoot.Close()
			writeRoot, errWriteRoot := os.OpenRoot(dir)
			if errWriteRoot != nil {
				t.Fatal(errWriteRoot)
			}
			defer writeRoot.Close()

			const fileName = "auth.json"
			unlock := tt.lock(t, lockRoot, fileName)
			defer unlock()

			ctx, cancel := context.WithCancel(t.Context())
			started := make(chan struct{})
			result := make(chan error, 1)
			go func() {
				close(started)
				result <- writeAuthFileAtomicallyAtRootContext(ctx, writeRoot, fileName, []byte(`{"type":"codex"}`), &authFileSnapshot{})
			}()
			<-started
			time.Sleep(50 * time.Millisecond)
			cancel()

			select {
			case errWrite := <-result:
				if !errors.Is(errWrite, context.Canceled) {
					t.Fatalf("write error = %v, want context canceled", errWrite)
				}
			case <-time.After(time.Second):
				t.Fatal("write did not stop after context cancellation")
			}
			if _, errStat := writeRoot.Lstat(fileName); !errors.Is(errStat, fs.ErrNotExist) {
				t.Fatalf("auth file exists after canceled lock wait: %v", errStat)
			}
		})
	}
}

func TestAuthSaveVerificationContextIgnoresCancellationAndDeadline(t *testing.T) {
	type contextKey string
	const key contextKey = "verification"

	parent, cancel := context.WithCancel(t.Context())
	parent = context.WithValue(parent, key, "value")
	cancel()
	verification := authSaveVerificationContext(parent)
	if errVerification := verification.Err(); errVerification != nil {
		t.Fatalf("verification context error = %v, want nil", errVerification)
	}
	if got := verification.Value(key); got != "value" {
		t.Fatalf("verification context value = %v, want value", got)
	}

	parentWithDeadline, cancelDeadline := context.WithTimeout(t.Context(), time.Minute)
	defer cancelDeadline()
	verificationWithDeadline := authSaveVerificationContext(parentWithDeadline)
	if _, hasDeadline := verificationWithDeadline.Deadline(); hasDeadline {
		t.Fatal("verification context unexpectedly retained parent deadline")
	}
}

func TestRestoreMovedAuthSnapshotPreservesConflict(t *testing.T) {
	dir := t.TempDir()
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatalf("OpenRoot() error = %v", errRoot)
	}
	defer root.Close()
	displaced := []byte(`{"type":"codex","access_token":"displaced"}`)
	current := []byte(`{"type":"codex","access_token":"current"}`)
	if errWrite := os.WriteFile(filepath.Join(dir, ".auth-displaced-test"), displaced, 0o600); errWrite != nil {
		t.Fatalf("write displaced auth: %v", errWrite)
	}
	if errWrite := os.WriteFile(filepath.Join(dir, "auth.json"), current, 0o600); errWrite != nil {
		t.Fatalf("write current auth: %v", errWrite)
	}

	errRestore := restoreMovedAuthSnapshot(root, ".auth-displaced-test", "auth.json", syncAuthSnapshotDirectory)
	if !errors.Is(errRestore, authfileguard.ErrPersistGenerationStale) {
		t.Fatalf("restore error = %v, want stale generation", errRestore)
	}
	if got, errRead := os.ReadFile(filepath.Join(dir, "auth.json")); errRead != nil || !bytes.Equal(got, current) {
		t.Fatalf("current auth = %s, %v; want %s", got, errRead, current)
	}
	if got, errRead := os.ReadFile(filepath.Join(dir, ".auth-displaced-test")); errRead != nil || !bytes.Equal(got, displaced) {
		t.Fatalf("displaced auth = %s, %v; want %s", got, errRead, displaced)
	}
}

func TestClearAuthDirectoryAtRootWaitsForMutationAndPreservesLockFiles(t *testing.T) {
	dir := t.TempDir()
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()
	clearRoot, errClearRoot := os.OpenRoot(dir)
	if errClearRoot != nil {
		t.Fatal(errClearRoot)
	}
	defer clearRoot.Close()
	if errMkdir := root.MkdirAll(filepath.Join("nested", "empty"), 0o700); errMkdir != nil {
		t.Fatal(errMkdir)
	}
	if errWrite := root.WriteFile("top.json", []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if errWrite := root.WriteFile(filepath.Join("nested", "auth.json"), []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	unlockTopTarget, errLockTop := authfileguard.LockRootTarget(root, "top.json")
	if errLockTop != nil {
		t.Fatal(errLockTop)
	}
	if errUnlock := unlockTopTarget(); errUnlock != nil {
		t.Fatal(errUnlock)
	}
	unlockNestedTarget, errLockNested := authfileguard.LockRootTarget(root, filepath.Join("nested", "auth.json"))
	if errLockNested != nil {
		t.Fatal(errLockNested)
	}
	if errUnlock := unlockNestedTarget(); errUnlock != nil {
		t.Fatal(errUnlock)
	}

	lockFilesBefore := findAuthLockFilesAtRoot(t, root)
	if len(lockFilesBefore) != 2 {
		t.Fatalf("root-level target locks = %d, want 2", len(lockFilesBefore))
	}
	if errWrite := root.WriteFile(".auth-lock-old.json", []byte("not a lock"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	uppercaseLock := ".auth-lock-0123456789ABCDEF0123456789ABCDEF"
	if errWrite := root.WriteFile(uppercaseLock, []byte("not a lock"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	lockLikeDir := ".auth-lock-ffffffffffffffffffffffffffffffff"
	if errMkdir := root.Mkdir(lockLikeDir, 0o700); errMkdir != nil {
		t.Fatal(errMkdir)
	}

	unlockMutation, errMutation := authfileguard.LockRootMutation(root)
	if errMutation != nil {
		t.Fatal(errMutation)
	}
	clearDone := make(chan error, 1)
	go func() {
		clearDone <- clearAuthDirectoryAtRoot(clearRoot)
	}()
	select {
	case errClear := <-clearDone:
		t.Fatalf("clear completed while mutation lock was held: %v", errClear)
	case <-time.After(100 * time.Millisecond):
	}
	if _, errStat := root.Lstat("top.json"); errStat != nil {
		t.Fatalf("top auth changed while clear waited: %v", errStat)
	}
	if errUnlock := unlockMutation(); errUnlock != nil {
		t.Fatal(errUnlock)
	}
	select {
	case errClear := <-clearDone:
		if errClear != nil {
			t.Fatal(errClear)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("clear did not resume after mutation lock release")
	}
	if _, errStat := root.Lstat("top.json"); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("top auth remains after clear: %v", errStat)
	}
	if _, errStat := root.Lstat(filepath.Join("nested", "auth.json")); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("nested auth remains after clear: %v", errStat)
	}
	if _, errStat := root.Lstat(filepath.Join("nested", "empty")); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("empty directory remains after clear: %v", errStat)
	}
	if _, errStat := root.Lstat(".auth-lock-old.json"); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("malformed lock file remains after clear: %v", errStat)
	}
	_, errUppercaseLock := root.Lstat(uppercaseLock)
	if runtime.GOOS == "darwin" || runtime.GOOS == "windows" {
		if errUppercaseLock != nil {
			t.Fatalf("case-folded lock file missing after clear: %v", errUppercaseLock)
		}
	} else if !errors.Is(errUppercaseLock, os.ErrNotExist) {
		t.Fatalf("uppercase lock file remains after clear: %v", errUppercaseLock)
	}
	if _, errStat := root.Lstat(lockLikeDir); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("lock-like directory remains after clear: %v", errStat)
	}
	rootLock, errRootLock := root.Lstat(".auth-root-lock")
	if errRootLock != nil || !rootLock.Mode().IsRegular() {
		t.Fatalf("root lock missing after clear: %v", errRootLock)
	}
	for name, before := range lockFilesBefore {
		after, errAfter := root.Lstat(name)
		if errAfter != nil || !os.SameFile(before, after) {
			t.Fatalf("target lock %s changed during clear: %v", name, errAfter)
		}
	}
}

func findAuthLockFilesAtRoot(t *testing.T, root *os.Root) map[string]os.FileInfo {
	t.Helper()
	directory, errOpen := root.Open(".")
	if errOpen != nil {
		t.Fatal(errOpen)
	}
	entries, errRead := directory.ReadDir(-1)
	errClose := directory.Close()
	if errRead != nil || errClose != nil {
		t.Fatal(errors.Join(errRead, errClose))
	}
	result := make(map[string]os.FileInfo)
	for _, entry := range entries {
		if entry != nil && strings.HasPrefix(entry.Name(), ".auth-lock-") && authfileguard.IsPersistentLockFileName(entry.Name()) {
			info, errInfo := root.Lstat(entry.Name())
			if errInfo != nil {
				t.Fatal(errInfo)
			}
			result[entry.Name()] = info
		}
	}
	return result
}

func TestMatchesExpectedAuthDeleteGeneration(t *testing.T) {
	original := []byte(`{"type":"codex","access_token":"original"}`)
	replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	ctx := authfileguard.WithExpectedDeleteHash(t.Context(), cliproxyauth.SourceHashFromBytes(original))
	ctx = authfileguard.WithDeleteIdentityBinding(ctx)
	if got := matchExpectedAuthDeleteGeneration(ctx, "object:auth.json", "etag-a", true, original); got != authDeleteGenerationMatched {
		t.Fatal("original content did not match expected delete generation")
	}
	if got := matchExpectedAuthDeleteGeneration(ctx, "object:auth.json", "etag-b", true, replacement); got != authDeleteGenerationReplaced {
		t.Fatal("replacement content matched stale delete generation")
	}
	if got := matchExpectedAuthDeleteGeneration(ctx, "object:auth.json", "etag-b", true, original); got != authDeleteGenerationUncertain {
		t.Fatal("same-content replacement matched stale backend identity")
	}
}

func TestMatchesExpectedAuthDeleteGenerationAcceptsCanonicalJSONHash(t *testing.T) {
	original := []byte(`{"type":"codex","access_token":"original","metadata":{"account":"one","priority":1}}`)
	remote := []byte("{\n  \"metadata\": {\"priority\": 1, \"account\": \"one\"},\n  \"access_token\": \"original\",\n  \"type\": \"codex\"\n}")
	expectedHash, errHash := cliproxyauth.CanonicalSourceHashFromBytes(original)
	if errHash != nil {
		t.Fatalf("CanonicalSourceHashFromBytes() error = %v", errHash)
	}
	generation := authfileguard.NewDeleteGeneration(expectedHash)
	ctx := authfileguard.WithDeleteGeneration(t.Context(), generation)
	ctx = authfileguard.WithDeleteIdentityBinding(ctx)
	if got := matchExpectedAuthDeleteGeneration(ctx, "postgres:auth.json", "row-version-a", true, remote); got != authDeleteGenerationMatched {
		t.Fatalf("canonical JSON delete generation = %v, want matched", got)
	}
}

func TestMapAuthCreateGenerationConflict(t *testing.T) {
	if err := mapAuthCreateGenerationConflict(false, authfileguard.ErrPersistGenerationStale); !errors.Is(err, authfileguard.ErrPersistGenerationStale) {
		t.Fatalf("replacement error = %v, want stale generation", err)
	}
	if err := mapAuthCreateGenerationConflict(true, authfileguard.ErrPersistGenerationStale); !errors.Is(err, cliproxyauth.ErrAuthAlreadyExists) {
		t.Fatalf("conditional create error = %v, want auth already exists", err)
	}
	for _, tt := range []struct {
		name              string
		outcome           cliproxyauth.SaveOutcome
		wantAlreadyExists bool
	}{
		{name: "rolled back", outcome: cliproxyauth.SaveOutcomeRolledBack, wantAlreadyExists: true},
		{name: "uncertain", outcome: cliproxyauth.SaveOutcomeUncertain},
		{name: "committed", outcome: cliproxyauth.SaveOutcomeCommitted},
	} {
		t.Run(tt.name, func(t *testing.T) {
			explicitErr := cliproxyauth.NewSaveOutcomeError(tt.outcome, authfileguard.ErrPersistGenerationStale)
			mapped := mapAuthCreateGenerationConflict(true, explicitErr)
			if gotAlreadyExists := errors.Is(mapped, cliproxyauth.ErrAuthAlreadyExists); gotAlreadyExists != tt.wantAlreadyExists {
				t.Fatalf("auth already exists = %t, want %t; error=%v", gotAlreadyExists, tt.wantAlreadyExists, mapped)
			}
			if outcome, explicit := cliproxyauth.SaveOutcomeFromError(mapped); !explicit || outcome != tt.outcome {
				t.Fatalf("mapped outcome = %v, %t; want %v", outcome, explicit, tt.outcome)
			}
		})
	}
}

type staticTokenStorage struct {
	data []byte
}

type blockingTokenStorage struct {
	started chan struct{}
	release chan struct{}
	data    []byte
}

type failingMetadataTokenStorage struct {
	metadata map[string]any
	err      error
}

type setterOnlyMetadataTokenStorage struct {
	metadata map[string]any
}

func (s staticTokenStorage) SaveTokenToFile(path string) error {
	return os.WriteFile(path, s.data, 0o600)
}

var _ internalauth.TokenStorage = staticTokenStorage{}

func (*blockingTokenStorage) SaveTokenToFile(string) error {
	return errors.New("legacy path should not be used")
}

func (s *blockingTokenStorage) MarshalTokenData() ([]byte, error) {
	close(s.started)
	<-s.release
	return bytes.Clone(s.data), nil
}

var _ internalauth.TokenStorage = (*blockingTokenStorage)(nil)
var _ internalauth.TokenDataMarshaler = (*blockingTokenStorage)(nil)

func (s *failingMetadataTokenStorage) SetMetadata(metadata map[string]any) {
	s.metadata = cloneAuthMetadata(metadata)
}

func (s *failingMetadataTokenStorage) MetadataSnapshot() map[string]any {
	return cloneAuthMetadata(s.metadata)
}

func (s *failingMetadataTokenStorage) SaveTokenToFile(string) error {
	return s.err
}

func (s *setterOnlyMetadataTokenStorage) SetMetadata(metadata map[string]any) {
	s.metadata = cloneAuthMetadata(metadata)
}

func (*setterOnlyMetadataTokenStorage) SaveTokenToFile(string) error {
	return errors.New("legacy path should not be used")
}

func (s *setterOnlyMetadataTokenStorage) MarshalTokenData() ([]byte, error) {
	return json.Marshal(s.metadata)
}

func TestPrepareAuthStorageDataRestoresMetadataAfterFailure(t *testing.T) {
	wantErr := errors.New("storage marshal failed")
	storage := &failingMetadataTokenStorage{
		metadata: map[string]any{"marker": "original"},
		err:      wantErr,
	}
	auth := &cliproxyauth.Auth{
		ID:         "auth.json",
		Provider:   "codex",
		Disabled:   true,
		Metadata:   map[string]any{"type": "codex"},
		Attributes: map[string]string{"path": "original"},
		Storage:    storage,
	}
	snapshot := captureAuthRuntimeSnapshot(auth)
	if _, errPrepare := prepareAuthStorageData(auth, snapshot); !errors.Is(errPrepare, wantErr) {
		t.Fatalf("prepareAuthStorageData() error = %v, want %v", errPrepare, wantErr)
	}
	if len(storage.metadata) != 1 || storage.metadata["marker"] != "original" {
		t.Fatalf("storage metadata = %#v, want original metadata", storage.metadata)
	}
	if !auth.Disabled || auth.Attributes["path"] != "original" || auth.Metadata["type"] != "codex" {
		t.Fatalf("auth runtime state changed after failure: %#v", auth)
	}
}

func TestPrepareAuthStorageDataUsesSetterOnlyMetadataContract(t *testing.T) {
	storage := &setterOnlyMetadataTokenStorage{metadata: map[string]any{"marker": "original"}}
	auth := &cliproxyauth.Auth{
		Metadata: map[string]any{"type": "codex", "marker": "replacement"},
		Storage:  storage,
	}
	snapshot := captureAuthRuntimeSnapshot(auth)
	data, errPrepare := prepareAuthStorageData(auth, snapshot)
	if errPrepare != nil {
		t.Fatal(errPrepare)
	}
	if storage.metadata["marker"] != "replacement" || storage.metadata["type"] != "codex" || storage.metadata["disabled"] != false {
		t.Fatalf("storage metadata = %#v, want injected runtime metadata", storage.metadata)
	}
	var persisted map[string]any
	if errUnmarshal := json.Unmarshal(data, &persisted); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	if persisted["marker"] != "replacement" || persisted["type"] != "codex" || persisted["disabled"] != false {
		t.Fatalf("persisted metadata = %#v", persisted)
	}
}

func TestProduceAuthStorageDataRejectsInvalidOutput(t *testing.T) {
	if _, errProduce := produceAuthStorageData(staticTokenStorage{data: []byte(`null`)}); errProduce == nil {
		t.Fatal("produceAuthStorageData() accepted invalid output")
	}
}

func TestPrepareAuthStorageDataValidatesRetiredCredentialAfterMetadataMerge(t *testing.T) {
	retiredStorage := &setterOnlyMetadataTokenStorage{
		metadata: map[string]any{"type": "gemini", "access_token": "legacy"},
	}
	repaired := &cliproxyauth.Auth{
		Metadata: map[string]any{"type": "codex"},
		Storage:  retiredStorage,
	}
	repairedData, errRepaired := prepareAuthStorageData(repaired, captureAuthRuntimeSnapshot(repaired))
	if errRepaired != nil {
		t.Fatalf("prepare repaired storage: %v", errRepaired)
	}
	var repairedMetadata map[string]any
	if errUnmarshal := json.Unmarshal(repairedData, &repairedMetadata); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	if repairedMetadata["type"] != "codex" {
		t.Fatalf("repaired type = %v, want codex", repairedMetadata["type"])
	}

	unsafeStorage := &setterOnlyMetadataTokenStorage{
		metadata: map[string]any{"type": "codex", "access_token": "token"},
	}
	unsafe := &cliproxyauth.Auth{
		Metadata: map[string]any{"type": "gemini"},
		Storage:  unsafeStorage,
	}
	if _, errPrepare := prepareAuthStorageData(unsafe, captureAuthRuntimeSnapshot(unsafe)); !errors.Is(errPrepare, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
		t.Fatalf("prepare unsafe storage error = %v, want retired credential rejection", errPrepare)
	}
}

func TestCaptureAuthFileSnapshotPreservesDeletionSemantics(t *testing.T) {
	dir := t.TempDir()
	emptyPath := filepath.Join(dir, "empty.json")
	if errWrite := os.WriteFile(emptyPath, nil, 0o600); errWrite != nil {
		t.Fatalf("write empty auth file: %v", errWrite)
	}
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatalf("OpenRoot() error = %v", errRoot)
	}
	defer root.Close()

	for _, path := range []string{filepath.Join(dir, "missing.json"), filepath.Join(dir, "missing", "nested.json"), emptyPath} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			relativePath, errRel := filepath.Rel(dir, path)
			if errRel != nil {
				t.Fatalf("resolve auth snapshot path: %v", errRel)
			}
			snapshot, errSnapshot := captureAuthFileSnapshot(root, relativePath)
			if errSnapshot != nil {
				t.Fatalf("captureAuthFileSnapshot() error = %v", errSnapshot)
			}
			if len(snapshot.data) != 0 {
				t.Fatalf("snapshot data = %q, want deletion", snapshot.data)
			}
		})
	}
}

func TestRestoreAuthFileSnapshotAtRootRestoresOriginalGeneration(t *testing.T) {
	dir := t.TempDir()
	const fileName = "auth.json"
	oldData := []byte(`{"type":"codex","access_token":"old"}`)
	newData := []byte(`{"type":"codex","access_token":"new"}`)
	path := filepath.Join(dir, fileName)
	if errWrite := os.WriteFile(path, oldData, 0o600); errWrite != nil {
		t.Fatalf("write old auth: %v", errWrite)
	}
	if errChmod := os.Chmod(path, 0o640); errChmod != nil {
		t.Fatalf("chmod old auth: %v", errChmod)
	}
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatalf("OpenRoot() error = %v", errRoot)
	}
	defer root.Close()
	original, errSnapshot := captureAuthFileSnapshot(root, fileName)
	if errSnapshot != nil {
		t.Fatalf("capture snapshot: %v", errSnapshot)
	}
	installed, errWrite := writeAuthFileAtomicallyAtRootWithReceipt(root, fileName, newData, &original)
	if errWrite != nil {
		t.Fatalf("write new auth: %v", errWrite)
	}
	if errRestore := restoreAuthFileSnapshotAtRoot(root, fileName, installed, original); errRestore != nil {
		t.Fatalf("restoreAuthFileSnapshotAtRoot() error = %v", errRestore)
	}
	got, errRead := os.ReadFile(path)
	if errRead != nil || !bytes.Equal(got, oldData) {
		t.Fatalf("restored auth = %s, %v; want %s", got, errRead, oldData)
	}
	info, errStat := os.Stat(path)
	if errStat != nil {
		t.Fatalf("stat restored auth: %v", errStat)
	}
	if gotMode := info.Mode().Perm(); gotMode != 0o640 {
		t.Fatalf("restored auth mode = %o, want 640", gotMode)
	}
}

func TestWriteAuthFileAtomicallyRollsBackDirectorySyncFailure(t *testing.T) {
	tests := []struct {
		name     string
		original []byte
	}{
		{name: "missing"},
		{name: "existing", original: []byte(`{"type":"codex","access_token":"old"}`)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			const fileName = "auth.json"
			path := filepath.Join(dir, fileName)
			if test.original != nil {
				if errWrite := os.WriteFile(path, test.original, 0o600); errWrite != nil {
					t.Fatalf("write original auth: %v", errWrite)
				}
				if errChmod := os.Chmod(path, 0o640); errChmod != nil {
					t.Fatalf("chmod original auth: %v", errChmod)
				}
			}
			root, errRoot := os.OpenRoot(dir)
			if errRoot != nil {
				t.Fatalf("OpenRoot() error = %v", errRoot)
			}
			defer root.Close()
			original, errSnapshot := captureAuthFileSnapshot(root, fileName)
			if errSnapshot != nil {
				t.Fatalf("capture snapshot: %v", errSnapshot)
			}
			wantErr := errors.New("sync failed")
			syncCalls := 0
			syncDirectory := func(*os.Root) error {
				syncCalls++
				if syncCalls == 1 {
					return wantErr
				}
				return nil
			}
			errWrite := writeAuthFileAtomicallyAtRootWithPolicy(
				root,
				fileName,
				[]byte(`{"type":"codex","access_token":"new"}`),
				&original,
				false,
				0o600,
				syncDirectory,
			)
			if !errors.Is(errWrite, wantErr) {
				t.Fatalf("write error = %v, want %v", errWrite, wantErr)
			}
			if outcome, ok := cliproxyauth.SaveOutcomeFromError(errWrite); !ok || outcome != cliproxyauth.SaveOutcomeRolledBack {
				t.Fatalf("write outcome = %v, %t; want rolled back", outcome, ok)
			}
			got, errRead := os.ReadFile(path)
			if test.original == nil {
				if !errors.Is(errRead, os.ErrNotExist) {
					t.Fatalf("auth remained after failed create: data=%s error=%v", got, errRead)
				}
				return
			}
			if errRead != nil || !bytes.Equal(got, test.original) {
				t.Fatalf("auth after failed replacement = %s, %v; want %s", got, errRead, test.original)
			}
			info, errStat := os.Stat(path)
			if errStat != nil {
				t.Fatalf("stat restored auth: %v", errStat)
			}
			if gotMode := info.Mode().Perm(); gotMode != 0o640 {
				t.Fatalf("restored auth mode = %o, want 640", gotMode)
			}
		})
	}
}

func TestWriteAuthFileAtomicallyDetectsParentReplacementDuringDirectorySync(t *testing.T) {
	rootDir := t.TempDir()
	parentDir := filepath.Join(rootDir, "nested")
	if errMkdir := os.Mkdir(parentDir, 0o700); errMkdir != nil {
		t.Fatalf("create parent: %v", errMkdir)
	}
	const fileName = "auth.json"
	relativePath := filepath.Join("nested", fileName)
	original := []byte(`{"type":"codex","access_token":"old"}`)
	if errWrite := os.WriteFile(filepath.Join(parentDir, fileName), original, 0o600); errWrite != nil {
		t.Fatalf("write original auth: %v", errWrite)
	}
	root, errRoot := os.OpenRoot(rootDir)
	if errRoot != nil {
		t.Fatalf("OpenRoot() error = %v", errRoot)
	}
	defer root.Close()
	snapshot, errSnapshot := captureAuthFileSnapshot(root, relativePath)
	if errSnapshot != nil {
		t.Fatalf("capture snapshot: %v", errSnapshot)
	}

	movedParent := parentDir + "-moved"
	replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	syncCalls := 0
	syncDirectory := func(*os.Root) error {
		syncCalls++
		if syncCalls != 1 {
			return nil
		}
		if errRename := os.Rename(parentDir, movedParent); errRename != nil {
			return errRename
		}
		if errMkdir := os.Mkdir(parentDir, 0o700); errMkdir != nil {
			return errMkdir
		}
		return os.WriteFile(filepath.Join(parentDir, fileName), replacement, 0o600)
	}
	errWrite := writeAuthFileAtomicallyAtRootWithPolicy(
		root,
		relativePath,
		[]byte(`{"type":"codex","access_token":"new"}`),
		&snapshot,
		false,
		0o600,
		syncDirectory,
	)
	if outcome, explicit := cliproxyauth.SaveOutcomeFromError(errWrite); !explicit || outcome != cliproxyauth.SaveOutcomeUncertain {
		t.Fatalf("write outcome = %v, %t; want uncertain; error=%v", outcome, explicit, errWrite)
	}
	if got, errRead := os.ReadFile(filepath.Join(parentDir, fileName)); errRead != nil || !bytes.Equal(got, replacement) {
		t.Fatalf("replacement path data = %s, %v; want %s", got, errRead, replacement)
	}
	if got, errRead := os.ReadFile(filepath.Join(movedParent, fileName)); errRead != nil || !bytes.Equal(got, original) {
		t.Fatalf("detached original data = %s, %v; want %s", got, errRead, original)
	}
}

func TestWriteAuthFileAtomicallyReportsUncertainWhenSyncFailsAfterParentReplacement(t *testing.T) {
	rootDir := t.TempDir()
	parentDir := filepath.Join(rootDir, "nested")
	if errMkdir := os.Mkdir(parentDir, 0o700); errMkdir != nil {
		t.Fatalf("create parent: %v", errMkdir)
	}
	const fileName = "auth.json"
	relativePath := filepath.Join("nested", fileName)
	if errWrite := os.WriteFile(filepath.Join(parentDir, fileName), []byte(`{"type":"codex","access_token":"old"}`), 0o600); errWrite != nil {
		t.Fatalf("write original auth: %v", errWrite)
	}
	root, errRoot := os.OpenRoot(rootDir)
	if errRoot != nil {
		t.Fatalf("OpenRoot() error = %v", errRoot)
	}
	defer root.Close()
	snapshot, errSnapshot := captureAuthFileSnapshot(root, relativePath)
	if errSnapshot != nil {
		t.Fatalf("capture snapshot: %v", errSnapshot)
	}

	movedParent := parentDir + "-moved"
	replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	wantSyncErr := errors.New("sync failed after parent replacement")
	syncCalls := 0
	syncDirectory := func(*os.Root) error {
		syncCalls++
		if syncCalls != 1 {
			return nil
		}
		if errRename := os.Rename(parentDir, movedParent); errRename != nil {
			return errRename
		}
		if errMkdir := os.Mkdir(parentDir, 0o700); errMkdir != nil {
			return errMkdir
		}
		if errWrite := os.WriteFile(filepath.Join(parentDir, fileName), replacement, 0o600); errWrite != nil {
			return errWrite
		}
		return wantSyncErr
	}
	errWrite := writeAuthFileAtomicallyAtRootWithPolicy(
		root,
		relativePath,
		[]byte(`{"type":"codex","access_token":"new"}`),
		&snapshot,
		false,
		0o600,
		syncDirectory,
	)
	if !errors.Is(errWrite, wantSyncErr) {
		t.Fatalf("write error = %v, want %v", errWrite, wantSyncErr)
	}
	if outcome, explicit := cliproxyauth.SaveOutcomeFromError(errWrite); !explicit || outcome != cliproxyauth.SaveOutcomeUncertain {
		t.Fatalf("write outcome = %v, %t; want uncertain; error=%v", outcome, explicit, errWrite)
	}
	if got, errRead := os.ReadFile(filepath.Join(parentDir, fileName)); errRead != nil || !bytes.Equal(got, replacement) {
		t.Fatalf("replacement path data = %s, %v; want %s", got, errRead, replacement)
	}
}

func TestWriteAuthFileAtomicallyWithoutSnapshotReportsUncertainSync(t *testing.T) {
	dir := t.TempDir()
	const fileName = "auth.json"
	path := filepath.Join(dir, fileName)
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()
	wantErr := errors.New("sync failed")
	data := []byte(`{"type":"codex","access_token":"new"}`)

	errWrite := writeAuthFileAtomicallyAtRootWithPolicy(
		root,
		fileName,
		data,
		nil,
		false,
		0o600,
		func(*os.Root) error { return wantErr },
	)
	if !errors.Is(errWrite, wantErr) {
		t.Fatalf("write error = %v, want %v", errWrite, wantErr)
	}
	if outcome, ok := cliproxyauth.SaveOutcomeFromError(errWrite); !ok || outcome != cliproxyauth.SaveOutcomeUncertain {
		t.Fatalf("write outcome = %v, %t; want uncertain", outcome, ok)
	}
	got, errRead := os.ReadFile(path)
	if errRead != nil || !bytes.Equal(got, data) {
		t.Fatalf("auth after uncertain sync = %s, %v; want %s", got, errRead, data)
	}
}

func TestWriteAuthFileAtomicallyPreservesConcurrentReplacementOnSyncFailure(t *testing.T) {
	dir := t.TempDir()
	const fileName = "auth.json"
	path := filepath.Join(dir, fileName)
	originalData := []byte(`{"type":"codex","access_token":"old"}`)
	concurrentData := []byte(`{"type":"codex","access_token":"concurrent"}`)
	if errWrite := os.WriteFile(path, originalData, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()
	original, errSnapshot := captureAuthFileSnapshot(root, fileName)
	if errSnapshot != nil {
		t.Fatal(errSnapshot)
	}
	wantErr := errors.New("sync failed")
	errWrite := writeAuthFileAtomicallyAtRootWithPolicy(
		root,
		fileName,
		[]byte(`{"type":"codex","access_token":"new"}`),
		&original,
		false,
		0o600,
		func(*os.Root) error {
			if errConcurrent := os.WriteFile(path, concurrentData, 0o600); errConcurrent != nil {
				t.Fatal(errConcurrent)
			}
			return wantErr
		},
	)
	if !errors.Is(errWrite, wantErr) {
		t.Fatalf("write error = %v, want %v", errWrite, wantErr)
	}
	if outcome, ok := cliproxyauth.SaveOutcomeFromError(errWrite); !ok || outcome != cliproxyauth.SaveOutcomeUncertain {
		t.Fatalf("write outcome = %v, %t; want uncertain", outcome, ok)
	}
	got, errRead := os.ReadFile(path)
	if errRead != nil || !bytes.Equal(got, concurrentData) {
		t.Fatalf("auth after concurrent replacement = %s, %v; want %s", got, errRead, concurrentData)
	}
}

func TestWriteAuthFileAtomicallyPreservesConcurrentModeChangeOnSyncFailure(t *testing.T) {
	dir := t.TempDir()
	const fileName = "auth.json"
	path := filepath.Join(dir, fileName)
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"old"}`), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()
	snapshot, errSnapshot := captureAuthFileSnapshot(root, fileName)
	if errSnapshot != nil {
		t.Fatal(errSnapshot)
	}
	newData := []byte(`{"type":"codex","access_token":"new"}`)
	wantErr := errors.New("sync failed")
	errWrite := writeAuthFileAtomicallyAtRootWithPolicy(root, fileName, newData, &snapshot, false, 0o600, func(*os.Root) error {
		if errChmod := os.Chmod(path, 0o400); errChmod != nil {
			t.Fatal(errChmod)
		}
		return wantErr
	})
	if !errors.Is(errWrite, wantErr) {
		t.Fatalf("write error = %v, want %v", errWrite, wantErr)
	}
	if outcome, ok := cliproxyauth.SaveOutcomeFromError(errWrite); !ok || outcome != cliproxyauth.SaveOutcomeUncertain {
		t.Fatalf("write outcome = %v, %t; want uncertain", outcome, ok)
	}
	got, errRead := os.ReadFile(path)
	if errRead != nil || !bytes.Equal(got, newData) {
		t.Fatalf("auth after concurrent chmod = %s, %v; want %s", got, errRead, newData)
	}
	info, errStat := os.Stat(path)
	if errStat != nil {
		t.Fatal(errStat)
	}
	if info.Mode().Perm() != 0o400 {
		t.Fatalf("mode after concurrent chmod = %o, want 400", info.Mode().Perm())
	}
}

func TestWriteAuthFileAtomicallyConditionalCreateRemovesStagingName(t *testing.T) {
	dir := t.TempDir()
	const fileName = "auth.json"
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()
	original, errSnapshot := captureAuthFileSnapshot(root, fileName)
	if errSnapshot != nil {
		t.Fatal(errSnapshot)
	}
	if errWrite := writeAuthFileAtomicallyAtRoot(root, fileName, []byte(`{"type":"codex"}`), &original); errWrite != nil {
		t.Fatal(errWrite)
	}
	entries, errReadDir := os.ReadDir(dir)
	if errReadDir != nil {
		t.Fatal(errReadDir)
	}
	foundAuth := false
	for _, entry := range entries {
		switch {
		case entry.Name() == fileName:
			foundAuth = true
		case authfileguard.IsPersistentLockFileName(entry.Name()):
			// Cross-process lock files are intentionally persistent.
		default:
			t.Fatalf("unexpected persisted directory entry %q", entry.Name())
		}
	}
	if !foundAuth {
		t.Fatalf("persisted directory entries = %v, want %s", entries, fileName)
	}
}

func TestJoinAuthSaveCleanupErrorMarksDurableWriteCommitted(t *testing.T) {
	wantErr := errors.New("unlock failed")
	errSave := joinAuthSaveCleanupError(nil, wantErr, true)
	if !errors.Is(errSave, wantErr) {
		t.Fatalf("cleanup error = %v, want %v", errSave, wantErr)
	}
	if outcome, ok := cliproxyauth.SaveOutcomeFromError(errSave); !ok || outcome != cliproxyauth.SaveOutcomeCommitted {
		t.Fatalf("cleanup outcome = %v, %t; want committed", outcome, ok)
	}

	rolledBack := cliproxyauth.NewSaveOutcomeError(cliproxyauth.SaveOutcomeRolledBack, errors.New("write rolled back"))
	errSave = joinAuthSaveCleanupError(rolledBack, wantErr, false)
	if outcome, ok := cliproxyauth.SaveOutcomeFromError(errSave); !ok || outcome != cliproxyauth.SaveOutcomeRolledBack {
		t.Fatalf("rolled-back cleanup outcome = %v, %t; want rolled back", outcome, ok)
	}
	errSave = joinAuthSaveCleanupError(errors.New("staging failed"), wantErr, false)
	if outcome, ok := cliproxyauth.SaveOutcomeFromError(errSave); !ok || outcome != cliproxyauth.SaveOutcomeRolledBack {
		t.Fatalf("pre-install cleanup outcome = %v, %t; want rolled back", outcome, ok)
	}
}

func TestRestoreAuthSnapshotPreservesZeroPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not preserve Unix permission bits")
	}
	dir := t.TempDir()
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()
	const name = "auth.json"
	if errWrite := root.WriteFile(name, []byte(`{"state":"installed"}`), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	installed, errInstalled := captureAuthFileSnapshot(root, name)
	if errInstalled != nil {
		t.Fatal(errInstalled)
	}
	original := authFileSnapshot{data: []byte(`{"state":"original"}`), mode: 0, exists: true}
	if errRestore := restoreAuthSnapshotAfterSyncFailure(root, name, &original, &installed, syncAuthSnapshotDirectory); errRestore != nil {
		t.Fatalf("restoreAuthSnapshotAfterSyncFailure() error = %v", errRestore)
	}
	info, errStat := root.Lstat(name)
	if errStat != nil {
		t.Fatal(errStat)
	}
	if info.Mode().Perm() != 0 {
		t.Fatalf("restored mode = %o, want 0", info.Mode().Perm())
	}
	if errChmod := os.Chmod(filepath.Join(dir, name), 0o600); errChmod != nil {
		t.Fatal(errChmod)
	}
	data, errRead := root.ReadFile(name)
	if errRead != nil || string(data) != `{"state":"original"}` {
		t.Fatalf("restored data = %q, error = %v", data, errRead)
	}
}

func TestAuthFileRollbackCompletedAcceptsCommittedCleanupWarning(t *testing.T) {
	if !authFileRollbackCompleted(nil) {
		t.Fatal("nil rollback error was not treated as completed")
	}
	cleanupErr := cliproxyauth.NewSaveOutcomeError(cliproxyauth.SaveOutcomeCommitted, errors.New("unlock failed"))
	if !authFileRollbackCompleted(cleanupErr) {
		t.Fatal("committed rollback cleanup warning was not treated as completed")
	}
	if authFileRollbackCompleted(cliproxyauth.NewSaveOutcomeError(cliproxyauth.SaveOutcomeRolledBack, errors.New("restore rolled back"))) {
		t.Fatal("rolled-back restore was treated as completed")
	}
	if authFileRollbackCompleted(errors.New("restore failed")) {
		t.Fatal("unknown restore failure was treated as completed")
	}
}

func TestRestoreAuthFileSnapshotAtRootPreservesConcurrentReplacement(t *testing.T) {
	dir := t.TempDir()
	const fileName = "auth.json"
	newData := []byte(`{"type":"codex","access_token":"new"}`)
	replacement := append([]byte(nil), newData...)
	path := filepath.Join(dir, fileName)
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatalf("OpenRoot() error = %v", errRoot)
	}
	defer root.Close()
	if errWrite := os.WriteFile(path, newData, 0o600); errWrite != nil {
		t.Fatalf("write new auth: %v", errWrite)
	}
	installed, errSnapshot := captureAuthFileSnapshot(root, fileName)
	if errSnapshot != nil {
		t.Fatalf("capture installed auth: %v", errSnapshot)
	}
	replacementPath := filepath.Join(dir, "replacement.json")
	if errWrite := os.WriteFile(replacementPath, replacement, 0o600); errWrite != nil {
		t.Fatalf("stage replacement auth: %v", errWrite)
	}
	if errRename := os.Rename(replacementPath, path); errRename != nil {
		t.Fatalf("install replacement auth: %v", errRename)
	}
	if errRestore := restoreAuthFileSnapshotAtRoot(root, fileName, installed, authFileSnapshot{}); !errors.Is(errRestore, authfileguard.ErrPersistGenerationStale) {
		t.Fatalf("restoreAuthFileSnapshotAtRoot() error = %v, want stale generation", errRestore)
	}
	got, errRead := os.ReadFile(path)
	if errRead != nil || !bytes.Equal(got, replacement) {
		t.Fatalf("replacement auth = %s, %v; want %s", got, errRead, replacement)
	}
}

func TestAuthRuntimeSnapshotRestoresTokenStorageMetadata(t *testing.T) {
	storage := &testTokenStorage{metadata: map[string]any{"state": "before"}}
	auth := &cliproxyauth.Auth{
		Storage:  storage,
		Metadata: map[string]any{"type": "codex"},
	}
	snapshot := captureAuthRuntimeSnapshot(auth)
	storage.SetMetadata(map[string]any{"state": "after"})
	auth.Metadata["type"] = "changed"

	snapshot.restore(auth)
	if got := storage.metadata["state"]; got != "before" {
		t.Fatalf("storage metadata state = %#v, want before", got)
	}
	if got := auth.Metadata["type"]; got != "codex" {
		t.Fatalf("auth metadata type = %#v, want codex", got)
	}
}

func TestDeleteAuthFileTransactionOutcomes(t *testing.T) {
	errDelete := errors.New("remote delete failed")
	errProbe := errors.New("remote probe failed")
	tests := []struct {
		name       string
		deleteErr  error
		remote     authDeleteProbeState
		probeErr   error
		wantErr    bool
		wantResult cliproxyauth.DeleteOutcome
		wantLocal  bool
	}{
		{name: "success", wantLocal: false},
		{name: "success_remote_original_still_exists", remote: authDeleteProbeOriginal, wantErr: true, wantResult: cliproxyauth.DeleteOutcomeUncertain, wantLocal: false},
		{name: "success_remote_replaced", remote: authDeleteProbeReplaced, wantErr: true, wantResult: cliproxyauth.DeleteOutcomeUncertain, wantLocal: false},
		{name: "committed", deleteErr: errDelete, remote: authDeleteProbeAbsent, wantErr: true, wantResult: cliproxyauth.DeleteOutcomeCommitted, wantLocal: false},
		{name: "remote_original_still_exists", deleteErr: errDelete, remote: authDeleteProbeOriginal, wantErr: true, wantResult: cliproxyauth.DeleteOutcomeUncertain, wantLocal: false},
		{name: "replacement_is_uncertain", deleteErr: errDelete, remote: authDeleteProbeReplaced, wantErr: true, wantResult: cliproxyauth.DeleteOutcomeUncertain, wantLocal: false},
		{name: "uncertain", deleteErr: errDelete, probeErr: errProbe, wantErr: true, wantResult: cliproxyauth.DeleteOutcomeUncertain, wantLocal: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			const fileName = "auth.json"
			wantData := []byte(`{"type":"codex","access_token":"token"}`)
			path := filepath.Join(dir, fileName)
			if errWrite := os.WriteFile(path, wantData, 0o600); errWrite != nil {
				t.Fatalf("write auth file: %v", errWrite)
			}
			root, errRoot := os.OpenRoot(dir)
			if errRoot != nil {
				t.Fatalf("OpenRoot() error = %v", errRoot)
			}
			defer root.Close()

			errTransaction := deleteAuthFileTransaction(
				root,
				fileName,
				func(authFileSnapshot) error { return nil },
				func() error { return tt.deleteErr },
				func() (authDeleteProbeState, error) { return tt.remote, tt.probeErr },
			)
			if tt.wantErr {
				if tt.deleteErr != nil && !errors.Is(errTransaction, errDelete) {
					t.Fatalf("deleteAuthFileTransaction() error = %v, want %v", errTransaction, errDelete)
				}
				if gotOutcome, ok := cliproxyauth.DeleteOutcomeFromError(errTransaction); !ok || gotOutcome != tt.wantResult {
					t.Fatalf("delete outcome = %v, %t; want %v", gotOutcome, ok, tt.wantResult)
				}
			} else if errTransaction != nil {
				t.Fatalf("deleteAuthFileTransaction() error = %v", errTransaction)
			}

			gotData, errRead := os.ReadFile(path)
			if tt.wantLocal {
				if errRead != nil || !bytes.Equal(gotData, wantData) {
					t.Fatalf("local auth = %s, %v; want %s", gotData, errRead, wantData)
				}
			} else if !errors.Is(errRead, os.ErrNotExist) {
				t.Fatalf("local auth still exists: %v", errRead)
			}
		})
	}
}

func TestDeleteAuthFileTransactionRequiresDurablePreparation(t *testing.T) {
	dir := t.TempDir()
	const fileName = "auth.json"
	path := filepath.Join(dir, fileName)
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatalf("OpenRoot() error = %v", errRoot)
	}
	defer root.Close()

	errPrepare := errors.New("disk full")
	remoteCalled := false
	errDelete := deleteAuthFileTransaction(root, fileName, func(authFileSnapshot) error { return errPrepare }, func() error {
		remoteCalled = true
		return nil
	}, func() (authDeleteProbeState, error) {
		return authDeleteProbeAbsent, nil
	})
	if !errors.Is(errDelete, errPrepare) {
		t.Fatalf("delete error = %v, want %v", errDelete, errPrepare)
	}
	if outcome, ok := cliproxyauth.DeleteOutcomeFromError(errDelete); !ok || outcome != cliproxyauth.DeleteOutcomeRolledBack {
		t.Fatalf("delete outcome = %v, %t; want rolled back", outcome, ok)
	}
	if remoteCalled {
		t.Fatal("remote deletion ran without durable preparation")
	}
	if _, errStat := os.Stat(path); errStat != nil {
		t.Fatalf("local auth was removed before durable preparation: %v", errStat)
	}
}

func TestDeleteAuthFileTransactionPreservesReplacementAfterPreparation(t *testing.T) {
	dir := t.TempDir()
	const fileName = "auth.json"
	path := filepath.Join(dir, fileName)
	original := []byte(`{"type":"codex","access_token":"original"}`)
	replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	if errWrite := os.WriteFile(path, original, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()
	remoteCalled := false

	errDelete := deleteAuthFileTransaction(root, fileName, func(authFileSnapshot) error {
		if errRemove := os.Remove(path); errRemove != nil {
			return errRemove
		}
		return os.WriteFile(path, replacement, 0o600)
	}, func() error {
		remoteCalled = true
		return nil
	}, func() (authDeleteProbeState, error) {
		return authDeleteProbeAbsent, nil
	})
	if !errors.Is(errDelete, authfileguard.ErrPersistGenerationStale) {
		t.Fatalf("delete error = %v, want stale generation", errDelete)
	}
	if outcome, explicit := cliproxyauth.DeleteOutcomeFromError(errDelete); !explicit || outcome != cliproxyauth.DeleteOutcomeUncertain {
		t.Fatalf("delete outcome = %v, %t; want uncertain", outcome, explicit)
	}
	if remoteCalled {
		t.Fatal("remote deletion ran after the local generation changed")
	}
	got, errRead := os.ReadFile(path)
	if errRead != nil || !bytes.Equal(got, replacement) {
		t.Fatalf("replacement after stale delete = %s, %v; want %s", got, errRead, replacement)
	}
}

func TestDeleteAuthFileTransactionPreservesFileCreatedAfterMissingPreparation(t *testing.T) {
	dir := t.TempDir()
	const relativePath = "missing/auth.json"
	path := filepath.Join(dir, filepath.FromSlash(relativePath))
	replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()
	remoteCalled := false

	errDelete := deleteAuthFileTransaction(root, filepath.FromSlash(relativePath), func(original authFileSnapshot) error {
		if original.exists {
			t.Fatal("missing target was reported as existing")
		}
		if errMkdir := os.MkdirAll(filepath.Dir(path), 0o700); errMkdir != nil {
			return errMkdir
		}
		return os.WriteFile(path, replacement, 0o600)
	}, func() error {
		remoteCalled = true
		return nil
	}, func() (authDeleteProbeState, error) {
		return authDeleteProbeAbsent, nil
	})
	if !errors.Is(errDelete, authfileguard.ErrPersistGenerationStale) {
		t.Fatalf("delete error = %v, want stale generation", errDelete)
	}
	if outcome, explicit := cliproxyauth.DeleteOutcomeFromError(errDelete); !explicit || outcome != cliproxyauth.DeleteOutcomeUncertain {
		t.Fatalf("delete outcome = %v, %t; want uncertain", outcome, explicit)
	}
	if remoteCalled {
		t.Fatal("remote deletion ran after a missing target was created")
	}
	if got, errRead := os.ReadFile(path); errRead != nil || !bytes.Equal(got, replacement) {
		t.Fatalf("replacement after stale delete = %s, %v; want %s", got, errRead, replacement)
	}
}

func TestDeleteAuthFileTransactionLocksMissingParentThroughRemoteOperation(t *testing.T) {
	dir := t.TempDir()
	const relativePath = "missing/auth.json"
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()
	remoteStarted := make(chan struct{})
	releaseRemote := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- deleteAuthFileTransaction(root, filepath.FromSlash(relativePath), func(authFileSnapshot) error {
			return nil
		}, func() error {
			close(remoteStarted)
			<-releaseRemote
			return nil
		}, func() (authDeleteProbeState, error) {
			return authDeleteProbeAbsent, nil
		})
	}()
	assertPersistentAuthTargetHeldDuringRemoteOperation(t, dir, filepath.FromSlash(relativePath), remoteStarted, releaseRemote, done)
}

func TestDeleteAuthFileTransactionIsUncertainAfterPreparedLocalFailure(t *testing.T) {
	dir := t.TempDir()
	const fileName = "auth.json"
	path := filepath.Join(dir, fileName)
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatalf("OpenRoot() error = %v", errRoot)
	}
	remoteCalled := false
	errDelete := deleteAuthFileTransaction(root, fileName, func(authFileSnapshot) error { return root.Close() }, func() error {
		remoteCalled = true
		return nil
	}, func() (authDeleteProbeState, error) {
		return authDeleteProbeAbsent, nil
	})
	if outcome, ok := cliproxyauth.DeleteOutcomeFromError(errDelete); !ok || outcome != cliproxyauth.DeleteOutcomeUncertain {
		t.Fatalf("delete outcome = %v, %t; want uncertain", outcome, ok)
	}
	if remoteCalled {
		t.Fatal("remote deletion ran after prepared local failure")
	}
	if _, errStat := os.Stat(path); errStat != nil {
		t.Fatalf("local auth changed after failed removal: %v", errStat)
	}
}

func TestDeleteAuthFileTransactionPreservesSameContentReplacement(t *testing.T) {
	dir := t.TempDir()
	const fileName = "auth.json"
	path := filepath.Join(dir, fileName)
	data := []byte(`{"type":"codex","access_token":"same"}`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	original, errOriginal := os.Stat(path)
	if errOriginal != nil {
		t.Fatalf("stat original auth: %v", errOriginal)
	}
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatalf("OpenRoot() error = %v", errRoot)
	}
	defer root.Close()
	remoteCalled := false
	errDelete := deleteAuthFileTransaction(root, fileName, func(authFileSnapshot) error {
		temporary := filepath.Join(dir, "replacement.json")
		if errWrite := os.WriteFile(temporary, data, 0o600); errWrite != nil {
			return errWrite
		}
		return os.Rename(temporary, path)
	}, func() error {
		remoteCalled = true
		return nil
	}, func() (authDeleteProbeState, error) {
		return authDeleteProbeAbsent, nil
	})
	if outcome, ok := cliproxyauth.DeleteOutcomeFromError(errDelete); !ok || outcome != cliproxyauth.DeleteOutcomeUncertain {
		t.Fatalf("delete outcome = %v, %t; want uncertain", outcome, ok)
	}
	if !errors.Is(errDelete, authfileguard.ErrPersistGenerationStale) {
		t.Fatalf("delete error = %v, want stale local generation", errDelete)
	}
	if remoteCalled {
		t.Fatal("remote deletion ran after same-content local replacement")
	}
	replacement, errReplacement := os.Stat(path)
	if errReplacement != nil || os.SameFile(original, replacement) {
		t.Fatalf("replacement inode = %v, %v; want a preserved new generation", replacement, errReplacement)
	}
}

func TestDurableAuthDeletePersistsBackendIdentityBeforeMutation(t *testing.T) {
	rootDir := t.TempDir()
	authDir := filepath.Join(rootDir, "auths")
	if errMkdir := os.Mkdir(authDir, 0o700); errMkdir != nil {
		t.Fatalf("create auth dir: %v", errMkdir)
	}
	path := filepath.Join(authDir, "auth.json")
	data := []byte(`{"type":"codex","access_token":"token"}`)
	deleteCtx, prepare, clear := durableAuthDelete(
		t.Context(),
		filepath.Join(rootDir, "config.yaml"),
		authDir,
		path,
		data,
		"postgres:auth.json",
		"revision-7",
		true,
		true,
		data,
	)
	if errPrepare := prepare(); errPrepare != nil {
		t.Fatalf("prepare() error = %v", errPrepare)
	}
	t.Cleanup(func() { _ = clear() })
	generation := authfileguard.DeleteGenerationFromContext(deleteCtx)
	if generation == nil {
		t.Fatal("delete generation is nil")
	}
	identity, ok := generation.Snapshot().Identities["postgres:auth.json"]
	if !ok || identity.Value != "revision-7" || !identity.RetrySafe {
		t.Fatalf("persisted identity = %#v, %t; want revision-7 retry-safe", identity, ok)
	}
	entries, errRead := os.ReadDir(filepath.Join(rootDir, ".cliproxy-delete-quarantine"))
	tombstones := 0
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".tombstone") {
			tombstones++
		}
	}
	if errRead != nil || tombstones != 1 {
		t.Fatalf("delete tombstones = %d, %v; want one", tombstones, errRead)
	}
}

func TestDurableAuthDeleteRetryCannotBindUnseenBackendIdentity(t *testing.T) {
	rootDir := t.TempDir()
	authDir := filepath.Join(rootDir, "auths")
	if errMkdir := os.Mkdir(authDir, 0o700); errMkdir != nil {
		t.Fatalf("create auth dir: %v", errMkdir)
	}
	generation := authfileguard.NewDeleteGeneration("hash")
	ctx := authfileguard.WithDeleteGeneration(t.Context(), generation)
	ctx = authfileguard.WithDeleteAttempt(ctx, 1)
	_, prepare, _ := durableAuthDelete(
		ctx,
		filepath.Join(rootDir, "config.yaml"),
		authDir,
		filepath.Join(authDir, "auth.json"),
		nil,
		"object:auth.json",
		"version-b",
		true,
		false,
		nil,
	)
	if errPrepare := prepare(); !errors.Is(errPrepare, authfileguard.ErrDeleteGenerationUncertain) {
		t.Fatalf("prepare() error = %v, want uncertain generation", errPrepare)
	}
	if len(generation.Snapshot().Identities) != 0 {
		t.Fatalf("retry bound unseen identity: %#v", generation.Snapshot().Identities)
	}
}

func TestDurableAuthDeleteRejectsRemoteContentMismatchBeforeQuarantine(t *testing.T) {
	rootDir := t.TempDir()
	authDir := filepath.Join(rootDir, "auths")
	if errMkdir := os.Mkdir(authDir, 0o700); errMkdir != nil {
		t.Fatalf("create auth dir: %v", errMkdir)
	}
	path := filepath.Join(authDir, "auth.json")
	localData := []byte(`{"type":"codex","access_token":"local"}`)
	remoteData := []byte(`{"type":"codex","access_token":"remote"}`)
	_, prepare, _ := durableAuthDelete(
		t.Context(),
		filepath.Join(rootDir, "config.yaml"),
		authDir,
		path,
		localData,
		"object:auth.json",
		"version-2",
		true,
		true,
		remoteData,
	)
	if errPrepare := prepare(); !errors.Is(errPrepare, authfileguard.ErrPersistGenerationStale) {
		t.Fatalf("prepare() error = %v, want stale generation", errPrepare)
	}
	if authfileguard.IsQuarantined(path) {
		t.Fatal("remote content mismatch created a delete quarantine")
	}
}

func TestJoinAuthDeleteParentClosePreservesCommittedOutcome(t *testing.T) {
	errClose := errors.New("close parent failed")
	errResult := joinAuthDeleteParentClose(nil, errClose)
	if outcome, ok := cliproxyauth.DeleteOutcomeFromError(errResult); !ok || outcome != cliproxyauth.DeleteOutcomeCommitted {
		t.Fatalf("delete outcome = %v, %t; want committed", outcome, ok)
	}
	if !errors.Is(errResult, errClose) {
		t.Fatalf("delete error = %v, want close error", errResult)
	}
}

func TestRejectReservedAuthLockPath(t *testing.T) {
	for _, path := range []string{
		".auth-root-lock",
		filepath.Join("nested", ".auth-lock-0123456789abcdef0123456789abcdef"),
	} {
		if err := rejectReservedAuthLockPath(path); !errors.Is(err, errReservedAuthLockPath) {
			t.Errorf("rejectReservedAuthLockPath(%q) = %v", path, err)
		}
	}
	if err := rejectReservedAuthLockPath(filepath.Join("nested", "auth.json")); err != nil {
		t.Fatalf("normal auth path rejected: %v", err)
	}
}

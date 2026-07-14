package store

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	internalauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestProbeAuthDeleteResultStopsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	called := false
	_, errProbe := probeAuthDeleteResult(ctx, func(context.Context) (authDeleteProbeState, error) {
		called = true
		return authDeleteProbeOriginal, nil
	})
	if !errors.Is(errProbe, context.Canceled) {
		t.Fatalf("probeAuthDeleteResult() error = %v, want context canceled", errProbe)
	}
	if called {
		t.Fatal("probe ran after context cancellation")
	}
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

type staticTokenStorage struct {
	data []byte
}

type blockingTokenStorage struct {
	started chan struct{}
	release chan struct{}
	data    []byte
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

func TestProduceAuthStorageDataRejectsInvalidAndRetiredOutput(t *testing.T) {
	for _, test := range []struct {
		name string
		data []byte
	}{
		{name: "null", data: []byte(`null`)},
		{name: "retired", data: []byte(`{"type":"gemini","access_token":"legacy"}`)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, errProduce := produceAuthStorageData(staticTokenStorage{data: test.data}); errProduce == nil {
				t.Fatal("produceAuthStorageData() accepted unsafe output")
			}
		})
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
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatalf("OpenRoot() error = %v", errRoot)
	}
	defer root.Close()
	original, errSnapshot := captureAuthFileSnapshot(root, fileName)
	if errSnapshot != nil {
		t.Fatalf("capture snapshot: %v", errSnapshot)
	}
	if errWrite := writeAuthFileAtomicallyAtRoot(root, fileName, newData, &original); errWrite != nil {
		t.Fatalf("write new auth: %v", errWrite)
	}
	if errRestore := restoreAuthFileSnapshotAtRoot(root, fileName, newData, original); errRestore != nil {
		t.Fatalf("restoreAuthFileSnapshotAtRoot() error = %v", errRestore)
	}
	got, errRead := os.ReadFile(path)
	if errRead != nil || !bytes.Equal(got, oldData) {
		t.Fatalf("restored auth = %s, %v; want %s", got, errRead, oldData)
	}
}

func TestRestoreAuthFileSnapshotAtRootPreservesConcurrentReplacement(t *testing.T) {
	dir := t.TempDir()
	const fileName = "auth.json"
	newData := []byte(`{"type":"codex","access_token":"new"}`)
	replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	path := filepath.Join(dir, fileName)
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatalf("OpenRoot() error = %v", errRoot)
	}
	defer root.Close()
	if errWrite := os.WriteFile(path, newData, 0o600); errWrite != nil {
		t.Fatalf("write new auth: %v", errWrite)
	}
	if errWrite := os.WriteFile(path, replacement, 0o600); errWrite != nil {
		t.Fatalf("write replacement auth: %v", errWrite)
	}
	if errRestore := restoreAuthFileSnapshotAtRoot(root, fileName, newData, authFileSnapshot{}); !errors.Is(errRestore, authfileguard.ErrPersistGenerationStale) {
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

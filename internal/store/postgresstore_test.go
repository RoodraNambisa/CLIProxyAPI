package store

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/auth/vertex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestPostgresStoreEnsureSchemaAddsDurableAuthRevision(t *testing.T) {
	backend := &postgresStoreTestBackend{}
	db := newPostgresStoreTestSQLDB(t, backend)
	store := &PostgresStore{db: db, cfg: PostgresStoreConfig{ConfigTable: defaultConfigTable, AuthTable: defaultAuthTable}}

	if errSchema := store.EnsureSchema(t.Context()); errSchema != nil {
		t.Fatalf("EnsureSchema() error = %v", errSchema)
	}
	var queries strings.Builder
	for _, call := range backend.execCallsSnapshot() {
		queries.WriteString(call.query)
		queries.WriteByte('\n')
	}
	got := queries.String()
	for _, want := range []string{
		"auth_revision TEXT NOT NULL",
		"ADD COLUMN IF NOT EXISTS auth_revision TEXT",
		"SET auth_revision = md5(",
		"ALTER COLUMN auth_revision SET DEFAULT",
		"ALTER COLUMN auth_revision SET NOT NULL",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("EnsureSchema() queries missing %q:\n%s", want, got)
		}
	}
}

func TestPostgresStoreSyncAuthFromDatabaseRejectsInsertedSymlink(t *testing.T) {
	rowStarted := make(chan struct{}, 1)
	rowContinue := make(chan struct{})
	backend := &postgresStoreTestBackend{
		queryColumns: []string{"id", "content"},
		queryRows: [][]driver.Value{
			{"nested/auth.json", `{"type":"codex","access_token":"remote"}`},
		},
		listRowStarted:  rowStarted,
		listRowContinue: rowContinue,
	}
	db := newPostgresStoreTestSQLDB(t, backend)
	authDir := t.TempDir()
	store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: authDir}
	done := make(chan error, 1)
	go func() {
		done <- store.syncAuthFromDatabase(t.Context())
	}()
	select {
	case <-rowStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("syncAuthFromDatabase() did not reach the first row")
	}
	externalDir := t.TempDir()
	if errLink := os.Symlink(externalDir, filepath.Join(authDir, "nested")); errLink != nil {
		close(rowContinue)
		t.Skipf("symlink is unavailable: %v", errLink)
	}
	close(rowContinue)
	select {
	case errSync := <-done:
		if errSync == nil {
			t.Fatal("syncAuthFromDatabase() error = nil, want symlinked subdirectory rejection")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("syncAuthFromDatabase() did not finish")
	}
	if _, errStat := os.Stat(filepath.Join(externalDir, "auth.json")); !errors.Is(errStat, fs.ErrNotExist) {
		t.Fatalf("external auth target was modified: %v", errStat)
	}
}

func TestPostgresStoreSaveMetadataOnlyUsesCanonicalPayloadAndSourceHash(t *testing.T) {
	backend := &postgresStoreTestBackend{}
	db := newPostgresStoreTestSQLDB(t, backend)

	authDir := t.TempDir()
	store := &PostgresStore{
		db:      db,
		cfg:     PostgresStoreConfig{AuthTable: defaultAuthTable},
		authDir: authDir,
	}

	auth := &cliproxyauth.Auth{
		ID:       "auth.json",
		FileName: "auth.json",
		Provider: "claude",
		Metadata: map[string]any{
			"type":  "claude",
			"email": "pg@example.com",
		},
	}

	path, err := store.Save(context.Background(), auth)
	if err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	rawFile, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	wantRaw, err := cliproxyauth.CanonicalMetadataBytes(auth)
	if err != nil {
		t.Fatalf("CanonicalMetadataBytes() error = %v", err)
	}
	if !bytes.Equal(rawFile, wantRaw) {
		t.Fatalf("persisted file = %s, want %s", rawFile, wantRaw)
	}
	if !bytes.Contains(rawFile, []byte(`"disabled":false`)) {
		t.Fatalf("persisted file = %s, want canonical disabled flag", rawFile)
	}
	wantHash := cliproxyauth.SourceHashFromBytes(wantRaw)
	if got := auth.Attributes[cliproxyauth.SourceHashAttributeKey]; got != wantHash {
		t.Fatalf("source hash = %q, want %q", got, wantHash)
	}

	execs := postgresStoreTestExecCallsContaining(backend.execCallsSnapshot(), "INSERT INTO")
	if len(execs) != 1 {
		t.Fatalf("auth upsert count = %d, want 1", len(execs))
	}
	if got, want := execArgumentBytes(t, execs[0].args, 1), wantRaw; !bytes.Equal(got, want) {
		t.Fatalf("persisted db payload = %s, want %s", got, want)
	}
}

func TestPostgresStoreSaveRestoresLocalFileWhenPersistenceFails(t *testing.T) {
	wantErr := errors.New("upsert failed")
	backend := &postgresStoreTestBackend{execErrors: []error{nil, wantErr}}
	db := newPostgresStoreTestSQLDB(t, backend)
	authDir := t.TempDir()
	store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: authDir}
	const fileName = "rollback-save.json"
	oldData := []byte(`{"type":"codex","access_token":"old"}`)
	path := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(path, oldData, 0o600); errWrite != nil {
		t.Fatalf("write old auth: %v", errWrite)
	}
	auth := &cliproxyauth.Auth{
		ID: fileName, FileName: fileName, Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "new"},
	}
	if _, errSave := store.Save(t.Context(), auth); !errors.Is(errSave, wantErr) {
		t.Fatalf("Save() error = %v, want %v", errSave, wantErr)
	}
	got, errRead := os.ReadFile(path)
	if errRead != nil || !bytes.Equal(got, oldData) {
		t.Fatalf("local auth after failed persistence = %s, %v; want %s", got, errRead, oldData)
	}
}

func TestPostgresStoreSaveRestoresLocalFileWhenCommitFails(t *testing.T) {
	wantErr := errors.New("commit failed")
	backend := &postgresStoreTestBackend{commitError: wantErr}
	db := newPostgresStoreTestSQLDB(t, backend)
	authDir := t.TempDir()
	store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: authDir}
	const fileName = "rollback-commit.json"
	oldData := []byte(`{"type":"codex","access_token":"old"}`)
	path := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(path, oldData, 0o600); errWrite != nil {
		t.Fatalf("write old auth: %v", errWrite)
	}
	auth := &cliproxyauth.Auth{
		ID: fileName, FileName: fileName, Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "new"},
	}
	if _, errSave := store.Save(t.Context(), auth); !errors.Is(errSave, wantErr) {
		t.Fatalf("Save() error = %v, want %v", errSave, wantErr)
	}
	got, errRead := os.ReadFile(path)
	if errRead != nil || !bytes.Equal(got, oldData) {
		t.Fatalf("local auth after failed commit = %s, %v; want %s", got, errRead, oldData)
	}
}

func TestPostgresStoreAllowsUpdatingFileBackedGeminiAPIKey(t *testing.T) {
	const fileName = "gemini-key.json"
	oldData := []byte(`{"type":"gemini","api_key":"old-key"}`)
	backend := &postgresStoreTestBackend{authRecords: map[string][]byte{fileName: oldData}}
	db := newPostgresStoreTestSQLDB(t, backend)
	authDir := t.TempDir()
	path := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(path, oldData, 0o600); errWrite != nil {
		t.Fatalf("write old Gemini API key: %v", errWrite)
	}
	store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: authDir}
	auth := &cliproxyauth.Auth{
		ID: fileName, FileName: fileName, Provider: "gemini",
		Metadata: map[string]any{"type": "gemini", "api_key": "new-key"},
	}
	if _, errSave := store.Save(t.Context(), auth); errSave != nil {
		t.Fatalf("Save() error = %v", errSave)
	}
	execs := postgresStoreTestExecCallsContaining(backend.execCallsSnapshot(), "INSERT INTO")
	if len(execs) != 1 || !strings.Contains(execs[0].query, "api_key") || !strings.Contains(execs[0].query, "jsonb_typeof") || !strings.Contains(execs[0].query, "[[:space:]]") {
		t.Fatalf("conditional upsert query = %#v", execs)
	}
	if got, ok := backend.authRecordSnapshot(fileName); !ok || !bytes.Contains(got, []byte(`"api_key":"new-key"`)) {
		t.Fatalf("database Gemini API key = %s, %t", got, ok)
	}
}

func TestPostgresResolveDeletePathAnchorsNestedIDToAuthDir(t *testing.T) {
	authDir := t.TempDir()
	store := &PostgresStore{authDir: authDir}
	got, errResolve := store.resolveDeletePath(filepath.Join("nested", "auth.json"))
	if errResolve != nil {
		t.Fatalf("resolveDeletePath() error = %v", errResolve)
	}
	want := filepath.Join(authDir, "nested", "auth.json")
	if got != want {
		t.Fatalf("resolveDeletePath() = %q, want %q", got, want)
	}
}

func TestPostgresStoreSaveRejectsIntermediateSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink setup requires elevated privileges on some Windows systems")
	}
	backend := &postgresStoreTestBackend{}
	db := newPostgresStoreTestSQLDB(t, backend)
	authDir := t.TempDir()
	outsideDir := t.TempDir()
	if errLink := os.Symlink(outsideDir, filepath.Join(authDir, "team")); errLink != nil {
		t.Skipf("symlink unavailable: %v", errLink)
	}
	store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: authDir}
	auth := &cliproxyauth.Auth{
		ID: "team/auth.json", FileName: "team/auth.json", Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "token"},
	}
	if _, errSave := store.Save(t.Context(), auth); errSave == nil {
		t.Fatal("Save() error = nil, want symlink rejection")
	}
	if _, errStat := os.Stat(filepath.Join(outsideDir, "auth.json")); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("outside auth path was modified: %v", errStat)
	}
}

func TestPostgresStoreSaveStorageBackedAuthSyncsPersistedMetadataAndSourceHash(t *testing.T) {
	backend := &postgresStoreTestBackend{}
	db := newPostgresStoreTestSQLDB(t, backend)

	authDir := t.TempDir()
	store := &PostgresStore{
		db:      db,
		cfg:     PostgresStoreConfig{AuthTable: defaultAuthTable},
		authDir: authDir,
	}

	auth := &cliproxyauth.Auth{
		ID:       "auth.json",
		FileName: "auth.json",
		Provider: "codex",
		Storage:  &testTokenStorage{},
		Metadata: map[string]any{
			"type":                 "codex",
			"email":                "writer@example.com",
			"tool_prefix_disabled": true,
		},
	}

	path, err := store.Save(context.Background(), auth)
	if err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	if got, ok := auth.Metadata["access_token"].(string); !ok || got != "tok-storage" {
		t.Fatalf("metadata access_token = %#v, want %q", auth.Metadata["access_token"], "tok-storage")
	}
	if got, ok := auth.Metadata["refresh_token"].(string); !ok || got != "refresh-storage" {
		t.Fatalf("metadata refresh_token = %#v, want %q", auth.Metadata["refresh_token"], "refresh-storage")
	}
	if got, ok := auth.Metadata["tool_prefix_disabled"].(bool); !ok || !got {
		t.Fatalf("metadata tool_prefix_disabled = %#v, want true", auth.Metadata["tool_prefix_disabled"])
	}

	rawFile, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	wantRaw, err := cliproxyauth.CanonicalMetadataBytes(auth)
	if err != nil {
		t.Fatalf("CanonicalMetadataBytes() error = %v", err)
	}
	wantHash := cliproxyauth.SourceHashFromBytes(wantRaw)
	if got := auth.Attributes[cliproxyauth.SourceHashAttributeKey]; got != wantHash {
		t.Fatalf("source hash = %q, want %q", got, wantHash)
	}
	if got, ok := auth.Metadata["disabled"].(bool); !ok || got {
		t.Fatalf("metadata disabled = %#v, want false", auth.Metadata["disabled"])
	}
	if rawHash := cliproxyauth.SourceHashFromBytes(rawFile); rawHash != wantHash {
		t.Fatalf("raw storage file hash = %q, want %q", rawHash, wantHash)
	}

	execs := postgresStoreTestExecCallsContaining(backend.execCallsSnapshot(), "INSERT INTO")
	if len(execs) != 1 {
		t.Fatalf("auth upsert count = %d, want 1", len(execs))
	}
	if got := execArgumentBytes(t, execs[0].args, 1); !bytes.Equal(got, rawFile) {
		t.Fatalf("persisted db payload = %s, want %s", got, rawFile)
	}
}

func TestPostgresStoreSaveRejectsStorageOutputBeforeReplacingLocalAuth(t *testing.T) {
	backend := &postgresStoreTestBackend{authRecords: make(map[string][]byte)}
	db := newPostgresStoreTestSQLDB(t, backend)
	authDir := t.TempDir()
	store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: authDir}
	path := filepath.Join(authDir, "auth.json")
	original := []byte(`{"type":"codex","access_token":"old"}`)
	if errWrite := os.WriteFile(path, original, 0o600); errWrite != nil {
		t.Fatalf("write original auth: %v", errWrite)
	}
	auth := &cliproxyauth.Auth{
		ID: "auth.json", FileName: "auth.json", Provider: "codex",
		Storage:  staticTokenStorage{data: []byte(`{"type":"gemini","access_token":"legacy"}`)},
		Metadata: map[string]any{"type": "codex"},
	}
	if _, errSave := store.Save(t.Context(), auth); !errors.Is(errSave, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
		t.Fatalf("Save() error = %v, want retired read-only", errSave)
	}
	if got, errRead := os.ReadFile(path); errRead != nil || !bytes.Equal(got, original) {
		t.Fatalf("local auth changed: data=%s error=%v", got, errRead)
	}
	if _, exists := backend.authRecordSnapshot("auth.json"); exists {
		t.Fatal("unsafe storage output was persisted to the database")
	}
}

func TestPostgresStoreSaveRejectsQuarantinedAuthPath(t *testing.T) {
	backend := &postgresStoreTestBackend{authRecords: make(map[string][]byte)}
	db := newPostgresStoreTestSQLDB(t, backend)
	authDir := t.TempDir()
	store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: authDir}
	path := filepath.Join(authDir, "pending.json")
	authfileguard.MarkQuarantined(path)
	t.Cleanup(func() { authfileguard.ClearQuarantined(path) })

	_, errSave := store.Save(t.Context(), &cliproxyauth.Auth{
		ID: "pending.json", FileName: "pending.json", Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "replacement"},
	})
	if !errors.Is(errSave, authfileguard.ErrDeleteGenerationUncertain) {
		t.Fatalf("Save() error = %v, want pending deletion error", errSave)
	}
	if _, errStat := os.Stat(path); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("quarantined auth file was created: %v", errStat)
	}
	if _, exists := backend.authRecordSnapshot("pending.json"); exists {
		t.Fatal("quarantined auth record was persisted")
	}
}

func TestPostgresStoreSaveProbesCommitErrorsByWrittenRevision(t *testing.T) {
	errCommit := errors.New("commit result lost")
	tests := []struct {
		name                   string
		commitApplies          bool
		sameContentReplacement bool
		wantSuccess            bool
	}{
		{name: "committed", commitApplies: true, wantSuccess: true},
		{name: "not committed"},
		{name: "same content replacement", sameContentReplacement: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const fileName = "save-outcome.json"
			oldData := []byte(`{"type":"codex","access_token":"old"}`)
			auth := &cliproxyauth.Auth{
				ID: fileName, FileName: fileName, Provider: "codex",
				Metadata: map[string]any{"type": "codex", "access_token": "new"},
			}
			newData, errCanonical := cliproxyauth.CanonicalMetadataBytes(auth)
			if errCanonical != nil {
				t.Fatalf("CanonicalMetadataBytes() error = %v", errCanonical)
			}
			backend := &postgresStoreTestBackend{
				authRecords:        map[string][]byte{fileName: append([]byte(nil), oldData...)},
				authRevisions:      map[string]int64{fileName: 10},
				nextAuthRevision:   10,
				commitError:        errCommit,
				commitErrorApplies: tt.commitApplies,
			}
			if tt.sameContentReplacement {
				backend.commitErrorReplacement = map[string][]byte{fileName: append([]byte(nil), newData...)}
			}
			db := newPostgresStoreTestSQLDB(t, backend)
			authDir := t.TempDir()
			store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: authDir}
			path := filepath.Join(authDir, fileName)
			if errWrite := os.WriteFile(path, oldData, 0o600); errWrite != nil {
				t.Fatalf("write old auth: %v", errWrite)
			}

			_, errSave := store.Save(t.Context(), auth)
			if tt.wantSuccess {
				if errSave != nil {
					t.Fatalf("Save() error = %v", errSave)
				}
			} else if !errors.Is(errSave, errCommit) {
				t.Fatalf("Save() error = %v, want %v", errSave, errCommit)
			}

			wantLocal := oldData
			wantRemote := oldData
			if tt.wantSuccess {
				wantLocal = newData
				wantRemote = newData
			} else if tt.sameContentReplacement {
				wantRemote = newData
			}
			if gotLocal, errRead := os.ReadFile(path); errRead != nil || !bytes.Equal(gotLocal, wantLocal) {
				t.Fatalf("local auth = %s, %v; want %s", gotLocal, errRead, wantLocal)
			}
			if gotRemote, exists := backend.authRecordSnapshot(fileName); !exists || !bytes.Equal(gotRemote, wantRemote) {
				t.Fatalf("database auth = %s, %t; want %s", gotRemote, exists, wantRemote)
			}
			if !tt.wantSuccess && auth.Attributes != nil {
				t.Fatalf("failed Save() retained runtime attributes = %#v", auth.Attributes)
			}
		})
	}
}

func TestPostgresStoreSaveAcceptsJSONBNormalizedCommittedPayload(t *testing.T) {
	errCommit := errors.New("commit result lost")
	backend := &postgresStoreTestBackend{
		authRecords:         make(map[string][]byte),
		commitError:         errCommit,
		commitErrorApplies:  true,
		normalizeJSONBReads: true,
	}
	db := newPostgresStoreTestSQLDB(t, backend)
	store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: t.TempDir()}
	auth := &cliproxyauth.Auth{
		ID: "normalized.json", FileName: "normalized.json", Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "token"},
	}

	if _, errSave := store.Save(t.Context(), auth); errSave != nil {
		t.Fatalf("Save() error = %v", errSave)
	}
}

func TestPostgresStoreSaveSemanticallyEqualRollbackPreservesLocalBytes(t *testing.T) {
	errCommit := errors.New("commit failed")
	backend := &postgresStoreTestBackend{authRecords: make(map[string][]byte), commitError: errCommit}
	db := newPostgresStoreTestSQLDB(t, backend)
	authDir := t.TempDir()
	store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: authDir}
	const fileName = "semantic.json"
	localData := []byte("{\n  \"type\": \"codex\",\n  \"disabled\": false,\n  \"access_token\": \"token\"\n}\n")
	path := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(path, localData, 0o600); errWrite != nil {
		t.Fatalf("write local auth: %v", errWrite)
	}
	auth := &cliproxyauth.Auth{
		ID: fileName, FileName: fileName, Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "token"},
	}

	_, errSave := store.Save(t.Context(), auth)
	if !errors.Is(errSave, errCommit) {
		t.Fatalf("Save() error = %v, want %v", errSave, errCommit)
	}
	if errors.Is(errSave, authfileguard.ErrPersistGenerationStale) {
		t.Fatalf("Save() reported a stale local generation without rewriting it: %v", errSave)
	}
	if got, errRead := os.ReadFile(path); errRead != nil || !bytes.Equal(got, localData) {
		t.Fatalf("local auth = %s, %v; want original bytes", got, errRead)
	}
}

func TestPostgresStoreSaveRejectsRetiredFileCreatedDuringMarshal(t *testing.T) {
	backend := &postgresStoreTestBackend{authRecords: make(map[string][]byte)}
	db := newPostgresStoreTestSQLDB(t, backend)
	authDir := t.TempDir()
	store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: authDir}
	path := filepath.Join(authDir, "auth.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"old"}`), 0o600); errWrite != nil {
		t.Fatalf("write original auth: %v", errWrite)
	}
	storage := &blockingTokenStorage{started: make(chan struct{}), release: make(chan struct{}), data: []byte(`{"type":"codex","access_token":"new"}`)}
	saveDone := make(chan error, 1)
	go func() {
		_, errSave := store.Save(t.Context(), &cliproxyauth.Auth{ID: "auth.json", FileName: "auth.json", Provider: "codex", Storage: storage})
		saveDone <- errSave
	}()
	<-storage.started
	retired := []byte(`{"type":"gemini","access_token":"legacy"}`)
	if errWrite := os.WriteFile(path, retired, 0o600); errWrite != nil {
		close(storage.release)
		t.Fatalf("write concurrent retired auth: %v", errWrite)
	}
	close(storage.release)
	if errSave := <-saveDone; !errors.Is(errSave, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
		t.Fatalf("Save() error = %v, want retired read-only", errSave)
	}
	if got, errRead := os.ReadFile(path); errRead != nil || !bytes.Equal(got, retired) {
		t.Fatalf("concurrent retired auth changed: data=%s error=%v", got, errRead)
	}
	if _, exists := backend.authRecordSnapshot("auth.json"); exists {
		t.Fatal("concurrent retired auth was persisted to the database")
	}
	authfileguard.ClearRetired(path)
}

func TestPostgresStoreSaveRetriesRemoteWhenLocalContentMatches(t *testing.T) {
	errUpsert := errors.New("upsert failed")
	backend := &postgresStoreTestBackend{
		execErrors:  []error{nil, errUpsert},
		authRecords: make(map[string][]byte),
	}
	db := newPostgresStoreTestSQLDB(t, backend)
	authDir := t.TempDir()
	store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: authDir}
	auth := &cliproxyauth.Auth{
		ID:       "retry.json",
		FileName: "retry.json",
		Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "retry"},
	}
	if _, errSave := store.Save(t.Context(), auth); !errors.Is(errSave, errUpsert) {
		t.Fatalf("first Save() error = %v, want %v", errSave, errUpsert)
	}
	path := filepath.Join(authDir, "retry.json")
	if _, errStat := os.Stat(path); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("local auth remained after failed persistence: %v", errStat)
	}
	if _, errSave := store.Save(t.Context(), auth); errSave != nil {
		t.Fatalf("retry Save() error = %v", errSave)
	}
	wantData, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read local auth after retry: %v", errRead)
	}
	gotData, ok := backend.authRecordSnapshot("retry.json")
	if !ok || !bytes.Equal(gotData, wantData) {
		t.Fatalf("database auth = %s, %t; want %s", gotData, ok, wantData)
	}
	if upserts := postgresStoreTestExecCallsContaining(backend.execCallsSnapshot(), "INSERT INTO"); len(upserts) != 2 {
		t.Fatalf("auth upserts = %d, want 2", len(upserts))
	}
}

func TestPostgresDeleteAuthFileRestoresLocalAndRemoteStateOnFailure(t *testing.T) {
	wantErr := errors.New("delete failed")
	backend := &postgresStoreTestBackend{
		execErrors: []error{nil, wantErr, nil, wantErr},
	}
	db := newPostgresStoreTestSQLDB(t, backend)
	authDir := t.TempDir()
	store := &PostgresStore{
		db:      db,
		cfg:     PostgresStoreConfig{AuthTable: defaultAuthTable},
		authDir: authDir,
	}
	const fileName = "rollback.json"
	wantData := []byte(`{"type":"codex","access_token":"token"}`)
	if errWrite := os.WriteFile(filepath.Join(authDir, fileName), wantData, 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	root, errRoot := os.OpenRoot(authDir)
	if errRoot != nil {
		t.Fatalf("open auth root: %v", errRoot)
	}
	defer root.Close()
	errDelete := store.DeleteAuthFileAtRoot(t.Context(), root, fileName)
	if !errors.Is(errDelete, wantErr) {
		t.Fatalf("DeleteAuthFileAtRoot() error = %v, want %v", errDelete, wantErr)
	}
	if outcome, ok := cliproxyauth.DeleteOutcomeFromError(errDelete); !ok || outcome != cliproxyauth.DeleteOutcomeRolledBack {
		t.Fatalf("delete outcome = %v, %t; want rolled back", outcome, ok)
	}
	gotData, errRead := os.ReadFile(filepath.Join(authDir, fileName))
	if errRead != nil {
		t.Fatalf("read restored auth file: %v", errRead)
	}
	if !bytes.Equal(gotData, wantData) {
		t.Fatalf("restored auth file = %s, want %s", gotData, wantData)
	}
	if calls := backend.execCallsSnapshot(); len(calls) != 2 {
		t.Fatalf("database calls = %d, want advisory lock and delete attempt", len(calls))
	}

	errDelete = store.Delete(t.Context(), fileName)
	if !errors.Is(errDelete, wantErr) {
		t.Fatalf("Delete() error = %v, want %v", errDelete, wantErr)
	}
	if outcome, ok := cliproxyauth.DeleteOutcomeFromError(errDelete); !ok || outcome != cliproxyauth.DeleteOutcomeRolledBack {
		t.Fatalf("plain delete outcome = %v, %t; want rolled back", outcome, ok)
	}
	gotData, errRead = os.ReadFile(filepath.Join(authDir, fileName))
	if errRead != nil || !bytes.Equal(gotData, wantData) {
		t.Fatalf("plain Delete() local auth = %s, %v; want %s", gotData, errRead, wantData)
	}
	if calls := backend.execCallsSnapshot(); len(calls) != 4 {
		t.Fatalf("database calls = %d, want two transactional delete attempts", len(calls))
	}
}

func TestPostgresPersistAuthFilesPreservesReplacementFromOlderDeleteGeneration(t *testing.T) {
	const fileName = "auth.json"
	original := []byte(`{"type":"codex","access_token":"original"}`)
	replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	backend := &postgresStoreTestBackend{authRecords: map[string][]byte{fileName: replacement}}
	db := newPostgresStoreTestSQLDB(t, backend)
	store := &PostgresStore{
		db:      db,
		cfg:     PostgresStoreConfig{AuthTable: defaultAuthTable},
		authDir: t.TempDir(),
	}
	path := filepath.Join(store.authDir, fileName)
	ctx := authfileguard.WithExpectedDeleteHash(t.Context(), cliproxyauth.SourceHashFromBytes(original))
	if errPersist := store.PersistAuthFiles(ctx, "retry old auth deletion", path); errPersist != nil {
		t.Fatalf("PersistAuthFiles() error = %v", errPersist)
	}
	gotData, ok := backend.authRecordSnapshot(fileName)
	if !ok || !bytes.Equal(gotData, replacement) {
		t.Fatalf("database auth = %s, %t; want replacement %s", gotData, ok, replacement)
	}
	gotLocal, errRead := os.ReadFile(path)
	if errRead != nil || !bytes.Equal(gotLocal, replacement) {
		t.Fatalf("restored local replacement = %s, %v; want %s", gotLocal, errRead, replacement)
	}
}

func TestPostgresPersistAuthFilesRejectsChangedExpectedSnapshot(t *testing.T) {
	const fileName = "auth.json"
	remoteData := []byte(`{"type":"codex","access_token":"remote"}`)
	backend := &postgresStoreTestBackend{authRecords: map[string][]byte{fileName: append([]byte(nil), remoteData...)}}
	db := newPostgresStoreTestSQLDB(t, backend)
	store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: t.TempDir()}
	path := filepath.Join(store.authDir, fileName)
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"original"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth: %v", errWrite)
	}
	expected := cliproxyauth.SourceHashFromBytes([]byte(`{"type":"codex","access_token":"replacement"}`))
	ctx := authfileguard.WithExpectedPersistHash(t.Context(), expected)
	if errPersist := store.PersistAuthFiles(ctx, "persist replacement", path); !errors.Is(errPersist, authfileguard.ErrPersistGenerationStale) {
		t.Fatalf("PersistAuthFiles() error = %v, want ErrPersistGenerationStale", errPersist)
	}
	gotData, ok := backend.authRecordSnapshot(fileName)
	if !ok || !bytes.Equal(gotData, remoteData) {
		t.Fatalf("database auth = %s, %t; want unchanged %s", gotData, ok, remoteData)
	}
}

func TestPostgresPersistAuthFilesPreservesSameContentNewRowRevision(t *testing.T) {
	const fileName = "auth.json"
	data := []byte(`{"type":"codex","access_token":"same"}`)
	commitErr := errors.New("commit conflict")
	backend := &postgresStoreTestBackend{
		authRecords:            map[string][]byte{fileName: append([]byte(nil), data...)},
		commitError:            commitErr,
		commitErrorReplacement: map[string][]byte{fileName: append([]byte(nil), data...)},
	}
	db := newPostgresStoreTestSQLDB(t, backend)
	store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: t.TempDir()}
	path := filepath.Join(store.authDir, fileName)
	generation := authfileguard.NewDeleteGeneration(cliproxyauth.SourceHashFromBytes(data))
	ctx := authfileguard.WithDeleteGeneration(t.Context(), generation)
	if errPersist := store.PersistAuthFiles(authfileguard.WithDeleteIdentityBinding(ctx), "first auth deletion", path); !errors.Is(errPersist, commitErr) {
		t.Fatalf("first PersistAuthFiles() error = %v, want %v", errPersist, commitErr)
	}
	if errPersist := store.PersistAuthFiles(ctx, "retry old auth deletion", path); !errors.Is(errPersist, authfileguard.ErrDeleteGenerationUncertain) {
		t.Fatalf("retry PersistAuthFiles() error = %v, want uncertain generation", errPersist)
	}
	gotData, ok := backend.authRecordSnapshot(fileName)
	if !ok || !bytes.Equal(gotData, data) {
		t.Fatalf("database auth = %s, %t; want same-content replacement", gotData, ok)
	}
}

func TestPostgresPersistAuthFilesCompletesResumedDeleteWhenRecordIsAbsent(t *testing.T) {
	const fileName = "auth.json"
	data := []byte(`{"type":"codex","access_token":"deleted"}`)
	backend := &postgresStoreTestBackend{}
	db := newPostgresStoreTestSQLDB(t, backend)
	store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: t.TempDir()}
	generation := authfileguard.NewDeleteGeneration(cliproxyauth.SourceHashFromBytes(data))
	if !generation.BindBackendIdentity("postgres:"+fileName, "old-revision") {
		t.Fatal("bind old postgres identity")
	}
	ctx := authfileguard.WithDeleteGeneration(t.Context(), generation)
	if errPersist := store.PersistAuthFiles(ctx, "resume completed deletion", filepath.Join(store.authDir, fileName)); errPersist != nil {
		t.Fatalf("PersistAuthFiles() error = %v", errPersist)
	}
}

func TestPostgresPersistAuthFilesProbesCommitErrors(t *testing.T) {
	errCommit := errors.New("commit result lost")
	for _, tt := range []struct {
		name                   string
		commitApplies          bool
		sameContentReplacement bool
		wantSuccess            bool
		wantUncertain          bool
	}{
		{name: "committed", commitApplies: true, wantSuccess: true},
		{name: "not committed"},
		{name: "same content replacement", sameContentReplacement: true, wantUncertain: true},
	} {
		t.Run("delete "+tt.name, func(t *testing.T) {
			const fileName = "delete-persist-outcome.json"
			data := []byte(`{"type":"codex","access_token":"original"}`)
			backend := &postgresStoreTestBackend{
				authRecords:        map[string][]byte{fileName: append([]byte(nil), data...)},
				authRevisions:      map[string]int64{fileName: 10},
				nextAuthRevision:   10,
				commitError:        errCommit,
				commitErrorApplies: tt.commitApplies,
			}
			if tt.sameContentReplacement {
				backend.commitErrorReplacement = map[string][]byte{fileName: append([]byte(nil), data...)}
			}
			db := newPostgresStoreTestSQLDB(t, backend)
			store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: t.TempDir()}
			generation := authfileguard.NewDeleteGeneration(cliproxyauth.SourceHashFromBytes(data))
			if !generation.BindBackendIdentity("postgres:"+fileName, "10") {
				t.Fatal("bind original database revision")
			}

			errPersist := store.PersistAuthFiles(
				authfileguard.WithDeleteGeneration(t.Context(), generation),
				"delete auth",
				filepath.Join(store.authDir, fileName),
			)
			if tt.wantSuccess {
				if errPersist != nil {
					t.Fatalf("PersistAuthFiles() error = %v", errPersist)
				}
			} else if !errors.Is(errPersist, errCommit) {
				t.Fatalf("PersistAuthFiles() error = %v, want %v", errPersist, errCommit)
			}
			if gotUncertain := errors.Is(errPersist, authfileguard.ErrDeleteGenerationUncertain); gotUncertain != tt.wantUncertain {
				t.Fatalf("uncertain error = %t, want %t; error=%v", gotUncertain, tt.wantUncertain, errPersist)
			}
			_, exists := backend.authRecordSnapshot(fileName)
			if exists == tt.wantSuccess {
				t.Fatalf("database auth exists = %t, want %t", exists, !tt.wantSuccess)
			}
		})

		t.Run("write "+tt.name, func(t *testing.T) {
			const fileName = "write-persist-outcome.json"
			oldData := []byte(`{"type":"codex","access_token":"old"}`)
			newData := []byte(`{"type":"codex","access_token":"new"}`)
			backend := &postgresStoreTestBackend{
				authRecords:        map[string][]byte{fileName: append([]byte(nil), oldData...)},
				authRevisions:      map[string]int64{fileName: 10},
				nextAuthRevision:   10,
				commitError:        errCommit,
				commitErrorApplies: tt.commitApplies,
			}
			if tt.sameContentReplacement {
				backend.commitErrorReplacement = map[string][]byte{fileName: append([]byte(nil), newData...)}
			}
			db := newPostgresStoreTestSQLDB(t, backend)
			authDir := t.TempDir()
			store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: authDir}
			path := filepath.Join(authDir, fileName)
			if errWrite := os.WriteFile(path, newData, 0o600); errWrite != nil {
				t.Fatalf("write local auth: %v", errWrite)
			}

			errPersist := store.PersistAuthFiles(t.Context(), "write auth", path)
			if tt.wantSuccess {
				if errPersist != nil {
					t.Fatalf("PersistAuthFiles() error = %v", errPersist)
				}
			} else if !errors.Is(errPersist, errCommit) {
				t.Fatalf("PersistAuthFiles() error = %v, want %v", errPersist, errCommit)
			}
			wantRemote := oldData
			if tt.wantSuccess || tt.sameContentReplacement {
				wantRemote = newData
			}
			if gotRemote, exists := backend.authRecordSnapshot(fileName); !exists || !bytes.Equal(gotRemote, wantRemote) {
				t.Fatalf("database auth = %s, %t; want %s", gotRemote, exists, wantRemote)
			}
		})
	}
}

func TestPostgresDeleteAuthFileAtRootRollsBackTransactionOnLocalFailure(t *testing.T) {
	const fileName = "local-failure.json"
	wantData := []byte(`{"type":"codex","access_token":"token"}`)
	backend := &postgresStoreTestBackend{authRecords: map[string][]byte{fileName: append([]byte(nil), wantData...)}}
	db := newPostgresStoreTestSQLDB(t, backend)
	authDir := t.TempDir()
	store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: authDir}
	path := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(path, wantData, 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	root, errRoot := os.OpenRoot(authDir)
	if errRoot != nil {
		t.Fatalf("OpenRoot() error = %v", errRoot)
	}
	if errClose := root.Close(); errClose != nil {
		t.Fatalf("close root before deletion: %v", errClose)
	}

	errDelete := store.DeleteAuthFileAtRoot(t.Context(), root, fileName)
	if errDelete == nil {
		t.Fatal("DeleteAuthFileAtRoot() error = nil, want local root failure")
	}
	if outcome, ok := cliproxyauth.DeleteOutcomeFromError(errDelete); !ok || outcome != cliproxyauth.DeleteOutcomeRolledBack {
		t.Fatalf("delete outcome = %v, %t; want rolled back", outcome, ok)
	}
	if gotData, errRead := os.ReadFile(path); errRead != nil || !bytes.Equal(gotData, wantData) {
		t.Fatalf("local auth = %s, %v; want unchanged %s", gotData, errRead, wantData)
	}
	if gotData, ok := backend.authRecordSnapshot(fileName); !ok || !bytes.Equal(gotData, wantData) {
		t.Fatalf("database auth = %s, %t; want rolled back %s", gotData, ok, wantData)
	}
	if begins, commits, rollbacks := backend.transactionSnapshot(); begins != 1 || commits != 0 || rollbacks != 1 {
		t.Fatalf("transactions = begin:%d commit:%d rollback:%d, want 1/0/1", begins, commits, rollbacks)
	}
}

func TestPostgresStoreDeleteReportsCommitErrorOutcome(t *testing.T) {
	errCommit := errors.New("commit result lost")
	tests := []struct {
		name          string
		commitApplies bool
		wantOutcome   cliproxyauth.DeleteOutcome
		wantLocal     bool
		wantRemote    bool
	}{
		{name: "committed", commitApplies: true, wantOutcome: cliproxyauth.DeleteOutcomeCommitted},
		{name: "uncertain", wantOutcome: cliproxyauth.DeleteOutcomeUncertain, wantRemote: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const fileName = "outcome.json"
			wantData := []byte(`{"type":"codex","access_token":"token"}`)
			backend := &postgresStoreTestBackend{
				authRecords:        map[string][]byte{fileName: append([]byte(nil), wantData...)},
				commitError:        errCommit,
				commitErrorApplies: tt.commitApplies,
			}
			db := newPostgresStoreTestSQLDB(t, backend)
			authDir := t.TempDir()
			store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: authDir}
			path := filepath.Join(authDir, fileName)
			if errWrite := os.WriteFile(path, wantData, 0o600); errWrite != nil {
				t.Fatalf("write auth file: %v", errWrite)
			}

			errDelete := store.Delete(t.Context(), fileName)
			if !errors.Is(errDelete, errCommit) {
				t.Fatalf("Delete() error = %v, want %v", errDelete, errCommit)
			}
			if gotOutcome, ok := cliproxyauth.DeleteOutcomeFromError(errDelete); !ok || gotOutcome != tt.wantOutcome {
				t.Fatalf("delete outcome = %v, %t; want %v", gotOutcome, ok, tt.wantOutcome)
			}
			gotLocal, errRead := os.ReadFile(path)
			if tt.wantLocal {
				if errRead != nil || !bytes.Equal(gotLocal, wantData) {
					t.Fatalf("local auth = %s, %v; want %s", gotLocal, errRead, wantData)
				}
			} else if !errors.Is(errRead, os.ErrNotExist) {
				t.Fatalf("local auth still exists: %v", errRead)
			}
			gotRemote, remoteExists := backend.authRecordSnapshot(fileName)
			if remoteExists != tt.wantRemote {
				t.Fatalf("database auth exists = %t, want %t", remoteExists, tt.wantRemote)
			}
			if remoteExists && !bytes.Equal(gotRemote, wantData) {
				t.Fatalf("database auth = %s, want %s", gotRemote, wantData)
			}
		})
	}
}

func TestPostgresStoreDeleteDoesNotRestoreOverCommittedReplacement(t *testing.T) {
	const fileName = "replacement-outcome.json"
	original := []byte(`{"type":"codex","access_token":"original"}`)
	replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	errCommit := errors.New("commit result lost")
	backend := &postgresStoreTestBackend{
		authRecords:            map[string][]byte{fileName: append([]byte(nil), original...)},
		commitError:            errCommit,
		commitErrorReplacement: map[string][]byte{fileName: append([]byte(nil), replacement...)},
	}
	db := newPostgresStoreTestSQLDB(t, backend)
	authDir := t.TempDir()
	store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: authDir}
	path := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(path, original, 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}

	errDelete := store.Delete(t.Context(), fileName)
	if !errors.Is(errDelete, errCommit) {
		t.Fatalf("Delete() error = %v, want %v", errDelete, errCommit)
	}
	if outcome, ok := cliproxyauth.DeleteOutcomeFromError(errDelete); !ok || outcome != cliproxyauth.DeleteOutcomeUncertain {
		t.Fatalf("delete outcome = %v, %t; want uncertain replacement", outcome, ok)
	}
	if _, errRead := os.ReadFile(path); !errors.Is(errRead, os.ErrNotExist) {
		t.Fatalf("stale local auth was restored: %v", errRead)
	}
	if got, ok := backend.authRecordSnapshot(fileName); !ok || !bytes.Equal(got, replacement) {
		t.Fatalf("database auth = %s, %t; want replacement", got, ok)
	}
}

func TestPostgresStoreDeleteRecognizesSameContentNewRevisionAsReplacement(t *testing.T) {
	const fileName = "same-content-replacement.json"
	data := []byte(`{"type":"codex","access_token":"same"}`)
	errCommit := errors.New("commit result lost")
	backend := &postgresStoreTestBackend{
		authRecords:            map[string][]byte{fileName: append([]byte(nil), data...)},
		authRevisions:          map[string]int64{fileName: 10},
		nextAuthRevision:       10,
		commitError:            errCommit,
		commitErrorReplacement: map[string][]byte{fileName: append([]byte(nil), data...)},
	}
	db := newPostgresStoreTestSQLDB(t, backend)
	authDir := t.TempDir()
	store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: authDir}
	path := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}

	errDelete := store.Delete(t.Context(), fileName)
	if !errors.Is(errDelete, errCommit) {
		t.Fatalf("Delete() error = %v, want %v", errDelete, errCommit)
	}
	if outcome, ok := cliproxyauth.DeleteOutcomeFromError(errDelete); !ok || outcome != cliproxyauth.DeleteOutcomeUncertain {
		t.Fatalf("delete outcome = %v, %t; want uncertain", outcome, ok)
	}
	if !strings.Contains(errDelete.Error(), "remote auth was replaced") {
		t.Fatalf("Delete() error = %v, want replacement classification", errDelete)
	}
	if _, errRead := os.ReadFile(path); !errors.Is(errRead, os.ErrNotExist) {
		t.Fatalf("stale local auth was restored: %v", errRead)
	}
	if gotRemote, exists := backend.authRecordSnapshot(fileName); !exists || !bytes.Equal(gotRemote, data) {
		t.Fatalf("database replacement = %s, %t; want %s", gotRemote, exists, data)
	}
}

func TestPostgresPersistAuthFilesRejectsRetiredGeminiCLIContent(t *testing.T) {
	backend := &postgresStoreTestBackend{}
	db := newPostgresStoreTestSQLDB(t, backend)
	authDir := t.TempDir()
	store := &PostgresStore{
		db:      db,
		cfg:     PostgresStoreConfig{AuthTable: defaultAuthTable},
		authDir: authDir,
	}
	path := filepath.Join(authDir, "legacy.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"gemini","access_token":"legacy"}`), 0o600); errWrite != nil {
		t.Fatalf("write retired auth file: %v", errWrite)
	}
	errPersist := store.PersistAuthFiles(t.Context(), "sync", path)
	if !errors.Is(errPersist, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
		t.Fatalf("PersistAuthFiles() error = %v, want retired read-only", errPersist)
	}
	if calls := postgresStoreTestExecCallsContaining(backend.execCallsSnapshot(), "INSERT INTO"); len(calls) != 0 {
		t.Fatalf("auth upserts = %d, want 0", len(calls))
	}
}

func TestPostgresPersistAuthFilesRejectsRewritingRetiredRecord(t *testing.T) {
	backend := &postgresStoreTestBackend{
		queryColumns: []string{"content"},
		queryRows:    [][]driver.Value{{[]byte(`{"type":"gemini","access_token":"legacy"}`)}},
	}
	db := newPostgresStoreTestSQLDB(t, backend)
	authDir := t.TempDir()
	store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: authDir}
	path := filepath.Join(authDir, "legacy.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"rewritten"}`), 0o600); errWrite != nil {
		t.Fatalf("write rewritten auth file: %v", errWrite)
	}
	errPersist := store.PersistAuthFiles(t.Context(), "sync", path)
	if !errors.Is(errPersist, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
		t.Fatalf("PersistAuthFiles() error = %v, want retired read-only", errPersist)
	}
	if calls := postgresStoreTestExecCallsContaining(backend.execCallsSnapshot(), "INSERT INTO"); len(calls) != 0 {
		t.Fatalf("auth upserts = %d, want 0", len(calls))
	}
}

func TestPostgresStoreSaveRejectsRewritingRetiredRecord(t *testing.T) {
	backend := &postgresStoreTestBackend{
		queryColumns: []string{"content"},
		queryRows:    [][]driver.Value{{[]byte(`{"type":"gemini","access_token":"legacy"}`)}},
	}
	db := newPostgresStoreTestSQLDB(t, backend)
	authDir := t.TempDir()
	store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: authDir}
	path := filepath.Join(authDir, "legacy.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"external"}`), 0o600); errWrite != nil {
		t.Fatalf("write rewritten auth file: %v", errWrite)
	}
	_, errSave := store.Save(t.Context(), &cliproxyauth.Auth{
		ID: "legacy.json", FileName: "legacy.json", Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "replacement"},
	})
	if !errors.Is(errSave, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
		t.Fatalf("Save() error = %v, want retired read-only", errSave)
	}
	if calls := postgresStoreTestExecCallsContaining(backend.execCallsSnapshot(), "INSERT INTO"); len(calls) != 0 {
		t.Fatalf("auth upserts = %d, want 0", len(calls))
	}
}

func TestPostgresAuthMutationsSerializeConcurrentRetiredInsert(t *testing.T) {
	const fileName = "legacy.json"
	replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	retired := []byte(`{"type":"gemini","access_token":"legacy"}`)
	tests := []struct {
		name      string
		localData []byte
		mutate    func(context.Context, *PostgresStore, string) error
	}{
		{
			name: "Save",
			mutate: func(ctx context.Context, store *PostgresStore, _ string) error {
				_, errSave := store.Save(ctx, &cliproxyauth.Auth{
					ID:       fileName,
					FileName: fileName,
					Provider: "codex",
					Metadata: map[string]any{"type": "codex", "access_token": "replacement"},
				})
				return errSave
			},
		},
		{
			name:      "PersistAuthFiles",
			localData: replacement,
			mutate: func(ctx context.Context, store *PostgresStore, path string) error {
				return store.PersistAuthFiles(ctx, "sync", path)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := &postgresStoreTestBackend{
				authRecords:     make(map[string][]byte),
				lockWaitStarted: make(chan string, 1),
			}
			db := newPostgresStoreTestSQLDB(t, backend)
			db.SetMaxOpenConns(2)
			authDir := t.TempDir()
			store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: authDir}
			path := filepath.Join(authDir, fileName)
			if tt.localData != nil {
				if errWrite := os.WriteFile(path, tt.localData, 0o600); errWrite != nil {
					t.Fatalf("write replacement auth file: %v", errWrite)
				}
			}

			writerTx, errBegin := db.BeginTx(t.Context(), nil)
			if errBegin != nil {
				t.Fatalf("begin competing transaction: %v", errBegin)
			}
			defer func() {
				if errRollback := writerTx.Rollback(); errRollback != nil && !errors.Is(errRollback, sql.ErrTxDone) {
					t.Errorf("rollback competing transaction: %v", errRollback)
				}
			}()
			lockNamespace := store.fullTableName(store.cfg.AuthTable)
			if _, errLock := writerTx.ExecContext(t.Context(), authRecordAdvisoryLockQuery, lockNamespace, fileName); errLock != nil {
				t.Fatalf("lock missing auth record: %v", errLock)
			}

			ctx := t.Context()
			result := make(chan error, 1)
			go func() {
				result <- tt.mutate(ctx, store, path)
			}()

			wantLockKey := lockNamespace + "\x00" + fileName
			select {
			case errMutation := <-result:
				_ = writerTx.Rollback()
				t.Fatalf("mutation completed before competing insert: %v", errMutation)
			case gotLockKey := <-backend.lockWaitStarted:
				if gotLockKey != wantLockKey {
					t.Fatalf("waited lock key = %q, want %q", gotLockKey, wantLockKey)
				}
			}

			insert := fmt.Sprintf("INSERT INTO %s (id, content) VALUES ($1, $2)", lockNamespace)
			if _, errInsert := writerTx.ExecContext(t.Context(), insert, fileName, json.RawMessage(retired)); errInsert != nil {
				t.Fatalf("insert retired auth record: %v", errInsert)
			}
			if errCommit := writerTx.Commit(); errCommit != nil {
				t.Fatalf("commit retired auth record: %v", errCommit)
			}

			if errMutation := <-result; !errors.Is(errMutation, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
				t.Fatalf("mutation error = %v, want retired read-only", errMutation)
			}
			if got, ok := backend.authRecordSnapshot(fileName); !ok || !bytes.Equal(got, retired) {
				t.Fatalf("database auth record = %s, %t; want %s", got, ok, retired)
			}
			if begins, commits, rollbacks := backend.transactionSnapshot(); begins != 2 || commits != 1 || rollbacks != 1 {
				t.Fatalf("transactions = begin:%d commit:%d rollback:%d, want 2/1/1", begins, commits, rollbacks)
			}
			if tt.localData == nil {
				if _, errStat := os.Stat(path); !errors.Is(errStat, fs.ErrNotExist) {
					t.Fatalf("Save() local file error = %v, want not exist", errStat)
				}
			} else if gotLocal, errRead := os.ReadFile(path); errRead != nil || !bytes.Equal(gotLocal, replacement) {
				t.Fatalf("PersistAuthFiles() local file = %s, %v; want %s", gotLocal, errRead, replacement)
			}
		})
	}
}

func TestPostgresAuthUpsertRejectsConcurrentRetiredInsertWithoutAdvisoryLock(t *testing.T) {
	const fileName = "legacy.json"
	replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	retired := []byte(`{"type":"gemini","access_token":"legacy"}`)
	tests := []struct {
		name      string
		localData []byte
		mutate    func(context.Context, *PostgresStore, string) error
	}{
		{
			name: "Save",
			mutate: func(ctx context.Context, store *PostgresStore, _ string) error {
				_, errSave := store.Save(ctx, &cliproxyauth.Auth{
					ID:       fileName,
					FileName: fileName,
					Provider: "codex",
					Metadata: map[string]any{"type": "codex", "access_token": "replacement"},
				})
				return errSave
			},
		},
		{
			name:      "PersistAuthFiles",
			localData: replacement,
			mutate: func(ctx context.Context, store *PostgresStore, path string) error {
				return store.PersistAuthFiles(ctx, "sync", path)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upsertStarted := make(chan struct{}, 1)
			upsertContinue := make(chan struct{})
			backend := &postgresStoreTestBackend{
				authRecords:               make(map[string][]byte),
				conditionalUpsertStarted:  upsertStarted,
				conditionalUpsertContinue: upsertContinue,
			}
			db := newPostgresStoreTestSQLDB(t, backend)
			db.SetMaxOpenConns(2)
			authDir := t.TempDir()
			store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: authDir}
			path := filepath.Join(authDir, fileName)
			if tt.localData != nil {
				if errWrite := os.WriteFile(path, tt.localData, 0o600); errWrite != nil {
					t.Fatalf("write replacement auth file: %v", errWrite)
				}
			}

			mutationDone := make(chan error, 1)
			go func() {
				mutationDone <- tt.mutate(t.Context(), store, path)
			}()
			select {
			case <-upsertStarted:
			case errMutation := <-mutationDone:
				t.Fatalf("mutation completed before conditional upsert: %v", errMutation)
			case <-time.After(5 * time.Second):
				t.Fatal("mutation did not reach conditional upsert")
			}

			insert := fmt.Sprintf("INSERT INTO %s (id, content) VALUES ($1, $2)", store.fullTableName(store.cfg.AuthTable))
			if _, errInsert := db.ExecContext(t.Context(), insert, fileName, json.RawMessage(retired)); errInsert != nil {
				close(upsertContinue)
				t.Fatalf("insert retired auth without advisory lock: %v", errInsert)
			}
			close(upsertContinue)

			select {
			case errMutation := <-mutationDone:
				if !errors.Is(errMutation, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
					t.Fatalf("mutation error = %v, want retired read-only", errMutation)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("mutation did not finish")
			}
			if got, ok := backend.authRecordSnapshot(fileName); !ok || !bytes.Equal(got, retired) {
				t.Fatalf("database auth record = %s, %t; want %s", got, ok, retired)
			}
			if !authfileguard.IsRetired(path) {
				t.Fatal("rejected retired database row did not mark the path retired")
			}
			authfileguard.ClearRetired(path)
		})
	}
}

func TestPostgresPersistAuthFilesAllowsDeletingRetiredRecord(t *testing.T) {
	const fileName = "legacy.json"
	retired := []byte(`{"type":"gemini","access_token":"legacy"}`)
	backend := &postgresStoreTestBackend{authRecords: map[string][]byte{fileName: retired}}
	db := newPostgresStoreTestSQLDB(t, backend)
	authDir := t.TempDir()
	store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: authDir}
	path := filepath.Join(authDir, fileName)
	authfileguard.MarkRetired(path)
	t.Cleanup(func() { authfileguard.ClearRetired(path) })
	if errDelete := store.PersistAuthFiles(t.Context(), "delete", path); errDelete != nil {
		t.Fatalf("PersistAuthFiles() error = %v", errDelete)
	}
	if _, exists := backend.authRecordSnapshot(fileName); exists {
		t.Fatal("retired database row still exists")
	}
	if authfileguard.IsRetired(path) {
		t.Fatal("successful retired deletion left the path marked retired")
	}
	if calls := backend.execCallsSnapshot(); len(calls) != 2 || !strings.Contains(calls[0].query, "pg_advisory_xact_lock") || !strings.Contains(calls[1].query, "DELETE FROM") {
		t.Fatalf("database calls = %#v, want advisory lock and delete", calls)
	}
}

func TestPostgresFinalizeRetiredDeletionPreservesReplacement(t *testing.T) {
	const fileName = "legacy.json"
	replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	backend := &postgresStoreTestBackend{authRecords: map[string][]byte{fileName: replacement}}
	db := newPostgresStoreTestSQLDB(t, backend)
	authDir := t.TempDir()
	store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: authDir}
	path := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(path, []byte(`{"type":"gemini","access_token":"stale"}`), 0o600); errWrite != nil {
		t.Fatalf("write stale local retired auth: %v", errWrite)
	}
	authfileguard.MarkRetired(path)
	t.Cleanup(func() { authfileguard.ClearRetired(path) })
	if errPersist := store.PersistAuthFiles(t.Context(), "ignore stale retired mirror", path); errPersist != nil {
		t.Fatalf("PersistAuthFiles() error = %v", errPersist)
	}
	if _, errStat := os.Stat(path); !errors.Is(errStat, fs.ErrNotExist) {
		t.Fatalf("stale local retired auth still exists: %v", errStat)
	}
	if !authfileguard.IsRetired(path) {
		t.Fatal("stale mirror removal cleared quarantine before database finalization")
	}

	if errFinalize := store.FinalizeAuthFileDeletion(t.Context(), fileName); errFinalize != nil {
		t.Fatalf("FinalizeAuthFileDeletion() error = %v", errFinalize)
	}
	if got, ok := backend.authRecordSnapshot(fileName); !ok || !bytes.Equal(got, replacement) {
		t.Fatalf("database auth record = %s, %t; want replacement", got, ok)
	}
	if authfileguard.IsRetired(path) {
		t.Fatal("replacement path remains retired")
	}
}

func TestPostgresFinalizeRetiredDeletionProbesCommitErrors(t *testing.T) {
	errCommit := errors.New("commit result lost")
	retired := []byte(`{"type":"gemini","access_token":"retired"}`)
	supportedReplacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	newRetiredGeneration := []byte(`{"type":"gemini","access_token":"new-retired"}`)
	for _, tt := range []struct {
		name              string
		commitApplies     bool
		replacement       []byte
		wantSuccess       bool
		wantUncertain     bool
		wantRemotePresent bool
	}{
		{name: "committed", commitApplies: true, wantSuccess: true},
		{name: "not committed", wantRemotePresent: true},
		{name: "supported replacement", replacement: supportedReplacement, wantSuccess: true, wantRemotePresent: true},
		{name: "new retired generation", replacement: newRetiredGeneration, wantUncertain: true, wantRemotePresent: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			const fileName = "finalize-outcome.json"
			backend := &postgresStoreTestBackend{
				authRecords:        map[string][]byte{fileName: append([]byte(nil), retired...)},
				authRevisions:      map[string]int64{fileName: 10},
				nextAuthRevision:   10,
				commitError:        errCommit,
				commitErrorApplies: tt.commitApplies,
			}
			if tt.replacement != nil {
				backend.commitErrorReplacement = map[string][]byte{fileName: append([]byte(nil), tt.replacement...)}
			}
			db := newPostgresStoreTestSQLDB(t, backend)
			authDir := t.TempDir()
			store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: authDir}
			path := filepath.Join(authDir, fileName)
			authfileguard.MarkRetired(path)
			t.Cleanup(func() { authfileguard.ClearRetired(path) })

			errFinalize := store.FinalizeAuthFileDeletion(t.Context(), fileName)
			if tt.wantSuccess {
				if errFinalize != nil {
					t.Fatalf("FinalizeAuthFileDeletion() error = %v", errFinalize)
				}
			} else if !errors.Is(errFinalize, errCommit) {
				t.Fatalf("FinalizeAuthFileDeletion() error = %v, want %v", errFinalize, errCommit)
			}
			if gotUncertain := errors.Is(errFinalize, authfileguard.ErrDeleteGenerationUncertain); gotUncertain != tt.wantUncertain {
				t.Fatalf("uncertain error = %t, want %t; error=%v", gotUncertain, tt.wantUncertain, errFinalize)
			}
			if gotRetired := authfileguard.IsRetired(path); gotRetired == tt.wantSuccess {
				t.Fatalf("retired marker = %t, want %t", gotRetired, !tt.wantSuccess)
			}
			gotRemote, remotePresent := backend.authRecordSnapshot(fileName)
			if remotePresent != tt.wantRemotePresent {
				t.Fatalf("database auth exists = %t, want %t", remotePresent, tt.wantRemotePresent)
			}
			if tt.replacement != nil && (!remotePresent || !bytes.Equal(gotRemote, tt.replacement)) {
				t.Fatalf("database replacement = %s, %t; want %s", gotRemote, remotePresent, tt.replacement)
			}
		})
	}
}

func TestPostgresFinalizeRetiredDeletionPreservesDifferentRetiredGeneration(t *testing.T) {
	const fileName = "legacy.json"
	original := []byte(`{"type":"gemini","access_token":"original"}`)
	replacement := []byte(`{"type":"gemini","access_token":"replacement"}`)
	backend := &postgresStoreTestBackend{
		authRecords:   map[string][]byte{fileName: replacement},
		authRevisions: map[string]int64{fileName: 2},
	}
	db := newPostgresStoreTestSQLDB(t, backend)
	store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: t.TempDir()}
	generation := authfileguard.NewDeleteGeneration(cliproxyauth.SourceHashFromBytes(original))
	if !generation.BindBackendIdentity("postgres:"+fileName, "1") {
		t.Fatal("bind original postgres generation")
	}
	ctx := authfileguard.WithDeleteGeneration(t.Context(), generation)
	if errFinalize := store.FinalizeAuthFileDeletion(ctx, fileName); !errors.Is(errFinalize, authfileguard.ErrDeleteGenerationUncertain) {
		t.Fatalf("FinalizeAuthFileDeletion() error = %v, want uncertain", errFinalize)
	}
	if got, ok := backend.authRecordSnapshot(fileName); !ok || !bytes.Equal(got, replacement) {
		t.Fatalf("database auth record = %s, %t; want replacement", got, ok)
	}
}

func TestPostgresFinalizeRetiredDeletionSerializesWithListMarkerPublication(t *testing.T) {
	const fileName = "legacy.json"
	retired := []byte(`{"type":"gemini","access_token":"legacy"}`)
	revalidationSelected := make(chan struct{}, 1)
	revalidationContinue := make(chan struct{})
	backend := &postgresStoreTestBackend{
		queryColumns: []string{"id", "content", "created_at", "updated_at"},
		queryRows: [][]driver.Value{
			{fileName, string(retired), time.Unix(1711929600, 0), time.Unix(1711929600, 0)},
		},
		authRecords:          map[string][]byte{fileName: append([]byte(nil), retired...)},
		revalidationSelected: revalidationSelected,
		revalidationContinue: revalidationContinue,
	}
	db := newPostgresStoreTestSQLDB(t, backend)
	db.SetMaxOpenConns(2)
	authDir := t.TempDir()
	store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: authDir}
	path := filepath.Join(authDir, fileName)
	authfileguard.MarkRetired(path)
	t.Cleanup(func() { authfileguard.ClearRetired(path) })

	type listResult struct {
		auths []*cliproxyauth.Auth
		err   error
	}
	listDone := make(chan listResult, 1)
	go func() {
		auths, errList := store.List(t.Context())
		listDone <- listResult{auths: auths, err: errList}
	}()
	select {
	case <-revalidationSelected:
	case result := <-listDone:
		t.Fatalf("List() completed before retired-row revalidation was released: auths=%#v error=%v", result.auths, result.err)
	case <-time.After(5 * time.Second):
		t.Fatal("List() did not reach retired-row revalidation")
	}

	finalizeDone := make(chan error, 1)
	finalizeStarted := make(chan struct{})
	go func() {
		close(finalizeStarted)
		finalizeDone <- store.FinalizeAuthFileDeletion(t.Context(), fileName)
	}()
	<-finalizeStarted

	finalizeCompletedEarly := false
	var earlyFinalizeErr error
	waitDeadline := time.NewTimer(5 * time.Second)
	for {
		if !store.mu.TryLock() {
			break
		}
		store.mu.Unlock()
		select {
		case earlyFinalizeErr = <-finalizeDone:
			finalizeCompletedEarly = true
		case <-waitDeadline.C:
			close(revalidationContinue)
			t.Fatal("FinalizeAuthFileDeletion() did not acquire the store mutex")
		default:
			runtime.Gosched()
		}
		if finalizeCompletedEarly {
			break
		}
	}
	if !waitDeadline.Stop() {
		select {
		case <-waitDeadline.C:
		default:
		}
	}
	begins, _, _ := backend.transactionSnapshot()

	close(revalidationContinue)
	select {
	case result := <-listDone:
		if result.err != nil {
			t.Fatalf("List() error = %v", result.err)
		}
		if len(result.auths) != 1 {
			t.Fatalf("List() auth count = %d, want 1", len(result.auths))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("List() did not finish")
	}
	if !finalizeCompletedEarly {
		select {
		case earlyFinalizeErr = <-finalizeDone:
		case <-time.After(5 * time.Second):
			t.Fatal("FinalizeAuthFileDeletion() did not finish")
		}
	}
	if earlyFinalizeErr != nil {
		t.Fatalf("FinalizeAuthFileDeletion() error = %v", earlyFinalizeErr)
	}
	if finalizeCompletedEarly || begins != 1 {
		t.Fatalf("FinalizeAuthFileDeletion() passed List marker publication: completed=%t transaction begins=%d", finalizeCompletedEarly, begins)
	}
	if _, ok := backend.authRecordSnapshot(fileName); ok {
		t.Fatal("retired database row still exists after finalization")
	}
	if authfileguard.IsRetired(path) {
		t.Fatal("List() marker publication survived committed finalization")
	}
}

func TestPostgresStoreSaveVertexStorageBackedAuthPreservesMetadataOnlyFields(t *testing.T) {
	backend := &postgresStoreTestBackend{}
	db := newPostgresStoreTestSQLDB(t, backend)

	authDir := t.TempDir()
	store := &PostgresStore{
		db:      db,
		cfg:     PostgresStoreConfig{AuthTable: defaultAuthTable},
		authDir: authDir,
	}

	auth := &cliproxyauth.Auth{
		ID:       "vertex.json",
		FileName: "vertex.json",
		Provider: "vertex",
		Storage: &vertex.VertexCredentialStorage{
			ServiceAccount: map[string]any{
				"type":         "service_account",
				"project_id":   "vertex-project",
				"client_email": "vertex@example.com",
			},
			ProjectID: "vertex-project",
			Email:     "vertex@example.com",
			Location:  "us-central1",
		},
		Metadata: map[string]any{
			"type":                 "vertex",
			"email":                "vertex@example.com",
			"label":                "vertex-label",
			"tool_prefix_disabled": true,
		},
	}

	path, err := store.Save(context.Background(), auth)
	if err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	if got, ok := auth.Metadata["label"].(string); !ok || got != "vertex-label" {
		t.Fatalf("metadata label = %#v, want %q", auth.Metadata["label"], "vertex-label")
	}
	if got, ok := auth.Metadata["tool_prefix_disabled"].(bool); !ok || !got {
		t.Fatalf("metadata tool_prefix_disabled = %#v, want true", auth.Metadata["tool_prefix_disabled"])
	}
	if _, ok := auth.Metadata["service_account"].(map[string]any); !ok {
		t.Fatalf("metadata service_account = %#v, want object", auth.Metadata["service_account"])
	}

	rawFile, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(rawFile, &payload); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if got, ok := payload["label"].(string); !ok || got != "vertex-label" {
		t.Fatalf("persisted label = %#v, want %q", payload["label"], "vertex-label")
	}
	wantRaw, err := cliproxyauth.CanonicalMetadataBytes(auth)
	if err != nil {
		t.Fatalf("CanonicalMetadataBytes() error = %v", err)
	}
	wantHash := cliproxyauth.SourceHashFromBytes(wantRaw)
	if got := auth.Attributes[cliproxyauth.SourceHashAttributeKey]; got != wantHash {
		t.Fatalf("source hash = %q, want %q", got, wantHash)
	}
}

func TestPostgresStoreListSetsCanonicalSourceHash(t *testing.T) {
	backend := &postgresStoreTestBackend{
		queryColumns: []string{"id", "content", "created_at", "updated_at"},
		queryRows: [][]driver.Value{
			{"auth.json", `{"type":"claude","email":"reader@example.com"}`, time.Unix(1711929600, 0), time.Unix(1711929600, 0)},
		},
	}
	db := newPostgresStoreTestSQLDB(t, backend)

	authDir := t.TempDir()
	store := &PostgresStore{
		db:      db,
		cfg:     PostgresStoreConfig{AuthTable: defaultAuthTable},
		authDir: authDir,
	}

	auths, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(auths) != 1 || auths[0] == nil {
		t.Fatalf("expected 1 auth, got %d", len(auths))
	}

	auth := auths[0]
	if auth.Disabled {
		t.Fatal("expected auth to remain enabled")
	}
	if auth.Status != cliproxyauth.StatusActive {
		t.Fatalf("status = %q, want %q", auth.Status, cliproxyauth.StatusActive)
	}
	wantRaw, err := cliproxyauth.CanonicalMetadataBytes(auth)
	if err != nil {
		t.Fatalf("CanonicalMetadataBytes() error = %v", err)
	}
	wantHash := cliproxyauth.SourceHashFromBytes(wantRaw)
	if got := auth.Attributes[cliproxyauth.SourceHashAttributeKey]; got != wantHash {
		t.Fatalf("source hash = %q, want %q", got, wantHash)
	}
	if rawHash := cliproxyauth.SourceHashFromBytes([]byte(`{"type":"claude","email":"reader@example.com"}`)); rawHash == wantHash {
		t.Fatal("expected canonical source hash to differ from raw database payload hash")
	}
	if got, want := auth.Attributes["path"], filepath.Join(authDir, "auth.json"); got != want {
		t.Fatalf("path = %q, want %q", got, want)
	}
}

func TestPostgresStoreListPreservesDisabledState(t *testing.T) {
	backend := &postgresStoreTestBackend{
		queryColumns: []string{"id", "content", "created_at", "updated_at"},
		queryRows: [][]driver.Value{
			{"auth.json", `{"type":"claude","email":"reader@example.com","disabled":true}`, time.Unix(1711929600, 0), time.Unix(1711929600, 0)},
		},
	}
	db := newPostgresStoreTestSQLDB(t, backend)

	store := &PostgresStore{
		db:      db,
		cfg:     PostgresStoreConfig{AuthTable: defaultAuthTable},
		authDir: t.TempDir(),
	}

	auths, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(auths) != 1 || auths[0] == nil {
		t.Fatalf("expected 1 auth, got %d", len(auths))
	}
	if !auths[0].Disabled {
		t.Fatal("expected auth to remain disabled")
	}
	if auths[0].Status != cliproxyauth.StatusDisabled {
		t.Fatalf("status = %q, want %q", auths[0].Status, cliproxyauth.StatusDisabled)
	}
}

func TestPostgresStoreListDoesNotRemarkRetiredPathAfterConcurrentDelete(t *testing.T) {
	const fileName = "legacy.json"
	retired := []byte(`{"type":"gemini","access_token":"legacy"}`)
	rowStarted := make(chan struct{}, 1)
	rowContinue := make(chan struct{})
	backend := &postgresStoreTestBackend{
		queryColumns: []string{"id", "content", "created_at", "updated_at"},
		queryRows: [][]driver.Value{
			{fileName, string(retired), time.Unix(1711929600, 0), time.Unix(1711929600, 0)},
		},
		authRecords:     map[string][]byte{fileName: append([]byte(nil), retired...)},
		listRowStarted:  rowStarted,
		listRowContinue: rowContinue,
	}
	db := newPostgresStoreTestSQLDB(t, backend)
	db.SetMaxOpenConns(2)
	authDir := t.TempDir()
	store := &PostgresStore{db: db, cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: authDir}
	path := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(path, retired, 0o600); errWrite != nil {
		t.Fatalf("write retired auth file: %v", errWrite)
	}
	authfileguard.MarkRetired(path)
	t.Cleanup(func() { authfileguard.ClearRetired(path) })

	type listResult struct {
		auths []*cliproxyauth.Auth
		err   error
	}
	listDone := make(chan listResult, 1)
	go func() {
		auths, errList := store.List(t.Context())
		listDone <- listResult{auths: auths, err: errList}
	}()
	select {
	case <-rowStarted:
	case result := <-listDone:
		t.Fatalf("List() completed before stale row was released: auths=%#v error=%v", result.auths, result.err)
	case <-time.After(5 * time.Second):
		t.Fatal("List() did not reach the stale row")
	}

	if errDelete := store.Delete(t.Context(), fileName); errDelete != nil {
		close(rowContinue)
		t.Fatalf("Delete() error = %v", errDelete)
	}
	if authfileguard.IsRetired(path) {
		close(rowContinue)
		t.Fatal("Delete() did not clear the retired path")
	}
	close(rowContinue)

	select {
	case result := <-listDone:
		if result.err != nil {
			t.Fatalf("List() error = %v", result.err)
		}
		if len(result.auths) != 0 {
			t.Fatalf("List() returned stale auths = %#v", result.auths)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("List() did not finish")
	}
	if authfileguard.IsRetired(path) {
		t.Fatal("stale List() resurrected the retired path marker")
	}
}

type postgresStoreTestBackend struct {
	mu                        sync.Mutex
	execCalls                 []postgresStoreTestExecCall
	execErrors                []error
	queryColumns              []string
	queryRows                 [][]driver.Value
	authRecords               map[string][]byte
	authRevisions             map[string]int64
	nextAuthRevision          int64
	advisoryLocks             map[string]*postgresStoreTestAdvisoryLock
	lockWaitStarted           chan string
	conditionalUpsertStarted  chan struct{}
	conditionalUpsertContinue chan struct{}
	revalidationSelected      chan struct{}
	revalidationContinue      chan struct{}
	listRowStarted            chan struct{}
	listRowContinue           chan struct{}
	beginCount                int
	commitCount               int
	rollbackCount             int
	commitError               error
	commitErrorApplies        bool
	commitErrorReplacement    map[string][]byte
	normalizeJSONBReads       bool
}

type postgresStoreTestExecCall struct {
	query string
	args  []driver.NamedValue
}

type postgresStoreTestAdvisoryLock struct {
	owner    *postgresStoreTestTx
	released chan struct{}
}

type postgresStoreTestAuthMutation struct {
	data    []byte
	deleted bool
}

func (b *postgresStoreTestBackend) execCallsSnapshot() []postgresStoreTestExecCall {
	b.mu.Lock()
	defer b.mu.Unlock()

	snapshot := make([]postgresStoreTestExecCall, len(b.execCalls))
	for i, call := range b.execCalls {
		args := make([]driver.NamedValue, len(call.args))
		copy(args, call.args)
		snapshot[i] = postgresStoreTestExecCall{
			query: call.query,
			args:  args,
		}
	}
	return snapshot
}

func (b *postgresStoreTestBackend) recordExec(query string, args []driver.NamedValue) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	cloned := make([]driver.NamedValue, len(args))
	copy(cloned, args)
	b.execCalls = append(b.execCalls, postgresStoreTestExecCall{
		query: query,
		args:  cloned,
	})
	index := len(b.execCalls) - 1
	if index < len(b.execErrors) {
		return b.execErrors[index]
	}
	return nil
}

func (b *postgresStoreTestBackend) querySnapshot() ([]string, [][]driver.Value) {
	b.mu.Lock()
	defer b.mu.Unlock()

	columns := append([]string(nil), b.queryColumns...)
	rows := make([][]driver.Value, len(b.queryRows))
	for i, row := range b.queryRows {
		rows[i] = append([]driver.Value(nil), row...)
	}
	return columns, rows
}

func (b *postgresStoreTestBackend) conditionalUpsertChannels() (chan struct{}, chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.conditionalUpsertStarted, b.conditionalUpsertContinue
}

func (b *postgresStoreTestBackend) listRowChannels() (chan struct{}, chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.listRowStarted, b.listRowContinue
}

func (b *postgresStoreTestBackend) revalidationChannels() (chan struct{}, chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.revalidationSelected, b.revalidationContinue
}

func (b *postgresStoreTestBackend) authRecordSnapshot(id string) ([]byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	data, ok := b.authRecords[id]
	return append([]byte(nil), data...), ok
}

func (b *postgresStoreTestBackend) transactionSnapshot() (begins, commits, rollbacks int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.beginCount, b.commitCount, b.rollbackCount
}

type postgresStoreTestDriver struct{}

type postgresStoreTestConn struct {
	backend *postgresStoreTestBackend
	tx      *postgresStoreTestTx
}

type postgresStoreTestTx struct {
	conn        *postgresStoreTestConn
	pendingAuth map[string]postgresStoreTestAuthMutation
	lockKeys    map[string]struct{}
	revision    int64
	done        bool
}

type postgresStoreTestRows struct {
	columns   []string
	rows      [][]driver.Value
	index     int
	startOnce sync.Once
	started   chan struct{}
	proceed   chan struct{}
}

var (
	postgresStoreTestDriverOnce sync.Once
	postgresStoreTestRegistry   sync.Map
	postgresStoreTestCounter    atomic.Uint64
)

func newPostgresStoreTestSQLDB(t *testing.T, backend *postgresStoreTestBackend) *sql.DB {
	t.Helper()

	postgresStoreTestDriverOnce.Do(func() {
		sql.Register("cliproxy-postgresstore-test", postgresStoreTestDriver{})
	})

	dsn := fmt.Sprintf("%s-%d", strings.ReplaceAll(t.Name(), "/", "-"), postgresStoreTestCounter.Add(1))
	postgresStoreTestRegistry.Store(dsn, backend)

	db, err := sql.Open("cliproxy-postgresstore-test", dsn)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		postgresStoreTestRegistry.Delete(dsn)
	})
	return db
}

func (postgresStoreTestDriver) Open(name string) (driver.Conn, error) {
	value, ok := postgresStoreTestRegistry.Load(name)
	if !ok {
		return nil, fmt.Errorf("postgres store test: unknown dsn %q", name)
	}
	backend, ok := value.(*postgresStoreTestBackend)
	if !ok {
		return nil, fmt.Errorf("postgres store test: invalid backend for %q", name)
	}
	return &postgresStoreTestConn{backend: backend}, nil
}

func (c *postgresStoreTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("postgres store test: Prepare not implemented")
}

func (c *postgresStoreTestConn) Close() error {
	return nil
}

func (c *postgresStoreTestConn) Begin() (driver.Tx, error) {
	return c.beginTx()
}

func (c *postgresStoreTestConn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.beginTx()
}

func (c *postgresStoreTestConn) beginTx() (driver.Tx, error) {
	c.backend.mu.Lock()
	defer c.backend.mu.Unlock()

	if c.tx != nil {
		return nil, fmt.Errorf("postgres store test: transaction already active")
	}
	tx := &postgresStoreTestTx{
		conn:        c,
		pendingAuth: make(map[string]postgresStoreTestAuthMutation),
		lockKeys:    make(map[string]struct{}),
	}
	c.tx = tx
	c.backend.beginCount++
	return tx, nil
}

func (c *postgresStoreTestConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if errExec := c.backend.recordExec(query, args); errExec != nil {
		return nil, errExec
	}
	if strings.Contains(query, "pg_advisory_xact_lock") {
		if c.tx == nil {
			return nil, fmt.Errorf("postgres store test: advisory lock outside transaction")
		}
		lockKey, errKey := postgresStoreTestAdvisoryLockKey(args)
		if errKey != nil {
			return nil, errKey
		}
		if errLock := c.backend.acquireAdvisoryLock(ctx, c.tx, lockKey); errLock != nil {
			return nil, errLock
		}
		return driver.RowsAffected(1), nil
	}
	if strings.Contains(query, "INSERT INTO") && strings.Contains(query, defaultAuthTable) {
		id, data, errArgs := postgresStoreTestAuthWriteArgs(args)
		if errArgs != nil {
			return nil, errArgs
		}
		if strings.Contains(query, "current_auth.content") {
			started, proceed := c.backend.conditionalUpsertChannels()
			if started != nil {
				select {
				case started <- struct{}{}:
				default:
				}
			}
			if proceed != nil {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-proceed:
				}
			}
			if handled, affected := c.backend.stageAuthMutationUnlessRetired(c.tx, id, data); handled {
				return driver.RowsAffected(affected), nil
			}
		}
		if handled := c.backend.stageAuthMutation(c.tx, id, postgresStoreTestAuthMutation{data: data}); handled {
			return driver.RowsAffected(1), nil
		}
	}
	if strings.Contains(query, "DELETE FROM") && strings.Contains(query, defaultAuthTable) {
		id, errID := postgresStoreTestAuthIDArg(args)
		if errID != nil {
			return nil, errID
		}
		if handled := c.backend.stageAuthMutation(c.tx, id, postgresStoreTestAuthMutation{deleted: true}); handled {
			return driver.RowsAffected(1), nil
		}
	}
	return driver.RowsAffected(1), nil
}

func (c *postgresStoreTestConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(query, "SELECT EXISTS") && strings.Contains(query, defaultAuthTable) {
		id, errID := postgresStoreTestAuthIDArg(args)
		if errID != nil {
			return nil, errID
		}
		if _, ok, handled := c.backend.authRecord(c.tx, id); handled {
			return &postgresStoreTestRows{columns: []string{"exists"}, rows: [][]driver.Value{{ok}}}, nil
		}
	}
	if strings.Contains(query, "SELECT content, auth_revision FROM") && strings.Contains(query, defaultAuthTable) {
		id, errID := postgresStoreTestAuthIDArg(args)
		if errID != nil {
			return nil, errID
		}
		if data, revision, ok, handled := c.backend.authRecordGeneration(c.tx, id); handled {
			selected, proceed := c.backend.revalidationChannels()
			if selected != nil {
				select {
				case selected <- struct{}{}:
				default:
				}
			}
			if proceed != nil {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-proceed:
				}
			}
			rows := make([][]driver.Value, 0, 1)
			if ok {
				rows = append(rows, []driver.Value{data, revision})
			}
			return &postgresStoreTestRows{columns: []string{"content", "auth_revision"}, rows: rows}, nil
		}
		columns, rows := c.backend.querySnapshot()
		if len(columns) == 1 && columns[0] == "content" {
			columns = []string{"content", "auth_revision"}
			for index := range rows {
				rows[index] = append(rows[index], "1")
			}
			return &postgresStoreTestRows{columns: columns, rows: rows}, nil
		}
	}
	if strings.Contains(query, "SELECT content FROM") && strings.Contains(query, defaultAuthTable) {
		id, errID := postgresStoreTestAuthIDArg(args)
		if errID != nil {
			return nil, errID
		}
		if data, ok, handled := c.backend.authRecord(c.tx, id); handled {
			if !strings.Contains(query, "FOR UPDATE") {
				selected, proceed := c.backend.revalidationChannels()
				if selected != nil {
					select {
					case selected <- struct{}{}:
					default:
					}
				}
				if proceed != nil {
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-proceed:
					}
				}
			}
			rows := make([][]driver.Value, 0, 1)
			if ok {
				rows = append(rows, []driver.Value{data})
			}
			return &postgresStoreTestRows{columns: []string{"content"}, rows: rows}, nil
		}
	}
	columns, rows := c.backend.querySnapshot()
	started, proceed := c.backend.listRowChannels()
	return &postgresStoreTestRows{columns: columns, rows: rows, started: started, proceed: proceed}, nil
}

func (tx *postgresStoreTestTx) Commit() error {
	return tx.finish(true)
}

func (tx *postgresStoreTestTx) Rollback() error {
	return tx.finish(false)
}

func (tx *postgresStoreTestTx) finish(commit bool) error {
	backend := tx.conn.backend
	backend.mu.Lock()
	defer backend.mu.Unlock()

	if tx.done {
		return fmt.Errorf("postgres store test: transaction already completed")
	}
	applyCommit := commit && (backend.commitError == nil || backend.commitErrorApplies)
	if applyCommit && backend.authRecords != nil {
		for id, mutation := range tx.pendingAuth {
			if mutation.deleted {
				delete(backend.authRecords, id)
				delete(backend.authRevisions, id)
				continue
			}
			backend.authRecords[id] = append([]byte(nil), mutation.data...)
			if tx.revision > 0 {
				if backend.authRevisions == nil {
					backend.authRevisions = make(map[string]int64)
				}
				backend.authRevisions[id] = tx.revision
			} else {
				backend.bumpAuthRevisionLocked(id)
			}
		}
	}
	if commit && backend.commitError != nil {
		for id, data := range backend.commitErrorReplacement {
			backend.authRecords[id] = append([]byte(nil), data...)
			backend.bumpAuthRevisionLocked(id)
		}
	}
	for lockKey := range tx.lockKeys {
		lock := backend.advisoryLocks[lockKey]
		if lock == nil || lock.owner != tx {
			continue
		}
		delete(backend.advisoryLocks, lockKey)
		close(lock.released)
	}
	if commit && backend.commitError == nil {
		backend.commitCount++
	} else {
		if !commit {
			backend.rollbackCount++
		}
	}
	tx.done = true
	tx.conn.tx = nil
	if commit {
		return backend.commitError
	}
	return nil
}

func (b *postgresStoreTestBackend) acquireAdvisoryLock(ctx context.Context, tx *postgresStoreTestTx, lockKey string) error {
	for {
		b.mu.Lock()
		if b.advisoryLocks == nil {
			b.advisoryLocks = make(map[string]*postgresStoreTestAdvisoryLock)
		}
		lock := b.advisoryLocks[lockKey]
		if lock == nil {
			b.advisoryLocks[lockKey] = &postgresStoreTestAdvisoryLock{owner: tx, released: make(chan struct{})}
			tx.lockKeys[lockKey] = struct{}{}
			b.mu.Unlock()
			return nil
		}
		if lock.owner == tx {
			b.mu.Unlock()
			return nil
		}
		released := lock.released
		waitStarted := b.lockWaitStarted
		b.mu.Unlock()

		if waitStarted != nil {
			select {
			case waitStarted <- lockKey:
			default:
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-released:
		}
	}
}

func (b *postgresStoreTestBackend) stageAuthMutation(tx *postgresStoreTestTx, id string, mutation postgresStoreTestAuthMutation) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.authRecords == nil {
		b.authRecords = make(map[string][]byte)
	}
	mutation.data = append([]byte(nil), mutation.data...)
	if tx == nil {
		if mutation.deleted {
			delete(b.authRecords, id)
			delete(b.authRevisions, id)
		} else {
			b.authRecords[id] = mutation.data
			b.bumpAuthRevisionLocked(id)
		}
		return true
	}
	tx.pendingAuth[id] = mutation
	return true
}

func (b *postgresStoreTestBackend) stageAuthMutationUnlessRetired(tx *postgresStoreTestTx, id string, data []byte) (bool, int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.authRecords == nil {
		b.authRecords = make(map[string][]byte)
	}
	currentData := b.authRecords[id]
	if tx != nil {
		if mutation, ok := tx.pendingAuth[id]; ok {
			if mutation.deleted {
				currentData = nil
			} else {
				currentData = mutation.data
			}
		}
	}
	if cliproxyauth.IsRetiredGeminiCLIAuthFileData(currentData) {
		return true, 0
	}
	mutation := postgresStoreTestAuthMutation{data: append([]byte(nil), data...)}
	if tx == nil {
		b.authRecords[id] = mutation.data
		b.bumpAuthRevisionLocked(id)
	} else {
		tx.pendingAuth[id] = mutation
	}
	return true, 1
}

func (b *postgresStoreTestBackend) authRecord(tx *postgresStoreTestTx, id string) ([]byte, bool, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.authRecords == nil {
		return nil, false, false
	}
	if tx != nil {
		if mutation, ok := tx.pendingAuth[id]; ok {
			if mutation.deleted {
				return nil, false, true
			}
			return append([]byte(nil), mutation.data...), true, true
		}
	}
	data, ok := b.authRecords[id]
	return append([]byte(nil), data...), ok, true
}

func (b *postgresStoreTestBackend) authRecordGeneration(tx *postgresStoreTestTx, id string) ([]byte, string, bool, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.authRecords == nil {
		return nil, "", false, false
	}
	if tx != nil {
		if mutation, ok := tx.pendingAuth[id]; ok {
			if mutation.deleted {
				return nil, "", false, true
			}
			return postgresStoreTestJSONBRead(mutation.data, b.normalizeJSONBReads), strconv.FormatInt(b.ensureTransactionRevisionLocked(tx), 10), true, true
		}
	}
	data, ok := b.authRecords[id]
	if !ok {
		return nil, "", false, true
	}
	return postgresStoreTestJSONBRead(data, b.normalizeJSONBReads), strconv.FormatInt(b.ensureAuthRevisionLocked(id), 10), true, true
}

func postgresStoreTestJSONBRead(data []byte, normalize bool) []byte {
	if !normalize {
		return append([]byte(nil), data...)
	}
	var value any
	if errUnmarshal := json.Unmarshal(data, &value); errUnmarshal != nil {
		return append([]byte(nil), data...)
	}
	normalized, errMarshal := json.MarshalIndent(value, "", "  ")
	if errMarshal != nil {
		return append([]byte(nil), data...)
	}
	return normalized
}

func (b *postgresStoreTestBackend) ensureTransactionRevisionLocked(tx *postgresStoreTestTx) int64 {
	if tx.revision > 0 {
		return tx.revision
	}
	b.nextAuthRevision++
	tx.revision = b.nextAuthRevision
	return tx.revision
}

func (b *postgresStoreTestBackend) ensureAuthRevisionLocked(id string) int64 {
	if b.authRevisions == nil {
		b.authRevisions = make(map[string]int64)
	}
	if revision := b.authRevisions[id]; revision > 0 {
		return revision
	}
	return b.bumpAuthRevisionLocked(id)
}

func (b *postgresStoreTestBackend) bumpAuthRevisionLocked(id string) int64 {
	if b.authRevisions == nil {
		b.authRevisions = make(map[string]int64)
	}
	b.nextAuthRevision++
	b.authRevisions[id] = b.nextAuthRevision
	return b.nextAuthRevision
}

func postgresStoreTestAdvisoryLockKey(args []driver.NamedValue) (string, error) {
	if len(args) < 2 {
		return "", fmt.Errorf("postgres store test: advisory lock requires 2 arguments")
	}
	return fmt.Sprint(args[0].Value) + "\x00" + fmt.Sprint(args[1].Value), nil
}

func postgresStoreTestAuthIDArg(args []driver.NamedValue) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("postgres store test: auth query requires an id")
	}
	id, ok := args[0].Value.(string)
	if !ok {
		return "", fmt.Errorf("postgres store test: auth id has type %T", args[0].Value)
	}
	return id, nil
}

func postgresStoreTestAuthWriteArgs(args []driver.NamedValue) (string, []byte, error) {
	id, errID := postgresStoreTestAuthIDArg(args)
	if errID != nil {
		return "", nil, errID
	}
	if len(args) < 2 {
		return "", nil, fmt.Errorf("postgres store test: auth write requires content")
	}
	switch value := args[1].Value.(type) {
	case []byte:
		return id, append([]byte(nil), value...), nil
	case string:
		return id, []byte(value), nil
	case json.RawMessage:
		return id, append([]byte(nil), value...), nil
	default:
		return "", nil, fmt.Errorf("postgres store test: auth content has type %T", value)
	}
}

func (r *postgresStoreTestRows) Columns() []string {
	return append([]string(nil), r.columns...)
}

func (r *postgresStoreTestRows) Close() error {
	return nil
}

func (r *postgresStoreTestRows) Next(dest []driver.Value) error {
	r.startOnce.Do(func() {
		if r.started != nil {
			select {
			case r.started <- struct{}{}:
			default:
			}
		}
		if r.proceed != nil {
			<-r.proceed
		}
	})
	if r.index >= len(r.rows) {
		return io.EOF
	}
	row := r.rows[r.index]
	r.index++
	for i := range dest {
		if i < len(row) {
			dest[i] = row[i]
			continue
		}
		dest[i] = nil
	}
	return nil
}

func postgresStoreTestExecCallsContaining(calls []postgresStoreTestExecCall, fragment string) []postgresStoreTestExecCall {
	filtered := make([]postgresStoreTestExecCall, 0, len(calls))
	for _, call := range calls {
		if strings.Contains(call.query, fragment) {
			filtered = append(filtered, call)
		}
	}
	return filtered
}

func execArgumentBytes(t *testing.T, args []driver.NamedValue, index int) []byte {
	t.Helper()

	if index >= len(args) {
		t.Fatalf("argument index %d out of range for %d args", index, len(args))
	}
	switch value := args[index].Value.(type) {
	case []byte:
		return append([]byte(nil), value...)
	case string:
		return []byte(value)
	case json.RawMessage:
		return append([]byte(nil), value...)
	default:
		t.Fatalf("unsupported argument type %T", value)
		return nil
	}
}

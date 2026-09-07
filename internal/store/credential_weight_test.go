package store

import (
	"context"
	"database/sql/driver"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestCredentialWeightsRejectRemoteSaveBeforeSideEffects(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	object := newObjectTokenStoreForServer(t, server.URL)
	backend := &postgresStoreTestBackend{}
	postgres := &PostgresStore{db: newPostgresStoreTestSQLDB(t, backend), cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: t.TempDir()}
	git := NewGitTokenStore(filepath.Join(t.TempDir(), "remote"), "", "", "main")
	git.SetBaseDir(filepath.Join(t.TempDir(), "auths"))
	for name, store := range map[string]interface {
		Save(context.Context, *coreauth.Auth) (string, error)
	}{"git": git, "object": object, "postgres": postgres} {
		t.Run(name, func(t *testing.T) {
			credential := &coreauth.Auth{ID: "weight.json", FileName: "weight.json", Provider: "codex", Metadata: map[string]any{"type": "codex", "weight": true}}
			if _, err := store.Save(t.Context(), credential); err == nil || !strings.Contains(err.Error(), "weight") {
				t.Fatal("invalid weight reached normal remote save processing")
			}
		})
	}
	if requests.Load() != 0 || len(backend.execCallsSnapshot()) != 0 {
		t.Fatal("invalid weight performed a remote request")
	}
	for _, dir := range []string{object.AuthDir(), postgres.authDir, git.AuthDir()} {
		if _, err := os.Stat(filepath.Join(dir, "weight.json")); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("invalid weight created a local credential")
		}
	}
}

func TestCredentialWeightsValidateGitRefreshQueueAdmission(t *testing.T) {
	store := NewGitTokenStore("unused", "", "", "main")
	credential := &coreauth.Auth{ID: "weight.json", Provider: "codex", Metadata: map[string]any{"weight": true}}
	if _, err := store.enqueueRefreshSave(t.Context(), credential, "expected", coreauth.RefreshPersistenceBatchInfo{}); err == nil || !strings.Contains(err.Error(), "weight") {
		t.Fatal("invalid weight reached the Git refresh queue")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := store.enqueueRefreshSave(ctx, credential, "expected", coreauth.RefreshPersistenceBatchInfo{}); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled refresh lost cancellation priority")
	}
}

func TestCredentialWeightsValidateGitAndObjectRawReads(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "weight.json")
	readers := map[string]func(string, string) (*coreauth.Auth, error){"git": (&GitTokenStore{}).readAuthFile, "object": (&ObjectTokenStore{}).readAuthFile}
	for name, read := range readers {
		t.Run(name, func(t *testing.T) {
			for _, weight := range []string{"-9223372036854775809", "1.5", "0,\"weight\":2", "0"} {
				if err := os.WriteFile(path, []byte(`{"type":"codex","weight":`+weight+`}`), 0600); err != nil {
					t.Fatal(err)
				}
				credential, err := read(path, dir)
				if weight == "0" {
					if err != nil || credential == nil || credential.Metadata["weight"] != float64(0) {
						t.Fatal("valid zero weight was not loaded")
					}
				} else if err == nil || credential != nil {
					t.Fatal("raw invalid weight was loaded after numeric coercion")
				}
			}
		})
	}
}

func TestCredentialWeightsPostgresListReportsSkippedRows(t *testing.T) {
	now := time.Now()
	backend := &postgresStoreTestBackend{
		queryColumns: []string{"id", "content", "created_at", "updated_at"},
		queryRows: [][]driver.Value{
			{"valid.json", `{"type":"codex","weight":0}`, now, now},
			{"overflow.json", `{"type":"codex","weight":-9223372036854775809}`, now, now},
			{"fraction.json", `{"type":"codex","weight":1.5}`, now, now},
		},
	}
	store := &PostgresStore{db: newPostgresStoreTestSQLDB(t, backend), cfg: PostgresStoreConfig{AuthTable: defaultAuthTable}, authDir: t.TempDir()}
	loaded, report, err := store.ListWithReport(t.Context())
	if err != nil || len(loaded) != 1 || loaded[0].ID != "valid.json" || report.Scanned != 3 || report.Loaded != 1 || report.Skipped != 2 {
		t.Fatalf("invalid-row accounting changed: %v, report=%+v", err, report)
	}
}

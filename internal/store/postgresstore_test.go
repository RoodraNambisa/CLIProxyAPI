package store

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/auth/vertex"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

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

	execs := backend.execCallsSnapshot()
	if len(execs) != 1 {
		t.Fatalf("exec count = %d, want 1", len(execs))
	}
	if got, want := execArgumentBytes(t, execs[0].args, 1), wantRaw; !bytes.Equal(got, want) {
		t.Fatalf("persisted db payload = %s, want %s", got, want)
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
		Provider: "gemini",
		Storage:  &testTokenStorage{},
		Metadata: map[string]any{
			"type":                 "gemini",
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

	execs := backend.execCallsSnapshot()
	if len(execs) != 1 {
		t.Fatalf("exec count = %d, want 1", len(execs))
	}
	if got := execArgumentBytes(t, execs[0].args, 1); !bytes.Equal(got, rawFile) {
		t.Fatalf("persisted db payload = %s, want %s", got, rawFile)
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

type postgresStoreTestBackend struct {
	mu           sync.Mutex
	execCalls    []postgresStoreTestExecCall
	queryColumns []string
	queryRows    [][]driver.Value
}

type postgresStoreTestExecCall struct {
	query string
	args  []driver.NamedValue
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

func (b *postgresStoreTestBackend) recordExec(query string, args []driver.NamedValue) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cloned := make([]driver.NamedValue, len(args))
	copy(cloned, args)
	b.execCalls = append(b.execCalls, postgresStoreTestExecCall{
		query: query,
		args:  cloned,
	})
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

type postgresStoreTestDriver struct{}

type postgresStoreTestConn struct {
	backend *postgresStoreTestBackend
}

type postgresStoreTestRows struct {
	columns []string
	rows    [][]driver.Value
	index   int
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
	return nil, fmt.Errorf("postgres store test: Begin not implemented")
}

func (c *postgresStoreTestConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.backend.recordExec(query, args)
	return driver.RowsAffected(1), nil
}

func (c *postgresStoreTestConn) QueryContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	columns, rows := c.backend.querySnapshot()
	return &postgresStoreTestRows{columns: columns, rows: rows}, nil
}

func (r *postgresStoreTestRows) Columns() []string {
	return append([]string(nil), r.columns...)
}

func (r *postgresStoreTestRows) Close() error {
	return nil
}

func (r *postgresStoreTestRows) Next(dest []driver.Value) error {
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

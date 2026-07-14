package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	internalauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/misc"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

const (
	defaultConfigTable          = "config_store"
	defaultAuthTable            = "auth_store"
	defaultConfigKey            = "config"
	authRecordAdvisoryLockQuery = "SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))"
)

// PostgresStoreConfig captures configuration required to initialize a Postgres-backed store.
type PostgresStoreConfig struct {
	DSN         string
	Schema      string
	ConfigTable string
	AuthTable   string
	SpoolDir    string
}

// PostgresStore persists configuration and authentication metadata using PostgreSQL as backend
// while mirroring data to a local workspace so existing file-based workflows continue to operate.
type PostgresStore struct {
	db         *sql.DB
	cfg        PostgresStoreConfig
	spoolRoot  string
	configPath string
	authDir    string
	mu         sync.Mutex
}

// NewPostgresStore establishes a connection to PostgreSQL and prepares the local workspace.
func NewPostgresStore(ctx context.Context, cfg PostgresStoreConfig) (*PostgresStore, error) {
	trimmedDSN := strings.TrimSpace(cfg.DSN)
	if trimmedDSN == "" {
		return nil, fmt.Errorf("postgres store: DSN is required")
	}
	cfg.DSN = trimmedDSN
	if cfg.ConfigTable == "" {
		cfg.ConfigTable = defaultConfigTable
	}
	if cfg.AuthTable == "" {
		cfg.AuthTable = defaultAuthTable
	}

	spoolRoot := strings.TrimSpace(cfg.SpoolDir)
	if spoolRoot == "" {
		if cwd, err := os.Getwd(); err == nil {
			spoolRoot = filepath.Join(cwd, "pgstore")
		} else {
			spoolRoot = filepath.Join(os.TempDir(), "pgstore")
		}
	}
	absSpool, err := filepath.Abs(spoolRoot)
	if err != nil {
		return nil, fmt.Errorf("postgres store: resolve spool directory: %w", err)
	}
	configDir := filepath.Join(absSpool, "config")
	authDir := filepath.Join(absSpool, "auths")
	if err = os.MkdirAll(configDir, 0o700); err != nil {
		return nil, fmt.Errorf("postgres store: create config directory: %w", err)
	}
	if err = os.MkdirAll(authDir, 0o700); err != nil {
		return nil, fmt.Errorf("postgres store: create auth directory: %w", err)
	}

	db, err := sql.Open("pgx", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres store: open database connection: %w", err)
	}
	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("postgres store: ping database: %w", err)
	}

	store := &PostgresStore{
		db:         db,
		cfg:        cfg,
		spoolRoot:  absSpool,
		configPath: filepath.Join(configDir, "config.yaml"),
		authDir:    authDir,
	}
	return store, nil
}

// Close releases the underlying database connection.
func (s *PostgresStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// EnsureSchema creates the required tables (and schema when provided).
func (s *PostgresStore) EnsureSchema(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("postgres store: not initialized")
	}
	if schema := strings.TrimSpace(s.cfg.Schema); schema != "" {
		query := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", quoteIdentifier(schema))
		if _, err := s.db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("postgres store: create schema: %w", err)
		}
	}
	configTable := s.fullTableName(s.cfg.ConfigTable)
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY,
			content TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`, configTable)); err != nil {
		return fmt.Errorf("postgres store: create config table: %w", err)
	}
	authTable := s.fullTableName(s.cfg.AuthTable)
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				id TEXT PRIMARY KEY,
				content JSONB NOT NULL,
				auth_revision TEXT NOT NULL DEFAULT (md5(random()::text || clock_timestamp()::text)),
				created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
			)
		`, authTable)); err != nil {
		return fmt.Errorf("postgres store: create auth table: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS auth_revision TEXT", authTable)); err != nil {
		return fmt.Errorf("postgres store: add auth revision column: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s
		SET auth_revision = md5(random()::text || clock_timestamp()::text || id || content::text)
		WHERE auth_revision IS NULL OR btrim(auth_revision) = ''
	`, authTable)); err != nil {
		return fmt.Errorf("postgres store: backfill auth revisions: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(
		"ALTER TABLE %s ALTER COLUMN auth_revision SET DEFAULT (md5(random()::text || clock_timestamp()::text))",
		authTable,
	)); err != nil {
		return fmt.Errorf("postgres store: set auth revision default: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN auth_revision SET NOT NULL", authTable)); err != nil {
		return fmt.Errorf("postgres store: require auth revisions: %w", err)
	}
	return nil
}

// Bootstrap synchronizes configuration and auth records between PostgreSQL and the local workspace.
func (s *PostgresStore) Bootstrap(ctx context.Context, exampleConfigPath string) error {
	if err := s.EnsureSchema(ctx); err != nil {
		return err
	}
	if err := s.syncConfigFromDatabase(ctx, exampleConfigPath); err != nil {
		return err
	}
	if err := s.syncAuthFromDatabase(ctx); err != nil {
		return err
	}
	return nil
}

// ConfigPath returns the managed configuration file path inside the spool directory.
func (s *PostgresStore) ConfigPath() string {
	if s == nil {
		return ""
	}
	return s.configPath
}

// AuthDir returns the local directory containing mirrored auth files.
func (s *PostgresStore) AuthDir() string {
	if s == nil {
		return ""
	}
	return s.authDir
}

// WorkDir exposes the root spool directory used for mirroring.
func (s *PostgresStore) WorkDir() string {
	if s == nil {
		return ""
	}
	return s.spoolRoot
}

// SetBaseDir implements the optional interface used by authenticators; it is a no-op because
// the Postgres-backed store controls its own workspace.
func (s *PostgresStore) SetBaseDir(string) {}

// Save persists authentication metadata to disk and PostgreSQL.
func (s *PostgresStore) Save(ctx context.Context, auth *cliproxyauth.Auth) (string, error) {
	if auth == nil {
		return "", fmt.Errorf("postgres store: auth is nil")
	}
	if cliproxyauth.IsRetiredGeminiCLIAuth(auth) {
		cliproxyauth.WarnRetiredGeminiCLIAuthIgnored()
		return "", fmt.Errorf("postgres store: %w", cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly)
	}

	path, err := s.resolveAuthPath(auth)
	if err != nil {
		return "", err
	}
	if path == "" {
		return "", fmt.Errorf("postgres store: missing file path attribute for %s", auth.ID)
	}
	relID, errRel := s.relativeAuthID(path)
	if errRel != nil {
		return "", errRel
	}
	relativePath := filepath.FromSlash(relID)
	path = filepath.Join(s.authDir, relativePath)
	root, errRoot := os.OpenRoot(s.authDir)
	if errRoot != nil {
		return "", fmt.Errorf("postgres store: open auth root: %w", errRoot)
	}
	defer func() {
		if errClose := root.Close(); errClose != nil {
			log.WithError(errClose).Error("postgres store: close auth root after save")
		}
	}()

	if auth.Disabled {
		if _, statErr := root.Lstat(relativePath); errors.Is(statErr, fs.ErrNotExist) {
			return "", nil
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	unlockPath := authfileguard.Lock(path)
	defer unlockPath()
	if authfileguard.IsRetired(path) {
		return "", fmt.Errorf("postgres store: %w", cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly)
	}
	if authfileguard.IsQuarantined(path) {
		return "", fmt.Errorf("postgres store: auth deletion is still pending: %w", authfileguard.ErrDeleteGenerationUncertain)
	}

	tx, errTx := s.beginAuthRecordMutation(ctx, relID)
	if errTx != nil {
		return "", errTx
	}
	defer rollbackAuthRecordMutation(tx)
	remoteData, _, remoteExists, errRemote := s.readAuthRecordGenerationTx(ctx, tx, relID)
	if errRemote != nil {
		return "", errRemote
	}
	if remoteExists {
		if errRetired := cliproxyauth.RejectRetiredGeminiCLIAuthFileMutation(remoteData); errRetired != nil {
			if errors.Is(errRetired, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
				authfileguard.MarkRetired(path)
			}
			return "", fmt.Errorf("postgres store: %w", errRetired)
		}
	}

	if err = mkdirAuthDirectoriesAtRoot(root, filepath.Dir(relativePath), 0o700); err != nil {
		return "", fmt.Errorf("postgres store: create auth directory: %w", err)
	}
	localSnapshot, errSnapshot := captureAuthFileSnapshot(root, relativePath)
	if errSnapshot != nil {
		return "", errSnapshot
	}
	if errRetired := localSnapshot.rejectRetiredGeminiCLIAuthPersistence(); errRetired != nil {
		authfileguard.MarkRetired(path)
		return "", errRetired
	}
	runtimeSnapshot := captureAuthRuntimeSnapshot(auth)

	var persistedData []byte
	switch {
	case auth.Storage != nil:
		if setter, ok := auth.Storage.(internalauth.MetadataSetter); ok {
			setter.SetMetadata(cliproxyauth.MetadataWithDisabled(auth))
		}
		data, errData := produceAuthStorageData(auth.Storage)
		if errData != nil {
			return "", fmt.Errorf("postgres store: produce storage auth: %w", errData)
		}
		if errWrite := writeAuthFileAtomicallyAtRoot(root, relativePath, data, &localSnapshot); errWrite != nil {
			if errors.Is(errWrite, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
				authfileguard.MarkRetired(path)
			}
			return "", fmt.Errorf("postgres store: persist storage auth: %w", errWrite)
		}
		persistedData = data
		if errSync := cliproxyauth.SyncPersistedMetadataAndSourceHash(auth, data); errSync != nil {
			return "", fmt.Errorf("postgres store: sync persisted storage auth: %w", errSync)
		}
	case auth.Metadata != nil:
		raw, errMarshal := cliproxyauth.CanonicalMetadataBytes(auth)
		if errMarshal != nil {
			return "", fmt.Errorf("postgres store: canonicalize metadata: %w", errMarshal)
		}
		writeLocal := true
		persistedData = raw
		if existing, errRead := root.ReadFile(relativePath); errRead == nil {
			if jsonEqual(existing, raw) {
				if !localSnapshot.exists || !bytes.Equal(existing, localSnapshot.data) {
					return "", authfileguard.ErrPersistGenerationStale
				}
				writeLocal = false
				persistedData = localSnapshot.data
			}
		} else if errRead != nil && !errors.Is(errRead, fs.ErrNotExist) {
			return "", fmt.Errorf("postgres store: read existing metadata: %w", errRead)
		}
		if writeLocal {
			if errWrite := writeAuthFileAtomicallyAtRoot(root, relativePath, raw, &localSnapshot); errWrite != nil {
				if errors.Is(errWrite, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
					authfileguard.MarkRetired(path)
				}
				return "", fmt.Errorf("postgres store: persist auth file: %w", errWrite)
			}
		}
		cliproxyauth.SetSourceHashAttribute(auth, raw)
	default:
		return "", fmt.Errorf("postgres store: nothing to persist for %s", auth.ID)
	}

	if auth.Attributes == nil {
		auth.Attributes = make(map[string]string)
	}
	auth.Attributes["path"] = path

	if strings.TrimSpace(auth.FileName) == "" {
		auth.FileName = auth.ID
	}

	if err = s.persistAuth(ctx, tx, relID, persistedData); err != nil {
		if errors.Is(err, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
			authfileguard.MarkRetired(path)
		}
		errRollback := restoreAuthFileSnapshotAtRoot(root, relativePath, persistedData, localSnapshot)
		runtimeSnapshot.restore(auth)
		if errRollback != nil {
			return "", errors.Join(err, fmt.Errorf("postgres store: roll back local auth after persistence failure: %w", errRollback))
		}
		return "", err
	}
	committedData, committedRevision, committedExists, errRevision := s.readAuthRecordGenerationTx(ctx, tx, relID)
	if errRevision != nil || !committedExists || !jsonEqual(committedData, persistedData) {
		if errRevision == nil {
			errRevision = errors.New("postgres store: persisted auth generation is missing or changed")
		}
		errRollback := restoreAuthFileSnapshotAtRoot(root, relativePath, persistedData, localSnapshot)
		runtimeSnapshot.restore(auth)
		return "", errors.Join(
			errRevision,
			wrapOptionalError("postgres store: roll back local auth after generation read failure", errRollback),
		)
	}
	if errCommit := tx.Commit(); errCommit != nil {
		currentData, currentRevision, currentExists, errProbe := s.readAuthRecordGeneration(ctx, relID)
		if errProbe == nil && currentExists && currentRevision == committedRevision && jsonEqual(currentData, persistedData) {
			return path, nil
		}
		errRollback := restoreAuthFileSnapshotAtRoot(root, relativePath, persistedData, localSnapshot)
		runtimeSnapshot.restore(auth)
		return "", errors.Join(
			fmt.Errorf("postgres store: commit auth record: %w", errCommit),
			wrapOptionalError("postgres store: verify auth after commit failure", errProbe),
			wrapOptionalError("postgres store: roll back local auth after commit failure", errRollback),
		)
	}
	return path, nil
}

// List enumerates all auth records stored in PostgreSQL.
func (s *PostgresStore) List(ctx context.Context) ([]*cliproxyauth.Auth, error) {
	query := fmt.Sprintf("SELECT id, content, created_at, updated_at FROM %s ORDER BY id", s.fullTableName(s.cfg.AuthTable))
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("postgres store: list auth: %w", err)
	}
	defer rows.Close()

	type listedAuthRecord struct {
		id        string
		payload   string
		createdAt time.Time
		updatedAt time.Time
	}
	records := make([]listedAuthRecord, 0, 32)
	for rows.Next() {
		record := listedAuthRecord{}
		if err = rows.Scan(&record.id, &record.payload, &record.createdAt, &record.updatedAt); err != nil {
			return nil, fmt.Errorf("postgres store: scan auth row: %w", err)
		}
		records = append(records, record)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres store: iterate auth rows: %w", err)
	}
	if err = rows.Close(); err != nil {
		return nil, fmt.Errorf("postgres store: close auth rows: %w", err)
	}

	auths := make([]*cliproxyauth.Auth, 0, len(records))
	for _, record := range records {
		id := record.id
		payload := record.payload
		path, errPath := s.absoluteAuthPath(id)
		if errPath != nil {
			log.WithError(errPath).Warnf("postgres store: skipping auth %s outside spool", id)
			continue
		}
		payloadData := []byte(payload)
		if cliproxyauth.IsRetiredGeminiCLIAuthFileData(payloadData) {
			current, errCurrent := s.markRetiredAuthRecordIfCurrent(ctx, id, path, payloadData)
			if errCurrent != nil {
				return nil, errCurrent
			}
			if !current {
				continue
			}
		}
		metadata := make(map[string]any)
		if err = json.Unmarshal(payloadData, &metadata); err != nil {
			log.WithError(err).Warnf("postgres store: skipping auth %s with invalid json", id)
			continue
		}
		provider := strings.TrimSpace(valueAsString(metadata["type"]))
		if provider == "" {
			provider = "unknown"
		}
		attr := map[string]string{"path": path}
		if email := strings.TrimSpace(valueAsString(metadata["email"])); email != "" {
			attr["email"] = email
		}
		disabled, _ := metadata["disabled"].(bool)
		status := cliproxyauth.StatusActive
		if disabled {
			status = cliproxyauth.StatusDisabled
		}
		auth := &cliproxyauth.Auth{
			ID:               normalizeAuthID(id),
			Provider:         provider,
			FileName:         normalizeAuthID(id),
			Label:            labelFor(metadata),
			Status:           status,
			Disabled:         disabled,
			Attributes:       attr,
			Metadata:         metadata,
			CreatedAt:        record.createdAt,
			UpdatedAt:        record.updatedAt,
			LastRefreshedAt:  time.Time{},
			NextRefreshAfter: time.Time{},
		}
		cliproxyauth.ApplyFileBackedGeminiAPIKey(auth)
		if errHash := cliproxyauth.SetCanonicalSourceHashAttribute(auth); errHash != nil {
			log.WithError(errHash).Warnf("postgres store: skipping auth %s with invalid canonical metadata", id)
			continue
		}
		cliproxyauth.ApplyCustomHeadersFromMetadata(auth)
		auths = append(auths, auth)
	}
	return auths, nil
}

// Delete removes an auth file and the corresponding database record.
func (s *PostgresStore) Delete(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("postgres store: id is empty"))
	}
	path, err := s.resolveDeletePath(id)
	if err != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, err)
	}
	relID, err := s.relativeAuthID(path)
	if err != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, err)
	}
	root, errRoot := os.OpenRoot(s.authDir)
	if errRoot != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("postgres store: open auth root: %w", errRoot))
	}
	defer func() {
		if errClose := root.Close(); errClose != nil {
			log.WithError(errClose).Error("postgres store: close auth root after deletion")
		}
	}()
	return s.deleteAuthFileAtRoot(ctx, root, path, relID)
}

// FinalizeAuthFileDeletion removes the database row after a caller has already
// removed the local mirror file through a stable filesystem root.
func (s *PostgresStore) FinalizeAuthFileDeletion(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return fmt.Errorf("postgres store: id is empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	path, err := s.resolveDeletePath(id)
	if err != nil {
		return err
	}
	unlockPath := authfileguard.Lock(path)
	defer unlockPath()
	retiredSnapshot := authfileguard.CaptureRetired(path)
	relID, err := s.relativeAuthID(path)
	if err != nil {
		return err
	}
	if errDelete := s.deleteRetiredAuthRecord(ctx, relID); errDelete != nil {
		return errDelete
	}
	authfileguard.ClearRetiredSnapshot(retiredSnapshot)
	return nil
}

// DeleteAuthFileAtRoot removes the local mirror and database row while
// serializing against concurrent saves.
func (s *PostgresStore) DeleteAuthFileAtRoot(ctx context.Context, root *os.Root, id string) error {
	if root == nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("postgres store: root is nil"))
	}
	path, errPath := s.absoluteAuthPath(id)
	if errPath != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errPath)
	}
	relID, errRel := s.relativeAuthID(path)
	if errRel != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errRel)
	}
	return s.deleteAuthFileAtRoot(ctx, root, path, relID)
}

func (s *PostgresStore) deleteAuthFileAtRoot(ctx context.Context, root *os.Root, path, relID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	unlockPath := authfileguard.Lock(path)
	defer unlockPath()
	retiredSnapshot := authfileguard.CaptureRetired(path)
	rootName := filepath.FromSlash(relID)
	tx, errTx := s.beginAuthRecordMutation(ctx, relID)
	if errTx != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errTx)
	}
	defer rollbackAuthRecordMutation(tx)
	originalData, originalRevision, originalExists, errOriginal := s.readAuthRecordGenerationTx(ctx, tx, relID)
	if errOriginal != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errOriginal)
	}
	if errDelete := s.deleteAuthRecordTx(ctx, tx, relID); errDelete != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errDelete)
	}
	remoteIdentity := "missing"
	if originalExists {
		remoteIdentity = originalRevision
	}
	localSnapshot, errLocalSnapshot := captureAuthFileSnapshot(root, rootName)
	if errLocalSnapshot != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errLocalSnapshot)
	}
	deleteCtx, prepareDelete, clearDelete := durableAuthDelete(
		ctx,
		s.configPath,
		s.authDir,
		path,
		localSnapshot.data,
		"postgres:"+relID,
		remoteIdentity,
		true,
		originalExists,
		originalData,
	)
	errDelete := deleteAuthFileTransaction(
		root,
		rootName,
		func(original authFileSnapshot) error {
			if !sameAuthFileGeneration(original, localSnapshot) {
				return authfileguard.ErrPersistGenerationStale
			}
			return prepareDelete()
		},
		func() error {
			if errCommit := tx.Commit(); errCommit != nil {
				return fmt.Errorf("postgres store: commit auth record deletion: %w", errCommit)
			}
			return nil
		},
		func() (authDeleteProbeState, error) {
			return probeAuthDeleteResult(deleteCtx, func(probeCtx context.Context) (authDeleteProbeState, error) {
				currentData, currentRevision, currentExists, errRead := s.readAuthRecordGeneration(probeCtx, relID)
				if errRead != nil {
					return authDeleteProbeOriginal, errRead
				}
				if !currentExists {
					return authDeleteProbeAbsent, nil
				}
				if originalExists && currentRevision == originalRevision && jsonEqual(currentData, originalData) {
					return authDeleteProbeOriginal, nil
				}
				return authDeleteProbeReplaced, nil
			})
		},
	)
	errDelete = finishDurableAuthDelete(errDelete, clearDelete)
	if deleteOutcomeIsCommitted(errDelete) {
		authfileguard.ClearRetiredSnapshot(retiredSnapshot)
	}
	return errDelete
}

// PersistAuthFiles stores the provided auth file changes in PostgreSQL.
func (s *PostgresStore) PersistAuthFiles(ctx context.Context, _ string, paths ...string) error {
	if len(paths) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cleanAuthDir := filepath.Clean(s.authDir)
	root, errRoot := os.OpenRoot(filepath.Dir(cleanAuthDir))
	if errRoot != nil {
		return fmt.Errorf("postgres store: open auth parent root for persistence: %w", errRoot)
	}
	defer func() {
		if errClose := root.Close(); errClose != nil {
			log.WithError(errClose).Error("postgres store: close auth root after persistence")
		}
	}()

	for _, p := range paths {
		trimmed := strings.TrimSpace(p)
		if trimmed == "" {
			continue
		}
		relID, err := s.relativeAuthID(trimmed)
		if err != nil {
			// Attempt to resolve absolute path under authDir.
			abs := trimmed
			if !filepath.IsAbs(abs) {
				abs = filepath.Join(s.authDir, trimmed)
			}
			relID, err = s.relativeAuthID(abs)
			if err != nil {
				log.WithError(err).Warnf("postgres store: ignoring auth path %s", trimmed)
				continue
			}
		}
		path, errPath := s.absoluteAuthPath(relID)
		if errPath != nil {
			return errPath
		}
		unlockPath := authfileguard.Lock(path)
		snapshotPath := filepath.Join(filepath.Base(cleanAuthDir), filepath.FromSlash(relID))
		errSnapshot := s.persistAuthFileSnapshot(ctx, root, snapshotPath, path, relID)
		unlockPath()
		if errSnapshot != nil {
			return errSnapshot
		}
	}
	return nil
}

func (s *PostgresStore) persistAuthFileSnapshot(ctx context.Context, root *os.Root, snapshotPath, path, relID string) error {
	retiredSnapshot := authfileguard.CaptureRetired(path)
	tx, errTx := s.beginAuthRecordMutation(ctx, relID)
	if errTx != nil {
		return errTx
	}
	defer rollbackAuthRecordMutation(tx)

	remoteData, remoteRevision, remoteExists, errRemote := s.readAuthRecordGenerationTx(ctx, tx, relID)
	if errRemote != nil {
		return errRemote
	}
	snapshot, errSnapshot := captureAuthFileSnapshot(root, snapshotPath)
	if errSnapshot == nil {
		errSnapshot = authfileguard.ValidatePersistSnapshot(ctx, snapshot.data, snapshot.exists)
	}
	if errSnapshot == nil {
		errSnapshot = snapshot.rejectRetiredGeminiCLIAuthPersistence()
	}
	if errors.Is(errSnapshot, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
		authfileguard.MarkRetired(path)
		if remoteExists && !cliproxyauth.IsRetiredGeminiCLIAuthFileData(remoteData) {
			return removeAuthFileAtRoot(root, snapshotPath)
		}
		return errSnapshot
	}
	if errSnapshot != nil {
		return errSnapshot
	}
	if snapshot.exists && cliproxyauth.IsRetiredGeminiCLIAuthFileData(remoteData) {
		authfileguard.MarkRetired(path)
		return fmt.Errorf("postgres store: %w", cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly)
	}
	deleteMutation := !snapshot.exists || len(snapshot.data) == 0
	persistedRevision := ""
	if deleteMutation {
		if !remoteExists && authfileguard.DeleteGenerationFromContext(ctx) != nil {
			// Absence is terminal: no backend row remains to compare or delete.
			authfileguard.ClearRetiredSnapshot(retiredSnapshot)
			return nil
		}
		identity := "missing"
		if remoteExists {
			identity = remoteRevision
		}
		switch matchExpectedAuthDeleteGeneration(ctx, "postgres:"+relID, identity, true, remoteData) {
		case authDeleteGenerationUncertain:
			return authfileguard.ErrDeleteGenerationUncertain
		case authDeleteGenerationReplaced:
			if remoteExists {
				if errRestore := writeAuthFileAtomicallyAtRoot(root, snapshotPath, remoteData, &snapshot); errRestore != nil {
					return fmt.Errorf("postgres store: restore remote auth replacement %s: %w", relID, errRestore)
				}
			}
			if !remoteExists || !cliproxyauth.IsRetiredGeminiCLIAuthFileData(remoteData) {
				authfileguard.ClearRetiredSnapshot(retiredSnapshot)
			}
			return nil
		}
		if errDelete := s.deleteAuthRecordTx(ctx, tx, relID); errDelete != nil {
			return errDelete
		}
	} else {
		if errPersist := s.persistAuth(ctx, tx, relID, snapshot.data); errPersist != nil {
			if errors.Is(errPersist, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
				authfileguard.MarkRetired(path)
			}
			return errPersist
		}
		persistedData, revision, exists, errPersisted := s.readAuthRecordGenerationTx(ctx, tx, relID)
		if errPersisted != nil {
			return errPersisted
		}
		if !exists || !jsonEqual(persistedData, snapshot.data) {
			return errors.New("postgres store: persisted auth generation is missing or changed")
		}
		persistedRevision = revision
	}
	if errCommit := tx.Commit(); errCommit != nil {
		commitErr := fmt.Errorf("postgres store: commit auth record persistence: %w", errCommit)
		currentData, currentRevision, currentExists, errProbe := s.readAuthRecordGeneration(ctx, relID)
		if errProbe != nil {
			return errors.Join(commitErr, fmt.Errorf("postgres store: verify auth persistence: %w", errProbe))
		}
		if deleteMutation {
			if !currentExists {
				authfileguard.ClearRetiredSnapshot(retiredSnapshot)
				return nil
			}
			if remoteExists && currentRevision == remoteRevision && jsonEqual(currentData, remoteData) {
				return commitErr
			}
			return errors.Join(commitErr, authfileguard.ErrDeleteGenerationUncertain)
		}
		if currentExists && currentRevision == persistedRevision && jsonEqual(currentData, snapshot.data) {
			return nil
		}
		return commitErr
	}
	if deleteMutation {
		authfileguard.ClearRetiredSnapshot(retiredSnapshot)
	}
	return nil
}

// PersistConfig mirrors the local configuration file to PostgreSQL.
func (s *PostgresStore) PersistConfig(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.configPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return s.deleteConfigRecord(ctx)
		}
		return fmt.Errorf("postgres store: read config file: %w", err)
	}
	return s.persistConfig(ctx, data)
}

// syncConfigFromDatabase writes the database-stored config to disk or seeds the database from template.
func (s *PostgresStore) syncConfigFromDatabase(ctx context.Context, exampleConfigPath string) error {
	query := fmt.Sprintf("SELECT content FROM %s WHERE id = $1", s.fullTableName(s.cfg.ConfigTable))
	var content string
	err := s.db.QueryRowContext(ctx, query, defaultConfigKey).Scan(&content)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, errStat := os.Stat(s.configPath); errors.Is(errStat, fs.ErrNotExist) {
			if exampleConfigPath != "" {
				if errCopy := misc.CopyConfigTemplate(exampleConfigPath, s.configPath); errCopy != nil {
					return fmt.Errorf("postgres store: copy example config: %w", errCopy)
				}
			} else {
				if errCreate := os.MkdirAll(filepath.Dir(s.configPath), 0o700); errCreate != nil {
					return fmt.Errorf("postgres store: prepare config directory: %w", errCreate)
				}
				if errWrite := os.WriteFile(s.configPath, []byte{}, 0o600); errWrite != nil {
					return fmt.Errorf("postgres store: create empty config: %w", errWrite)
				}
			}
		}
		data, errRead := os.ReadFile(s.configPath)
		if errRead != nil {
			return fmt.Errorf("postgres store: read local config: %w", errRead)
		}
		if errPersist := s.persistConfig(ctx, data); errPersist != nil {
			return errPersist
		}
	case err != nil:
		return fmt.Errorf("postgres store: load config from database: %w", err)
	default:
		if err = os.MkdirAll(filepath.Dir(s.configPath), 0o700); err != nil {
			return fmt.Errorf("postgres store: prepare config directory: %w", err)
		}
		normalized := normalizeLineEndings(content)
		if err = os.WriteFile(s.configPath, []byte(normalized), 0o600); err != nil {
			return fmt.Errorf("postgres store: write config to spool: %w", err)
		}
	}
	return nil
}

// syncAuthFromDatabase populates the local auth directory from PostgreSQL data.
func (s *PostgresStore) syncAuthFromDatabase(ctx context.Context) error {
	query := fmt.Sprintf("SELECT id, content FROM %s", s.fullTableName(s.cfg.AuthTable))
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("postgres store: load auth from database: %w", err)
	}
	defer rows.Close()

	if err = os.MkdirAll(s.authDir, 0o700); err != nil {
		return fmt.Errorf("postgres store: create auth directory: %w", err)
	}
	root, errRoot := os.OpenRoot(s.authDir)
	if errRoot != nil {
		return fmt.Errorf("postgres store: open auth mirror root: %w", errRoot)
	}
	defer func() {
		if errClose := root.Close(); errClose != nil {
			log.WithError(errClose).Error("postgres store: close auth mirror root after sync")
		}
	}()
	if err = clearAuthDirectoryAtRoot(root); err != nil {
		return fmt.Errorf("postgres store: reset auth directory: %w", err)
	}

	for rows.Next() {
		var (
			id      string
			payload string
		)
		if err = rows.Scan(&id, &payload); err != nil {
			return fmt.Errorf("postgres store: scan auth row: %w", err)
		}
		path, errPath := s.absoluteAuthPath(id)
		if errPath != nil {
			log.WithError(errPath).Warnf("postgres store: skipping auth %s outside spool", id)
			continue
		}
		relID, errRelative := s.relativeAuthID(path)
		if errRelative != nil {
			log.WithError(errRelative).Warnf("postgres store: skipping auth %s outside spool", id)
			continue
		}
		relativePath := filepath.FromSlash(relID)
		if err = mkdirAuthDirectoriesAtRoot(root, filepath.Dir(relativePath), 0o700); err != nil {
			return fmt.Errorf("postgres store: create auth subdir: %w", err)
		}
		localSnapshot, errSnapshot := captureAuthFileSnapshot(root, relativePath)
		if errSnapshot != nil {
			return fmt.Errorf("postgres store: inspect auth file before sync: %w", errSnapshot)
		}
		if err = writeAuthMirrorFileAtomicallyAtRoot(root, relativePath, []byte(payload), &localSnapshot); err != nil {
			return fmt.Errorf("postgres store: write auth file: %w", err)
		}
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("postgres store: iterate auth rows: %w", err)
	}
	return nil
}

func (s *PostgresStore) upsertAuthRecord(ctx context.Context, tx *sql.Tx, relID, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("postgres store: read auth file: %w", err)
	}
	if len(data) == 0 {
		query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", s.fullTableName(s.cfg.AuthTable))
		if _, errDelete := tx.ExecContext(ctx, query, relID); errDelete != nil {
			return fmt.Errorf("postgres store: delete auth record: %w", errDelete)
		}
		return nil
	}
	return s.persistAuth(ctx, tx, relID, data)
}

func (s *PostgresStore) persistAuth(ctx context.Context, tx *sql.Tx, relID string, data []byte) error {
	jsonPayload := json.RawMessage(data)
	query := fmt.Sprintf(`
		INSERT INTO %s AS current_auth (id, content, auth_revision, created_at, updated_at)
		VALUES ($1, $2, $3, NOW(), NOW())
			ON CONFLICT (id)
			DO UPDATE SET content = EXCLUDED.content, auth_revision = EXCLUDED.auth_revision, updated_at = NOW()
			WHERE lower(btrim(COALESCE(current_auth.content ->> 'type', ''))) <> 'gemini-cli'
			  AND (
				lower(btrim(COALESCE(current_auth.content ->> 'type', ''))) <> 'gemini'
				OR (
					jsonb_typeof(current_auth.content -> 'api_key') = 'string'
					AND regexp_replace(COALESCE(current_auth.content ->> 'api_key', ''), '^[[:space:]]+|[[:space:]]+$', '', 'g') <> ''
				)
			  )
	`, s.fullTableName(s.cfg.AuthTable))
	result, errExec := tx.ExecContext(ctx, query, relID, jsonPayload, uuid.NewString())
	if errExec != nil {
		return fmt.Errorf("postgres store: upsert auth record: %w", errExec)
	}
	rowsAffected, errRows := result.RowsAffected()
	if errRows != nil {
		return fmt.Errorf("postgres store: inspect auth upsert result: %w", errRows)
	}
	switch rowsAffected {
	case 1:
		return nil
	case 0:
		return fmt.Errorf("postgres store: %w", cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly)
	default:
		return fmt.Errorf("postgres store: auth upsert affected %d rows, want 1", rowsAffected)
	}
}

func (s *PostgresStore) beginAuthRecordMutation(ctx context.Context, relID string) (*sql.Tx, error) {
	tx, errBegin := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if errBegin != nil {
		return nil, fmt.Errorf("postgres store: begin auth record transaction: %w", errBegin)
	}
	if _, errLock := tx.ExecContext(ctx, authRecordAdvisoryLockQuery, s.fullTableName(s.cfg.AuthTable), relID); errLock != nil {
		rollbackAuthRecordMutation(tx)
		return nil, fmt.Errorf("postgres store: lock auth record: %w", errLock)
	}
	return tx, nil
}

func rollbackAuthRecordMutation(tx *sql.Tx) {
	if tx == nil {
		return
	}
	if errRollback := tx.Rollback(); errRollback != nil && !errors.Is(errRollback, sql.ErrTxDone) {
		log.WithError(errRollback).Error("postgres store: rollback auth record transaction")
	}
}

func (s *PostgresStore) deleteAuthRecord(ctx context.Context, relID string) error {
	tx, errTx := s.beginAuthRecordMutation(ctx, relID)
	if errTx != nil {
		return errTx
	}
	defer rollbackAuthRecordMutation(tx)
	if errDelete := s.deleteAuthRecordTx(ctx, tx, relID); errDelete != nil {
		return errDelete
	}
	if errCommit := tx.Commit(); errCommit != nil {
		return fmt.Errorf("postgres store: commit auth record deletion: %w", errCommit)
	}
	return nil
}

func (s *PostgresStore) deleteAuthRecordTx(ctx context.Context, tx *sql.Tx, relID string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", s.fullTableName(s.cfg.AuthTable))
	if _, err := tx.ExecContext(ctx, query, relID); err != nil {
		return fmt.Errorf("postgres store: delete auth record: %w", err)
	}
	return nil
}

func (s *PostgresStore) deleteRetiredAuthRecord(ctx context.Context, relID string) error {
	tx, errTx := s.beginAuthRecordMutation(ctx, relID)
	if errTx != nil {
		return errTx
	}
	defer rollbackAuthRecordMutation(tx)
	data, revision, exists, errRead := s.readAuthRecordGenerationTx(ctx, tx, relID)
	if errRead != nil {
		return errRead
	}
	if !exists {
		return nil
	}
	if !cliproxyauth.IsRetiredGeminiCLIAuthFileData(data) {
		return nil
	}
	switch matchExpectedAuthDeleteGeneration(ctx, "postgres:"+relID, revision, true, data) {
	case authDeleteGenerationUncertain, authDeleteGenerationReplaced:
		return authfileguard.ErrDeleteGenerationUncertain
	}
	if errDelete := s.deleteAuthRecordTx(ctx, tx, relID); errDelete != nil {
		return errDelete
	}
	if errCommit := tx.Commit(); errCommit != nil {
		commitErr := fmt.Errorf("postgres store: commit retired auth deletion: %w", errCommit)
		currentData, currentRevision, currentExists, errProbe := s.readAuthRecordGeneration(ctx, relID)
		if errProbe != nil {
			return errors.Join(commitErr, fmt.Errorf("postgres store: verify retired auth deletion: %w", errProbe))
		}
		if !currentExists || !cliproxyauth.IsRetiredGeminiCLIAuthFileData(currentData) {
			return nil
		}
		if currentRevision == revision && jsonEqual(currentData, data) {
			return commitErr
		}
		return errors.Join(commitErr, authfileguard.ErrDeleteGenerationUncertain)
	}
	return nil
}

func (s *PostgresStore) readAuthRecordGeneration(ctx context.Context, relID string) ([]byte, string, bool, error) {
	query := fmt.Sprintf("SELECT content, auth_revision FROM %s WHERE id = $1", s.fullTableName(s.cfg.AuthTable))
	var data []byte
	var revision string
	if err := s.db.QueryRowContext(ctx, query, relID).Scan(&data, &revision); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, "", false, nil
		}
		return nil, "", false, fmt.Errorf("postgres store: inspect auth record generation: %w", err)
	}
	return data, strings.TrimSpace(revision), true, nil
}

func (s *PostgresStore) readAuthRecordDataTx(ctx context.Context, tx *sql.Tx, relID string) ([]byte, bool, error) {
	query := fmt.Sprintf("SELECT content FROM %s WHERE id = $1 FOR UPDATE", s.fullTableName(s.cfg.AuthTable))
	var data []byte
	if err := tx.QueryRowContext(ctx, query, relID).Scan(&data); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("postgres store: inspect auth record for deletion: %w", err)
	}
	return data, true, nil
}

func (s *PostgresStore) readAuthRecordGenerationTx(ctx context.Context, tx *sql.Tx, relID string) ([]byte, string, bool, error) {
	query := fmt.Sprintf("SELECT content, auth_revision FROM %s WHERE id = $1 FOR UPDATE", s.fullTableName(s.cfg.AuthTable))
	var data []byte
	var revision string
	if err := tx.QueryRowContext(ctx, query, relID).Scan(&data, &revision); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, "", false, nil
		}
		return nil, "", false, fmt.Errorf("postgres store: inspect auth record generation: %w", err)
	}
	return data, strings.TrimSpace(revision), true, nil
}

func (s *PostgresStore) markRetiredAuthRecordIfCurrent(ctx context.Context, relID, path string, listedData []byte) (bool, error) {
	unlockPath := authfileguard.Lock(path)
	defer unlockPath()

	tx, errTx := s.beginAuthRecordMutation(ctx, relID)
	if errTx != nil {
		return false, errTx
	}
	defer rollbackAuthRecordMutation(tx)
	currentData, _, exists, errCurrent := s.readAuthRecordGenerationTx(ctx, tx, relID)
	if errCurrent != nil {
		return false, errCurrent
	}
	if !exists || !jsonEqual(listedData, currentData) || !cliproxyauth.IsRetiredGeminiCLIAuthFileData(currentData) {
		return false, nil
	}
	// Publish the in-process marker while the row generation remains locked.
	// A supported replacement must first delete this retired generation, which
	// clears the marker through the same serialized mutation path.
	marker, created := authfileguard.MarkRetired(path)
	if errCommit := tx.Commit(); errCommit != nil {
		if created {
			authfileguard.ClearRetiredSnapshot(marker)
		}
		return false, fmt.Errorf("postgres store: commit retired auth revalidation: %w", errCommit)
	}
	return true, nil
}

func (s *PostgresStore) persistConfig(ctx context.Context, data []byte) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (id, content, created_at, updated_at)
		VALUES ($1, $2, NOW(), NOW())
		ON CONFLICT (id)
		DO UPDATE SET content = EXCLUDED.content, updated_at = NOW()
	`, s.fullTableName(s.cfg.ConfigTable))
	normalized := normalizeLineEndings(string(data))
	if _, err := s.db.ExecContext(ctx, query, defaultConfigKey, normalized); err != nil {
		return fmt.Errorf("postgres store: upsert config: %w", err)
	}
	return nil
}

func (s *PostgresStore) deleteConfigRecord(ctx context.Context) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", s.fullTableName(s.cfg.ConfigTable))
	if _, err := s.db.ExecContext(ctx, query, defaultConfigKey); err != nil {
		return fmt.Errorf("postgres store: delete config: %w", err)
	}
	return nil
}

func (s *PostgresStore) resolveAuthPath(auth *cliproxyauth.Auth) (string, error) {
	if auth == nil {
		return "", fmt.Errorf("postgres store: auth is nil")
	}
	if auth.Attributes != nil {
		if p := strings.TrimSpace(auth.Attributes["path"]); p != "" {
			return p, nil
		}
	}
	if fileName := strings.TrimSpace(auth.FileName); fileName != "" {
		if filepath.IsAbs(fileName) {
			return fileName, nil
		}
		return filepath.Join(s.authDir, fileName), nil
	}
	if auth.ID == "" {
		return "", fmt.Errorf("postgres store: missing id")
	}
	if filepath.IsAbs(auth.ID) {
		return auth.ID, nil
	}
	return filepath.Join(s.authDir, filepath.FromSlash(auth.ID)), nil
}

func (s *PostgresStore) resolveDeletePath(id string) (string, error) {
	path := filepath.FromSlash(id)
	if filepath.IsAbs(path) {
		return filepath.Clean(path), nil
	}
	return filepath.Join(s.authDir, path), nil
}

func (s *PostgresStore) relativeAuthID(path string) (string, error) {
	if s == nil {
		return "", fmt.Errorf("postgres store: store not initialized")
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(s.authDir, path)
	}
	clean := filepath.Clean(path)
	rel, err := filepath.Rel(s.authDir, clean)
	if err != nil {
		return "", fmt.Errorf("postgres store: compute relative path: %w", err)
	}
	if strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("postgres store: path %s outside managed directory", path)
	}
	return filepath.ToSlash(rel), nil
}

func (s *PostgresStore) absoluteAuthPath(id string) (string, error) {
	if s == nil {
		return "", fmt.Errorf("postgres store: store not initialized")
	}
	clean := filepath.Clean(filepath.FromSlash(id))
	if strings.HasPrefix(clean, "..") {
		return "", fmt.Errorf("postgres store: invalid auth identifier %s", id)
	}
	path := filepath.Join(s.authDir, clean)
	rel, err := filepath.Rel(s.authDir, path)
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("postgres store: resolved auth path escapes auth directory")
	}
	return path, nil
}

func (s *PostgresStore) fullTableName(name string) string {
	if strings.TrimSpace(s.cfg.Schema) == "" {
		return quoteIdentifier(name)
	}
	return quoteIdentifier(s.cfg.Schema) + "." + quoteIdentifier(name)
}

func quoteIdentifier(identifier string) string {
	replaced := strings.ReplaceAll(identifier, "\"", "\"\"")
	return "\"" + replaced + "\""
}

func valueAsString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case fmt.Stringer:
		return t.String()
	default:
		return ""
	}
}

func labelFor(metadata map[string]any) string {
	if metadata == nil {
		return ""
	}
	if v := strings.TrimSpace(valueAsString(metadata["label"])); v != "" {
		return v
	}
	if v := strings.TrimSpace(valueAsString(metadata["email"])); v != "" {
		return v
	}
	if v := strings.TrimSpace(valueAsString(metadata["project_id"])); v != "" {
		return v
	}
	return ""
}

func normalizeAuthID(id string) string {
	return filepath.ToSlash(filepath.Clean(id))
}

func normalizeLineEndings(s string) string {
	if s == "" {
		return s
	}
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	return s
}

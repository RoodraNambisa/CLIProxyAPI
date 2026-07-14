package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	internalauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth"
	internalcodex "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/misc"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

const (
	objectStoreConfigKey       = "config/config.yaml"
	objectStoreAuthPrefix      = "auths"
	objectStoreWriteIDMetadata = "X-Amz-Meta-Cliproxy-Write-Id"
)

// ObjectStoreConfig captures configuration for the object storage-backed token store.
type ObjectStoreConfig struct {
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
	Region    string
	Prefix    string
	LocalRoot string
	UseSSL    bool
	PathStyle bool
}

// ObjectTokenStore persists configuration and authentication metadata using an S3-compatible object storage backend.
// Files are mirrored to a local workspace so existing file-based flows continue to operate.
type ObjectTokenStore struct {
	client     *minio.Client
	httpClient *http.Client
	cfg        ObjectStoreConfig
	spoolRoot  string
	configPath string
	authDir    string
	mu         sync.Mutex
}

type objectAuthWritePrecondition struct {
	etag         string
	versionID    string
	writeID      string
	lastModified time.Time
	exists       bool
}

// NewObjectTokenStore initializes an object storage backed token store.
func NewObjectTokenStore(cfg ObjectStoreConfig) (*ObjectTokenStore, error) {
	cfg.Endpoint = strings.TrimSpace(cfg.Endpoint)
	cfg.Bucket = strings.TrimSpace(cfg.Bucket)
	cfg.AccessKey = strings.TrimSpace(cfg.AccessKey)
	cfg.SecretKey = strings.TrimSpace(cfg.SecretKey)
	cfg.Prefix = strings.Trim(cfg.Prefix, "/")

	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("object store: endpoint is required")
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("object store: bucket is required")
	}
	if cfg.AccessKey == "" {
		return nil, fmt.Errorf("object store: access key is required")
	}
	if cfg.SecretKey == "" {
		return nil, fmt.Errorf("object store: secret key is required")
	}

	root := strings.TrimSpace(cfg.LocalRoot)
	if root == "" {
		if cwd, err := os.Getwd(); err == nil {
			root = filepath.Join(cwd, "objectstore")
		} else {
			root = filepath.Join(os.TempDir(), "objectstore")
		}
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("object store: resolve spool directory: %w", err)
	}

	configDir := filepath.Join(absRoot, "config")
	authDir := filepath.Join(absRoot, "auths")

	if err = os.MkdirAll(configDir, 0o700); err != nil {
		return nil, fmt.Errorf("object store: create config directory: %w", err)
	}
	if err = os.MkdirAll(authDir, 0o700); err != nil {
		return nil, fmt.Errorf("object store: create auth directory: %w", err)
	}

	transport, errTransport := minio.DefaultTransport(cfg.UseSSL)
	if errTransport != nil {
		return nil, fmt.Errorf("object store: create transport: %w", errTransport)
	}
	options := &minio.Options{
		Creds:     credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure:    cfg.UseSSL,
		Region:    cfg.Region,
		Transport: transport,
	}
	if cfg.PathStyle {
		options.BucketLookup = minio.BucketLookupPath
	}

	client, err := minio.New(cfg.Endpoint, options)
	if err != nil {
		return nil, fmt.Errorf("object store: create client: %w", err)
	}

	return &ObjectTokenStore{
		client:     client,
		httpClient: &http.Client{Transport: transport},
		cfg:        cfg,
		spoolRoot:  absRoot,
		configPath: filepath.Join(configDir, "config.yaml"),
		authDir:    authDir,
	}, nil
}

// SetBaseDir implements the optional interface used by authenticators; it is a no-op because
// the object store controls its own workspace.
func (s *ObjectTokenStore) SetBaseDir(string) {}

// ConfigPath returns the managed configuration file path inside the spool directory.
func (s *ObjectTokenStore) ConfigPath() string {
	if s == nil {
		return ""
	}
	return s.configPath
}

// AuthDir returns the local directory containing mirrored auth files.
func (s *ObjectTokenStore) AuthDir() string {
	if s == nil {
		return ""
	}
	return s.authDir
}

// Bootstrap ensures the target bucket exists and synchronizes data from the object storage backend.
func (s *ObjectTokenStore) Bootstrap(ctx context.Context, exampleConfigPath string) error {
	if s == nil {
		return fmt.Errorf("object store: not initialized")
	}
	if err := s.ensureBucket(ctx); err != nil {
		return err
	}
	if err := s.syncConfigFromBucket(ctx, exampleConfigPath); err != nil {
		return err
	}
	if err := s.syncAuthFromBucket(ctx); err != nil {
		return err
	}
	return nil
}

// Save persists authentication metadata to disk and uploads it to the object storage backend.
func (s *ObjectTokenStore) Save(ctx context.Context, auth *cliproxyauth.Auth) (string, error) {
	if auth == nil {
		return "", fmt.Errorf("object store: auth is nil")
	}
	if cliproxyauth.IsRetiredGeminiCLIAuth(auth) {
		cliproxyauth.WarnRetiredGeminiCLIAuthIgnored()
		return "", fmt.Errorf("object store: %w", cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly)
	}

	path, err := s.resolveAuthPath(auth)
	if err != nil {
		return "", err
	}
	if path == "" {
		return "", fmt.Errorf("object store: missing file path attribute for %s", auth.ID)
	}
	relativePath, errRelative := s.relativeAuthPath(path)
	if errRelative != nil {
		return "", errRelative
	}
	path = filepath.Join(s.authDir, filepath.FromSlash(relativePath))
	root, errRoot := os.OpenRoot(s.authDir)
	if errRoot != nil {
		return "", fmt.Errorf("object store: open auth root: %w", errRoot)
	}
	defer func() {
		if errClose := root.Close(); errClose != nil {
			log.WithError(errClose).Error("object store: close auth root after save")
		}
	}()

	if auth.Disabled {
		if _, statErr := root.Lstat(filepath.FromSlash(relativePath)); errors.Is(statErr, fs.ErrNotExist) {
			return "", nil
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	unlockPath := authfileguard.Lock(path)
	defer unlockPath()
	if authfileguard.IsRetired(path) {
		return "", fmt.Errorf("object store: %w", cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly)
	}
	if authfileguard.IsQuarantined(path) {
		return "", fmt.Errorf("object store: auth deletion is still pending: %w", authfileguard.ErrDeleteGenerationUncertain)
	}

	remoteState, errRemote := s.authObjectWritePrecondition(ctx, path)
	if errRemote != nil {
		if errors.Is(errRemote, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
			authfileguard.MarkRetired(path)
		}
		return "", errRemote
	}

	if err = mkdirAuthDirectoriesAtRoot(root, filepath.Dir(filepath.FromSlash(relativePath)), 0o700); err != nil {
		return "", fmt.Errorf("object store: create auth directory: %w", err)
	}
	localSnapshot, errSnapshot := captureAuthFileSnapshot(root, filepath.FromSlash(relativePath))
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
			return "", fmt.Errorf("object store: produce storage auth: %w", errData)
		}
		if errWrite := writeAuthFileAtomicallyAtRoot(root, filepath.FromSlash(relativePath), data, &localSnapshot); errWrite != nil {
			if errors.Is(errWrite, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
				authfileguard.MarkRetired(path)
			}
			return "", fmt.Errorf("object store: persist storage auth: %w", errWrite)
		}
		persistedData = data
		if errSync := cliproxyauth.SyncPersistedMetadataAndSourceHash(auth, data); errSync != nil {
			return "", fmt.Errorf("object store: sync persisted storage auth: %w", errSync)
		}
	case auth.Metadata != nil:
		raw, errMarshal := cliproxyauth.CanonicalMetadataBytes(auth)
		if errMarshal != nil {
			return "", fmt.Errorf("object store: canonicalize metadata: %w", errMarshal)
		}
		writeLocal := true
		persistedData = raw
		if existing, errRead := root.ReadFile(filepath.FromSlash(relativePath)); errRead == nil {
			if jsonEqual(existing, raw) {
				if !localSnapshot.exists || !bytes.Equal(existing, localSnapshot.data) {
					return "", authfileguard.ErrPersistGenerationStale
				}
				writeLocal = false
				persistedData = localSnapshot.data
			}
		} else if errRead != nil && !errors.Is(errRead, fs.ErrNotExist) {
			return "", fmt.Errorf("object store: read existing metadata: %w", errRead)
		}
		if writeLocal {
			if errWrite := writeAuthFileAtomicallyAtRoot(root, filepath.FromSlash(relativePath), raw, &localSnapshot); errWrite != nil {
				if errors.Is(errWrite, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
					authfileguard.MarkRetired(path)
				}
				return "", fmt.Errorf("object store: persist auth file: %w", errWrite)
			}
		}
		cliproxyauth.SetSourceHashAttribute(auth, raw)
	default:
		return "", fmt.Errorf("object store: nothing to persist for %s", auth.ID)
	}

	if auth.Attributes == nil {
		auth.Attributes = make(map[string]string)
	}
	auth.Attributes["path"] = path

	if strings.TrimSpace(auth.FileName) == "" {
		auth.FileName = auth.ID
	}

	if err = s.uploadAuthData(ctx, path, persistedData, &remoteState); err != nil {
		errRollback := restoreAuthFileSnapshotAtRoot(root, filepath.FromSlash(relativePath), persistedData, localSnapshot)
		runtimeSnapshot.restore(auth)
		if errRollback != nil {
			return "", errors.Join(err, fmt.Errorf("object store: roll back local auth after upload failure: %w", errRollback))
		}
		return "", err
	}
	return path, nil
}

// List enumerates auth JSON files from the mirrored workspace.
func (s *ObjectTokenStore) List(_ context.Context) ([]*cliproxyauth.Auth, error) {
	dir := strings.TrimSpace(s.AuthDir())
	if dir == "" {
		return nil, fmt.Errorf("object store: auth directory not configured")
	}
	entries := make([]*cliproxyauth.Auth, 0, 32)
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(strings.ToLower(d.Name()), ".json") {
			return nil
		}
		auth, err := s.readAuthFile(path, dir)
		if err != nil {
			log.WithError(err).Warnf("object store: skip auth %s", path)
			return nil
		}
		if auth != nil {
			entries = append(entries, auth)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("object store: walk auth directory: %w", err)
	}
	return entries, nil
}

// Delete removes an auth file locally and remotely.
func (s *ObjectTokenStore) Delete(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("object store: id is empty"))
	}
	path, errResolve := s.resolveDeletePath(id)
	if errResolve != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errResolve)
	}
	relativePath, errRel := s.relativeAuthPath(path)
	if errRel != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errRel)
	}
	root, errRoot := os.OpenRoot(s.authDir)
	if errRoot != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("object store: open auth root: %w", errRoot))
	}
	defer func() {
		if errClose := root.Close(); errClose != nil {
			log.WithError(errClose).Error("object store: close auth root after deletion")
		}
	}()
	return s.deleteAuthFileAtRoot(ctx, root, path, relativePath)
}

// FinalizeAuthFileDeletion removes the object-store copy after a caller has
// already removed the local mirror file through a stable filesystem root.
func (s *ObjectTokenStore) FinalizeAuthFileDeletion(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return fmt.Errorf("object store: id is empty")
	}
	path, errResolve := s.resolveDeletePath(id)
	if errResolve != nil {
		return errResolve
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlockPath := authfileguard.Lock(path)
	defer unlockPath()
	retiredSnapshot := authfileguard.CaptureRetired(path)
	remoteState, data, errInspect := s.inspectAuthObject(ctx, path)
	if errInspect != nil {
		return errInspect
	}
	if !remoteState.exists || !cliproxyauth.IsRetiredGeminiCLIAuthFileData(data) {
		authfileguard.ClearRetiredSnapshot(retiredSnapshot)
		return nil
	}
	relativePath, errRelative := s.relativeAuthPath(path)
	if errRelative != nil {
		return errRelative
	}
	switch matchExpectedAuthDeleteGeneration(ctx, "object:"+filepath.ToSlash(relativePath), objectAuthWriteIdentity(remoteState), stableObjectDeleteIdentity(remoteState), data) {
	case authDeleteGenerationUncertain, authDeleteGenerationReplaced:
		return authfileguard.ErrDeleteGenerationUncertain
	}
	if errDelete := s.deleteAuthObjectConditionally(ctx, path, remoteState); errDelete != nil {
		if deleteOutcomeIsCommitted(errDelete) {
			authfileguard.ClearRetiredSnapshot(retiredSnapshot)
			return nil
		}
		state, errProbe := s.authObjectDeleteState(ctx, path, remoteState)
		if errProbe == nil && state == authDeleteProbeAbsent {
			authfileguard.ClearRetiredSnapshot(retiredSnapshot)
			return nil
		}
		return errors.Join(errDelete, wrapOptionalError("object store: verify retired auth deletion", errProbe))
	}
	authfileguard.ClearRetiredSnapshot(retiredSnapshot)
	return nil
}

// DeleteAuthFileAtRoot removes the local mirror and object-store copy while
// serializing against concurrent saves.
func (s *ObjectTokenStore) DeleteAuthFileAtRoot(ctx context.Context, root *os.Root, id string) error {
	if root == nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("object store: root is nil"))
	}
	path, errResolve := s.resolveDeletePath(id)
	if errResolve != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errResolve)
	}
	relativePath, errRel := s.relativeAuthPath(path)
	if errRel != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errRel)
	}
	return s.deleteAuthFileAtRoot(ctx, root, path, relativePath)
}

func (s *ObjectTokenStore) deleteAuthFileAtRoot(ctx context.Context, root *os.Root, path, relativePath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	unlockPath := authfileguard.Lock(path)
	defer unlockPath()
	retiredSnapshot := authfileguard.CaptureRetired(path)
	remoteState, remoteData, errInspect := s.inspectAuthObject(ctx, path)
	if errInspect != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errInspect)
	}
	localSnapshot, errLocalSnapshot := captureAuthFileSnapshot(root, relativePath)
	if errLocalSnapshot != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errLocalSnapshot)
	}
	deleteCtx, prepareDelete, clearDelete := durableAuthDelete(
		ctx,
		s.configPath,
		s.authDir,
		path,
		localSnapshot.data,
		"object:"+filepath.ToSlash(relativePath),
		objectAuthWriteIdentity(remoteState),
		stableObjectDeleteIdentity(remoteState),
		remoteState.exists,
		remoteData,
	)
	errDelete := deleteAuthFileTransaction(
		root,
		relativePath,
		func(original authFileSnapshot) error {
			if !sameAuthFileGeneration(original, localSnapshot) {
				return authfileguard.ErrPersistGenerationStale
			}
			return prepareDelete()
		},
		func() error { return s.deleteAuthObjectConditionally(deleteCtx, path, remoteState) },
		func() (authDeleteProbeState, error) {
			return probeAuthDeleteResult(deleteCtx, func(probeCtx context.Context) (authDeleteProbeState, error) {
				return s.authObjectDeleteState(probeCtx, path, remoteState)
			})
		},
	)
	errDelete = finishDurableAuthDelete(errDelete, clearDelete)
	if deleteOutcomeIsCommitted(errDelete) {
		authfileguard.ClearRetiredSnapshot(retiredSnapshot)
	}
	return errDelete
}

// PersistAuthFiles uploads the provided auth files to the object storage backend.
func (s *ObjectTokenStore) PersistAuthFiles(ctx context.Context, _ string, paths ...string) error {
	if len(paths) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	cleanAuthDir := filepath.Clean(s.authDir)
	root, errRoot := os.OpenRoot(filepath.Dir(cleanAuthDir))
	if errRoot != nil {
		return fmt.Errorf("object store: open auth parent root for persistence: %w", errRoot)
	}
	defer func() {
		if errClose := root.Close(); errClose != nil {
			log.WithError(errClose).Error("object store: close auth root after persistence")
		}
	}()

	for _, p := range paths {
		trimmed := strings.TrimSpace(p)
		if trimmed == "" {
			continue
		}
		abs := trimmed
		if !filepath.IsAbs(abs) {
			abs = filepath.Join(s.authDir, trimmed)
		}
		relativePath, errRel := s.relativeAuthPath(abs)
		if errRel != nil {
			return errRel
		}
		unlockPath := authfileguard.Lock(abs)
		remoteState, remoteData, errRemote := s.inspectAuthObject(ctx, abs)
		if errRemote != nil {
			unlockPath()
			return errRemote
		}
		snapshotPath := filepath.Join(filepath.Base(cleanAuthDir), relativePath)
		snapshot, errSnapshot := captureAuthFileSnapshot(root, snapshotPath)
		if errSnapshot == nil {
			errSnapshot = authfileguard.ValidatePersistSnapshot(ctx, snapshot.data, snapshot.exists)
		}
		if errSnapshot == nil {
			errSnapshot = snapshot.rejectRetiredGeminiCLIAuthPersistence()
		}
		if errors.Is(errSnapshot, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
			authfileguard.MarkRetired(abs)
			if remoteState.exists && !cliproxyauth.IsRetiredGeminiCLIAuthFileData(remoteData) {
				errSnapshot = removeAuthFileAtRoot(root, snapshotPath)
				unlockPath()
				if errSnapshot != nil {
					return errSnapshot
				}
				continue
			}
		}
		if errSnapshot != nil {
			unlockPath()
			return errSnapshot
		}

		var errPersist error
		if snapshot.exists {
			if errRetired := cliproxyauth.RejectRetiredGeminiCLIAuthFileMutation(remoteData); errRetired != nil {
				if errors.Is(errRetired, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
					authfileguard.MarkRetired(abs)
				}
				errPersist = fmt.Errorf("object store: %w", errRetired)
			} else {
				errPersist = s.uploadAuthData(ctx, abs, snapshot.data, &remoteState)
			}
		} else {
			if !remoteState.exists && authfileguard.DeleteGenerationFromContext(ctx) != nil {
				errPersist = nil
			} else {
				switch matchExpectedAuthDeleteGeneration(ctx, "object:"+filepath.ToSlash(relativePath), objectAuthWriteIdentity(remoteState), stableObjectDeleteIdentity(remoteState), remoteData) {
				case authDeleteGenerationMatched:
					errPersist = s.uploadAuthData(ctx, abs, nil, &remoteState)
				case authDeleteGenerationUncertain:
					errPersist = authfileguard.ErrDeleteGenerationUncertain
				case authDeleteGenerationReplaced:
					if remoteState.exists {
						errPersist = writeAuthFileAtomicallyAtRoot(root, snapshotPath, remoteData, &snapshot)
					}
				}
			}
		}
		unlockPath()
		if errPersist != nil && deleteOutcomeIsCommitted(errPersist) {
			errPersist = nil
		}
		if errPersist != nil {
			return errPersist
		}
	}
	return nil
}

func stableObjectVersionID(versionID string) bool {
	versionID = strings.TrimSpace(versionID)
	return versionID != "" && !strings.EqualFold(versionID, "null")
}

func stableObjectDeleteIdentity(state objectAuthWritePrecondition) bool {
	return !state.exists || stableObjectVersionID(state.versionID)
}

// PersistConfig uploads the local configuration file to the object storage backend.
func (s *ObjectTokenStore) PersistConfig(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.configPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return s.deleteObject(ctx, objectStoreConfigKey)
		}
		return fmt.Errorf("object store: read config file: %w", err)
	}
	if len(data) == 0 {
		return s.deleteObject(ctx, objectStoreConfigKey)
	}
	return s.putObject(ctx, objectStoreConfigKey, data, "application/x-yaml")
}

func (s *ObjectTokenStore) ensureBucket(ctx context.Context) error {
	exists, err := s.client.BucketExists(ctx, s.cfg.Bucket)
	if err != nil {
		return fmt.Errorf("object store: check bucket: %w", err)
	}
	if exists {
		return nil
	}
	if err = s.client.MakeBucket(ctx, s.cfg.Bucket, minio.MakeBucketOptions{Region: s.cfg.Region}); err != nil {
		return fmt.Errorf("object store: create bucket: %w", err)
	}
	return nil
}

func (s *ObjectTokenStore) syncConfigFromBucket(ctx context.Context, example string) error {
	key := s.prefixedKey(objectStoreConfigKey)
	_, err := s.client.StatObject(ctx, s.cfg.Bucket, key, minio.StatObjectOptions{})
	switch {
	case err == nil:
		object, errGet := s.client.GetObject(ctx, s.cfg.Bucket, key, minio.GetObjectOptions{})
		if errGet != nil {
			return fmt.Errorf("object store: fetch config: %w", errGet)
		}
		defer object.Close()
		data, errRead := io.ReadAll(object)
		if errRead != nil {
			return fmt.Errorf("object store: read config: %w", errRead)
		}
		if errWrite := os.WriteFile(s.configPath, normalizeLineEndingsBytes(data), 0o600); errWrite != nil {
			return fmt.Errorf("object store: write config: %w", errWrite)
		}
	case isObjectNotFound(err):
		if _, statErr := os.Stat(s.configPath); errors.Is(statErr, fs.ErrNotExist) {
			if example != "" {
				if errCopy := misc.CopyConfigTemplate(example, s.configPath); errCopy != nil {
					return fmt.Errorf("object store: copy example config: %w", errCopy)
				}
			} else {
				if errCreate := os.MkdirAll(filepath.Dir(s.configPath), 0o700); errCreate != nil {
					return fmt.Errorf("object store: prepare config directory: %w", errCreate)
				}
				if errWrite := os.WriteFile(s.configPath, []byte{}, 0o600); errWrite != nil {
					return fmt.Errorf("object store: create empty config: %w", errWrite)
				}
			}
		}
		data, errRead := os.ReadFile(s.configPath)
		if errRead != nil {
			return fmt.Errorf("object store: read local config: %w", errRead)
		}
		if len(data) > 0 {
			if errPut := s.putObject(ctx, objectStoreConfigKey, data, "application/x-yaml"); errPut != nil {
				return errPut
			}
		}
	default:
		return fmt.Errorf("object store: stat config: %w", err)
	}
	return nil
}

func (s *ObjectTokenStore) syncAuthFromBucket(ctx context.Context) error {
	// NOTE: We intentionally do NOT use os.RemoveAll here.
	// Wiping the directory triggers file watcher delete events, which then
	// propagate deletions to the remote object store (race condition).
	// Instead, we just ensure the directory exists and overwrite files incrementally.
	if err := os.MkdirAll(s.authDir, 0o700); err != nil {
		return fmt.Errorf("object store: create auth directory: %w", err)
	}
	root, errRoot := os.OpenRoot(s.authDir)
	if errRoot != nil {
		return fmt.Errorf("object store: open auth mirror root: %w", errRoot)
	}
	defer func() {
		if errClose := root.Close(); errClose != nil {
			log.WithError(errClose).Error("object store: close auth mirror root after sync")
		}
	}()

	prefix := s.prefixedKey(objectStoreAuthPrefix + "/")
	objectCh := s.client.ListObjects(ctx, s.cfg.Bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})
	for object := range objectCh {
		if object.Err != nil {
			return fmt.Errorf("object store: list auth objects: %w", object.Err)
		}
		rel := strings.TrimPrefix(object.Key, prefix)
		if rel == "" || strings.HasSuffix(rel, "/") {
			continue
		}
		relPath := filepath.FromSlash(rel)
		if filepath.IsAbs(relPath) {
			log.WithField("key", object.Key).Warn("object store: skip auth outside mirror")
			continue
		}
		cleanRel := filepath.Clean(relPath)
		if cleanRel == "." || cleanRel == ".." || strings.HasPrefix(cleanRel, ".."+string(os.PathSeparator)) {
			log.WithField("key", object.Key).Warn("object store: skip auth outside mirror")
			continue
		}
		if err := mkdirAuthDirectoriesAtRoot(root, filepath.Dir(cleanRel), 0o700); err != nil {
			return fmt.Errorf("object store: prepare auth subdir: %w", err)
		}
		reader, errGet := s.client.GetObject(ctx, s.cfg.Bucket, object.Key, minio.GetObjectOptions{})
		if errGet != nil {
			return fmt.Errorf("object store: download auth %s: %w", object.Key, errGet)
		}
		data, errRead := io.ReadAll(reader)
		_ = reader.Close()
		if errRead != nil {
			return fmt.Errorf("object store: read auth %s: %w", object.Key, errRead)
		}
		localSnapshot, errSnapshot := captureAuthFileSnapshot(root, cleanRel)
		if errSnapshot != nil {
			return fmt.Errorf("object store: inspect auth %s before sync: %w", cleanRel, errSnapshot)
		}
		if errWrite := writeAuthMirrorFileAtomicallyAtRoot(root, cleanRel, data, &localSnapshot); errWrite != nil {
			return fmt.Errorf("object store: write auth %s: %w", cleanRel, errWrite)
		}
	}
	return nil
}

func (s *ObjectTokenStore) uploadAuth(ctx context.Context, path string, remoteState *objectAuthWritePrecondition) error {
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return s.uploadAuthData(ctx, path, nil, remoteState)
		}
		return fmt.Errorf("object store: read auth file: %w", err)
	}
	return s.uploadAuthData(ctx, path, data, remoteState)
}

func (s *ObjectTokenStore) uploadAuthData(ctx context.Context, path string, data []byte, remoteState *objectAuthWritePrecondition) error {
	if path == "" {
		return nil
	}
	if data == nil {
		if remoteState != nil {
			return s.deleteAuthObjectConditionally(ctx, path, *remoteState)
		}
		return s.deleteAuthObject(ctx, path)
	}
	rel, err := filepath.Rel(s.authDir, path)
	if err != nil {
		return fmt.Errorf("object store: resolve auth relative path: %w", err)
	}
	key := objectStoreAuthPrefix + "/" + filepath.ToSlash(rel)
	if remoteState != nil {
		return s.putAuthObjectConditionally(ctx, key, data, "application/json", *remoteState)
	}
	fullKey := s.prefixedKey(key)
	reader := bytes.NewReader(data)
	if _, errPut := s.client.PutObject(ctx, s.cfg.Bucket, fullKey, reader, int64(len(data)), minio.PutObjectOptions{ContentType: "application/json"}); errPut != nil {
		return fmt.Errorf("object store: put auth object %s: %w", fullKey, errPut)
	}
	return nil
}

func (s *ObjectTokenStore) deleteAuthObject(ctx context.Context, path string) error {
	if path == "" {
		return nil
	}
	rel, err := filepath.Rel(s.authDir, path)
	if err != nil {
		return fmt.Errorf("object store: resolve auth relative path: %w", err)
	}
	key := objectStoreAuthPrefix + "/" + filepath.ToSlash(rel)
	return s.deleteObject(ctx, key)
}

func (s *ObjectTokenStore) authObjectDeleteState(ctx context.Context, path string, original objectAuthWritePrecondition) (authDeleteProbeState, error) {
	rel, err := filepath.Rel(s.authDir, path)
	if err != nil {
		return authDeleteProbeOriginal, fmt.Errorf("object store: resolve auth relative path: %w", err)
	}
	key := s.prefixedKey(objectStoreAuthPrefix + "/" + filepath.ToSlash(rel))
	info, errStat := s.client.StatObject(ctx, s.cfg.Bucket, key, minio.StatObjectOptions{})
	if errStat != nil {
		if isObjectNotFound(errStat) {
			return authDeleteProbeAbsent, nil
		}
		return authDeleteProbeOriginal, fmt.Errorf("object store: verify auth object %s: %w", key, errStat)
	}
	current := objectAuthWritePrecondition{
		etag:         strings.Trim(strings.TrimSpace(info.ETag), "\""),
		versionID:    strings.TrimSpace(info.VersionID),
		writeID:      objectAuthWriteID(info),
		lastModified: info.LastModified,
		exists:       true,
	}
	if original.exists && objectAuthWriteIdentity(current) == objectAuthWriteIdentity(original) {
		return authDeleteProbeOriginal, nil
	}
	return authDeleteProbeReplaced, nil
}

func (s *ObjectTokenStore) authObjectWritePrecondition(ctx context.Context, path string) (objectAuthWritePrecondition, error) {
	state, data, errInspect := s.inspectAuthObject(ctx, path)
	if errInspect != nil {
		return objectAuthWritePrecondition{}, errInspect
	}
	if errRetired := cliproxyauth.RejectRetiredGeminiCLIAuthFileMutation(data); errRetired != nil {
		return objectAuthWritePrecondition{}, fmt.Errorf("object store: %w", errRetired)
	}
	return state, nil
}

func (s *ObjectTokenStore) inspectAuthObject(ctx context.Context, path string) (objectAuthWritePrecondition, []byte, error) {
	rel, errRel := filepath.Rel(s.authDir, path)
	if errRel != nil {
		return objectAuthWritePrecondition{}, nil, fmt.Errorf("object store: resolve auth relative path: %w", errRel)
	}
	key := s.prefixedKey(objectStoreAuthPrefix + "/" + filepath.ToSlash(rel))
	info, errStat := s.client.StatObject(ctx, s.cfg.Bucket, key, minio.StatObjectOptions{})
	if errStat != nil {
		if isExplicitObjectNotFound(errStat) {
			return objectAuthWritePrecondition{}, nil, nil
		}
		return objectAuthWritePrecondition{}, nil, fmt.Errorf("object store: stat existing auth object %s: %w", key, errStat)
	}
	etag := strings.Trim(strings.TrimSpace(info.ETag), "\"")
	if etag == "" {
		return objectAuthWritePrecondition{}, nil, fmt.Errorf("object store: inspected auth object %s has no ETag", key)
	}
	getOptions := minio.GetObjectOptions{}
	if errMatch := getOptions.SetMatchETag(etag); errMatch != nil {
		return objectAuthWritePrecondition{}, nil, fmt.Errorf("object store: bind auth object inspection %s: %w", key, errMatch)
	}
	object, errGet := s.client.GetObject(ctx, s.cfg.Bucket, key, getOptions)
	if errGet != nil {
		return objectAuthWritePrecondition{}, nil, fmt.Errorf("object store: inspect existing auth object %s: %w", key, errGet)
	}
	data, errRead := io.ReadAll(object)
	errClose := object.Close()
	if errRead != nil {
		return objectAuthWritePrecondition{}, nil, fmt.Errorf("object store: read existing auth object %s: %w", key, errRead)
	}
	if errClose != nil {
		return objectAuthWritePrecondition{}, nil, fmt.Errorf("object store: close inspected auth object %s: %w", key, errClose)
	}
	return objectAuthWritePrecondition{
		etag:         etag,
		versionID:    strings.TrimSpace(info.VersionID),
		writeID:      objectAuthWriteID(info),
		lastModified: info.LastModified,
		exists:       true,
	}, data, nil
}

func objectAuthWriteIdentity(state objectAuthWritePrecondition) string {
	if !state.exists {
		return "missing"
	}
	identity := fmt.Sprintf("%s\x00%s\x00%d", state.etag, state.versionID, state.lastModified.UnixNano())
	if writeID := strings.TrimSpace(state.writeID); writeID != "" {
		identity += "\x00" + writeID
	}
	return identity
}

func (s *ObjectTokenStore) putAuthObjectConditionally(ctx context.Context, key string, data []byte, contentType string, remoteState objectAuthWritePrecondition) error {
	fullKey := s.prefixedKey(key)
	writeID := uuid.NewString()
	var errPut error
	if !remoteState.exists {
		errPut = s.putObjectIfAbsent(ctx, fullKey, data, contentType, writeID)
	} else {
		if strings.TrimSpace(remoteState.etag) == "" {
			return fmt.Errorf("object store: missing ETag precondition for %s", fullKey)
		}
		options := minio.PutObjectOptions{
			ContentType:      contentType,
			DisableMultipart: true,
			UserMetadata:     map[string]string{objectStoreWriteIDMetadata: writeID},
		}
		options.SetMatchETag(remoteState.etag)
		reader := bytes.NewReader(data)
		if _, errUpload := s.client.PutObject(ctx, s.cfg.Bucket, fullKey, reader, int64(len(data)), options); errUpload != nil {
			errPut = fmt.Errorf("object store: conditional put object %s: %w", fullKey, errUpload)
		}
	}
	if errPut == nil {
		return nil
	}
	committed, errProbe := s.authObjectWriteCommitted(ctx, fullKey, data, writeID)
	if errProbe == nil && committed {
		return nil
	}
	return errors.Join(errPut, wrapOptionalError("object store: verify auth write after failure", errProbe))
}

func (s *ObjectTokenStore) putObjectIfAbsent(ctx context.Context, fullKey string, data []byte, contentType, writeID string) error {
	headers := make(http.Header)
	headers.Set("If-None-Match", "*")
	headers.Set(objectStoreWriteIDMetadata, writeID)
	presignedURL, errPresign := s.client.PresignHeader(ctx, http.MethodPut, s.cfg.Bucket, fullKey, time.Minute, nil, headers)
	if errPresign != nil {
		return fmt.Errorf("object store: prepare conditional create for %s: %w", fullKey, errPresign)
	}
	request, errRequest := http.NewRequestWithContext(ctx, http.MethodPut, presignedURL.String(), bytes.NewReader(data))
	if errRequest != nil {
		return fmt.Errorf("object store: create conditional request for %s: %w", fullKey, errRequest)
	}
	request.Header.Set("Content-Type", contentType)
	request.Header.Set("If-None-Match", "*")
	request.Header.Set(objectStoreWriteIDMetadata, writeID)
	response, errDo := s.conditionalHTTPClient().Do(request)
	if errDo != nil {
		return fmt.Errorf("object store: conditional create object %s: %w", fullKey, sanitizeObjectStoreRequestError(errDo))
	}
	_, errDrain := io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
	errClose := response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("object store: conditional create object %s: status %s", fullKey, response.Status)
	}
	if errDrain != nil || errClose != nil {
		return fmt.Errorf("object store: finish conditional create object %s: %w", fullKey, errors.Join(errDrain, errClose))
	}
	return nil
}

func (s *ObjectTokenStore) authObjectWriteCommitted(ctx context.Context, fullKey string, data []byte, writeID string) (bool, error) {
	info, errStat := s.client.StatObject(ctx, s.cfg.Bucket, fullKey, minio.StatObjectOptions{})
	if errStat != nil {
		if isObjectNotFound(errStat) {
			return false, nil
		}
		return false, fmt.Errorf("stat auth object %s: %w", fullKey, errStat)
	}
	if objectAuthWriteID(info) != strings.TrimSpace(writeID) {
		return false, nil
	}
	etag := strings.Trim(strings.TrimSpace(info.ETag), `"`)
	if etag == "" {
		return false, fmt.Errorf("auth object %s has no ETag", fullKey)
	}
	getOptions := minio.GetObjectOptions{}
	if errMatch := getOptions.SetMatchETag(etag); errMatch != nil {
		return false, fmt.Errorf("bind auth object verification %s: %w", fullKey, errMatch)
	}
	object, errGet := s.client.GetObject(ctx, s.cfg.Bucket, fullKey, getOptions)
	if errGet != nil {
		return false, fmt.Errorf("get auth object %s: %w", fullKey, errGet)
	}
	remoteData, errRead := io.ReadAll(object)
	errClose := object.Close()
	if errRead != nil || errClose != nil {
		return false, errors.Join(errRead, errClose)
	}
	return bytes.Equal(remoteData, data), nil
}

func objectAuthWriteID(info minio.ObjectInfo) string {
	if info.Metadata != nil {
		if writeID := strings.TrimSpace(info.Metadata.Get(objectStoreWriteIDMetadata)); writeID != "" {
			return writeID
		}
	}
	for key, value := range info.UserMetadata {
		if strings.EqualFold(strings.TrimSpace(key), "Cliproxy-Write-Id") {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func (s *ObjectTokenStore) deleteAuthObjectConditionally(ctx context.Context, path string, remoteState objectAuthWritePrecondition) error {
	if !remoteState.exists {
		return nil
	}
	if strings.TrimSpace(remoteState.etag) == "" {
		return fmt.Errorf("object store: missing ETag precondition for auth deletion")
	}
	rel, errRel := filepath.Rel(s.authDir, path)
	if errRel != nil {
		return fmt.Errorf("object store: resolve auth relative path: %w", errRel)
	}
	fullKey := s.prefixedKey(objectStoreAuthPrefix + "/" + filepath.ToSlash(rel))
	if !stableObjectVersionID(remoteState.versionID) {
		if errRemove := s.deleteAuthObjectIfMatch(ctx, fullKey, remoteState.etag); errRemove != nil {
			state, errProbe := s.authObjectDeleteState(ctx, path, remoteState)
			if errProbe == nil && state == authDeleteProbeAbsent {
				return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeCommitted, errRemove)
			}
			return errors.Join(errRemove, wrapOptionalError("object store: verify conditional auth deletion", errProbe))
		}
		state, errProbe := s.authObjectDeleteState(ctx, path, remoteState)
		if errProbe != nil {
			return errProbe
		}
		if state == authDeleteProbeAbsent {
			return nil
		}
		return fmt.Errorf("object store: auth object generation changed while deleting %s", fullKey)
	}
	versions, errVersions := s.authObjectVersionsForDeletion(ctx, fullKey, remoteState.versionID)
	if errVersions != nil {
		return errVersions
	}
	var acknowledgementErrors []error
	if errRemove := s.deleteAuthObjectVersion(ctx, fullKey, remoteState.versionID); errRemove != nil {
		if !deleteOutcomeIsCommitted(errRemove) {
			return fmt.Errorf("object store: delete current auth object version %s: %w", fullKey, errRemove)
		}
		acknowledgementErrors = append(acknowledgementErrors, errRemove)
	}
	current, errCurrent := s.authObjectCurrentState(ctx, fullKey)
	if errCurrent != nil {
		return errCurrent
	}
	if current.exists && !objectVersionIDInSet(current.versionID, versions, remoteState.versionID) {
		return fmt.Errorf("object store: auth object generation changed while deleting %s", fullKey)
	}
	for _, versionID := range versions {
		if versionID == remoteState.versionID {
			continue
		}
		if errRemove := s.deleteAuthObjectVersion(ctx, fullKey, versionID); errRemove != nil {
			if !deleteOutcomeIsCommitted(errRemove) {
				return fmt.Errorf("object store: delete historical auth object version %s: %w", fullKey, errRemove)
			}
			acknowledgementErrors = append(acknowledgementErrors, errRemove)
		}
	}
	state, errProbe := s.authObjectDeleteState(ctx, path, remoteState)
	if errProbe != nil {
		return errProbe
	}
	if state == authDeleteProbeAbsent {
		if len(acknowledgementErrors) > 0 {
			return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeCommitted, errors.Join(acknowledgementErrors...))
		}
		return nil
	}
	return fmt.Errorf("object store: auth object generation changed while deleting %s", fullKey)
}

func (s *ObjectTokenStore) deleteAuthObjectVersion(ctx context.Context, fullKey, versionID string) error {
	errRemove := s.client.RemoveObject(ctx, s.cfg.Bucket, fullKey, minio.RemoveObjectOptions{VersionID: versionID})
	if errRemove == nil {
		return nil
	}
	exists, errProbe := s.authObjectVersionExists(ctx, fullKey, versionID)
	if errProbe != nil {
		return errors.Join(errRemove, fmt.Errorf("verify deleted auth object version: %w", errProbe))
	}
	if exists {
		return errRemove
	}
	return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeCommitted, errRemove)
}

func (s *ObjectTokenStore) authObjectVersionExists(ctx context.Context, fullKey, versionID string) (bool, error) {
	objects := s.client.ListObjects(ctx, s.cfg.Bucket, minio.ListObjectsOptions{
		Prefix:       fullKey,
		Recursive:    true,
		WithVersions: true,
	})
	for object := range objects {
		if object.Err != nil {
			return false, object.Err
		}
		if object.Key == fullKey && strings.TrimSpace(object.VersionID) == strings.TrimSpace(versionID) {
			return true, nil
		}
	}
	return false, nil
}

func (s *ObjectTokenStore) deleteAuthObjectIfMatch(ctx context.Context, fullKey, etag string) error {
	headers := make(http.Header)
	headers.Set("If-Match", fmt.Sprintf(`"%s"`, strings.Trim(strings.TrimSpace(etag), `"`)))
	presignedURL, errPresign := s.client.PresignHeader(ctx, http.MethodDelete, s.cfg.Bucket, fullKey, time.Minute, nil, headers)
	if errPresign != nil {
		return fmt.Errorf("object store: prepare conditional delete for %s: %w", fullKey, errPresign)
	}
	request, errRequest := http.NewRequestWithContext(ctx, http.MethodDelete, presignedURL.String(), nil)
	if errRequest != nil {
		return fmt.Errorf("object store: create conditional delete request for %s: %w", fullKey, errRequest)
	}
	request.Header.Set("If-Match", headers.Get("If-Match"))
	response, errDo := s.conditionalHTTPClient().Do(request)
	if errDo != nil {
		return fmt.Errorf("object store: conditional delete object %s: %w", fullKey, sanitizeObjectStoreRequestError(errDo))
	}
	_, errDrain := io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
	errClose := response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("object store: conditional delete object %s: status %s", fullKey, response.Status)
	}
	if errDrain != nil || errClose != nil {
		return fmt.Errorf("object store: finish conditional delete object %s: %w", fullKey, errors.Join(errDrain, errClose))
	}
	return nil
}

func (s *ObjectTokenStore) authObjectCurrentState(ctx context.Context, fullKey string) (objectAuthWritePrecondition, error) {
	info, errStat := s.client.StatObject(ctx, s.cfg.Bucket, fullKey, minio.StatObjectOptions{})
	if errStat != nil {
		if isObjectNotFound(errStat) {
			return objectAuthWritePrecondition{}, nil
		}
		return objectAuthWritePrecondition{}, fmt.Errorf("object store: inspect auth object after version deletion %s: %w", fullKey, errStat)
	}
	return objectAuthWritePrecondition{
		etag:         strings.Trim(strings.TrimSpace(info.ETag), `"`),
		versionID:    strings.TrimSpace(info.VersionID),
		writeID:      objectAuthWriteID(info),
		lastModified: info.LastModified,
		exists:       true,
	}, nil
}

func objectVersionIDInSet(versionID string, versions []string, excluded string) bool {
	versionID = strings.TrimSpace(versionID)
	for _, candidate := range versions {
		if candidate != excluded && candidate == versionID {
			return true
		}
	}
	return false
}

func (s *ObjectTokenStore) authObjectVersionsForDeletion(ctx context.Context, fullKey, expectedVersionID string) ([]string, error) {
	versions := make([]string, 0, 4)
	expectedFound := false
	expectedLatest := false
	objects := s.client.ListObjects(ctx, s.cfg.Bucket, minio.ListObjectsOptions{
		Prefix:       fullKey,
		Recursive:    true,
		WithVersions: true,
	})
	for object := range objects {
		if object.Err != nil {
			return nil, fmt.Errorf("object store: list auth object versions %s: %w", fullKey, object.Err)
		}
		if object.Key != fullKey {
			continue
		}
		versionID := strings.TrimSpace(object.VersionID)
		if versionID == "" {
			return nil, fmt.Errorf("object store: auth object version %s has no version ID", fullKey)
		}
		versions = append(versions, versionID)
		if versionID == expectedVersionID {
			expectedFound = true
			expectedLatest = object.IsLatest
		}
	}
	if !expectedFound || !expectedLatest {
		return nil, authfileguard.ErrDeleteGenerationUncertain
	}
	return versions, nil
}

func sanitizeObjectStoreRequestError(err error) error {
	var requestErr *url.Error
	if errors.As(err, &requestErr) && requestErr != nil && requestErr.Err != nil {
		return requestErr.Err
	}
	return err
}

func (s *ObjectTokenStore) conditionalHTTPClient() *http.Client {
	if s != nil && s.httpClient != nil {
		return s.httpClient
	}
	return http.DefaultClient
}

func (s *ObjectTokenStore) putObject(ctx context.Context, key string, data []byte, contentType string) error {
	if len(data) == 0 {
		return s.deleteObject(ctx, key)
	}
	fullKey := s.prefixedKey(key)
	reader := bytes.NewReader(data)
	_, err := s.client.PutObject(ctx, s.cfg.Bucket, fullKey, reader, int64(len(data)), minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return fmt.Errorf("object store: put object %s: %w", fullKey, err)
	}
	return nil
}

func (s *ObjectTokenStore) deleteObject(ctx context.Context, key string) error {
	fullKey := s.prefixedKey(key)
	err := s.client.RemoveObject(ctx, s.cfg.Bucket, fullKey, minio.RemoveObjectOptions{})
	if err != nil {
		if isObjectNotFound(err) {
			return nil
		}
		return fmt.Errorf("object store: delete object %s: %w", fullKey, err)
	}
	return nil
}

func (s *ObjectTokenStore) prefixedKey(key string) string {
	key = strings.TrimLeft(key, "/")
	if s.cfg.Prefix == "" {
		return key
	}
	return strings.TrimLeft(s.cfg.Prefix+"/"+key, "/")
}

func (s *ObjectTokenStore) resolveAuthPath(auth *cliproxyauth.Auth) (string, error) {
	if auth == nil {
		return "", fmt.Errorf("object store: auth is nil")
	}
	if auth.Attributes != nil {
		if path := strings.TrimSpace(auth.Attributes["path"]); path != "" {
			if filepath.IsAbs(path) {
				return path, nil
			}
			return filepath.Join(s.authDir, path), nil
		}
	}
	fileName := strings.TrimSpace(auth.FileName)
	if fileName == "" {
		fileName = strings.TrimSpace(auth.ID)
	}
	if fileName == "" {
		return "", fmt.Errorf("object store: auth %s missing filename", auth.ID)
	}
	if !strings.HasSuffix(strings.ToLower(fileName), ".json") {
		fileName += ".json"
	}
	return filepath.Join(s.authDir, fileName), nil
}

func (s *ObjectTokenStore) resolveDeletePath(id string) (string, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return "", fmt.Errorf("object store: id is empty")
	}
	// Absolute paths are honored as-is; callers must ensure they point inside the mirror.
	if filepath.IsAbs(id) {
		return id, nil
	}
	// Treat any non-absolute id (including nested like "team/foo") as relative to the mirror authDir.
	// Normalize separators and guard against path traversal.
	clean := filepath.Clean(filepath.FromSlash(id))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("object store: invalid auth identifier %s", id)
	}
	// Ensure .json suffix.
	if !strings.HasSuffix(strings.ToLower(clean), ".json") {
		clean += ".json"
	}
	return filepath.Join(s.authDir, clean), nil
}

func (s *ObjectTokenStore) relativeAuthPath(path string) (string, error) {
	relativePath, errRel := filepath.Rel(s.authDir, filepath.Clean(path))
	if errRel != nil {
		return "", fmt.Errorf("object store: resolve auth relative path: %w", errRel)
	}
	if relativePath == "." || relativePath == ".." || strings.HasPrefix(relativePath, ".."+string(os.PathSeparator)) || filepath.IsAbs(relativePath) {
		return "", fmt.Errorf("object store: auth path is outside mirror directory")
	}
	return relativePath, nil
}

func (s *ObjectTokenStore) readAuthFile(path, baseDir string) (*cliproxyauth.Auth, error) {
	unlockPath := authfileguard.Lock(path)
	defer unlockPath()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}
	if len(data) == 0 {
		return nil, nil
	}
	if cliproxyauth.IsRetiredGeminiCLIAuthFileData(data) {
		authfileguard.MarkRetired(path)
	}
	metadata := make(map[string]any)
	if err = json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("unmarshal auth json: %w", err)
	}
	provider := strings.TrimSpace(valueAsString(metadata["type"]))
	if provider == "" {
		provider = "unknown"
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat auth file: %w", err)
	}
	rel, errRel := filepath.Rel(baseDir, path)
	if errRel != nil {
		rel = filepath.Base(path)
	}
	rel = normalizeAuthID(rel)
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
		ID:               rel,
		Provider:         provider,
		FileName:         rel,
		Label:            labelFor(metadata),
		Status:           status,
		Disabled:         disabled,
		Attributes:       attr,
		Metadata:         metadata,
		CreatedAt:        info.ModTime(),
		UpdatedAt:        info.ModTime(),
		LastRefreshedAt:  time.Time{},
		NextRefreshAfter: time.Time{},
	}
	cliproxyauth.ApplyFileBackedGeminiAPIKey(auth)
	if strings.EqualFold(strings.TrimSpace(provider), "codex") {
		if planType := internalcodex.EffectivePlanType(metadata); planType != "" {
			auth.Attributes["plan_type"] = planType
		}
	}
	if errHash := cliproxyauth.SetCanonicalSourceHashAttribute(auth); errHash != nil {
		return nil, fmt.Errorf("canonicalize auth metadata: %w", errHash)
	}
	cliproxyauth.ApplyCustomHeadersFromMetadata(auth)
	return auth, nil
}

func normalizeLineEndingsBytes(data []byte) []byte {
	replaced := bytes.ReplaceAll(data, []byte{'\r', '\n'}, []byte{'\n'})
	return bytes.ReplaceAll(replaced, []byte{'\r'}, []byte{'\n'})
}

func isObjectNotFound(err error) bool {
	if err == nil {
		return false
	}
	resp := minio.ToErrorResponse(err)
	if resp.StatusCode == http.StatusNotFound {
		return true
	}
	switch resp.Code {
	case "NoSuchKey", "NotFound", "NoSuchBucket":
		return true
	}
	return false
}

func isExplicitObjectNotFound(err error) bool {
	return err != nil && minio.ToErrorResponse(err).StatusCode == http.StatusNotFound
}

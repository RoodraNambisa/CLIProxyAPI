package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	internalauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth"
	internalcodex "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/sirupsen/logrus"
)

// FileTokenStore persists token records and auth metadata using the filesystem as backing storage.
type FileTokenStore struct {
	mu                  sync.Mutex
	dirLock             sync.RWMutex
	baseDir             string
	lastResolvedBaseDir string
	syncDirectory       func(*os.Root, string) error
}

type fileTokenSnapshot struct {
	data   []byte
	info   fs.FileInfo
	exists bool
}

var fetchAntigravityProjectID = FetchAntigravityProjectID

func resolveFileTokenBaseDir(baseDir string, create bool) (string, string, error) {
	baseDir = strings.TrimSpace(baseDir)
	if baseDir == "" {
		return "", "", nil
	}
	lexicalBaseDir, errAbs := filepath.Abs(baseDir)
	if errAbs != nil {
		return "", "", fmt.Errorf("auth filestore: resolve base dir: %w", errAbs)
	}
	lexicalBaseDir = filepath.Clean(lexicalBaseDir)
	if create {
		if errMkdir := createFileTokenBaseDir(lexicalBaseDir); errMkdir != nil {
			return "", "", fmt.Errorf("auth filestore: create base dir: %w", errMkdir)
		}
	}
	resolvedBaseDir := lexicalBaseDir
	if resolved, errEval := filepath.EvalSymlinks(lexicalBaseDir); errEval == nil {
		resolvedBaseDir = filepath.Clean(resolved)
	} else if !os.IsNotExist(errEval) || !create {
		return lexicalBaseDir, resolvedBaseDir, fmt.Errorf("auth filestore: resolve base dir: %w", errEval)
	}
	return lexicalBaseDir, resolvedBaseDir, nil
}

func createFileTokenBaseDir(path string) error {
	if info, errStat := os.Stat(path); errStat == nil {
		if !info.IsDir() {
			return fmt.Errorf("path is not a directory")
		}
		return nil
	} else if !os.IsNotExist(errStat) {
		return errStat
	}
	root, relativePath, _, errRoot := openRootForPath(path, "")
	if errRoot != nil {
		return errRoot
	}
	defer closeFileTokenRoot(root)
	return mkdirAllRootAndSync(root, relativePath, 0o700)
}

func mkdirAllRootAndSync(root *os.Root, relativePath string, mode fs.FileMode) error {
	if errMkdir := root.MkdirAll(relativePath, mode); errMkdir != nil {
		return errMkdir
	}
	clean := filepath.Clean(relativePath)
	if clean == "." {
		return nil
	}
	parent := "."
	for _, component := range strings.Split(clean, string(filepath.Separator)) {
		if component == "" || component == "." {
			continue
		}
		if errSync := syncAuthRootDirectory(root, parent); errSync != nil {
			return errSync
		}
		parent = filepath.Join(parent, component)
	}
	return nil
}

func openRootForPath(path, resolvedBaseDir string) (*os.Root, string, bool, error) {
	if resolvedBaseDir != "" {
		if relativePath, inside := relativePathWithin(resolvedBaseDir, path); inside {
			root, errOpen := os.OpenRoot(resolvedBaseDir)
			if errOpen != nil {
				return nil, "", false, fmt.Errorf("auth filestore: open base dir: %w", errOpen)
			}
			return root, relativePath, true, nil
		}
	}

	existingAncestor := filepath.Dir(path)
	for {
		info, errStat := os.Stat(existingAncestor)
		if errStat == nil {
			if !info.IsDir() {
				return nil, "", false, fmt.Errorf("auth filestore: path ancestor is not a directory: %s", existingAncestor)
			}
			break
		}
		if !os.IsNotExist(errStat) {
			return nil, "", false, fmt.Errorf("auth filestore: inspect path ancestor: %w", errStat)
		}
		parent := filepath.Dir(existingAncestor)
		if parent == existingAncestor {
			return nil, "", false, fmt.Errorf("auth filestore: no existing path ancestor for %s", path)
		}
		existingAncestor = parent
	}
	resolvedAncestor, errEval := filepath.EvalSymlinks(existingAncestor)
	if errEval != nil {
		return nil, "", false, fmt.Errorf("auth filestore: resolve path ancestor: %w", errEval)
	}
	root, errOpen := os.OpenRoot(resolvedAncestor)
	if errOpen != nil {
		return nil, "", false, fmt.Errorf("auth filestore: open path ancestor: %w", errOpen)
	}
	relativePath, errRel := filepath.Rel(existingAncestor, path)
	if errRel != nil || relativePath == "." || filepath.IsAbs(relativePath) || relativePath == ".." || strings.HasPrefix(relativePath, ".."+string(filepath.Separator)) {
		closeFileTokenRoot(root)
		return nil, "", false, fmt.Errorf("auth filestore: invalid path below ancestor")
	}
	return root, relativePath, false, nil
}

func closeFileTokenRoot(root *os.Root) {
	if root != nil {
		if errClose := root.Close(); errClose != nil {
			logrus.WithError(errClose).Error("auth filestore: close filesystem root failed")
		}
	}
}

func writeRootFileAtomically(root *os.Root, relativePath string, data []byte) (err error) {
	return writeRootFileAtomicallyForSnapshot(root, relativePath, data, nil, "")
}

func lockRootAuthTarget(root *os.Root, relativePath string) (func() error, error) {
	return authfileguard.LockRootTarget(root, relativePath)
}

func writeRootFileAtomicallyForSnapshot(root *os.Root, relativePath string, data []byte, expected *fileTokenSnapshot, absolutePath string) (err error) {
	if root == nil {
		return fmt.Errorf("auth filestore: root is nil")
	}
	tempPath := filepath.Join(filepath.Dir(relativePath), ".auth-save-"+uuid.NewString())
	file, errOpen := root.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errOpen != nil {
		return errOpen
	}
	defer func() {
		if errRemove := root.Remove(tempPath); errRemove != nil && !os.IsNotExist(errRemove) {
			err = errors.Join(err, fmt.Errorf("remove temporary auth file: %w", errRemove))
		}
	}()
	closed := false
	defer func() {
		if !closed {
			if errClose := file.Close(); errClose != nil {
				err = errors.Join(err, fmt.Errorf("close temporary auth file: %w", errClose))
			}
		}
	}()
	if _, errWrite := file.Write(data); errWrite != nil {
		return errWrite
	}
	if errSync := file.Sync(); errSync != nil {
		return errSync
	}
	if errClose := file.Close(); errClose != nil {
		closed = true
		return errClose
	}
	closed = true
	unlockTarget, errLock := lockRootAuthTarget(root, relativePath)
	if errLock != nil {
		return fmt.Errorf("lock auth target: %w", errLock)
	}
	defer func() {
		err = errors.Join(err, unlockTarget())
	}()
	if expected != nil {
		if errValidate := expected.validate(root, relativePath, absolutePath); errValidate != nil {
			return errValidate
		}
	}
	if errReplace := replaceRootFile(root, tempPath, relativePath); errReplace != nil {
		return errReplace
	}
	return syncAuthRootDirectory(root, filepath.Dir(relativePath))
}

func (s fileTokenSnapshot) validate(root *os.Root, leaf, absolutePath string) error {
	current, errRead := captureFileTokenSnapshot(root, leaf)
	currentExists := current.exists
	if errRead != nil && !os.IsNotExist(errRead) {
		return errRead
	}
	if currentExists {
		if errRetired := cliproxyauth.RejectRetiredGeminiCLIAuthFileMutation(current.data); errRetired != nil {
			authfileguard.MarkRetired(absolutePath)
			return errRetired
		}
	}
	if currentExists != s.exists || !bytes.Equal(current.data, s.data) {
		return authfileguard.ErrPersistGenerationStale
	}
	if currentExists && (current.info == nil || s.info == nil || !os.SameFile(current.info, s.info)) {
		return authfileguard.ErrPersistGenerationStale
	}
	return nil
}

func validatePersistedAuthData(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("persisted auth is empty")
	}
	var metadata map[string]any
	if errUnmarshal := json.Unmarshal(data, &metadata); errUnmarshal != nil {
		return errUnmarshal
	}
	if metadata == nil {
		return fmt.Errorf("persisted auth must be a JSON object")
	}
	return nil
}

func openFileTokenParent(root *os.Root, relativePath string) (*os.Root, string, func(), error) {
	clean := filepath.Clean(filepath.FromSlash(strings.TrimSpace(relativePath)))
	if clean == "." || clean == ".." || filepath.IsAbs(clean) || filepath.VolumeName(clean) != "" || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		return nil, "", nil, fmt.Errorf("auth filestore: invalid root-relative auth path %q", relativePath)
	}
	parts := strings.Split(clean, string(os.PathSeparator))
	current := root
	owned := false
	closeCurrent := func() {
		if owned {
			closeFileTokenRoot(current)
		}
	}
	for _, component := range parts[:len(parts)-1] {
		before, errBefore := current.Lstat(component)
		if errBefore != nil {
			closeCurrent()
			return nil, "", nil, errBefore
		}
		if before.Mode()&os.ModeSymlink != 0 || !before.IsDir() {
			closeCurrent()
			return nil, "", nil, fmt.Errorf("auth filestore: auth path component %s is not a stable directory", component)
		}
		next, errOpen := current.OpenRoot(component)
		if errOpen != nil {
			closeCurrent()
			return nil, "", nil, errOpen
		}
		opened, errOpened := next.Stat(".")
		after, errAfter := current.Lstat(component)
		if errOpened != nil || errAfter != nil || after.Mode()&os.ModeSymlink != 0 || !after.IsDir() || !os.SameFile(before, opened) || !os.SameFile(after, opened) {
			closeFileTokenRoot(next)
			closeCurrent()
			return nil, "", nil, errors.Join(errOpened, errAfter, fmt.Errorf("auth filestore: auth path component %s changed while opening", component))
		}
		closeCurrent()
		current = next
		owned = true
	}
	closeParent := func() {}
	if owned {
		closeParent = func() { closeFileTokenRoot(current) }
	}
	return current, parts[len(parts)-1], closeParent, nil
}

func readFileTokenData(root *os.Root, leaf string) ([]byte, error) {
	snapshot, errSnapshot := captureFileTokenSnapshot(root, leaf)
	return snapshot.data, errSnapshot
}

func captureFileTokenSnapshot(root *os.Root, leaf string) (fileTokenSnapshot, error) {
	before, errBefore := root.Lstat(leaf)
	if errBefore != nil {
		return fileTokenSnapshot{}, errBefore
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() {
		return fileTokenSnapshot{}, fmt.Errorf("auth filestore: auth path is not a regular file")
	}
	file, errOpen := root.Open(leaf)
	if errOpen != nil {
		return fileTokenSnapshot{}, errOpen
	}
	defer func() {
		if errClose := file.Close(); errClose != nil {
			logrus.WithError(errClose).Error("auth filestore: close auth file failed")
		}
	}()
	opened, errOpened := file.Stat()
	after, errAfter := root.Lstat(leaf)
	if errOpened != nil || errAfter != nil || after.Mode()&os.ModeSymlink != 0 || !after.Mode().IsRegular() || !os.SameFile(before, opened) || !os.SameFile(after, opened) {
		return fileTokenSnapshot{}, errors.Join(errOpened, errAfter, fmt.Errorf("auth filestore: auth path changed while opening"))
	}
	data, errRead := io.ReadAll(file)
	if errRead != nil {
		return fileTokenSnapshot{}, errRead
	}
	return fileTokenSnapshot{data: data, info: opened, exists: true}, nil
}

// ReadAuthFileSnapshot reads an auth file through a stable parent and file
// handle, rejecting symlink and replacement races at the leaf path.
func ReadAuthFileSnapshot(path string) ([]byte, error) {
	if path == "" {
		return nil, fmt.Errorf("auth filestore: auth path is empty")
	}
	path = filepath.Clean(path)
	root, errRoot := os.OpenRoot(filepath.Dir(path))
	if errRoot != nil {
		return nil, errRoot
	}
	defer closeFileTokenRoot(root)
	return readFileTokenData(root, filepath.Base(path))
}

func marshalTokenStorageData(storage internalauth.TokenStorage) ([]byte, error) {
	if marshaler, ok := storage.(internalauth.TokenDataMarshaler); ok {
		data, errMarshal := marshaler.MarshalTokenData()
		if errMarshal != nil {
			return nil, errMarshal
		}
		return data, nil
	}
	return marshalLegacyTokenStorageData(storage)
}

func marshalLegacyTokenStorageData(storage internalauth.TokenStorage) (data []byte, err error) {
	tempDir := os.TempDir()
	resolvedTempDir, errEval := filepath.EvalSymlinks(tempDir)
	if errEval != nil {
		return nil, fmt.Errorf("resolve storage sandbox root: %w", errEval)
	}
	tempDir = filepath.Clean(resolvedTempDir)
	tempRoot, errOpen := os.OpenRoot(tempDir)
	if errOpen != nil {
		return nil, fmt.Errorf("open storage sandbox root: %w", errOpen)
	}
	defer func() {
		if errClose := tempRoot.Close(); errClose != nil {
			err = errors.Join(err, fmt.Errorf("close storage sandbox root: %w", errClose))
		}
	}()
	sandboxName := ".cli-proxy-auth-storage-" + uuid.NewString()
	if errMkdir := tempRoot.Mkdir(sandboxName, 0o700); errMkdir != nil {
		return nil, fmt.Errorf("create storage sandbox: %w", errMkdir)
	}
	defer func() {
		if errRemove := tempRoot.RemoveAll(sandboxName); errRemove != nil && !os.IsNotExist(errRemove) {
			err = errors.Join(err, fmt.Errorf("remove storage sandbox: %w", errRemove))
		}
	}()
	sandboxRoot, errSandbox := tempRoot.OpenRoot(sandboxName)
	if errSandbox != nil {
		return nil, fmt.Errorf("open storage sandbox: %w", errSandbox)
	}
	defer func() {
		if errClose := sandboxRoot.Close(); errClose != nil {
			err = errors.Join(err, fmt.Errorf("close storage sandbox: %w", errClose))
		}
	}()

	const outputName = "token.json"
	outputFile, errCreate := sandboxRoot.OpenFile(outputName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errCreate != nil {
		return nil, fmt.Errorf("create storage sandbox output: %w", errCreate)
	}
	if errClose := outputFile.Close(); errClose != nil {
		return nil, fmt.Errorf("close storage sandbox output: %w", errClose)
	}
	outputPath := filepath.Join(tempDir, sandboxName, outputName)
	if errSave := storage.SaveTokenToFile(outputPath); errSave != nil {
		return nil, errSave
	}
	data, errRead := readFileTokenData(sandboxRoot, outputName)
	if errRead != nil {
		return nil, fmt.Errorf("read storage sandbox output: %w", errRead)
	}
	return data, nil
}

// NewFileTokenStore creates a token store that saves credentials to disk through the
// TokenStorage implementation embedded in the token record.
func NewFileTokenStore() *FileTokenStore {
	return &FileTokenStore{}
}

func (s *FileTokenStore) syncRootDirectory(root *os.Root, relativePath string) error {
	if s != nil && s.syncDirectory != nil {
		return s.syncDirectory(root, relativePath)
	}
	return syncAuthRootDirectory(root, relativePath)
}

// SetBaseDir updates the default directory used for auth JSON persistence when no explicit path is provided.
func (s *FileTokenStore) SetBaseDir(dir string) {
	dir = strings.TrimSpace(dir)
	s.dirLock.Lock()
	if s.baseDir != dir {
		s.lastResolvedBaseDir = ""
	}
	s.baseDir = dir
	s.dirLock.Unlock()
}

// Save persists token storage and metadata to the resolved auth file path.
func (s *FileTokenStore) Save(ctx context.Context, auth *cliproxyauth.Auth) (string, error) {
	if auth == nil {
		return "", fmt.Errorf("auth filestore: auth is nil")
	}
	if cliproxyauth.IsRetiredGeminiCLIAuth(auth) {
		cliproxyauth.WarnRetiredGeminiCLIAuthIgnored()
		return "", fmt.Errorf("auth filestore: %w", cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly)
	}

	lexicalBaseDir, resolvedBaseDir, errBase := resolveFileTokenBaseDir(s.baseDirSnapshot(), true)
	if errBase != nil {
		return "", errBase
	}
	s.rememberResolvedBaseDir(resolvedBaseDir)
	path, allowOutsideBaseDir, err := s.resolveAuthPath(auth, lexicalBaseDir, resolvedBaseDir)
	if err != nil {
		return "", err
	}
	if path == "" {
		return "", fmt.Errorf("auth filestore: missing file path attribute for %s", auth.ID)
	}
	persistedPath := path
	path, err = s.secureAuthPath(path, allowOutsideBaseDir, lexicalBaseDir, resolvedBaseDir)
	if err != nil {
		return "", err
	}
	root, relativePath, managed, errRoot := openRootForPath(path, resolvedBaseDir)
	if errRoot != nil {
		return "", errRoot
	}
	defer closeFileTokenRoot(root)
	if !managed && !allowOutsideBaseDir {
		return "", fmt.Errorf("auth filestore: managed auth path is outside base dir")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	unlockPath := authfileguard.Lock(path)
	defer unlockPath()
	if authfileguard.IsRetired(path) {
		return "", fmt.Errorf("auth filestore: %w", cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly)
	}
	if authfileguard.IsQuarantined(path) {
		return "", fmt.Errorf("auth filestore: auth deletion is still pending: %w", authfileguard.ErrDeleteGenerationUncertain)
	}
	parentPath := filepath.Dir(relativePath)
	if !auth.Disabled && parentPath != "." {
		if errMkdir := mkdirAllRootAndSync(root, parentPath, 0o700); errMkdir != nil {
			return "", fmt.Errorf("auth filestore: create dir failed: %w", errMkdir)
		}
	}
	parentRoot, leaf, closeParent, errParent := openFileTokenParent(root, relativePath)
	if errParent != nil {
		if auth.Disabled && os.IsNotExist(errParent) {
			return "", nil
		}
		return "", fmt.Errorf("auth filestore: open auth parent: %w", errParent)
	}
	defer closeParent()

	initialSnapshot, errExisting := captureFileTokenSnapshot(parentRoot, leaf)
	existingData := initialSnapshot.data
	if errExisting == nil {
		if errRetired := cliproxyauth.RejectRetiredGeminiCLIAuthFileMutation(existingData); errRetired != nil {
			authfileguard.MarkRetired(path)
			return "", fmt.Errorf("auth filestore: %w", errRetired)
		}
	} else if !os.IsNotExist(errExisting) {
		return "", fmt.Errorf("auth filestore: read existing failed: %w", errExisting)
	}
	if auth.Disabled && os.IsNotExist(errExisting) {
		return "", nil
	}

	switch {
	case auth.Storage != nil:
		if setter, ok := auth.Storage.(internalauth.MetadataSetter); ok {
			setter.SetMetadata(cliproxyauth.MetadataWithDisabled(auth))
		}
		data, errData := marshalTokenStorageData(auth.Storage)
		if errData != nil {
			return "", fmt.Errorf("auth filestore: produce storage auth: %w", errData)
		}
		if errValidate := validatePersistedAuthData(data); errValidate != nil {
			return "", fmt.Errorf("auth filestore: invalid persisted storage auth: %w", errValidate)
		}
		if errRetired := cliproxyauth.RejectRetiredGeminiCLIAuthFileMutation(data); errRetired != nil {
			return "", fmt.Errorf("auth filestore: %w", errRetired)
		}
		err = writeRootFileAtomicallyForSnapshot(parentRoot, leaf, data, &initialSnapshot, path)
		if err != nil {
			return "", fmt.Errorf("auth filestore: persist storage auth failed: %w", err)
		}
		if errSync := cliproxyauth.SyncPersistedMetadataAndSourceHash(auth, data); errSync != nil {
			return "", fmt.Errorf("auth filestore: sync persisted storage auth failed: %w", errSync)
		}
	case auth.Metadata != nil:
		raw, errMarshal := cliproxyauth.CanonicalMetadataBytes(auth)
		if errMarshal != nil {
			return "", fmt.Errorf("auth filestore: canonicalize metadata failed: %w", errMarshal)
		}
		if errValidate := initialSnapshot.validate(parentRoot, leaf, path); errValidate != nil {
			return "", fmt.Errorf("auth filestore: local auth changed before replacement: %w", errValidate)
		}
		if errExisting == nil {
			if jsonEqual(existingData, raw) {
				cliproxyauth.SetSourceHashAttribute(auth, raw)
				return persistedPath, nil
			}
			if errWrite := writeRootFileAtomicallyForSnapshot(parentRoot, leaf, raw, &initialSnapshot, path); errWrite != nil {
				return "", fmt.Errorf("auth filestore: write existing failed: %w", errWrite)
			}
			cliproxyauth.SetSourceHashAttribute(auth, raw)
			return persistedPath, nil
		}
		if errWrite := writeRootFileAtomicallyForSnapshot(parentRoot, leaf, raw, &initialSnapshot, path); errWrite != nil {
			return "", fmt.Errorf("auth filestore: write file failed: %w", errWrite)
		}
		cliproxyauth.SetSourceHashAttribute(auth, raw)
	default:
		return "", fmt.Errorf("auth filestore: nothing to persist for %s", auth.ID)
	}

	if auth.Attributes == nil {
		auth.Attributes = make(map[string]string)
	}
	auth.Attributes["path"] = persistedPath

	if strings.TrimSpace(auth.FileName) == "" {
		auth.FileName = auth.ID
	}

	return persistedPath, nil
}

// List enumerates all auth JSON files under the configured directory.
func (s *FileTokenStore) List(ctx context.Context) ([]*cliproxyauth.Auth, error) {
	_, dir, errBase := resolveFileTokenBaseDir(s.baseDirSnapshot(), false)
	if errBase != nil {
		return nil, errBase
	}
	s.rememberResolvedBaseDir(dir)
	if dir == "" {
		return nil, fmt.Errorf("auth filestore: directory not configured")
	}
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		return nil, fmt.Errorf("auth filestore: open directory: %w", errRoot)
	}
	defer closeFileTokenRoot(root)
	entries := make([]*cliproxyauth.Auth, 0)
	err := fs.WalkDir(root.FS(), ".", func(walkPath string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}
		if !strings.HasSuffix(strings.ToLower(d.Name()), ".json") {
			return nil
		}
		relativePath := filepath.FromSlash(walkPath)
		path := filepath.Join(dir, relativePath)
		auth, err := s.readAuthFileFromRoot(ctx, root, relativePath, path, dir)
		if err != nil {
			if errContext := ctx.Err(); errContext != nil {
				return errContext
			}
			return nil
		}
		if auth != nil {
			entries = append(entries, auth)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// Delete removes the auth file.
func (s *FileTokenStore) Delete(ctx context.Context, id string) (resultErr error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return fmt.Errorf("auth filestore: id is empty")
	}
	lexicalBaseDir, resolvedBaseDir, errBase := resolveFileTokenBaseDir(s.baseDirSnapshot(), false)
	if errBase != nil {
		if errors.Is(errBase, os.ErrNotExist) {
			path := filepath.FromSlash(id)
			if !filepath.IsAbs(path) {
				guardBaseDir := s.resolvedBaseDirSnapshot()
				if guardBaseDir == "" {
					guardBaseDir = lexicalBaseDir
				}
				guardPath := filepath.Join(guardBaseDir, path)
				if _, inside := relativePathWithin(guardBaseDir, guardPath); inside {
					s.mu.Lock()
					defer s.mu.Unlock()
					unlockPath := authfileguard.Lock(guardPath)
					defer unlockPath()
					retiredSnapshot := authfileguard.CaptureRetired(guardPath)
					authfileguard.ClearRetiredSnapshot(retiredSnapshot)
				}
				return nil
			}
			if !isExplicitOutsideBaseDir(path, lexicalBaseDir, resolvedBaseDir) {
				return nil
			}
		} else {
			return errBase
		}
	}
	s.rememberResolvedBaseDir(resolvedBaseDir)
	path, guardPath, allowOutsideBaseDir, err := s.resolveDeletePath(id, lexicalBaseDir, resolvedBaseDir)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlockPath := authfileguard.Lock(guardPath)
	defer unlockPath()
	retiredSnapshot := authfileguard.CaptureRetired(guardPath)
	root, relativePath, managed, errRoot := openRootForPath(path, resolvedBaseDir)
	if errRoot != nil {
		if os.IsNotExist(errRoot) {
			authfileguard.ClearRetiredSnapshot(retiredSnapshot)
			return nil
		}
		return errRoot
	}
	defer closeFileTokenRoot(root)
	if !managed && !allowOutsideBaseDir {
		return fmt.Errorf("auth filestore: managed auth path is outside base dir")
	}
	parentRoot, leaf, closeParent, errParent := openFileTokenParent(root, relativePath)
	if errParent != nil {
		if errors.Is(errParent, fs.ErrNotExist) {
			authfileguard.ClearRetiredSnapshot(retiredSnapshot)
			return nil
		}
		return fmt.Errorf("auth filestore: open auth parent: %w", errParent)
	}
	defer closeParent()
	unlockTarget, errLock := lockRootAuthTarget(parentRoot, leaf)
	if errLock != nil {
		return fmt.Errorf("auth filestore: lock auth target for deletion: %w", errLock)
	}
	defer func() {
		resultErr = joinFileTokenDeleteUnlock(resultErr, unlockTarget())
	}()
	err = parentRoot.Remove(leaf)
	if err != nil && !os.IsNotExist(err) {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth filestore: delete failed: %w", err))
	}
	if err == nil {
		if errSync := s.syncRootDirectory(parentRoot, "."); errSync != nil {
			return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeUncertain, fmt.Errorf("auth filestore: sync deleted auth: %w", errSync))
		}
	}
	authfileguard.ClearRetiredSnapshot(retiredSnapshot)
	return nil
}

// DeleteAuthFileAtRoot removes one managed auth file through a stable root while
// serializing against concurrent saves to the same store and path.
func (s *FileTokenStore) DeleteAuthFileAtRoot(baseDir string, root *os.Root, id string) (resultErr error) {
	return s.DeleteAuthFileAtRootPrepared(baseDir, root, id, nil)
}

// DeleteAuthFileAtRootPrepared runs prepare after locking and reading the
// current file, then removes that exact managed path.
func (s *FileTokenStore) DeleteAuthFileAtRootPrepared(baseDir string, root *os.Root, id string, prepare func(path string, data []byte) error) (resultErr error) {
	if root == nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth filestore: root is nil"))
	}
	id = strings.TrimSpace(id)
	cleanID := filepath.Clean(filepath.FromSlash(id))
	if cleanID == "." || cleanID == ".." || strings.HasPrefix(cleanID, ".."+string(os.PathSeparator)) || filepath.IsAbs(cleanID) {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth filestore: invalid auth identifier %s", id))
	}
	_, resolvedBaseDir, errBase := resolveFileTokenBaseDir(baseDir, false)
	if errBase != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errBase)
	}
	guardBaseDir := resolvedBaseDir
	if guardBaseDir == "" {
		guardBaseDir = filepath.Clean(root.Name())
	}
	guardPath := filepath.Join(guardBaseDir, cleanID)

	s.mu.Lock()
	defer s.mu.Unlock()
	unlockPath := authfileguard.Lock(guardPath)
	defer unlockPath()
	retiredSnapshot := authfileguard.CaptureRetired(guardPath)
	parentRoot, leaf, closeParent, errParent := openFileTokenParent(root, cleanID)
	if errParent != nil {
		if errors.Is(errParent, fs.ErrNotExist) {
			prepared := false
			if prepare != nil {
				if errPrepare := prepare(guardPath, nil); errPrepare != nil {
					return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth filestore: prepare missing deletion: %w", errPrepare))
				}
				prepared = true
			}
			checkRoot, checkLeaf, closeCheck, errCheck := openFileTokenParent(root, cleanID)
			if errCheck == nil {
				defer closeCheck()
				if _, errSnapshot := captureFileAuthSnapshot(checkRoot, checkLeaf); errSnapshot == nil {
					return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeUncertain, authfileguard.ErrPersistGenerationStale)
				} else if !errors.Is(errSnapshot, fs.ErrNotExist) {
					return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeUncertain, fmt.Errorf("auth filestore: recheck missing auth: %w", errSnapshot))
				}
			} else if !errors.Is(errCheck, fs.ErrNotExist) {
				outcome := cliproxyauth.DeleteOutcomeRolledBack
				if prepared {
					outcome = cliproxyauth.DeleteOutcomeUncertain
				}
				return cliproxyauth.NewDeleteOutcomeError(outcome, fmt.Errorf("auth filestore: recheck missing auth parent: %w", errCheck))
			}
			authfileguard.ClearRetiredSnapshot(retiredSnapshot)
			return nil
		}
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth filestore: open auth parent: %w", errParent))
	}
	defer closeParent()
	unlockTarget, errLock := lockRootAuthTarget(parentRoot, leaf)
	if errLock != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth filestore: lock auth target for deletion: %w", errLock))
	}
	defer func() {
		resultErr = joinFileTokenDeleteUnlock(resultErr, unlockTarget())
	}()
	prepared := false
	var preparedSnapshot *fileAuthSnapshot
	if prepare != nil {
		snapshot, errRead := captureFileAuthSnapshot(parentRoot, leaf)
		if errRead != nil && !errors.Is(errRead, fs.ErrNotExist) {
			return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth filestore: read auth before deletion: %w", errRead))
		}
		if errRead == nil {
			if errPrepare := prepare(guardPath, snapshot.data); errPrepare != nil {
				return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth filestore: prepare deletion: %w", errPrepare))
			}
			preparedSnapshot = snapshot
			prepared = true
		} else if errPrepare := prepare(guardPath, nil); errPrepare != nil {
			return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth filestore: prepare missing deletion: %w", errPrepare))
		} else {
			prepared = true
		}
	}
	if preparedSnapshot != nil {
		current, errCurrent := captureFileAuthSnapshot(parentRoot, leaf)
		if errCurrent != nil {
			return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeUncertain, fmt.Errorf("auth filestore: revalidate auth after preparing deletion: %w", errCurrent))
		}
		if !os.SameFile(preparedSnapshot.info, current.info) || !bytes.Equal(preparedSnapshot.data, current.data) {
			return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeUncertain, authfileguard.ErrPersistGenerationStale)
		}
	}
	if errRemove := parentRoot.Remove(leaf); errRemove != nil {
		if !os.IsNotExist(errRemove) {
			outcome := cliproxyauth.DeleteOutcomeRolledBack
			if prepared {
				outcome = cliproxyauth.DeleteOutcomeUncertain
			}
			return cliproxyauth.NewDeleteOutcomeError(outcome, fmt.Errorf("auth filestore: delete failed: %w", errRemove))
		}
	} else if errSync := s.syncRootDirectory(parentRoot, "."); errSync != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeUncertain, fmt.Errorf("auth filestore: sync deleted auth: %w", errSync))
	}
	authfileguard.ClearRetiredSnapshot(retiredSnapshot)
	return nil
}

func joinFileTokenDeleteUnlock(resultErr, errUnlock error) error {
	if errUnlock == nil {
		return resultErr
	}
	if resultErr == nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeCommitted, errUnlock)
	}
	outcome, explicit := cliproxyauth.DeleteOutcomeFromError(resultErr)
	if !explicit {
		return errors.Join(resultErr, errUnlock)
	}
	return cliproxyauth.NewDeleteOutcomeError(outcome, errors.Join(resultErr, errUnlock))
}

func (s *FileTokenStore) resolveDeletePath(id, lexicalBaseDir, resolvedBaseDir string) (string, string, bool, error) {
	path := filepath.FromSlash(id)
	allowOutsideBaseDir := isExplicitOutsideBaseDir(path, lexicalBaseDir, resolvedBaseDir)
	if !filepath.IsAbs(path) {
		if lexicalBaseDir == "" {
			absolutePath, errAbs := filepath.Abs(path)
			if errAbs != nil {
				return "", "", false, fmt.Errorf("auth filestore: resolve delete path: %w", errAbs)
			}
			path = absolutePath
			allowOutsideBaseDir = true
		} else {
			path = filepath.Join(lexicalBaseDir, path)
		}
	}
	resolved, err := s.secureAuthPath(path, allowOutsideBaseDir, lexicalBaseDir, resolvedBaseDir)
	return resolved, resolved, allowOutsideBaseDir, err
}

func (s *FileTokenStore) secureAuthPath(path string, allowOutsideBaseDir bool, lexicalBaseDir, resolvedBaseDir string) (string, error) {
	path, errAbs := filepath.Abs(path)
	if errAbs != nil {
		return "", fmt.Errorf("auth filestore: resolve auth path: %w", errAbs)
	}
	path = filepath.Clean(path)
	if lexicalBaseDir != "" {
		if rel, inside := relativePathWithin(lexicalBaseDir, path); inside {
			path = filepath.Join(resolvedBaseDir, rel)
		}
		if _, inside := relativePathWithin(resolvedBaseDir, path); inside {
			if hasSymlink, errSymlink := pathHasSymlinkBelow(resolvedBaseDir, path); errSymlink != nil {
				return "", fmt.Errorf("auth filestore: inspect auth path: %w", errSymlink)
			} else if hasSymlink {
				return "", fmt.Errorf("auth filestore: refusing symlink auth path")
			}
			return path, nil
		}
		if allowOutsideBaseDir {
			if info, errInfo := os.Lstat(path); errInfo == nil && info.Mode()&os.ModeSymlink != 0 {
				return "", fmt.Errorf("auth filestore: refusing symlink auth path")
			}
			return path, nil
		}
		return "", fmt.Errorf("auth filestore: auth path is outside base dir")
	}
	if info, errInfo := os.Lstat(path); errInfo == nil && info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("auth filestore: refusing symlink auth path")
	}
	return path, nil
}

func isExplicitOutsideBaseDir(path, lexicalBaseDir, resolvedBaseDir string) bool {
	if !filepath.IsAbs(path) {
		return false
	}
	path = filepath.Clean(path)
	if lexicalBaseDir != "" {
		if _, inside := relativePathWithin(lexicalBaseDir, path); inside {
			return false
		}
		if _, inside := relativePathWithin(resolvedBaseDir, path); inside {
			return false
		}
	}
	return true
}

func relativePathWithin(root, path string) (string, bool) {
	rel, errRel := filepath.Rel(root, path)
	if errRel != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", false
	}
	return rel, true
}

func pathHasSymlinkBelow(root, path string) (bool, error) {
	rel, inside := relativePathWithin(root, path)
	if !inside {
		return false, fmt.Errorf("path is outside base dir")
	}
	current := root
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		info, errInfo := os.Lstat(current)
		if errInfo != nil {
			if os.IsNotExist(errInfo) {
				return false, nil
			}
			return false, errInfo
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return true, nil
		}
	}
	return false, nil
}

func (s *FileTokenStore) readAuthFile(ctx context.Context, path, baseDir string) (*cliproxyauth.Auth, error) {
	root, errOpen := os.OpenRoot(baseDir)
	if errOpen != nil {
		return nil, fmt.Errorf("open auth root: %w", errOpen)
	}
	defer closeFileTokenRoot(root)
	relativePath, errRel := filepath.Rel(baseDir, path)
	if errRel != nil || relativePath == ".." || strings.HasPrefix(relativePath, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("read file: path is outside base dir")
	}
	return s.readAuthFileFromRoot(ctx, root, relativePath, path, baseDir)
}

type fileAuthSnapshot struct {
	data []byte
	info fs.FileInfo
}

func captureFileAuthSnapshot(root *os.Root, relativePath string) (snapshot *fileAuthSnapshot, err error) {
	parentRoot, leaf, closeParent, errParent := openFileTokenParent(root, relativePath)
	if errParent != nil {
		return nil, errParent
	}
	defer closeParent()
	before, errBefore := parentRoot.Lstat(leaf)
	if errBefore != nil {
		return nil, errBefore
	}
	if errValidate := validateFileAuthSnapshotInfo(before); errValidate != nil {
		return nil, errValidate
	}
	file, errOpen := parentRoot.Open(leaf)
	if errOpen != nil {
		return nil, errOpen
	}
	defer func() {
		if errClose := file.Close(); errClose != nil {
			err = errors.Join(err, fmt.Errorf("close auth file: %w", errClose))
		}
	}()
	opened, errOpened := file.Stat()
	after, errAfter := parentRoot.Lstat(leaf)
	if errOpened != nil || errAfter != nil {
		return nil, errors.Join(errOpened, errAfter)
	}
	if errValidate := validateFileAuthSnapshotInfo(opened); errValidate != nil {
		return nil, errValidate
	}
	if errValidate := validateFileAuthSnapshotInfo(after); errValidate != nil {
		return nil, errValidate
	}
	if !os.SameFile(before, opened) || !os.SameFile(after, opened) {
		return nil, errors.New("auth filestore: auth path changed while opening")
	}
	data, errRead := io.ReadAll(file)
	if errRead != nil {
		return nil, errRead
	}
	return &fileAuthSnapshot{data: data, info: opened}, nil
}

func validateFileAuthSnapshotInfo(info fs.FileInfo) error {
	if info == nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return errors.New("auth filestore: auth path is not a regular file")
	}
	return nil
}

func captureLockedFileAuthSnapshot(root *os.Root, relativePath, path string) (*fileAuthSnapshot, error) {
	unlockPath := authfileguard.Lock(path)
	defer unlockPath()
	snapshot, err := captureFileAuthSnapshot(root, relativePath)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}
	if cliproxyauth.IsRetiredGeminiCLIAuthFileData(snapshot.data) {
		authfileguard.MarkRetired(path)
	}
	return snapshot, nil
}

func fileAuthSnapshotIsCurrent(root *os.Root, relativePath, path string, snapshot *fileAuthSnapshot) (bool, error) {
	unlockPath := authfileguard.Lock(path)
	defer unlockPath()
	current, err := captureFileAuthSnapshot(root, relativePath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("revalidate auth file: %w", err)
	}
	return os.SameFile(snapshot.info, current.info) && bytes.Equal(snapshot.data, current.data), nil
}

func (s *FileTokenStore) readAuthFileFromRoot(ctx context.Context, root *os.Root, relativePath, path, baseDir string) (*cliproxyauth.Auth, error) {
	snapshot, err := captureLockedFileAuthSnapshot(root, relativePath, path)
	if err != nil {
		return nil, err
	}
	if len(snapshot.data) == 0 {
		return nil, nil
	}
	data := snapshot.data
	metadata := make(map[string]any)
	if err = json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("unmarshal auth json: %w", err)
	}
	provider, _ := metadata["type"].(string)
	provider = strings.TrimSpace(provider)
	if provider == "" {
		provider = "unknown"
	}
	diskMetadata := make(map[string]any, len(metadata))
	for key, value := range metadata {
		diskMetadata[key] = value
	}
	diskDisabled, _ := diskMetadata["disabled"].(bool)
	diskAuth := &cliproxyauth.Auth{Metadata: diskMetadata, Disabled: diskDisabled}
	diskCanonical, errCanonical := cliproxyauth.CanonicalMetadataBytes(diskAuth)
	if errCanonical != nil {
		return nil, fmt.Errorf("canonicalize auth metadata: %w", errCanonical)
	}
	fetchedProjectID := ""
	revalidateSnapshot := false
	if provider == "antigravity" {
		projectID := ""
		if pid, ok := metadata["project_id"].(string); ok {
			projectID = strings.TrimSpace(pid)
		}
		if projectID == "" {
			accessToken := extractAccessToken(metadata)
			if accessToken != "" {
				revalidateSnapshot = true
				projectIDFromUpstream, errFetch := fetchAntigravityProjectID(ctx, accessToken, http.DefaultClient)
				if errContext := ctx.Err(); errContext != nil {
					return nil, errContext
				}
				if errFetch == nil {
					fetchedProjectID = strings.TrimSpace(projectIDFromUpstream)
				}
			}
		}
	}
	if revalidateSnapshot {
		current, errCurrent := fileAuthSnapshotIsCurrent(root, relativePath, path, snapshot)
		if errCurrent != nil {
			return nil, errCurrent
		}
		if !current {
			return nil, nil
		}
	}
	if fetchedProjectID != "" {
		metadata["project_id"] = fetchedProjectID
	}
	id := s.idFor(path, baseDir)
	disabled, _ := metadata["disabled"].(bool)
	status := cliproxyauth.StatusActive
	if disabled {
		status = cliproxyauth.StatusDisabled
	}
	auth := &cliproxyauth.Auth{
		ID:               id,
		Provider:         provider,
		FileName:         id,
		Label:            s.labelFor(metadata),
		Status:           status,
		Disabled:         disabled,
		Attributes:       map[string]string{"path": path},
		Metadata:         metadata,
		CreatedAt:        snapshot.info.ModTime(),
		UpdatedAt:        snapshot.info.ModTime(),
		LastRefreshedAt:  time.Time{},
		NextRefreshAfter: time.Time{},
	}
	cliproxyauth.ApplyFileBackedGeminiAPIKey(auth)
	if strings.EqualFold(strings.TrimSpace(provider), "codex") {
		if planType := internalcodex.EffectivePlanType(metadata); planType != "" {
			auth.Attributes["plan_type"] = planType
		}
	}
	cliproxyauth.SetSourceHashAttribute(auth, diskCanonical)
	if email, ok := metadata["email"].(string); ok && email != "" {
		auth.Attributes["email"] = email
	}
	cliproxyauth.ApplyCustomHeadersFromMetadata(auth)
	return auth, nil
}

func (s *FileTokenStore) idFor(path, baseDir string) string {
	id := path
	if baseDir != "" {
		if rel, errRel := filepath.Rel(baseDir, path); errRel == nil && rel != "" {
			id = rel
		}
	}
	// On Windows, normalize ID casing to avoid duplicate auth entries caused by case-insensitive paths.
	if runtime.GOOS == "windows" {
		id = strings.ToLower(id)
	}
	return id
}

func (s *FileTokenStore) resolveAuthPath(auth *cliproxyauth.Auth, lexicalBaseDir, resolvedBaseDir string) (string, bool, error) {
	if auth == nil {
		return "", false, fmt.Errorf("auth filestore: auth is nil")
	}
	if auth.Attributes != nil {
		if p := strings.TrimSpace(auth.Attributes["path"]); p != "" {
			return p, lexicalBaseDir == "" || isExplicitOutsideBaseDir(p, lexicalBaseDir, resolvedBaseDir), nil
		}
	}
	if fileName := strings.TrimSpace(auth.FileName); fileName != "" {
		if filepath.IsAbs(fileName) {
			return fileName, isExplicitOutsideBaseDir(fileName, lexicalBaseDir, resolvedBaseDir), nil
		}
		if lexicalBaseDir != "" {
			return filepath.Join(lexicalBaseDir, fileName), false, nil
		}
		return fileName, true, nil
	}
	if auth.ID == "" {
		return "", false, fmt.Errorf("auth filestore: missing id")
	}
	if filepath.IsAbs(auth.ID) {
		return auth.ID, isExplicitOutsideBaseDir(auth.ID, lexicalBaseDir, resolvedBaseDir), nil
	}
	if lexicalBaseDir == "" {
		return "", false, fmt.Errorf("auth filestore: directory not configured")
	}
	return filepath.Join(lexicalBaseDir, auth.ID), false, nil
}

func (s *FileTokenStore) labelFor(metadata map[string]any) string {
	if metadata == nil {
		return ""
	}
	if v, ok := metadata["label"].(string); ok && v != "" {
		return v
	}
	if v, ok := metadata["email"].(string); ok && v != "" {
		return v
	}
	if project, ok := metadata["project_id"].(string); ok && project != "" {
		return project
	}
	return ""
}

func (s *FileTokenStore) baseDirSnapshot() string {
	s.dirLock.RLock()
	defer s.dirLock.RUnlock()
	return s.baseDir
}

func (s *FileTokenStore) rememberResolvedBaseDir(dir string) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return
	}
	s.dirLock.Lock()
	s.lastResolvedBaseDir = dir
	s.dirLock.Unlock()
}

func (s *FileTokenStore) resolvedBaseDirSnapshot() string {
	s.dirLock.RLock()
	defer s.dirLock.RUnlock()
	return s.lastResolvedBaseDir
}

func extractAccessToken(metadata map[string]any) string {
	if at, ok := metadata["access_token"].(string); ok {
		if v := strings.TrimSpace(at); v != "" {
			return v
		}
	}
	if tokenMap, ok := metadata["token"].(map[string]any); ok {
		if at, ok := tokenMap["access_token"].(string); ok {
			if v := strings.TrimSpace(at); v != "" {
				return v
			}
		}
	}
	return ""
}

// jsonEqual compares two JSON blobs by parsing them into Go objects and deep comparing.
func jsonEqual(a, b []byte) bool {
	var objA any
	var objB any
	if err := json.Unmarshal(a, &objA); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &objB); err != nil {
		return false
	}
	return deepEqualJSON(objA, objB)
}

func deepEqualJSON(a, b any) bool {
	switch valA := a.(type) {
	case map[string]any:
		valB, ok := b.(map[string]any)
		if !ok || len(valA) != len(valB) {
			return false
		}
		for key, subA := range valA {
			subB, ok1 := valB[key]
			if !ok1 || !deepEqualJSON(subA, subB) {
				return false
			}
		}
		return true
	case []any:
		sliceB, ok := b.([]any)
		if !ok || len(valA) != len(sliceB) {
			return false
		}
		for i := range valA {
			if !deepEqualJSON(valA[i], sliceB[i]) {
				return false
			}
		}
		return true
	case float64:
		valB, ok := b.(float64)
		if !ok {
			return false
		}
		return valA == valB
	case string:
		valB, ok := b.(string)
		if !ok {
			return false
		}
		return valA == valB
	case bool:
		valB, ok := b.(bool)
		if !ok {
			return false
		}
		return valA == valB
	case nil:
		return b == nil
	default:
		return false
	}
}

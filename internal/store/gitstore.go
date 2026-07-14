package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/format/index"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/plumbing/transport/http"
	internalauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth"
	internalcodex "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

// gcInterval defines minimum time between garbage collection runs.
const gcInterval = 5 * time.Minute

// GitTokenStore persists token records and auth metadata using git as the backing storage.
type GitTokenStore struct {
	mu        sync.Mutex
	bindingMu sync.RWMutex
	dirLock   sync.RWMutex
	baseDir   string
	repoDir   string
	configDir string
	remote    string
	branch    string
	username  string
	password  string
	lastGC    time.Time
	pushRepo  func(*git.Repository, *git.PushOptions) error
}

type resolvedRemoteBranch struct {
	name plumbing.ReferenceName
	hash plumbing.Hash
}

type gitRemotePrecondition struct {
	branch plumbing.ReferenceName
	hash   plumbing.Hash
	exists bool
}

type gitLocalHeadSnapshot struct {
	name   plumbing.ReferenceName
	hash   plumbing.Hash
	exists bool
}

type gitRemoteAuthBlob struct {
	data   []byte
	mode   filemode.FileMode
	exists bool
}

var errUnsafeGitAuthPath = errors.New("git token store: unsafe auth path")

func lockGitRepository(repoDir string) (func() error, error) {
	repoDir = filepath.Clean(strings.TrimSpace(repoDir))
	if repoDir == "" || repoDir == "." {
		return nil, errors.New("git token store: repository path not configured")
	}
	parentDir := filepath.Dir(repoDir)
	if errMkdir := os.MkdirAll(parentDir, 0o700); errMkdir != nil {
		return nil, fmt.Errorf("git token store: create repository parent: %w", errMkdir)
	}
	root, errRoot := os.OpenRoot(parentDir)
	if errRoot != nil {
		return nil, fmt.Errorf("git token store: open repository parent: %w", errRoot)
	}
	lockName := "." + filepath.Base(repoDir) + ".cliproxy-git-store.lock"
	before, errBefore := root.Lstat(lockName)
	if errBefore != nil && !errors.Is(errBefore, fs.ErrNotExist) {
		_ = root.Close()
		return nil, fmt.Errorf("git token store: inspect repository lock: %w", errBefore)
	}
	if errBefore == nil && (before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular()) {
		_ = root.Close()
		return nil, errors.New("git token store: repository lock is not a regular file")
	}
	file, errOpen := root.OpenFile(lockName, os.O_RDWR|os.O_CREATE, 0o600)
	if errOpen != nil {
		_ = root.Close()
		return nil, fmt.Errorf("git token store: open repository lock: %w", errOpen)
	}
	opened, errOpened := file.Stat()
	after, errAfter := root.Lstat(lockName)
	if errOpened != nil || errAfter != nil || after.Mode()&os.ModeSymlink != 0 || !after.Mode().IsRegular() || !os.SameFile(opened, after) || (errBefore == nil && !os.SameFile(before, opened)) {
		_ = file.Close()
		_ = root.Close()
		return nil, errors.Join(errOpened, errAfter, errors.New("git token store: repository lock changed while opening"))
	}
	unlockFile, errLock := acquireStoreFileLock(file)
	if errLock != nil {
		_ = file.Close()
		_ = root.Close()
		return nil, fmt.Errorf("git token store: lock repository: %w", errLock)
	}
	return func() error {
		return errors.Join(unlockFile(), file.Close(), root.Close())
	}, nil
}

// NewGitTokenStore creates a token store that saves credentials to disk through the
// TokenStorage implementation embedded in the token record.
// When branch is non-empty, clone/pull/push operations target that branch instead of the remote default.
func NewGitTokenStore(remote, username, password, branch string) *GitTokenStore {
	return &GitTokenStore{
		remote:   remote,
		branch:   strings.TrimSpace(branch),
		username: username,
		password: password,
	}
}

// SetBaseDir updates the default directory used for auth JSON persistence when no explicit path is provided.
func (s *GitTokenStore) SetBaseDir(dir string) {
	s.bindingMu.Lock()
	defer s.bindingMu.Unlock()
	s.setBaseDirLocked(dir)
}

func (s *GitTokenStore) setBaseDirLocked(dir string) {
	clean := strings.TrimSpace(dir)
	if clean == "" {
		s.dirLock.Lock()
		s.baseDir = ""
		s.repoDir = ""
		s.configDir = ""
		s.dirLock.Unlock()
		return
	}
	if abs, err := filepath.Abs(clean); err == nil {
		clean = abs
	}
	repoDir := filepath.Dir(clean)
	if repoDir == "" || repoDir == "." {
		repoDir = clean
	}
	configDir := filepath.Join(repoDir, "config")
	s.dirLock.Lock()
	s.baseDir = clean
	s.repoDir = repoDir
	s.configDir = configDir
	s.dirLock.Unlock()
}

// AuthDir returns the directory used for auth persistence.
func (s *GitTokenStore) AuthDir() string {
	return s.baseDirSnapshot()
}

// ConfigPath returns the managed config file path.
func (s *GitTokenStore) ConfigPath() string {
	s.dirLock.RLock()
	defer s.dirLock.RUnlock()
	if s.configDir == "" {
		return ""
	}
	return filepath.Join(s.configDir, "config.yaml")
}

// EnsureRepository prepares the local git working tree by cloning or opening the repository.
func (s *GitTokenStore) EnsureRepository() error {
	s.bindingMu.RLock()
	defer s.bindingMu.RUnlock()
	return s.ensureRepositoryLocked()
}

func (s *GitTokenStore) ensureRepositoryLocked() (resultErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dirLock.Lock()
	if s.remote == "" {
		s.dirLock.Unlock()
		return fmt.Errorf("git token store: remote not configured")
	}
	if s.baseDir == "" {
		s.dirLock.Unlock()
		return fmt.Errorf("git token store: base directory not configured")
	}
	repoDir := s.repoDir
	if repoDir == "" {
		repoDir = filepath.Dir(s.baseDir)
		if repoDir == "" || repoDir == "." {
			repoDir = s.baseDir
		}
		s.repoDir = repoDir
	}
	if s.configDir == "" {
		s.configDir = filepath.Join(repoDir, "config")
	}
	if errPrepare := prepareGitRepositoryDirectory(repoDir); errPrepare != nil {
		s.dirLock.Unlock()
		return errPrepare
	}
	unlockRepository, errRepositoryLock := lockGitRepository(repoDir)
	if errRepositoryLock != nil {
		s.dirLock.Unlock()
		return errRepositoryLock
	}
	defer func() {
		resultErr = errors.Join(resultErr, unlockRepository())
	}()
	authDir := filepath.Join(repoDir, "auths")
	configDir := filepath.Join(repoDir, "config")
	gitDir := filepath.Join(repoDir, ".git")
	authMethod := s.gitAuth()
	var initPaths []string
	if gitInfo, err := os.Lstat(gitDir); errors.Is(err, fs.ErrNotExist) {
		cloneOpts := &git.CloneOptions{Auth: authMethod, URL: s.remote}
		if s.branch != "" {
			cloneOpts.ReferenceName = plumbing.NewBranchReferenceName(s.branch)
		}
		if _, errClone := git.PlainClone(repoDir, cloneOpts); errClone != nil {
			if errors.Is(errClone, transport.ErrEmptyRemoteRepository) {
				_ = os.RemoveAll(gitDir)
				repo, errInit := git.PlainInit(repoDir, false)
				if errInit != nil {
					s.dirLock.Unlock()
					return fmt.Errorf("git token store: init empty repo: %w", errInit)
				}
				if s.branch != "" {
					headRef := plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName(s.branch))
					if errHead := repo.Storer.SetReference(headRef); errHead != nil {
						s.dirLock.Unlock()
						return fmt.Errorf("git token store: set head to branch %s: %w", s.branch, errHead)
					}
				}
				if _, errRemote := repo.Remote("origin"); errRemote != nil {
					if _, errCreate := repo.CreateRemote(&config.RemoteConfig{
						Name: "origin",
						URLs: []string{s.remote},
					}); errCreate != nil && !errors.Is(errCreate, git.ErrRemoteExists) {
						s.dirLock.Unlock()
						return fmt.Errorf("git token store: configure remote: %w", errCreate)
					}
				}
				if err := validateGitAuthFilesystemPath(repoDir, authDir, true, true); err != nil {
					s.dirLock.Unlock()
					return err
				}
				if err := os.MkdirAll(authDir, 0o700); err != nil {
					s.dirLock.Unlock()
					return fmt.Errorf("git token store: create auth dir: %w", err)
				}
				if err := os.MkdirAll(configDir, 0o700); err != nil {
					s.dirLock.Unlock()
					return fmt.Errorf("git token store: create config dir: %w", err)
				}
				if err := ensureEmptyFile(filepath.Join(authDir, ".gitkeep")); err != nil {
					s.dirLock.Unlock()
					return fmt.Errorf("git token store: create auth placeholder: %w", err)
				}
				if err := ensureEmptyFile(filepath.Join(configDir, ".gitkeep")); err != nil {
					s.dirLock.Unlock()
					return fmt.Errorf("git token store: create config placeholder: %w", err)
				}
				initPaths = []string{
					filepath.Join("auths", ".gitkeep"),
					filepath.Join("config", ".gitkeep"),
				}
			} else {
				s.dirLock.Unlock()
				return fmt.Errorf("git token store: clone remote: %w", errClone)
			}
		}
	} else if err != nil {
		s.dirLock.Unlock()
		return fmt.Errorf("git token store: stat repo: %w", err)
	} else if gitInfo.Mode()&os.ModeSymlink != 0 || !gitInfo.IsDir() {
		s.dirLock.Unlock()
		return fmt.Errorf("%w: git metadata path is not a stable directory", errUnsafeGitAuthPath)
	} else {
		repo, errOpen := git.PlainOpen(repoDir)
		if errOpen != nil {
			s.dirLock.Unlock()
			return fmt.Errorf("git token store: open repo: %w", errOpen)
		}
		worktree, errWorktree := repo.Worktree()
		if errWorktree != nil {
			s.dirLock.Unlock()
			return fmt.Errorf("git token store: worktree: %w", errWorktree)
		}
		if s.branch != "" {
			if errCheckout := s.checkoutConfiguredBranch(repo, worktree, authMethod); errCheckout != nil {
				s.dirLock.Unlock()
				return errCheckout
			}
		} else {
			// When branch is unset, ensure the working tree follows the remote default branch
			if err := checkoutRemoteDefaultBranch(repo, worktree, authMethod); err != nil {
				if !shouldFallbackToCurrentBranch(repo, err) {
					s.dirLock.Unlock()
					return fmt.Errorf("git token store: checkout remote default: %w", err)
				}
			}
		}
		pullOpts := &git.PullOptions{Auth: authMethod, RemoteName: "origin"}
		if s.branch != "" {
			pullOpts.ReferenceName = plumbing.NewBranchReferenceName(s.branch)
		}
		if errPull := worktree.Pull(pullOpts); errPull != nil {
			switch {
			case errors.Is(errPull, git.NoErrAlreadyUpToDate),
				errors.Is(errPull, git.ErrUnstagedChanges),
				errors.Is(errPull, git.ErrNonFastForwardUpdate):
				// Ignore clean syncs, local edits, and remote divergence—local changes win.
			case errors.Is(errPull, transport.ErrAuthenticationRequired),
				errors.Is(errPull, transport.ErrEmptyRemoteRepository):
				// Ignore authentication prompts and empty remote references on initial sync.
			case errors.Is(errPull, plumbing.ErrReferenceNotFound):
				if s.branch != "" {
					s.dirLock.Unlock()
					return fmt.Errorf("git token store: pull: %w", errPull)
				}
				// Ignore missing references only when following the remote default branch.
			default:
				s.dirLock.Unlock()
				return fmt.Errorf("git token store: pull: %w", errPull)
			}
		}
	}
	if err := validateGitAuthFilesystemPath(repoDir, s.baseDir, true, true); err != nil {
		s.dirLock.Unlock()
		return err
	}
	if err := os.MkdirAll(s.baseDir, 0o700); err != nil {
		s.dirLock.Unlock()
		return fmt.Errorf("git token store: create auth dir: %w", err)
	}
	if err := validateGitAuthFilesystemPath(repoDir, s.baseDir, false, true); err != nil {
		s.dirLock.Unlock()
		return err
	}
	if err := os.MkdirAll(s.configDir, 0o700); err != nil {
		s.dirLock.Unlock()
		return fmt.Errorf("git token store: create config dir: %w", err)
	}
	if errExclude := ensureGitLocalAuthLockExclusion(repoDir); errExclude != nil {
		s.dirLock.Unlock()
		return errExclude
	}
	s.dirLock.Unlock()
	if len(initPaths) > 0 {
		err := s.commitAndPushLocked("Initialize git token store", initPaths...)
		if err != nil {
			return err
		}
	}
	return nil
}

func prepareGitRepositoryDirectory(repoDir string) error {
	repoDir = filepath.Clean(strings.TrimSpace(repoDir))
	if repoDir == "." || !filepath.IsAbs(repoDir) {
		return fmt.Errorf("%w: repository path must be absolute", errUnsafeGitAuthPath)
	}
	info, errInfo := os.Lstat(repoDir)
	if errors.Is(errInfo, fs.ErrNotExist) {
		if errMkdir := os.MkdirAll(repoDir, 0o700); errMkdir != nil {
			return fmt.Errorf("git token store: create repository directory: %w", errMkdir)
		}
		info, errInfo = os.Lstat(repoDir)
	}
	if errInfo != nil {
		return fmt.Errorf("git token store: inspect repository directory: %w", errInfo)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("%w: repository path is not a stable directory", errUnsafeGitAuthPath)
	}
	gitDir := filepath.Join(repoDir, ".git")
	gitInfo, errGit := os.Lstat(gitDir)
	if errors.Is(errGit, fs.ErrNotExist) {
		return nil
	}
	if errGit != nil {
		return fmt.Errorf("git token store: inspect git directory: %w", errGit)
	}
	if gitInfo.Mode()&os.ModeSymlink != 0 || !gitInfo.IsDir() {
		return fmt.Errorf("%w: git metadata path is not a stable directory", errUnsafeGitAuthPath)
	}
	return nil
}

func ensureGitLocalAuthLockExclusion(repoDir string) (err error) {
	const pattern = ".auth-lock-*"
	root, errRoot := os.OpenRoot(repoDir)
	if errRoot != nil {
		return fmt.Errorf("git token store: open repository root for local excludes: %w", errRoot)
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	if gitInfo, errInfo := root.Lstat(".git"); errInfo != nil || gitInfo.Mode()&os.ModeSymlink != 0 || !gitInfo.IsDir() {
		return fmt.Errorf("git token store: unsafe local git directory")
	}
	if errMkdir := root.MkdirAll(filepath.Join(".git", "info"), 0o700); errMkdir != nil {
		return fmt.Errorf("git token store: create local exclude directory: %w", errMkdir)
	}
	if info, errInfo := root.Lstat(filepath.Join(".git", "info")); errInfo != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("git token store: unsafe local exclude directory")
	}
	path := filepath.Join(".git", "info", "exclude")
	if info, errInfo := root.Lstat(path); errInfo == nil && info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("git token store: local exclude file is a symlink")
	} else if errInfo != nil && !errors.Is(errInfo, fs.ErrNotExist) {
		return fmt.Errorf("git token store: inspect local excludes: %w", errInfo)
	}
	data, errRead := root.ReadFile(path)
	if errRead != nil && !errors.Is(errRead, fs.ErrNotExist) {
		return fmt.Errorf("git token store: read local excludes: %w", errRead)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) == pattern {
			return nil
		}
	}
	file, errOpen := root.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if errOpen != nil {
		return fmt.Errorf("git token store: open local excludes: %w", errOpen)
	}
	defer func() {
		err = errors.Join(err, file.Close())
	}()
	if len(data) > 0 && data[len(data)-1] != '\n' {
		if _, errWrite := file.WriteString("\n"); errWrite != nil {
			return fmt.Errorf("git token store: separate local exclude: %w", errWrite)
		}
	}
	if _, errWrite := file.WriteString(pattern + "\n"); errWrite != nil {
		return fmt.Errorf("git token store: write local exclude: %w", errWrite)
	}
	if errSync := file.Sync(); errSync != nil {
		return fmt.Errorf("git token store: sync local excludes: %w", errSync)
	}
	return nil
}

// Save persists token storage and metadata to the resolved auth file path.
func (s *GitTokenStore) Save(_ context.Context, auth *cliproxyauth.Auth) (string, error) {
	if auth == nil {
		return "", fmt.Errorf("auth filestore: auth is nil")
	}
	if cliproxyauth.IsRetiredGeminiCLIAuth(auth) {
		cliproxyauth.WarnRetiredGeminiCLIAuthIgnored()
		return "", fmt.Errorf("auth filestore: %w", cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly)
	}
	s.bindingMu.RLock()
	defer s.bindingMu.RUnlock()

	path, err := s.resolveAuthPath(auth)
	if err != nil {
		return "", err
	}
	if path == "" {
		return "", fmt.Errorf("auth filestore: missing file path attribute for %s", auth.ID)
	}
	if errValidate := validateGitAuthDirectoryTree(s.repoDirSnapshot(), s.baseDirSnapshot(), true); errValidate != nil {
		return "", errValidate
	}

	if err = s.ensureRepositoryLocked(); err != nil {
		return "", err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	unlockRepository, errRepositoryLock := lockGitRepository(s.repoDirSnapshot())
	if errRepositoryLock != nil {
		return "", errRepositoryLock
	}
	defer func() {
		if errUnlock := unlockRepository(); errUnlock != nil {
			log.WithError(errUnlock).Error("git token store: unlock repository after save")
		}
	}()
	unlockPath := authfileguard.Lock(path)
	defer unlockPath()
	repoDir := s.repoDirSnapshot()
	baseDir := s.baseDirSnapshot()
	if errValidate := validateGitAuthDirectoryTree(repoDir, baseDir, false); errValidate != nil {
		return "", errValidate
	}
	if _, errTarget := authFileNameAtBaseDir(baseDir, path); errTarget != nil {
		return "", fmt.Errorf("%w: %v", errUnsafeGitAuthPath, errTarget)
	}
	if errValidate := validateGitAuthFilesystemPath(repoDir, baseDir, false, true); errValidate != nil {
		return "", errValidate
	}
	if errValidate := validateGitAuthFilesystemPath(repoDir, path, true, false); errValidate != nil {
		return "", errValidate
	}
	if auth.Disabled {
		if _, statErr := os.Lstat(path); errors.Is(statErr, fs.ErrNotExist) {
			return "", nil
		} else if statErr != nil {
			return "", fmt.Errorf("auth filestore: inspect disabled auth path: %w", statErr)
		}
	}
	if authfileguard.IsRetired(path) {
		return "", fmt.Errorf("auth filestore: %w", cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly)
	}
	if authfileguard.IsQuarantined(path) {
		return "", fmt.Errorf("auth filestore: auth deletion is still pending: %w", authfileguard.ErrDeleteGenerationUncertain)
	}

	if existing, errRead := os.ReadFile(path); errRead == nil {
		if errRetired := cliproxyauth.RejectRetiredGeminiCLIAuthFileMutation(existing); errRetired != nil {
			if errors.Is(errRetired, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
				authfileguard.MarkRetired(path)
			}
			return "", fmt.Errorf("auth filestore: %w", errRetired)
		}
	} else if !os.IsNotExist(errRead) {
		return "", fmt.Errorf("auth filestore: read existing failed: %w", errRead)
	}
	relExisting, errRelExisting := s.relativeToRepo(path)
	if errRelExisting != nil {
		return "", errRelExisting
	}
	remoteState, errRemote := s.remoteBranchPreconditionLocked()
	if errRemote != nil {
		return "", errRemote
	}
	localHead, errLocalHead := captureGitLocalHead(s.repoDirSnapshot())
	if errLocalHead != nil {
		return "", errLocalHead
	}
	if errRetired := rejectRetiredGeminiCLIAuthGitRemoteMutation(s.repoDirSnapshot(), remoteState, relExisting); errRetired != nil {
		if errors.Is(errRetired, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
			authfileguard.MarkRetired(path)
		}
		return "", errRetired
	}

	if err = os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", fmt.Errorf("auth filestore: create dir failed: %w", err)
	}
	if errValidate := validateGitAuthFilesystemPath(repoDir, filepath.Dir(path), false, true); errValidate != nil {
		return "", errValidate
	}
	localSnapshot, errSnapshot := captureAuthFileSnapshotAtPath(path)
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
			return "", fmt.Errorf("auth filestore: produce storage auth failed: %w", errData)
		}
		if errWrite := writeAuthFileAtomicallyForSnapshot(path, data, &localSnapshot); errWrite != nil {
			return "", errWrite
		}
		persistedData = data
		if errSync := cliproxyauth.SyncPersistedMetadataAndSourceHash(auth, data); errSync != nil {
			return "", fmt.Errorf("auth filestore: sync persisted storage auth failed: %w", errSync)
		}
	case auth.Metadata != nil:
		raw, errMarshal := cliproxyauth.CanonicalMetadataBytes(auth)
		if errMarshal != nil {
			return "", fmt.Errorf("auth filestore: canonicalize metadata failed: %w", errMarshal)
		}
		writeLocal := true
		persistedData = raw
		if existing, errRead := os.ReadFile(path); errRead == nil {
			if jsonEqual(existing, raw) {
				if !localSnapshot.exists || !bytes.Equal(existing, localSnapshot.data) {
					return "", authfileguard.ErrPersistGenerationStale
				}
				writeLocal = false
				persistedData = localSnapshot.data
			}
		} else if !os.IsNotExist(errRead) {
			return "", fmt.Errorf("auth filestore: read existing failed: %w", errRead)
		}
		if writeLocal {
			if errWrite := writeAuthFileAtomicallyForSnapshot(path, raw, &localSnapshot); errWrite != nil {
				return "", errWrite
			}
		}
		if errValidate := validateGitAuthFilesystemPath(repoDir, path, false, false); errValidate != nil {
			return "", errValidate
		}
		cliproxyauth.SetSourceHashAttribute(auth, raw)
	default:
		return "", fmt.Errorf("auth filestore: nothing to persist for %s", auth.ID)
	}

	if auth.Attributes == nil {
		auth.Attributes = make(map[string]string)
	}
	auth.Attributes["path"] = path

	if strings.TrimSpace(auth.FileName) == "" {
		auth.FileName = auth.ID
	}

	relPath, errRel := s.relativeToRepo(path)
	if errRel != nil {
		return "", errRel
	}
	messageID := auth.ID
	if strings.TrimSpace(messageID) == "" {
		messageID = filepath.Base(path)
	}
	if errCommit := s.commitAndPushAgainstRemoteLocked(fmt.Sprintf("Update auth %s", strings.TrimSpace(messageID)), remoteState, relPath); errCommit != nil {
		remoteCommitted, errProbe := s.remoteBranchMatchesLocalHeadLocked()
		if errProbe == nil && remoteCommitted {
			return path, nil
		}
		errRollback := restoreAuthFileSnapshotAtPath(path, persistedData, localSnapshot)
		errReset := s.resetGitWorkspaceAfterFailedSaveLocked(remoteState, localHead, relPath)
		runtimeSnapshot.restore(auth)
		return "", errors.Join(
			errCommit,
			wrapOptionalError("git token store: verify auth after push failure", errProbe),
			wrapOptionalError("git token store: roll back local auth after push failure", errRollback),
			wrapOptionalError("git token store: reset local repository after push failure", errReset),
		)
	}

	return path, nil
}

// List enumerates all auth JSON files under the configured directory.
func (s *GitTokenStore) List(_ context.Context) ([]*cliproxyauth.Auth, error) {
	s.bindingMu.RLock()
	defer s.bindingMu.RUnlock()
	if repoDir, baseDir := s.repoDirSnapshot(), s.baseDirSnapshot(); repoDir != "" && baseDir != "" {
		if errValidate := validateGitAuthDirectoryTree(repoDir, baseDir, true); errValidate != nil {
			return nil, errValidate
		}
	}
	if err := s.ensureRepositoryLocked(); err != nil {
		return nil, err
	}
	dir := s.baseDirSnapshot()
	if dir == "" {
		return nil, fmt.Errorf("auth filestore: directory not configured")
	}
	if errValidate := validateGitAuthFilesystemPath(s.repoDirSnapshot(), dir, false, true); errValidate != nil {
		return nil, errValidate
	}
	entries := make([]*cliproxyauth.Auth, 0)
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, errInfo := d.Info()
		if errInfo != nil {
			return errInfo
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: %s is a symbolic link", errUnsafeGitAuthPath, path)
		}
		if d.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("%w: %s is not a regular file", errUnsafeGitAuthPath, path)
		}
		if !strings.HasSuffix(strings.ToLower(d.Name()), ".json") {
			return nil
		}
		auth, err := s.readAuthFile(path, dir)
		if err != nil {
			if errors.Is(err, errUnsafeGitAuthPath) {
				return err
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
func (s *GitTokenStore) Delete(ctx context.Context, id string) error {
	baseDir := s.baseDirSnapshot()
	if strings.TrimSpace(baseDir) == "" {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth filestore: directory not configured"))
	}
	return s.authFileDeletionAtBaseDir(ctx, baseDir, nil, id, true)
}

// FinalizeAuthFileDeletionAtBaseDir commits and pushes a deletion using an immutable auth directory snapshot.
func (s *GitTokenStore) FinalizeAuthFileDeletionAtBaseDir(ctx context.Context, baseDir string, id string) error {
	return s.authFileDeletionAtBaseDir(ctx, baseDir, nil, id, false)
}

// DeleteAuthFileAtRoot removes, commits, and pushes one auth file while holding
// the store binding lock against concurrent saves and preserving root safety.
func (s *GitTokenStore) DeleteAuthFileAtRoot(ctx context.Context, baseDir string, root *os.Root, id string) error {
	if root == nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth filestore: root is nil"))
	}
	return s.authFileDeletionAtBaseDir(ctx, baseDir, root, id, true)
}

func (s *GitTokenStore) authFileDeletionAtBaseDir(ctx context.Context, baseDir string, root *os.Root, id string, remove bool) error {
	s.bindingMu.Lock()
	defer s.bindingMu.Unlock()
	cleanID, errID := authFileNameAtBaseDir(baseDir, id)
	if errID != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errID)
	}
	previousBaseDir := s.baseDirSnapshot()
	s.setBaseDirLocked(baseDir)
	defer s.setBaseDirLocked(previousBaseDir)
	if err := s.ensureRepositoryLocked(); err != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, err)
	}
	path := filepath.Join(s.baseDirSnapshot(), cleanID)
	rel, errRel := s.relativeToRepo(path)
	if errRel != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errRel)
	}
	if remove && root == nil {
		var errRoot error
		root, errRoot = os.OpenRoot(s.baseDirSnapshot())
		if errRoot != nil {
			return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth filestore: open auth root: %w", errRoot))
		}
		defer func() {
			if errClose := root.Close(); errClose != nil {
				log.WithError(errClose).Error("git token store: close auth root after deletion")
			}
		}()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlockRepository, errRepositoryLock := lockGitRepository(s.repoDirSnapshot())
	if errRepositoryLock != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errRepositoryLock)
	}
	defer func() {
		if errUnlock := unlockRepository(); errUnlock != nil {
			log.WithError(errUnlock).Error("git token store: unlock repository after deletion")
		}
	}()
	unlockPath := authfileguard.Lock(path)
	defer unlockPath()
	retiredSnapshot := authfileguard.CaptureRetired(path)
	remoteState, errRemote := s.remoteBranchPreconditionLocked()
	if errRemote != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errRemote)
	}
	originalRemoteBlob, errBlob := readGitRemoteAuthBlob(s.repoDirSnapshot(), remoteState, rel)
	if errBlob != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errBlob)
	}
	deleteRemote := func() error {
		return s.commitAndPushAgainstRemoteWithSnapshotsLocked(
			fmt.Sprintf("Delete auth %s", filepath.ToSlash(rel)),
			remoteState,
			map[string]authFileSnapshot{rel: {}},
			rel,
		)
	}
	if !remove {
		return s.finalizeRetiredAuthDeletionAgainstRemoteLocked(ctx, path, rel, remoteState, retiredSnapshot)
	}
	remoteIdentity := "missing"
	if remoteState.exists {
		remoteIdentity = remoteState.branch.String() + "@" + remoteState.hash.String()
	}
	localSnapshot, errLocalSnapshot := captureAuthFileSnapshot(root, cleanID)
	if errLocalSnapshot != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errLocalSnapshot)
	}
	_, prepareDelete, clearDelete := durableAuthDelete(
		ctx,
		s.ConfigPath(),
		s.baseDirSnapshot(),
		path,
		localSnapshot.data,
		"git:"+filepath.ToSlash(rel),
		remoteIdentity,
		true,
		originalRemoteBlob.exists,
		originalRemoteBlob.data,
	)
	errDelete := deleteAuthFileTransaction(root, cleanID, func(original authFileSnapshot) error {
		if !sameAuthFileGeneration(original, localSnapshot) {
			return authfileguard.ErrPersistGenerationStale
		}
		return prepareDelete()
	}, deleteRemote, func() (authDeleteProbeState, error) {
		return s.remoteAuthBlobDeleteStateLocked(rel, originalRemoteBlob)
	})
	errDelete = finishDurableAuthDelete(errDelete, clearDelete)
	if deleteOutcomeIsCommitted(errDelete) {
		authfileguard.ClearRetiredSnapshot(retiredSnapshot)
	}
	return errDelete
}

func (s *GitTokenStore) finalizeRetiredAuthDeletionAgainstRemoteLocked(ctx context.Context, path, rel string, remoteState gitRemotePrecondition, retiredSnapshot authfileguard.RetiredSnapshot) error {
	remoteBlob, errBlob := readGitRemoteAuthBlob(s.repoDirSnapshot(), remoteState, rel)
	if errBlob != nil {
		return errBlob
	}
	if !remoteBlob.exists || !cliproxyauth.IsRetiredGeminiCLIAuthFileData(remoteBlob.data) {
		authfileguard.ClearRetiredSnapshot(retiredSnapshot)
		return nil
	}
	remoteIdentity := "missing"
	if remoteState.exists {
		remoteIdentity = remoteState.branch.String() + "@" + remoteState.hash.String()
	}
	switch matchExpectedAuthDeleteGeneration(ctx, "git:"+filepath.ToSlash(rel), remoteIdentity, true, remoteBlob.data) {
	case authDeleteGenerationUncertain, authDeleteGenerationReplaced:
		return authfileguard.ErrDeleteGenerationUncertain
	}
	errDelete := s.commitAndPushAgainstRemoteWithSnapshotsLocked(
		fmt.Sprintf("Delete auth %s", filepath.ToSlash(rel)),
		remoteState,
		map[string]authFileSnapshot{rel: {}},
		rel,
	)
	if errDelete == nil {
		authfileguard.ClearRetiredSnapshot(retiredSnapshot)
		return nil
	}
	remoteDeleteState, errProbe := s.remoteAuthBlobDeleteStateLocked(rel, remoteBlob)
	if errProbe != nil {
		return cliproxyauth.NewDeleteOutcomeError(
			cliproxyauth.DeleteOutcomeUncertain,
			errors.Join(errDelete, fmt.Errorf("git token store: verify retired auth deletion: %w", errProbe)),
		)
	}
	if remoteDeleteState == authDeleteProbeAbsent {
		authfileguard.ClearRetiredSnapshot(retiredSnapshot)
		return nil
	}
	return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeUncertain, errDelete)
}

func authFileNameAtBaseDir(baseDir, id string) (string, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return "", fmt.Errorf("auth filestore: id is empty")
	}
	cleanID := filepath.Clean(filepath.FromSlash(id))
	if filepath.IsAbs(cleanID) {
		rel, errRel := filepath.Rel(filepath.Clean(baseDir), cleanID)
		if errRel != nil {
			return "", fmt.Errorf("auth filestore: resolve auth identifier %s: %w", id, errRel)
		}
		cleanID = rel
	}
	if cleanID == "." || cleanID == ".." || strings.HasPrefix(cleanID, ".."+string(os.PathSeparator)) || filepath.IsAbs(cleanID) {
		return "", fmt.Errorf("auth filestore: invalid auth identifier %s", id)
	}
	return cleanID, nil
}

func (s *GitTokenStore) remoteAuthBlobDeleteStateLocked(rel string, original gitRemoteAuthBlob) (authDeleteProbeState, error) {
	remoteState, errRemoteState := s.remoteBranchPreconditionLocked()
	if errRemoteState != nil {
		return authDeleteProbeOriginal, fmt.Errorf("git token store: refresh remote for delete verification: %w", errRemoteState)
	}
	current, errBlob := readGitRemoteAuthBlob(s.repoDirSnapshot(), remoteState, rel)
	if errBlob != nil {
		return authDeleteProbeOriginal, fmt.Errorf("git token store: inspect remote auth during delete verification: %w", errBlob)
	}
	if !current.exists {
		return authDeleteProbeAbsent, nil
	}
	if original.exists && current.mode == original.mode && bytes.Equal(current.data, original.data) {
		return authDeleteProbeOriginal, nil
	}
	return authDeleteProbeReplaced, nil
}

func (s *GitTokenStore) remoteBranchMatchesLocalHeadLocked() (bool, error) {
	localHead, errLocalHead := captureGitLocalHead(s.repoDirSnapshot())
	if errLocalHead != nil {
		return false, errLocalHead
	}
	remoteState, errRemoteState := s.remoteBranchPreconditionLocked()
	if errRemoteState != nil {
		return false, fmt.Errorf("git token store: refresh remote for save verification: %w", errRemoteState)
	}
	return localHead.exists && remoteState.exists && localHead.name == remoteState.branch && localHead.hash == remoteState.hash, nil
}

// PersistAuthFiles commits and pushes the provided paths to the remote repository.
// It no-ops when the store is not fully configured or when there are no paths.
func (s *GitTokenStore) PersistAuthFiles(ctx context.Context, message string, paths ...string) error {
	if len(paths) == 0 {
		return nil
	}
	s.bindingMu.RLock()
	defer s.bindingMu.RUnlock()
	if err := s.ensureRepositoryLocked(); err != nil {
		return err
	}

	filtered := make([]string, 0, len(paths))
	for _, p := range paths {
		trimmed := strings.TrimSpace(p)
		if trimmed == "" {
			continue
		}
		rel, err := s.relativeToRepo(trimmed)
		if err != nil {
			return err
		}
		filtered = append(filtered, rel)
	}
	if len(filtered) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	unlockRepository, errRepositoryLock := lockGitRepository(s.repoDirSnapshot())
	if errRepositoryLock != nil {
		return errRepositoryLock
	}
	defer func() {
		if errUnlock := unlockRepository(); errUnlock != nil {
			log.WithError(errUnlock).Error("git token store: unlock repository after auth persistence")
		}
	}()
	repoDir := s.repoDirSnapshot()
	root, errRoot := os.OpenRoot(repoDir)
	if errRoot != nil {
		return fmt.Errorf("git token store: open repository root for auth persistence: %w", errRoot)
	}
	defer func() {
		if errClose := root.Close(); errClose != nil {
			log.WithError(errClose).Error("git token store: close repository root after auth persistence")
		}
	}()
	remoteState, errRemote := s.remoteBranchPreconditionLocked()
	if errRemote != nil {
		return errRemote
	}
	snapshots := make(map[string]authFileSnapshot, len(filtered))
	pathsByRel := make(map[string]string, len(filtered))
	replacementPaths := make([]string, 0, len(filtered))
	persistPaths := make([]string, 0, len(filtered))
	for _, rel := range filtered {
		path := filepath.Join(repoDir, filepath.FromSlash(rel))
		unlockPath := authfileguard.Lock(path)
		snapshot, errSnapshot := captureAuthFileSnapshot(root, filepath.FromSlash(rel))
		if errSnapshot == nil {
			errSnapshot = authfileguard.ValidatePersistSnapshot(ctx, snapshot.data, snapshot.exists)
		}
		if errSnapshot == nil {
			errSnapshot = snapshot.rejectRetiredGeminiCLIAuthPersistence()
		}
		if errors.Is(errSnapshot, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
			authfileguard.MarkRetired(path)
			remoteBlob, errBlob := readGitRemoteAuthBlob(repoDir, remoteState, rel)
			if errBlob != nil {
				unlockPath()
				return errBlob
			}
			if remoteBlob.exists && !cliproxyauth.IsRetiredGeminiCLIAuthFileData(remoteBlob.data) {
				errSnapshot = removeAuthFileAtRoot(root, filepath.FromSlash(rel))
				unlockPath()
				if errSnapshot != nil {
					return errSnapshot
				}
				continue
			}
		}
		unlockPath()
		if errSnapshot != nil {
			return errSnapshot
		}
		if !snapshot.exists {
			if authfileguard.DeleteGenerationFromContext(ctx) != nil {
				remoteBlob, errBlob := readGitRemoteAuthBlob(repoDir, remoteState, rel)
				if errBlob != nil {
					return errBlob
				}
				if !remoteBlob.exists {
					continue
				}
				identity := "missing"
				if remoteState.exists {
					identity = remoteState.branch.String() + "@" + remoteState.hash.String()
				}
				switch matchExpectedAuthDeleteGeneration(ctx, "git:"+filepath.ToSlash(rel), identity, true, remoteBlob.data) {
				case authDeleteGenerationUncertain:
					return authfileguard.ErrDeleteGenerationUncertain
				case authDeleteGenerationReplaced:
					if !remoteBlob.exists {
						continue
					}
					if errRestore := writeAuthFileAtomicallyForSnapshot(path, remoteBlob.data, &snapshot); errRestore != nil {
						return fmt.Errorf("git token store: restore remote auth replacement %s: %w", rel, errRestore)
					}
					continue
				}
			}
		}
		snapshots[rel] = snapshot
		persistPaths = append(persistPaths, rel)
		pathsByRel[rel] = path
		if snapshot.exists {
			replacementPaths = append(replacementPaths, rel)
		}
	}
	if len(persistPaths) == 0 {
		return nil
	}
	for _, rel := range replacementPaths {
		if errRetired := rejectRetiredGeminiCLIAuthGitRemoteMutation(repoDir, remoteState, rel); errRetired != nil {
			if errors.Is(errRetired, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
				authfileguard.MarkRetired(pathsByRel[rel])
			}
			return errRetired
		}
	}

	if strings.TrimSpace(message) == "" {
		message = "Sync watcher updates"
	}
	return s.commitAndPushAgainstRemoteWithSnapshotsLocked(message, remoteState, snapshots, persistPaths...)
}

func rejectRetiredGeminiCLIAuthGitRemoteMutation(repoDir string, remoteState gitRemotePrecondition, rel string) error {
	remoteBlob, errBlob := readGitRemoteAuthBlob(repoDir, remoteState, rel)
	if errBlob != nil {
		return errBlob
	}
	if !remoteBlob.exists {
		return nil
	}
	if errRetired := cliproxyauth.RejectRetiredGeminiCLIAuthFileMutation(remoteBlob.data); errRetired != nil {
		return fmt.Errorf("git token store: %w", errRetired)
	}
	return nil
}

func readGitRemoteAuthBlob(repoDir string, remoteState gitRemotePrecondition, rel string) (gitRemoteAuthBlob, error) {
	if !remoteState.exists {
		return gitRemoteAuthBlob{}, nil
	}
	repo, errOpen := git.PlainOpen(repoDir)
	if errOpen != nil {
		return gitRemoteAuthBlob{}, fmt.Errorf("git token store: inspect existing auth: %w", errOpen)
	}
	commit, errCommit := repo.CommitObject(remoteState.hash)
	if errCommit != nil {
		return gitRemoteAuthBlob{}, fmt.Errorf("git token store: inspect remote auth commit: %w", errCommit)
	}
	tree, errTree := commit.Tree()
	if errTree != nil {
		return gitRemoteAuthBlob{}, fmt.Errorf("git token store: inspect remote auth tree: %w", errTree)
	}
	entry, exists, errEntry := findGitTreeEntry(tree, filepath.ToSlash(rel))
	if errEntry != nil {
		return gitRemoteAuthBlob{}, errEntry
	}
	if !exists {
		return gitRemoteAuthBlob{}, nil
	}
	if !isGitRegularBlobMode(entry.Mode) {
		return gitRemoteAuthBlob{}, fmt.Errorf("%w: remote %s has mode %s", errUnsafeGitAuthPath, filepath.ToSlash(rel), entry.Mode)
	}
	file, errFile := tree.TreeEntryFile(entry)
	if errFile != nil {
		return gitRemoteAuthBlob{}, fmt.Errorf("git token store: open remote auth path: %w", errFile)
	}
	contents, errContents := file.Contents()
	if errContents != nil {
		return gitRemoteAuthBlob{}, fmt.Errorf("git token store: read remote auth path: %w", errContents)
	}
	return gitRemoteAuthBlob{data: []byte(contents), mode: entry.Mode, exists: true}, nil
}

func findGitTreeEntry(tree *object.Tree, rel string) (*object.TreeEntry, bool, error) {
	if tree == nil {
		return nil, false, fmt.Errorf("git token store: remote auth tree is nil")
	}
	clean := strings.Trim(filepath.ToSlash(rel), "/")
	if clean == "" {
		return nil, false, fmt.Errorf("%w: remote auth path is empty", errUnsafeGitAuthPath)
	}
	parts := strings.Split(clean, "/")
	current := tree
	for indexPart, part := range parts {
		if part == "" || part == "." || part == ".." {
			return nil, false, fmt.Errorf("%w: invalid remote auth path %s", errUnsafeGitAuthPath, rel)
		}
		var entry *object.TreeEntry
		for indexEntry := range current.Entries {
			if current.Entries[indexEntry].Name == part {
				entryCopy := current.Entries[indexEntry]
				entry = &entryCopy
				break
			}
		}
		if entry == nil {
			return nil, false, nil
		}
		if indexPart == len(parts)-1 {
			return entry, true, nil
		}
		if entry.Mode != filemode.Dir {
			return nil, false, fmt.Errorf("%w: remote path component %s has mode %s", errUnsafeGitAuthPath, part, entry.Mode)
		}
		next, errNext := current.Tree(part)
		if errNext != nil {
			return nil, false, fmt.Errorf("git token store: read remote auth directory %s: %w", part, errNext)
		}
		current = next
	}
	return nil, false, nil
}

func isGitRegularBlobMode(mode filemode.FileMode) bool {
	return mode.IsRegular() || mode == filemode.Executable
}

func validateGitAuthFilesystemPath(repoDir, target string, allowMissing, wantDirectory bool) error {
	repoDir = filepath.Clean(repoDir)
	target = filepath.Clean(target)
	if repoDir == "." || target == "." || !filepath.IsAbs(repoDir) || !filepath.IsAbs(target) {
		return fmt.Errorf("%w: repository and auth paths must be absolute", errUnsafeGitAuthPath)
	}
	rel, errRel := filepath.Rel(repoDir, target)
	if errRel != nil {
		return fmt.Errorf("%w: resolve %s against repository: %v", errUnsafeGitAuthPath, target, errRel)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || filepath.IsAbs(rel) {
		return fmt.Errorf("%w: %s is outside repository %s", errUnsafeGitAuthPath, target, repoDir)
	}

	components := []string{repoDir}
	if rel != "." {
		current := repoDir
		for _, component := range strings.Split(rel, string(os.PathSeparator)) {
			current = filepath.Join(current, component)
			components = append(components, current)
		}
	}
	for index, component := range components {
		info, errInfo := os.Lstat(component)
		if errInfo != nil {
			if allowMissing && errors.Is(errInfo, fs.ErrNotExist) {
				return nil
			}
			return fmt.Errorf("git token store: inspect auth path component %s: %w", component, errInfo)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: %s is a symbolic link", errUnsafeGitAuthPath, component)
		}
		leaf := index == len(components)-1
		if !leaf || wantDirectory {
			if !info.IsDir() {
				return fmt.Errorf("%w: %s is not a directory", errUnsafeGitAuthPath, component)
			}
			continue
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("%w: %s is not a regular file", errUnsafeGitAuthPath, component)
		}
	}
	return nil
}

func validateGitAuthDirectoryTree(repoDir, authDir string, allowMissing bool) error {
	if errValidate := validateGitAuthFilesystemPath(repoDir, authDir, allowMissing, true); errValidate != nil {
		return errValidate
	}
	errWalk := filepath.WalkDir(authDir, func(path string, entry fs.DirEntry, errWalk error) error {
		if errWalk != nil {
			return errWalk
		}
		info, errInfo := entry.Info()
		if errInfo != nil {
			return errInfo
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: %s is a symbolic link", errUnsafeGitAuthPath, path)
		}
		if entry.IsDir() || info.Mode().IsRegular() {
			return nil
		}
		return fmt.Errorf("%w: %s is not a regular file", errUnsafeGitAuthPath, path)
	})
	if allowMissing && errors.Is(errWalk, fs.ErrNotExist) {
		return nil
	}
	return errWalk
}

func (s *GitTokenStore) readAuthFile(path, baseDir string) (*cliproxyauth.Auth, error) {
	unlockPath := authfileguard.Lock(path)
	defer unlockPath()
	repoDir := s.repoDirSnapshot()
	if repoDir == "" {
		repoDir = filepath.Dir(filepath.Clean(baseDir))
	}
	if _, errTarget := authFileNameAtBaseDir(baseDir, path); errTarget != nil {
		return nil, fmt.Errorf("%w: %v", errUnsafeGitAuthPath, errTarget)
	}
	if errValidate := validateGitAuthFilesystemPath(repoDir, baseDir, false, true); errValidate != nil {
		return nil, errValidate
	}
	if errValidate := validateGitAuthFilesystemPath(repoDir, path, false, false); errValidate != nil {
		return nil, errValidate
	}
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
	provider, _ := metadata["type"].(string)
	if provider == "" {
		provider = "unknown"
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("stat file: %w", err)
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
	if email, ok := metadata["email"].(string); ok && email != "" {
		auth.Attributes["email"] = email
	}
	cliproxyauth.ApplyCustomHeadersFromMetadata(auth)
	return auth, nil
}

func (s *GitTokenStore) idFor(path, baseDir string) string {
	if baseDir == "" {
		return path
	}
	rel, err := filepath.Rel(baseDir, path)
	if err != nil {
		return path
	}
	return rel
}

func (s *GitTokenStore) resolveAuthPath(auth *cliproxyauth.Auth) (string, error) {
	if auth == nil {
		return "", fmt.Errorf("auth filestore: auth is nil")
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
		if dir := s.baseDirSnapshot(); dir != "" {
			return filepath.Join(dir, fileName), nil
		}
		return fileName, nil
	}
	if auth.ID == "" {
		return "", fmt.Errorf("auth filestore: missing id")
	}
	if filepath.IsAbs(auth.ID) {
		return auth.ID, nil
	}
	dir := s.baseDirSnapshot()
	if dir == "" {
		return "", fmt.Errorf("auth filestore: directory not configured")
	}
	return filepath.Join(dir, auth.ID), nil
}

func (s *GitTokenStore) labelFor(metadata map[string]any) string {
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

func (s *GitTokenStore) baseDirSnapshot() string {
	s.dirLock.RLock()
	defer s.dirLock.RUnlock()
	return s.baseDir
}

func (s *GitTokenStore) repoDirSnapshot() string {
	s.dirLock.RLock()
	defer s.dirLock.RUnlock()
	return s.repoDir
}

func (s *GitTokenStore) gitAuth() transport.AuthMethod {
	if s.username == "" && s.password == "" {
		return nil
	}
	user := s.username
	if user == "" {
		user = "git"
	}
	return &http.BasicAuth{Username: user, Password: s.password}
}

func (s *GitTokenStore) relativeToRepo(path string) (string, error) {
	repoDir := s.repoDirSnapshot()
	if repoDir == "" {
		return "", fmt.Errorf("git token store: repository path not configured")
	}
	absRepo := repoDir
	if abs, err := filepath.Abs(repoDir); err == nil {
		absRepo = abs
	}
	cleanPath := path
	if abs, err := filepath.Abs(path); err == nil {
		cleanPath = abs
	}
	rel, err := filepath.Rel(absRepo, cleanPath)
	if err != nil {
		return "", fmt.Errorf("git token store: relative path: %w", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("git token store: path outside repository")
	}
	return rel, nil
}

func (s *GitTokenStore) checkoutConfiguredBranch(repo *git.Repository, worktree *git.Worktree, authMethod transport.AuthMethod) error {
	branchRefName := plumbing.NewBranchReferenceName(s.branch)
	headRef, errHead := repo.Head()
	switch {
	case errHead == nil && headRef.Name() == branchRefName:
		return nil
	case errHead != nil && !errors.Is(errHead, plumbing.ErrReferenceNotFound):
		return fmt.Errorf("git token store: get head: %w", errHead)
	}

	if err := worktree.Checkout(&git.CheckoutOptions{Branch: branchRefName}); err == nil {
		return nil
	} else if _, errRef := repo.Reference(branchRefName, true); errRef == nil {
		return fmt.Errorf("git token store: checkout branch %s: %w", s.branch, err)
	} else if !errors.Is(errRef, plumbing.ErrReferenceNotFound) {
		return fmt.Errorf("git token store: inspect branch %s: %w", s.branch, errRef)
	} else if err := s.checkoutConfiguredRemoteTrackingBranch(repo, worktree, branchRefName, authMethod); err != nil {
		return fmt.Errorf("git token store: checkout branch %s: %w", s.branch, err)
	}

	return nil
}

func (s *GitTokenStore) checkoutConfiguredRemoteTrackingBranch(repo *git.Repository, worktree *git.Worktree, branchRefName plumbing.ReferenceName, authMethod transport.AuthMethod) error {
	remoteRefName := plumbing.ReferenceName("refs/remotes/origin/" + s.branch)
	remoteRef, err := repo.Reference(remoteRefName, true)
	if errors.Is(err, plumbing.ErrReferenceNotFound) {
		if errSync := syncRemoteReferences(repo, authMethod); errSync != nil {
			return fmt.Errorf("sync remote refs: %w", errSync)
		}
		remoteRef, err = repo.Reference(remoteRefName, true)
	}
	if err != nil {
		return err
	}
	if err := worktree.Checkout(&git.CheckoutOptions{Branch: branchRefName, Create: true, Hash: remoteRef.Hash()}); err != nil {
		return err
	}

	cfg, err := repo.Config()
	if err != nil {
		return fmt.Errorf("git token store: repo config: %w", err)
	}
	if _, ok := cfg.Branches[s.branch]; !ok {
		cfg.Branches[s.branch] = &config.Branch{Name: s.branch}
	}
	cfg.Branches[s.branch].Remote = "origin"
	cfg.Branches[s.branch].Merge = branchRefName
	if err := repo.SetConfig(cfg); err != nil {
		return fmt.Errorf("git token store: set branch config: %w", err)
	}
	return nil
}

func syncRemoteReferences(repo *git.Repository, authMethod transport.AuthMethod) error {
	if err := repo.Fetch(&git.FetchOptions{Auth: authMethod, RemoteName: "origin"}); err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
		return err
	}
	return nil
}

// resolveRemoteDefaultBranch queries the origin remote to determine the remote's default branch
// (the target of HEAD) and returns the corresponding local branch reference name (e.g. refs/heads/master).
func resolveRemoteDefaultBranch(repo *git.Repository, authMethod transport.AuthMethod) (resolvedRemoteBranch, error) {
	if err := syncRemoteReferences(repo, authMethod); err != nil {
		return resolvedRemoteBranch{}, fmt.Errorf("resolve remote default: sync remote refs: %w", err)
	}
	remote, err := repo.Remote("origin")
	if err != nil {
		return resolvedRemoteBranch{}, fmt.Errorf("resolve remote default: get remote: %w", err)
	}
	refs, err := remote.List(&git.ListOptions{Auth: authMethod})
	if err != nil {
		if resolved, ok := resolveRemoteDefaultBranchFromLocal(repo); ok {
			return resolved, nil
		}
		return resolvedRemoteBranch{}, fmt.Errorf("resolve remote default: list remote refs: %w", err)
	}
	for _, r := range refs {
		if r.Name() == plumbing.HEAD {
			if r.Type() == plumbing.SymbolicReference {
				if target, ok := normalizeRemoteBranchReference(r.Target()); ok {
					return resolvedRemoteBranch{name: target}, nil
				}
			}
			s := r.String()
			if idx := strings.Index(s, "->"); idx != -1 {
				if target, ok := normalizeRemoteBranchReference(plumbing.ReferenceName(strings.TrimSpace(s[idx+2:]))); ok {
					return resolvedRemoteBranch{name: target}, nil
				}
			}
		}
	}
	if resolved, ok := resolveRemoteDefaultBranchFromLocal(repo); ok {
		return resolved, nil
	}
	for _, r := range refs {
		if normalized, ok := normalizeRemoteBranchReference(r.Name()); ok {
			return resolvedRemoteBranch{name: normalized, hash: r.Hash()}, nil
		}
	}
	return resolvedRemoteBranch{}, fmt.Errorf("resolve remote default: remote default branch not found")
}

func resolveRemoteDefaultBranchFromLocal(repo *git.Repository) (resolvedRemoteBranch, bool) {
	ref, err := repo.Reference(plumbing.ReferenceName("refs/remotes/origin/HEAD"), true)
	if err != nil || ref.Type() != plumbing.SymbolicReference {
		return resolvedRemoteBranch{}, false
	}
	target, ok := normalizeRemoteBranchReference(ref.Target())
	if !ok {
		return resolvedRemoteBranch{}, false
	}
	return resolvedRemoteBranch{name: target}, true
}

func normalizeRemoteBranchReference(name plumbing.ReferenceName) (plumbing.ReferenceName, bool) {
	switch {
	case strings.HasPrefix(name.String(), "refs/heads/"):
		return name, true
	case strings.HasPrefix(name.String(), "refs/remotes/origin/"):
		return plumbing.NewBranchReferenceName(strings.TrimPrefix(name.String(), "refs/remotes/origin/")), true
	default:
		return "", false
	}
}

func shouldFallbackToCurrentBranch(repo *git.Repository, err error) bool {
	if !errors.Is(err, transport.ErrAuthenticationRequired) && !errors.Is(err, transport.ErrEmptyRemoteRepository) {
		return false
	}
	_, headErr := repo.Head()
	return headErr == nil
}

// checkoutRemoteDefaultBranch ensures the working tree is checked out to the remote's default branch
// (the branch target of origin/HEAD). If the local branch does not exist it will be created to track
// the remote branch.
func checkoutRemoteDefaultBranch(repo *git.Repository, worktree *git.Worktree, authMethod transport.AuthMethod) error {
	resolved, err := resolveRemoteDefaultBranch(repo, authMethod)
	if err != nil {
		return err
	}
	branchRefName := resolved.name
	// If HEAD already points to the desired branch, nothing to do.
	headRef, errHead := repo.Head()
	if errHead == nil && headRef.Name() == branchRefName {
		return nil
	}
	// If local branch exists, attempt a checkout
	if _, err := repo.Reference(branchRefName, true); err == nil {
		if err := worktree.Checkout(&git.CheckoutOptions{Branch: branchRefName}); err != nil {
			return fmt.Errorf("checkout branch %s: %w", branchRefName.String(), err)
		}
		return nil
	}
	// Try to find the corresponding remote tracking ref (refs/remotes/origin/<name>)
	branchShort := strings.TrimPrefix(branchRefName.String(), "refs/heads/")
	remoteRefName := plumbing.ReferenceName("refs/remotes/origin/" + branchShort)
	hash := resolved.hash
	if remoteRef, err := repo.Reference(remoteRefName, true); err == nil {
		hash = remoteRef.Hash()
	} else if err != nil && !errors.Is(err, plumbing.ErrReferenceNotFound) {
		return fmt.Errorf("checkout remote default: remote ref %s: %w", remoteRefName.String(), err)
	}
	if hash == plumbing.ZeroHash {
		return fmt.Errorf("checkout remote default: remote ref %s not found", remoteRefName.String())
	}
	if err := worktree.Checkout(&git.CheckoutOptions{Branch: branchRefName, Create: true, Hash: hash}); err != nil {
		return fmt.Errorf("checkout create branch %s: %w", branchRefName.String(), err)
	}
	cfg, err := repo.Config()
	if err != nil {
		return fmt.Errorf("git token store: repo config: %w", err)
	}
	if _, ok := cfg.Branches[branchShort]; !ok {
		cfg.Branches[branchShort] = &config.Branch{Name: branchShort}
	}
	cfg.Branches[branchShort].Remote = "origin"
	cfg.Branches[branchShort].Merge = branchRefName
	if err := repo.SetConfig(cfg); err != nil {
		return fmt.Errorf("git token store: set branch config: %w", err)
	}
	return nil
}

func (s *GitTokenStore) remoteBranchPreconditionLocked() (gitRemotePrecondition, error) {
	repoDir := s.repoDirSnapshot()
	if repoDir == "" {
		return gitRemotePrecondition{}, fmt.Errorf("git token store: repository path not configured")
	}
	repo, err := git.PlainOpen(repoDir)
	if err != nil {
		return gitRemotePrecondition{}, fmt.Errorf("git token store: open repo for remote check: %w", err)
	}
	branch := plumbing.ReferenceName("")
	if s.branch != "" {
		branch = plumbing.NewBranchReferenceName(s.branch)
	} else {
		head, errHead := repo.Reference(plumbing.HEAD, false)
		if errHead != nil {
			return gitRemotePrecondition{}, fmt.Errorf("git token store: resolve target branch: %w", errHead)
		}
		if head.Type() == plumbing.SymbolicReference {
			branch = head.Target()
		} else {
			branch = head.Name()
		}
	}
	if !branch.IsBranch() {
		return gitRemotePrecondition{}, fmt.Errorf("git token store: target reference %s is not a branch", branch)
	}
	state := gitRemotePrecondition{branch: branch}
	remote, errRemote := repo.Remote("origin")
	if errRemote != nil {
		return gitRemotePrecondition{}, fmt.Errorf("git token store: open origin for remote check: %w", errRemote)
	}
	refs, errList := remote.List(&git.ListOptions{Auth: s.gitAuth()})
	if errList != nil {
		if errors.Is(errList, transport.ErrEmptyRemoteRepository) {
			return state, nil
		}
		return gitRemotePrecondition{}, fmt.Errorf("git token store: list origin for remote check: %w", errList)
	}
	found := false
	for _, ref := range refs {
		if ref != nil && ref.Name() == branch {
			found = true
			break
		}
	}
	if !found {
		return state, nil
	}

	remoteRef := plumbing.ReferenceName("refs/remotes/origin/" + strings.TrimPrefix(branch.String(), "refs/heads/"))
	refSpec := config.RefSpec("+" + branch.String() + ":" + remoteRef.String())
	errFetch := repo.Fetch(&git.FetchOptions{
		Auth:       s.gitAuth(),
		RemoteName: "origin",
		RefSpecs:   []config.RefSpec{refSpec},
		Force:      true,
		Tags:       plumbing.NoTags,
	})
	if errFetch != nil && !errors.Is(errFetch, git.NoErrAlreadyUpToDate) {
		return gitRemotePrecondition{}, fmt.Errorf("git token store: fetch target branch %s: %w", branch, errFetch)
	}
	fetched, errReference := repo.Reference(remoteRef, true)
	if errReference != nil {
		return gitRemotePrecondition{}, fmt.Errorf("git token store: resolve fetched target branch %s: %w", branch, errReference)
	}
	if fetched.Hash() == plumbing.ZeroHash {
		return gitRemotePrecondition{}, fmt.Errorf("git token store: fetched target branch %s has no commit", branch)
	}
	state.hash = fetched.Hash()
	state.exists = true
	return state, nil
}

func captureGitLocalHead(repoDir string) (gitLocalHeadSnapshot, error) {
	repo, errOpen := git.PlainOpen(repoDir)
	if errOpen != nil {
		return gitLocalHeadSnapshot{}, fmt.Errorf("git token store: open repo before save: %w", errOpen)
	}
	head, errHead := repo.Head()
	if errors.Is(errHead, plumbing.ErrReferenceNotFound) {
		return gitLocalHeadSnapshot{}, nil
	}
	if errHead != nil {
		return gitLocalHeadSnapshot{}, fmt.Errorf("git token store: resolve local head before save: %w", errHead)
	}
	return gitLocalHeadSnapshot{name: head.Name(), hash: head.Hash(), exists: true}, nil
}

func (s *GitTokenStore) resetGitWorkspaceAfterFailedSaveLocked(remote gitRemotePrecondition, local gitLocalHeadSnapshot, relPaths ...string) error {
	repo, errOpen := git.PlainOpen(s.repoDirSnapshot())
	if errOpen != nil {
		return fmt.Errorf("open repo: %w", errOpen)
	}
	worktree, errWorktree := repo.Worktree()
	if errWorktree != nil {
		return fmt.Errorf("open worktree: %w", errWorktree)
	}

	targetName := remote.branch
	targetHash := remote.hash
	targetExists := remote.exists
	if !targetExists && local.exists {
		targetName = local.name
		targetHash = local.hash
		targetExists = true
	}
	if targetExists {
		if errReference := repo.Storer.SetReference(plumbing.NewHashReference(targetName, targetHash)); errReference != nil {
			return fmt.Errorf("restore branch reference: %w", errReference)
		}
		if errReset := worktree.Reset(&git.ResetOptions{Commit: targetHash, Mode: git.MixedReset}); errReset != nil {
			return fmt.Errorf("reset index: %w", errReset)
		}
		return nil
	}

	if targetName != "" {
		if errRemove := repo.Storer.RemoveReference(targetName); errRemove != nil && !errors.Is(errRemove, plumbing.ErrReferenceNotFound) {
			return fmt.Errorf("remove failed branch reference: %w", errRemove)
		}
	}
	idx, errIndex := repo.Storer.Index()
	if errIndex != nil {
		return fmt.Errorf("load index: %w", errIndex)
	}
	for _, relPath := range relPaths {
		if _, errRemove := idx.Remove(relPath); errRemove != nil && !errors.Is(errRemove, index.ErrEntryNotFound) {
			return fmt.Errorf("remove %s from index: %w", relPath, errRemove)
		}
	}
	idx.Cache = nil
	if errStore := repo.Storer.SetIndex(idx); errStore != nil {
		return fmt.Errorf("store restored index: %w", errStore)
	}
	return nil
}

func wrapOptionalError(message string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", message, err)
}

func (s *GitTokenStore) commitAndPushLocked(message string, relPaths ...string) error {
	remoteState, errRemote := s.remoteBranchPreconditionLocked()
	if errRemote != nil {
		return errRemote
	}
	return s.commitAndPushAgainstRemoteLocked(message, remoteState, relPaths...)
}

func (s *GitTokenStore) commitAndPushAgainstRemoteLocked(message string, remoteState gitRemotePrecondition, relPaths ...string) error {
	return s.commitAndPushAgainstRemoteWithSnapshotsLocked(message, remoteState, nil, relPaths...)
}

func (s *GitTokenStore) commitAndPushAgainstRemoteWithSnapshotsLocked(message string, remoteState gitRemotePrecondition, snapshots map[string]authFileSnapshot, relPaths ...string) error {
	repoDir := s.repoDirSnapshot()
	if repoDir == "" {
		return fmt.Errorf("git token store: repository path not configured")
	}
	repo, err := git.PlainOpen(repoDir)
	if err != nil {
		return fmt.Errorf("git token store: open repo: %w", err)
	}
	worktree, err := repo.Worktree()
	if err != nil {
		return fmt.Errorf("git token store: worktree: %w", err)
	}
	if remoteState.exists {
		head, errHead := repo.Head()
		if errHead != nil {
			return fmt.Errorf("git token store: resolve local branch before remote reset: %w", errHead)
		}
		if head.Name() != remoteState.branch {
			return fmt.Errorf("git token store: local branch %s does not match target branch %s", head.Name(), remoteState.branch)
		}
		if errReset := worktree.Reset(&git.ResetOptions{Commit: remoteState.hash, Mode: git.MixedReset}); errReset != nil {
			return fmt.Errorf("git token store: reset index to checked remote %s: %w", remoteState.hash, errReset)
		}
	}
	added := false
	hasExistingSnapshot := false
	if snapshots != nil {
		// Stage captured bytes directly so a later working-tree change cannot replace validated content.
		idx, errIndex := repo.Storer.Index()
		if errIndex != nil {
			return fmt.Errorf("git token store: load index for auth snapshots: %w", errIndex)
		}
		for _, rel := range relPaths {
			if strings.TrimSpace(rel) == "" {
				continue
			}
			snapshot, ok := snapshots[rel]
			if !ok {
				return fmt.Errorf("git token store: missing immutable snapshot for %s", rel)
			}
			if !snapshot.exists {
				if _, errRemove := idx.Remove(rel); errRemove != nil && !errors.Is(errRemove, index.ErrEntryNotFound) {
					return fmt.Errorf("git token store: remove %s from index: %w", rel, errRemove)
				}
				added = true
				continue
			}
			hasExistingSnapshot = true

			blob := &plumbing.MemoryObject{}
			blob.SetType(plumbing.BlobObject)
			if _, errWrite := blob.Write(snapshot.data); errWrite != nil {
				return fmt.Errorf("git token store: encode auth snapshot %s: %w", rel, errWrite)
			}
			hash, errStore := repo.Storer.SetEncodedObject(blob)
			if errStore != nil {
				return fmt.Errorf("git token store: store auth snapshot %s: %w", rel, errStore)
			}
			entry, errEntry := idx.Entry(rel)
			if errors.Is(errEntry, index.ErrEntryNotFound) {
				entry = idx.Add(rel)
			} else if errEntry != nil {
				return fmt.Errorf("git token store: find auth snapshot %s in index: %w", rel, errEntry)
			}
			entry.Hash = hash
			entry.Size = uint32(len(snapshot.data))
			if entry.Mode == filemode.Empty {
				entry.Mode = filemode.Regular
			}
			added = true
		}
		idx.Cache = nil
		if added {
			if errIndex := repo.Storer.SetIndex(idx); errIndex != nil {
				return fmt.Errorf("git token store: store auth snapshot index: %w", errIndex)
			}
		}
	} else {
		for _, rel := range relPaths {
			if strings.TrimSpace(rel) == "" {
				continue
			}
			if _, err = worktree.Add(rel); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					if _, errRemove := worktree.Remove(rel); errRemove != nil && !errors.Is(errRemove, os.ErrNotExist) {
						return fmt.Errorf("git token store: remove %s: %w", rel, errRemove)
					}
				} else {
					return fmt.Errorf("git token store: add %s: %w", rel, err)
				}
			}
			added = true
		}
	}
	if !added {
		return nil
	}
	if snapshots == nil {
		status, errStatus := worktree.Status()
		if errStatus != nil {
			return fmt.Errorf("git token store: status: %w", errStatus)
		}
		if status.IsClean() {
			return nil
		}
	}
	if strings.TrimSpace(message) == "" {
		message = "Update auth store"
	}
	signature := &object.Signature{
		Name:  "CLIProxyAPI",
		Email: "cliproxy@local",
		When:  time.Now(),
	}
	commitHash, err := worktree.Commit(message, &git.CommitOptions{
		Author: signature,
	})
	if err != nil {
		if errors.Is(err, git.ErrEmptyCommit) {
			if remoteState.exists || !hasExistingSnapshot {
				return nil
			}
		} else {
			return fmt.Errorf("git token store: commit: %w", err)
		}
	}
	headRef, errHead := repo.Head()
	if errHead != nil {
		return fmt.Errorf("git token store: get head: %w", errHead)
	}
	if headRef.Name() != remoteState.branch {
		return fmt.Errorf("git token store: committed branch %s does not match target branch %s", headRef.Name(), remoteState.branch)
	}
	if err == nil {
		if errRewrite := s.rewriteHeadAsSingleCommit(repo, headRef.Name(), commitHash, message, signature); errRewrite != nil {
			return errRewrite
		}
	}
	s.maybeRunGC(repo)
	pushOpts := &git.PushOptions{
		Auth:     s.gitAuth(),
		RefSpecs: []config.RefSpec{config.RefSpec(remoteState.branch.String() + ":" + remoteState.branch.String())},
	}
	if remoteState.exists {
		pushOpts.ForceWithLease = &git.ForceWithLease{
			RefName: remoteState.branch,
			Hash:    remoteState.hash,
		}
	}
	pushRepo := s.pushRepo
	if pushRepo == nil {
		pushRepo = func(repo *git.Repository, options *git.PushOptions) error {
			return repo.Push(options)
		}
	}
	if err = pushRepo(repo, pushOpts); err != nil {
		if errors.Is(err, git.NoErrAlreadyUpToDate) {
			return nil
		}
		return fmt.Errorf("git token store: push: %w", err)
	}
	return nil
}

// rewriteHeadAsSingleCommit rewrites the current branch tip to a single-parentless commit and leaves history squashed.
func (s *GitTokenStore) rewriteHeadAsSingleCommit(repo *git.Repository, branch plumbing.ReferenceName, commitHash plumbing.Hash, message string, signature *object.Signature) error {
	commitObj, err := repo.CommitObject(commitHash)
	if err != nil {
		return fmt.Errorf("git token store: inspect head commit: %w", err)
	}
	squashed := &object.Commit{
		Author:       *signature,
		Committer:    *signature,
		Message:      message,
		TreeHash:     commitObj.TreeHash,
		ParentHashes: nil,
		Encoding:     commitObj.Encoding,
		ExtraHeaders: commitObj.ExtraHeaders,
	}
	mem := &plumbing.MemoryObject{}
	mem.SetType(plumbing.CommitObject)
	if err := squashed.Encode(mem); err != nil {
		return fmt.Errorf("git token store: encode squashed commit: %w", err)
	}
	newHash, err := repo.Storer.SetEncodedObject(mem)
	if err != nil {
		return fmt.Errorf("git token store: write squashed commit: %w", err)
	}
	if err := repo.Storer.SetReference(plumbing.NewHashReference(branch, newHash)); err != nil {
		return fmt.Errorf("git token store: update branch reference: %w", err)
	}
	return nil
}

func (s *GitTokenStore) maybeRunGC(repo *git.Repository) {
	now := time.Now()
	if now.Sub(s.lastGC) < gcInterval {
		return
	}
	s.lastGC = now

	pruneOpts := git.PruneOptions{
		OnlyObjectsOlderThan: now,
		Handler:              repo.DeleteObject,
	}
	if err := repo.Prune(pruneOpts); err != nil && !errors.Is(err, git.ErrLooseObjectsNotSupported) {
		return
	}
	_ = repo.RepackObjects(&git.RepackConfig{})
}

// PersistConfig commits and pushes configuration changes to git.
func (s *GitTokenStore) PersistConfig(_ context.Context) error {
	s.bindingMu.RLock()
	defer s.bindingMu.RUnlock()
	if err := s.ensureRepositoryLocked(); err != nil {
		return err
	}
	configPath := s.ConfigPath()
	if configPath == "" {
		return fmt.Errorf("git token store: config path not configured")
	}
	if _, err := os.Stat(configPath); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("git token store: stat config: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlockRepository, errRepositoryLock := lockGitRepository(s.repoDirSnapshot())
	if errRepositoryLock != nil {
		return errRepositoryLock
	}
	defer func() {
		if errUnlock := unlockRepository(); errUnlock != nil {
			log.WithError(errUnlock).Error("git token store: unlock repository after config persistence")
		}
	}()
	rel, err := s.relativeToRepo(configPath)
	if err != nil {
		return err
	}
	return s.commitAndPushLocked("Update config", rel)
}

func ensureEmptyFile(path string) error {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return os.WriteFile(path, []byte{}, 0o600)
		}
		return err
	}
	return nil
}

func writeAuthFileAtomically(path string, data []byte) error {
	return writeAuthFileAtomicallyForSnapshot(path, data, nil)
}

func writeAuthFileAtomicallyForSnapshot(path string, data []byte, expected *authFileSnapshot) error {
	root, errRoot := os.OpenRoot(filepath.Dir(path))
	if errRoot != nil {
		return fmt.Errorf("auth filestore: open auth directory failed: %w", errRoot)
	}
	defer func() {
		if errClose := root.Close(); errClose != nil {
			log.WithError(errClose).Error("git token store: close auth directory after write")
		}
	}()
	if errWrite := writeAuthFileAtomicallyAtRoot(root, filepath.Base(path), data, expected); errWrite != nil {
		return fmt.Errorf("auth filestore: write auth file failed: %w", errWrite)
	}
	return nil
}

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

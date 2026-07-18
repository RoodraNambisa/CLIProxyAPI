//go:build windows

package authfileguard

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

var replaceFileW = windows.NewLazySystemDLL("kernel32.dll").NewProc("ReplaceFileW")

type windowsPinnedDirectory struct {
	path string
	file *os.File
}

type windowsRootPin struct {
	directories []windowsPinnedDirectory
}

func (pin *windowsRootPin) Close() error {
	if pin == nil {
		return nil
	}
	var err error
	for index := len(pin.directories) - 1; index >= 0; index-- {
		entry := pin.directories[index]
		if entry.file == nil {
			continue
		}
		if errClose := entry.file.Close(); errClose != nil {
			err = errors.Join(err, fmt.Errorf("close pinned Windows directory %q: %w", entry.path, errClose))
		}
	}
	return err
}

func exchangeFile(root *os.Root, stagedName, targetName string) (displaced string, err error) {
	if root == nil {
		return "", errors.New("auth file guard: root is nil")
	}
	parentPath, pinnedParent, errParent := pinWindowsRootPath(root)
	if errParent != nil {
		return "", errParent
	}
	defer func() {
		if errClose := pinnedParent.Close(); errClose != nil {
			err = errors.Join(err, fmt.Errorf("auth file guard: close pinned Windows exchange parent: %w", errClose))
		}
	}()
	var suffix [16]byte
	if _, errRandom := rand.Read(suffix[:]); errRandom != nil {
		return "", fmt.Errorf("auth file guard: generate displaced auth name: %w", errRandom)
	}
	displacedName := ".auth-displaced-" + hex.EncodeToString(suffix[:])
	targetPath, errTarget := windows.UTF16PtrFromString(filepath.Join(parentPath, targetName))
	stagedPath, errStaged := windows.UTF16PtrFromString(filepath.Join(parentPath, stagedName))
	displacedPath, errDisplaced := windows.UTF16PtrFromString(filepath.Join(parentPath, displacedName))
	if errTarget != nil || errStaged != nil || errDisplaced != nil {
		return "", errors.Join(errTarget, errStaged, errDisplaced)
	}
	result, _, errCall := replaceFileW.Call(
		uintptr(unsafe.Pointer(targetPath)),
		uintptr(unsafe.Pointer(stagedPath)),
		uintptr(unsafe.Pointer(displacedPath)),
		0,
		0,
		0,
	)
	if result == 0 {
		if _, errStat := root.Lstat(displacedName); errStat == nil {
			return displacedName, fmt.Errorf("auth file guard: replace auth file: %w", errCall)
		}
		return "", fmt.Errorf("auth file guard: replace auth file: %w", errCall)
	}
	if _, errStat := root.Lstat(displacedName); errStat != nil {
		return displacedName, fmt.Errorf("auth file guard: inspect displaced auth generation: %w", errStat)
	}
	if errIdentity := revalidatePinnedWindowsRootPath(root, pinnedParent); errIdentity != nil {
		return displacedName, errIdentity
	}
	return displacedName, nil
}

func pinWindowsRootPath(root *os.Root) (path string, pinned *windowsRootPin, err error) {
	path, err = windowsRootPath(root)
	if err != nil {
		return "", nil, err
	}
	paths, errPaths := windowsDirectoryChain(path)
	if errPaths != nil {
		return "", nil, errPaths
	}
	pinned = &windowsRootPin{directories: make([]windowsPinnedDirectory, 0, len(paths))}
	for _, directoryPath := range paths {
		pathPtr, errPath := windows.UTF16PtrFromString(directoryPath)
		if errPath != nil {
			errClose := pinned.Close()
			return "", nil, errors.Join(errPath, errClose)
		}
		// windowsRootPath returns the fully resolved path. Denying delete sharing
		// pins each directory name while allowing unrelated writers to proceed.
		handle, errOpen := windows.CreateFile(
			pathPtr,
			windows.FILE_READ_ATTRIBUTES|windows.SYNCHRONIZE,
			windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
			nil,
			windows.OPEN_EXISTING,
			windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
			0,
		)
		if errOpen != nil {
			errClose := pinned.Close()
			return "", nil, errors.Join(fmt.Errorf("auth file guard: pin Windows directory %q: %w", directoryPath, errOpen), errClose)
		}
		file := os.NewFile(uintptr(handle), directoryPath)
		if file == nil {
			errHandleClose := windows.CloseHandle(handle)
			errClose := pinned.Close()
			return "", nil, errors.Join(fmt.Errorf("auth file guard: wrap pinned Windows directory %q", directoryPath), errHandleClose, errClose)
		}
		pinned.directories = append(pinned.directories, windowsPinnedDirectory{path: directoryPath, file: file})
	}
	if errIdentity := revalidatePinnedWindowsRootPath(root, pinned); errIdentity != nil {
		errClose := pinned.Close()
		return "", nil, errors.Join(errIdentity, errClose)
	}
	return path, pinned, nil
}

func windowsDirectoryChain(path string) ([]string, error) {
	clean := filepath.Clean(path)
	volume := filepath.VolumeName(clean)
	if volume == "" || !filepath.IsAbs(clean) {
		return nil, fmt.Errorf("auth file guard: resolved Windows root path %q is not absolute", path)
	}
	remainder := strings.TrimPrefix(clean, volume)
	components := strings.FieldsFunc(remainder, func(char rune) bool {
		return char == '\\' || char == '/'
	})
	if len(components) == 0 {
		return []string{clean}, nil
	}
	current := volume + string(os.PathSeparator)
	paths := make([]string, 0, len(components))
	for _, component := range components {
		current = filepath.Join(current, component)
		paths = append(paths, current)
	}
	return paths, nil
}

func windowsRootPath(root *os.Root) (path string, err error) {
	directory, errOpen := root.Open(".")
	if errOpen != nil {
		return "", errOpen
	}
	defer func() { err = errors.Join(err, directory.Close()) }()
	buffer := make([]uint16, 32768)
	length, errPath := windows.GetFinalPathNameByHandle(windows.Handle(directory.Fd()), &buffer[0], uint32(len(buffer)), 0)
	if errPath != nil {
		return "", errPath
	}
	if length == 0 || length >= uint32(len(buffer)) {
		return "", errors.New("auth file guard: resolved Windows root path is too long")
	}
	return windows.UTF16ToString(buffer[:length]), nil
}

func revalidatePinnedWindowsRootPath(root *os.Root, pinned *windowsRootPin) error {
	if root == nil || pinned == nil || len(pinned.directories) == 0 {
		return errors.Join(errors.New("auth file guard: incomplete Windows root pin"), ErrPersistGenerationStale)
	}
	opened, errOpened := root.Stat(".")
	var validationErr error
	stale := errOpened != nil
	if errOpened != nil {
		validationErr = errors.Join(validationErr, fmt.Errorf("stat opened Windows root: %w", errOpened))
	}
	for _, entry := range pinned.directories {
		pinnedInfo, errPinned := entry.file.Stat()
		live, errLive := os.Stat(entry.path)
		if errPinned != nil {
			validationErr = errors.Join(validationErr, fmt.Errorf("stat pinned Windows directory %q: %w", entry.path, errPinned))
		}
		if errLive != nil {
			validationErr = errors.Join(validationErr, fmt.Errorf("stat live Windows directory %q: %w", entry.path, errLive))
		}
		if errPinned != nil || errLive != nil || !os.SameFile(pinnedInfo, live) {
			stale = true
		}
	}
	last := pinned.directories[len(pinned.directories)-1]
	lastInfo, errLast := last.file.Stat()
	if errLast != nil {
		validationErr = errors.Join(validationErr, fmt.Errorf("stat pinned Windows root %q: %w", last.path, errLast))
	}
	if errOpened != nil || errLast != nil || !os.SameFile(opened, lastInfo) {
		stale = true
	}
	if stale {
		return errors.Join(validationErr, ErrPersistGenerationStale)
	}
	return nil
}

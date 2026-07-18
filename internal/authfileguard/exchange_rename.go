package authfileguard

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
)

const displacedFilePrefix = ".auth-displaced-"

type exchangeFileOperations struct {
	link          func(string, string) error
	rename        func(string, string) error
	remove        func(string) error
	syncDirectory func() error
}

func newExchangeFileOperations(root *os.Root) exchangeFileOperations {
	return exchangeFileOperations{
		link:          root.Link,
		rename:        root.Rename,
		remove:        root.Remove,
		syncDirectory: func() error { return syncExchangeDirectory(root) },
	}
}

func exchangeFileByRename(root *os.Root, stagedName, targetName string) (string, error) {
	if root == nil {
		return "", errors.New("auth file guard: root is nil")
	}
	return exchangeFileByRenameWith(root, stagedName, targetName, newExchangeFileOperations(root))
}

func exchangeFileByRenameWith(root *os.Root, stagedName, targetName string, operations exchangeFileOperations) (string, error) {
	if root == nil {
		return "", errors.New("auth file guard: root is nil")
	}
	if operations.link == nil || operations.rename == nil || operations.remove == nil || operations.syncDirectory == nil {
		return "", errors.New("auth file guard: rename fallback operations are incomplete")
	}
	stagedInfo, errStaged := root.Lstat(stagedName)
	if errStaged != nil {
		return "", fmt.Errorf("auth file guard: inspect staged auth generation: %w", errStaged)
	}
	targetInfo, errTarget := root.Lstat(targetName)
	if errTarget != nil {
		return "", fmt.Errorf("auth file guard: inspect current auth generation: %w", errTarget)
	}
	displacedName, errName := unusedDisplacedFileName(root)
	if errName != nil {
		return "", errName
	}

	errLink := exchangeOperationError("link current auth generation", operations.link(targetName, displacedName))
	displacedMatches, errDisplaced := exchangePathMatches(root, displacedName, targetInfo)
	if !displacedMatches {
		if errLink == nil {
			errLink = errors.New("auth file guard: linked auth generation could not be confirmed")
		}
		displacedMissing, errMissing := exchangePathMissing(root, displacedName)
		if displacedMissing {
			return "", errors.Join(errLink, errDisplaced, errMissing)
		}
		return displacedName, errors.Join(
			ErrExchangeCleanupRequired,
			errLink,
			errDisplaced,
			errMissing,
		)
	}
	if errSync := operations.syncDirectory(); errSync != nil {
		return cleanupLinkedExchangeBackup(
			root,
			displacedName,
			operations,
			fmt.Errorf("auth file guard: sync linked auth generation: %w", errSync),
		)
	}

	errInstall := exchangeOperationError("install staged auth generation", operations.rename(stagedName, targetName))
	installed, errInstalled := linkedExchangeInstalled(root, stagedName, targetName, displacedName, stagedInfo, targetInfo)
	if errInstall == nil {
		if installed {
			if errSync := operations.syncDirectory(); errSync != nil {
				return restoreLinkedExchange(
					root,
					stagedName,
					targetName,
					displacedName,
					stagedInfo,
					targetInfo,
					operations,
					fmt.Errorf("auth file guard: sync installed auth generation: %w", errSync),
				)
			}
			return displacedName, nil
		}
		return displacedName, errors.Join(
			ErrExchangeOutcomeUncertain,
			errors.New("auth file guard: installed exchange state could not be confirmed"),
			errInstalled,
		)
	}
	if installed {
		return restoreLinkedExchange(root, stagedName, targetName, displacedName, stagedInfo, targetInfo, operations, errInstall)
	}
	original, errOriginal := linkedExchangeOriginal(root, stagedName, targetName, displacedName, stagedInfo, targetInfo)
	if original {
		return cleanupLinkedExchangeBackup(root, displacedName, operations, errors.Join(errInstall, errOriginal))
	}
	return displacedName, errors.Join(
		ErrExchangeOutcomeUncertain,
		errInstall,
		errInstalled,
		errOriginal,
		errors.New("auth file guard: exchange state is ambiguous after install failure"),
	)
}

func restoreLinkedExchange(
	root *os.Root,
	stagedName, targetName, displacedName string,
	stagedInfo, targetInfo fs.FileInfo,
	operations exchangeFileOperations,
	installErr error,
) (string, error) {
	errLink := exchangeOperationError("preserve staged auth generation for restore", operations.link(targetName, stagedName))
	stagedMatches, errStaged := exchangePathMatches(root, stagedName, stagedInfo)
	if !stagedMatches {
		return displacedName, errors.Join(
			ErrExchangeOutcomeUncertain,
			ErrExchangeRestoreFailed,
			installErr,
			errLink,
			errStaged,
		)
	}
	if errSync := operations.syncDirectory(); errSync != nil {
		return displacedName, errors.Join(
			ErrExchangeOutcomeUncertain,
			ErrExchangeRestoreFailed,
			installErr,
			fmt.Errorf("auth file guard: sync staged auth generation for restore: %w", errSync),
		)
	}

	errRestore := exchangeOperationError("restore displaced auth generation", operations.rename(displacedName, targetName))
	restored, errState := linkedExchangeRestored(root, stagedName, targetName, displacedName, stagedInfo, targetInfo)
	if !restored {
		return displacedName, errors.Join(
			ErrExchangeOutcomeUncertain,
			ErrExchangeRestoreFailed,
			installErr,
			errRestore,
			errState,
		)
	}
	errSync := operations.syncDirectory()
	if errSync != nil {
		errSync = fmt.Errorf("auth file guard: sync restored auth generation: %w", errSync)
		return "", errors.Join(ErrExchangeOutcomeUncertain, installErr, errRestore, errSync)
	}
	return "", errors.Join(installErr, errRestore)
}

func cleanupLinkedExchangeBackup(
	root *os.Root,
	displacedName string,
	operations exchangeFileOperations,
	cause error,
) (string, error) {
	errRemove := exchangeOperationError("remove linked auth generation", operations.remove(displacedName))
	displacedMissing, errState := exchangePathMissing(root, displacedName)
	errSync := operations.syncDirectory()
	if errSync != nil {
		errSync = fmt.Errorf("auth file guard: sync linked auth generation cleanup: %w", errSync)
	}
	if displacedMissing {
		return "", errors.Join(cause, errSync)
	}
	return displacedName, errors.Join(
		ErrExchangeCleanupRequired,
		cause,
		errRemove,
		errState,
		errSync,
	)
}

func linkedExchangeOriginal(root *os.Root, stagedName, targetName, displacedName string, stagedInfo, targetInfo fs.FileInfo) (bool, error) {
	targetMatches, errTarget := exchangePathMatches(root, targetName, targetInfo)
	stagedMatches, errStaged := exchangePathMatches(root, stagedName, stagedInfo)
	displacedMatches, errDisplaced := exchangePathMatches(root, displacedName, targetInfo)
	return targetMatches && stagedMatches && displacedMatches, errors.Join(errTarget, errStaged, errDisplaced)
}

func linkedExchangeRestored(root *os.Root, stagedName, targetName, displacedName string, stagedInfo, targetInfo fs.FileInfo) (bool, error) {
	targetMatches, errTarget := exchangePathMatches(root, targetName, targetInfo)
	stagedMatches, errStaged := exchangePathMatches(root, stagedName, stagedInfo)
	displacedMissing, errDisplaced := exchangePathMissing(root, displacedName)
	return targetMatches && stagedMatches && displacedMissing, errors.Join(errTarget, errStaged, errDisplaced)
}

func linkedExchangeInstalled(root *os.Root, stagedName, targetName, displacedName string, stagedInfo, targetInfo fs.FileInfo) (bool, error) {
	targetMatches, errTarget := exchangePathMatches(root, targetName, stagedInfo)
	displacedMatches, errDisplaced := exchangePathMatches(root, displacedName, targetInfo)
	stagedMissing, errStaged := exchangePathMissing(root, stagedName)
	return targetMatches && displacedMatches && stagedMissing, errors.Join(errTarget, errDisplaced, errStaged)
}

func exchangePathMatches(root *os.Root, name string, expected fs.FileInfo) (bool, error) {
	current, errCurrent := root.Lstat(name)
	if errors.Is(errCurrent, fs.ErrNotExist) {
		return false, nil
	}
	if errCurrent != nil {
		return false, fmt.Errorf("auth file guard: inspect exchange path %q: %w", name, errCurrent)
	}
	return expected != nil && os.SameFile(current, expected), nil
}

func exchangePathMissing(root *os.Root, name string) (bool, error) {
	_, errCurrent := root.Lstat(name)
	if errors.Is(errCurrent, fs.ErrNotExist) {
		return true, nil
	}
	if errCurrent != nil {
		return false, fmt.Errorf("auth file guard: inspect exchange path %q: %w", name, errCurrent)
	}
	return false, nil
}

func exchangeOperationError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("auth file guard: %s: %w", operation, err)
}

func unusedDisplacedFileName(root *os.Root) (string, error) {
	for attempt := 0; attempt < 8; attempt++ {
		var suffix [16]byte
		if _, errRandom := rand.Read(suffix[:]); errRandom != nil {
			return "", fmt.Errorf("auth file guard: generate displaced auth name: %w", errRandom)
		}
		name := displacedFilePrefix + hex.EncodeToString(suffix[:])
		if _, errStat := root.Lstat(name); errors.Is(errStat, fs.ErrNotExist) {
			return name, nil
		} else if errStat != nil {
			return "", fmt.Errorf("auth file guard: inspect displaced auth name: %w", errStat)
		}
	}
	return "", errors.New("auth file guard: generate unused displaced auth name")
}

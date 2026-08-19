//go:build windows

package management

import "golang.org/x/sys/windows"

func replaceChatGPTWebManualReloginJournal(stagedPath, targetPath string) error {
	staged, errStaged := windows.UTF16PtrFromString(stagedPath)
	if errStaged != nil {
		return errStaged
	}
	target, errTarget := windows.UTF16PtrFromString(targetPath)
	if errTarget != nil {
		return errTarget
	}
	return windows.MoveFileEx(
		staged,
		target,
		windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH,
	)
}

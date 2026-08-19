//go:build !windows

package management

import "os"

func replaceChatGPTWebManualReloginJournal(stagedPath, targetPath string) error {
	return os.Rename(stagedPath, targetPath)
}

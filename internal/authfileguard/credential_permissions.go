package authfileguard

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"runtime"
	"strings"
)

// HardenChatGPTWebCredentialFile restricts an opened ChatGPT Web credential
// to owner-only access before callers accept its contents.
func HardenChatGPTWebCredentialFile(file *os.File, info fs.FileInfo, data []byte) (fs.FileInfo, error) {
	if file == nil || info == nil || runtime.GOOS == "windows" || info.Mode().Perm() == 0o600 || !isChatGPTWebCredentialData(data) {
		return info, nil
	}
	if errChmod := file.Chmod(0o600); errChmod != nil {
		return nil, fmt.Errorf("auth file guard: restrict chatgpt-web credential permissions: %w", errChmod)
	}
	updated, errStat := file.Stat()
	if errStat != nil {
		return nil, fmt.Errorf("auth file guard: inspect restricted chatgpt-web credential: %w", errStat)
	}
	return updated, nil
}

func isChatGPTWebCredentialData(data []byte) bool {
	var envelope struct {
		Type string `json:"type"`
	}
	return json.Unmarshal(data, &envelope) == nil && strings.EqualFold(strings.TrimSpace(envelope.Type), "chatgpt-web")
}

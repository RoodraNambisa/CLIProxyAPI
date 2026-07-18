package authfileguard

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestHardenChatGPTWebCredentialFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not expose Unix credential permission bits")
	}

	tests := []struct {
		name     string
		data     string
		wantMode os.FileMode
	}{
		{name: "chatgpt web", data: `{"type":" chatGPT-WEB ","access_token":"secret"}`, wantMode: 0o600},
		{name: "other provider", data: `{"type":"codex","access_token":"secret"}`, wantMode: 0o644},
		{name: "invalid json", data: `{`, wantMode: 0o644},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "auth.json")
			if errWrite := os.WriteFile(path, []byte(test.data), 0o600); errWrite != nil {
				t.Fatal(errWrite)
			}
			if errChmod := os.Chmod(path, 0o644); errChmod != nil {
				t.Fatal(errChmod)
			}
			file, errOpen := os.Open(path)
			if errOpen != nil {
				t.Fatal(errOpen)
			}
			defer func() {
				if errClose := file.Close(); errClose != nil {
					t.Errorf("close credential: %v", errClose)
				}
			}()
			info, errStat := file.Stat()
			if errStat != nil {
				t.Fatal(errStat)
			}
			updated, errHarden := HardenChatGPTWebCredentialFile(file, info, []byte(test.data))
			if errHarden != nil {
				t.Fatal(errHarden)
			}
			if got := updated.Mode().Perm(); got != test.wantMode {
				t.Fatalf("returned mode = %o, want %o", got, test.wantMode)
			}
			live, errLive := os.Stat(path)
			if errLive != nil {
				t.Fatal(errLive)
			}
			if got := live.Mode().Perm(); got != test.wantMode {
				t.Fatalf("file mode = %o, want %o", got, test.wantMode)
			}
		})
	}
}

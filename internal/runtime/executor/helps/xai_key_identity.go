package helps

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Config keys have no credential file to carry a seed. Keep their random seed
// in a private non-credential sidecar, created only when identity is enabled.
func persistentXAIKeySeed(authDir, authID string) ([]byte, error) {
	if strings.TrimSpace(authDir) == "" || authID == "" {
		return nil, fmt.Errorf("xai key identity requires a persistent auth directory")
	}
	directory := filepath.Join(authDir, ".xai-identities")
	id := sha256.Sum256([]byte(authID))
	path := filepath.Join(directory, hex.EncodeToString(id[:])+".seed")
	read := func() ([]byte, error) {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		seed, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(string(raw)))
		if err != nil || len(seed) != 32 {
			return nil, fmt.Errorf("invalid saved Grok identity seed")
		}
		return seed, nil
	}
	if seed, err := read(); err == nil {
		return seed, nil
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	seed := make([]byte, 32)
	if _, err := rand.Read(seed); err != nil {
		return nil, err
	}
	file, err := os.CreateTemp(directory, ".seed-*")
	if err != nil {
		return nil, err
	}
	temporary := file.Name()
	defer func() { _ = os.Remove(temporary) }()
	_, errWrite := file.WriteString(base64.RawURLEncoding.EncodeToString(seed))
	errClose := file.Close()
	if errWrite != nil {
		return nil, errWrite
	}
	if errClose != nil {
		return nil, errClose
	}
	// Linking an already complete file is an atomic create-if-absent across
	// processes; a competing request reads the winning seed instead of replacing it.
	if errLink := os.Link(temporary, path); errLink != nil && !os.IsExist(errLink) {
		return nil, errLink
	}
	return read()
}

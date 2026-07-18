//go:build !aix

package authfileguard

import (
	"errors"
	"os"
	"path/filepath"
)

func persistentProcessLockIdentity(root *os.Root, lockPath string) (string, error) {
	identity := PathIdentity(filepath.Join(root.Name(), lockPath))
	if identity == "" {
		return "", errors.New("auth file guard: persistent lock path is empty")
	}
	return identity, nil
}

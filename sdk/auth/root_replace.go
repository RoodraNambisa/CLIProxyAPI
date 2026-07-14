package auth

import "os"

func replaceRootFile(root *os.Root, oldName, newName string) error {
	// Root.Rename is handle-relative on Windows and Unix, so it preserves the
	// pinned parent identity if the lexical directory is replaced concurrently.
	return root.Rename(oldName, newName)
}

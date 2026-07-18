//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || windows

package authfileguard

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const (
	authFileGuardProcessAction  = "CLIPROXY_AUTHFILEGUARD_PROCESS_ACTION"
	authFileGuardProcessRoot    = "CLIPROXY_AUTHFILEGUARD_PROCESS_ROOT"
	authFileGuardProcessControl = "CLIPROXY_AUTHFILEGUARD_PROCESS_CONTROL"
)

func TestAuthFileGuardProcessHelper(t *testing.T) {
	action := os.Getenv(authFileGuardProcessAction)
	if action == "" {
		return
	}
	root, errRoot := os.OpenRoot(os.Getenv(authFileGuardProcessRoot))
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()
	if _, errWrite := fmt.Fprintln(os.Stdout, "ready"); errWrite != nil {
		t.Fatal(errWrite)
	}
	control := os.Getenv(authFileGuardProcessControl) != ""
	reader := bufio.NewReader(os.Stdin)
	if control {
		if _, errRead := reader.ReadString('\n'); errRead != nil {
			t.Fatal(errRead)
		}
	}
	if action == "exchange-crash" {
		operations := newExchangeFileOperations(root)
		operations.rename = func(oldPath, newPath string) error {
			if _, errWrite := fmt.Fprintln(os.Stdout, "before-install"); errWrite != nil {
				return errWrite
			}
			if _, errRead := reader.ReadString('\n'); errRead != nil {
				return errRead
			}
			return root.Rename(oldPath, newPath)
		}
		if _, errExchange := exchangeFileByRenameWith(root, "staged.json", "auth.json", operations); errExchange != nil {
			t.Fatal(errExchange)
		}
		return
	}

	var unlock func() error
	var errLock error
	switch action {
	case "target":
		unlock, errLock = LockRootTarget(root, "auth.json")
	case "mutation":
		unlock, errLock = LockRootMutation(root)
	case "mutation-wait":
		result := make(chan struct {
			unlock func() error
			err    error
		}, 1)
		go func() {
			unlockMutation, errMutation := LockRootMutation(root)
			result <- struct {
				unlock func() error
				err    error
			}{unlock: unlockMutation, err: errMutation}
		}()
		processExclusive, _ := rootWriterTurnstileModes(false)
		if errWait := waitForPersistentProcessState(root, rootWriterTurnstileFileName, 2*time.Second, func(state persistentProcessLockTestState) bool {
			if processExclusive {
				return state.writer
			}
			return state.readers >= 1
		}); errWait != nil {
			t.Fatal(errWait)
		}
		if _, errWrite := fmt.Fprintln(os.Stdout, "waiting"); errWrite != nil {
			t.Fatal(errWrite)
		}
		lockResult := <-result
		unlock, errLock = lockResult.unlock, lockResult.err
	case "rebuild":
		unlock, errLock = LockRootRebuild(root)
	default:
		t.Fatalf("unknown helper action %q", action)
	}
	if errLock != nil {
		t.Fatal(errLock)
	}
	if _, errWrite := fmt.Fprintln(os.Stdout, "acquired"); errWrite != nil {
		_ = unlock()
		t.Fatal(errWrite)
	}
	if control {
		if _, errRead := reader.ReadString('\n'); errRead != nil {
			_ = unlock()
			t.Fatal(errRead)
		}
	}
	if errUnlock := unlock(); errUnlock != nil {
		t.Fatal(errUnlock)
	}
}

type controlledAuthFileGuardProcess struct {
	command *exec.Cmd
	stdin   io.WriteCloser
	lines   <-chan string
	stderr  *bytes.Buffer
	waited  bool
	waitErr error
}

func startControlledAuthFileGuardProcess(t *testing.T, executable, root string) *controlledAuthFileGuardProcess {
	return startControlledAuthFileGuardProcessAction(t, executable, root, "target")
}

func startControlledAuthFileGuardProcessAction(t *testing.T, executable, root, action string) *controlledAuthFileGuardProcess {
	t.Helper()
	command := exec.Command(executable, "-test.run=^TestAuthFileGuardProcessHelper$")
	command.Env = append(os.Environ(),
		authFileGuardProcessAction+"="+action,
		authFileGuardProcessRoot+"="+root,
		authFileGuardProcessControl+"=1",
	)
	stdin, errStdin := command.StdinPipe()
	if errStdin != nil {
		t.Fatal(errStdin)
	}
	stdout, errStdout := command.StdoutPipe()
	if errStdout != nil {
		t.Fatal(errStdout)
	}
	stderr := &bytes.Buffer{}
	command.Stderr = stderr
	if errStart := command.Start(); errStart != nil {
		t.Fatal(errStart)
	}
	lines := make(chan string, 2)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()
	child := &controlledAuthFileGuardProcess{command: command, stdin: stdin, lines: lines, stderr: stderr}
	select {
	case output, ok := <-lines:
		if !ok || output != "ready" {
			child.stop()
			t.Fatalf("child did not become ready: output=%q stderr=%q", output, stderr.String())
		}
	case <-time.After(2 * time.Second):
		child.stop()
		t.Fatal("child did not become ready")
	}
	return child
}

func waitForAuthFileGuardWriterIntent(t *testing.T, root *os.Root) {
	t.Helper()
	file, errOpen := openPersistentLockFile(root, rootWriterTurnstileFileName)
	if errOpen != nil {
		t.Fatal(errOpen)
	}
	defer file.Close()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		unlock, acquired, errLock := tryAcquirePersistentFileLock(file, true)
		if errLock != nil {
			t.Fatal(errLock)
		}
		if !acquired {
			return
		}
		if errUnlock := unlock(); errUnlock != nil {
			t.Fatal(errUnlock)
		}
		time.Sleep(lockRetryInterval)
	}
	t.Fatal("rebuild process did not publish writer intent")
}

func (child *controlledAuthFileGuardProcess) signal() error {
	_, errWrite := io.WriteString(child.stdin, "continue\n")
	return errWrite
}

func (child *controlledAuthFileGuardProcess) wait() error {
	if !child.waited {
		child.waitErr = child.command.Wait()
		child.waited = true
	}
	return child.waitErr
}

func (child *controlledAuthFileGuardProcess) stop() {
	if child == nil || child.waited {
		return
	}
	_ = child.command.Process.Kill()
	_ = child.wait()
}

func TestTargetLockFirstCreationSerializesAcrossProcesses(t *testing.T) {
	executable, errExecutable := os.Executable()
	if errExecutable != nil {
		t.Fatal(errExecutable)
	}
	dir := t.TempDir()
	first := startControlledAuthFileGuardProcess(t, executable, dir)
	defer first.stop()
	second := startControlledAuthFileGuardProcess(t, executable, dir)
	defer second.stop()
	if errSignal := first.signal(); errSignal != nil {
		t.Fatal(errSignal)
	}
	if errSignal := second.signal(); errSignal != nil {
		t.Fatal(errSignal)
	}

	var winner, waiting *controlledAuthFileGuardProcess
	select {
	case output := <-first.lines:
		if output != "acquired" {
			t.Fatalf("first child output = %q, stderr=%q", output, first.stderr.String())
		}
		winner, waiting = first, second
	case output := <-second.lines:
		if output != "acquired" {
			t.Fatalf("second child output = %q, stderr=%q", output, second.stderr.String())
		}
		winner, waiting = second, first
	case <-time.After(2 * time.Second):
		t.Fatal("neither child acquired the first-created target lock")
	}
	select {
	case output := <-waiting.lines:
		t.Fatalf("second child acquired before release: output=%q stderr=%q", output, waiting.stderr.String())
	case <-time.After(150 * time.Millisecond):
	}
	if errSignal := winner.signal(); errSignal != nil {
		t.Fatal(errSignal)
	}
	select {
	case output := <-waiting.lines:
		if output != "acquired" {
			t.Fatalf("waiting child output = %q, stderr=%q", output, waiting.stderr.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiting child did not acquire after release")
	}
	if errSignal := waiting.signal(); errSignal != nil {
		t.Fatal(errSignal)
	}
	if errWait := winner.wait(); errWait != nil {
		t.Fatalf("winning child failed: %v, stderr=%q", errWait, winner.stderr.String())
	}
	if errWait := waiting.wait(); errWait != nil {
		t.Fatalf("waiting child failed: %v, stderr=%q", errWait, waiting.stderr.String())
	}
}

func TestRootLocksSerializeAcrossProcesses(t *testing.T) {
	tests := []struct {
		name       string
		child      string
		lockParent func(*os.Root) (func() error, error)
	}{
		{
			name:  "target",
			child: "target",
			lockParent: func(root *os.Root) (func() error, error) {
				return LockRootTarget(root, "auth.json")
			},
		},
		{
			name:  "mutation_blocks_rebuild",
			child: "rebuild",
			lockParent: func(root *os.Root) (func() error, error) {
				return LockRootMutation(root)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			root, errRoot := os.OpenRoot(dir)
			if errRoot != nil {
				t.Fatal(errRoot)
			}
			defer root.Close()
			unlockParent, errLock := test.lockParent(root)
			if errLock != nil {
				t.Fatal(errLock)
			}

			executable, errExecutable := os.Executable()
			if errExecutable != nil {
				_ = unlockParent()
				t.Fatal(errExecutable)
			}
			command := exec.Command(executable, "-test.run=^TestAuthFileGuardProcessHelper$")
			command.Env = append(os.Environ(),
				authFileGuardProcessAction+"="+test.child,
				authFileGuardProcessRoot+"="+dir,
			)
			stdout, errStdout := command.StdoutPipe()
			if errStdout != nil {
				_ = unlockParent()
				t.Fatal(errStdout)
			}
			var stderr bytes.Buffer
			command.Stderr = &stderr
			if errStart := command.Start(); errStart != nil {
				_ = unlockParent()
				t.Fatal(errStart)
			}
			lines := make(chan string, 2)
			go func() {
				defer close(lines)
				scanner := bufio.NewScanner(stdout)
				for scanner.Scan() {
					lines <- scanner.Text()
				}
			}()
			select {
			case output, ok := <-lines:
				if !ok || output != "ready" {
					_ = unlockParent()
					_ = command.Wait()
					t.Fatalf("child did not reach lock attempt: output=%q stderr=%q", output, stderr.String())
				}
			case <-time.After(2 * time.Second):
				_ = unlockParent()
				_ = command.Process.Kill()
				_ = command.Wait()
				t.Fatal("child did not become ready")
			}

			select {
			case output := <-lines:
				_ = unlockParent()
				_ = command.Wait()
				t.Fatalf("child acquired %s lock before parent release: output=%q stderr=%q", test.child, output, stderr.String())
			case <-time.After(150 * time.Millisecond):
			}
			if errUnlock := unlockParent(); errUnlock != nil {
				_ = command.Process.Kill()
				_ = command.Wait()
				t.Fatal(errUnlock)
			}

			select {
			case output := <-lines:
				if output != "acquired" {
					_ = command.Wait()
					t.Fatalf("child output = %q, stderr=%q", output, stderr.String())
				}
			case <-time.After(2 * time.Second):
				_ = command.Process.Kill()
				_ = command.Wait()
				t.Fatalf("child did not acquire %s lock after parent release", test.child)
			}
			if errWait := command.Wait(); errWait != nil {
				t.Fatalf("child failed: %v, stderr=%q", errWait, stderr.String())
			}
		})
	}
}

func TestRootRebuildWriterTurnstileBlocksNewProcessMutations(t *testing.T) {
	executable, errExecutable := os.Executable()
	if errExecutable != nil {
		t.Fatal(errExecutable)
	}
	dir := t.TempDir()
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()

	unlockInitial, errInitial := LockRootMutation(root)
	if errInitial != nil {
		t.Fatal(errInitial)
	}
	initialLocked := true
	defer func() {
		if initialLocked {
			_ = unlockInitial()
		}
	}()

	writer := startControlledAuthFileGuardProcessAction(t, executable, dir, "rebuild")
	defer writer.stop()
	if errSignal := writer.signal(); errSignal != nil {
		t.Fatal(errSignal)
	}
	waitForAuthFileGuardWriterIntent(t, root)

	mutation := startControlledAuthFileGuardProcessAction(t, executable, dir, "mutation-wait")
	defer mutation.stop()
	if errSignal := mutation.signal(); errSignal != nil {
		t.Fatal(errSignal)
	}
	select {
	case output := <-mutation.lines:
		if output != "waiting" {
			t.Fatalf("mutation barrier output=%q stderr=%q", output, mutation.stderr.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("new mutation did not enter turnstile contention")
	}

	if errUnlock := unlockInitial(); errUnlock != nil {
		t.Fatal(errUnlock)
	}
	initialLocked = false
	select {
	case output := <-writer.lines:
		if output != "acquired" {
			t.Fatalf("writer output = %q, stderr=%q", output, writer.stderr.String())
		}
	case output := <-mutation.lines:
		t.Fatalf("mutation acquired before waiting rebuild: output=%q stderr=%q", output, mutation.stderr.String())
	case <-time.After(2 * time.Second):
		t.Fatal("waiting rebuild did not acquire after existing mutation released")
	}
	select {
	case output := <-mutation.lines:
		t.Fatalf("mutation acquired while rebuild held root: output=%q stderr=%q", output, mutation.stderr.String())
	case <-time.After(150 * time.Millisecond):
	}
	if errSignal := writer.signal(); errSignal != nil {
		t.Fatal(errSignal)
	}
	select {
	case output := <-mutation.lines:
		if output != "acquired" {
			t.Fatalf("mutation output = %q, stderr=%q", output, mutation.stderr.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("mutation did not acquire after rebuild released")
	}
	if errSignal := mutation.signal(); errSignal != nil {
		t.Fatal(errSignal)
	}
	if errWait := writer.wait(); errWait != nil {
		t.Fatalf("writer failed: %v, stderr=%q", errWait, writer.stderr.String())
	}
	if errWait := mutation.wait(); errWait != nil {
		t.Fatalf("mutation failed: %v, stderr=%q", errWait, mutation.stderr.String())
	}
}

func TestRenameFallbackCrashBeforeInstallKeepsCanonicalTarget(t *testing.T) {
	executable, errExecutable := os.Executable()
	if errExecutable != nil {
		t.Fatal(errExecutable)
	}
	dir := t.TempDir()
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()
	writeExchangeFallbackGenerations(t, root)
	originalInfo, errOriginal := root.Lstat("auth.json")
	if errOriginal != nil {
		t.Fatal(errOriginal)
	}

	child := startControlledAuthFileGuardProcessAction(t, executable, dir, "exchange-crash")
	if errSignal := child.signal(); errSignal != nil {
		child.stop()
		t.Fatal(errSignal)
	}
	select {
	case output := <-child.lines:
		if output != "before-install" {
			child.stop()
			t.Fatalf("exchange child output=%q stderr=%q", output, child.stderr.String())
		}
	case <-time.After(2 * time.Second):
		child.stop()
		t.Fatal("exchange child did not reach pre-install crash point")
	}
	assertExchangeFallbackFile(t, root, "auth.json", []byte("old"))
	assertExchangeFallbackFile(t, root, "staged.json", []byte("new"))

	child.stop()
	directory, errOpen := root.Open(".")
	if errOpen != nil {
		t.Fatal(errOpen)
	}
	entries, errRead := directory.ReadDir(-1)
	errClose := directory.Close()
	if errRead != nil || errClose != nil {
		t.Fatal(errors.Join(errRead, errClose))
	}
	var displaced string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), displacedFilePrefix) {
			if displaced != "" {
				t.Fatalf("multiple displaced generations after crash: %q and %q", displaced, entry.Name())
			}
			displaced = entry.Name()
		}
	}
	if displaced == "" {
		t.Fatal("crashed fallback did not preserve displaced generation")
	}
	displacedInfo, errDisplaced := root.Lstat(displaced)
	if errDisplaced != nil || !os.SameFile(originalInfo, displacedInfo) {
		t.Fatalf("crashed fallback displaced identity mismatch: %v", errDisplaced)
	}
	assertExchangeFallbackFile(t, root, "auth.json", []byte("old"))
	assertExchangeFallbackFile(t, root, "staged.json", []byte("new"))
}

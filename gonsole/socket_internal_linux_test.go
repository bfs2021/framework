// SPDX-License-Identifier: Apache-2.0

//go:build linux

package gonsole

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// openLock returns a new open file of the lock file at path, closed once t ends.
func openLock(t *testing.T, path string) *os.File {
	t.Helper()
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, socketMode)
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return file
}

// closedLock returns an open file of the lock file at path that is already closed again.
func closedLock(t *testing.T, path string) *os.File {
	t.Helper()
	file := openLock(t, path)
	if err := file.Close(); err != nil {
		t.Fatalf("closing %s: %v", path, err)
	}
	return file
}

func TestExclusivelyTellsAHeldLockFromAFailure(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "myapp.sock.lock")
	if held, err := exclusively(openLock(t, path)); held || err != nil {
		t.Fatalf("exclusively() on a free lock = %t, %v, want false and nil", held, err)
	}
	cases := []struct {
		name string
		file *os.File
		held bool
		err  error
	}{
		{"a lock another open file holds", openLock(t, path), true, syscall.EWOULDBLOCK},
		{"a file closed before the lock", closedLock(t, path), false, syscall.EBADF},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			held, err := exclusively(tc.file)

			if held != tc.held || !errors.Is(err, tc.err) {
				t.Errorf("exclusively() = %t, %v, want %t, %v", held, err, tc.held, tc.err)
			}
		})
	}
}

// tryLock takes the lock file at path from an open file of its own and frees it at once.
func tryLock(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	return syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

// probedListener is a pipe listener that tries the lock file at path as it closes.
type probedListener struct {
	*pipeListener
	path   string
	probed error
}

// Close tries the lock file at path, then makes every later Accept fail.
func (l *probedListener) Close() error {
	l.probed = tryLock(l.path)
	return l.pipeListener.Close()
}

func TestLockedHoldsTheLockWhileTheSocketCloses(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "myapp.sock.lock")
	lock := openLock(t, path)
	if held, err := exclusively(lock); held || err != nil {
		t.Fatalf("exclusively() on a free lock = %t, %v, want false and nil", held, err)
	}
	listener := &probedListener{pipeListener: newPipeListener(), path: path}

	err := locked{Listener: listener, lock: lock}.Close()

	if !errors.Is(listener.probed, syscall.EWOULDBLOCK) {
		t.Errorf("locking %s while the socket closed = %v, want it still held", path, listener.probed)
	}
	if err != nil {
		t.Errorf("Close() = %v, want nil", err)
	}
}

func TestBindLockedRefusesALockItCannotTake(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "myapp.sock")
	socket := socketFile(path)

	listener, err := socket.bindLocked(t.Context(), closedLock(t, socket.lockPath()))

	if want := "lock the socket at " + path + ": bad file descriptor"; err == nil || err.Error() != want {
		t.Errorf("bindLocked() = %v, want %q", err, want)
	}
	if listener != nil {
		t.Errorf("bindLocked() listened on %s, want nothing bound", listener.Addr())
	}
}

func TestClearStaleRefusesAPathItCannotRead(t *testing.T) {
	t.Parallel()

	above := filepath.Join(t.TempDir(), "notes")
	if err := os.WriteFile(above, []byte("quarterly\n"), 0o600); err != nil {
		t.Fatalf("writing %s: %v", above, err)
	}
	path := filepath.Join(above, "myapp.sock")

	err := socketFile(path).clearStale(t.Context())

	if want := "lstat " + path + ": not a directory"; err == nil || err.Error() != want {
		t.Errorf("clearStale() = %v, want %q", err, want)
	}
}

// SPDX-License-Identifier: Apache-2.0

//go:build linux

package gonsole

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"strings"
	"syscall"
)

// socketMode is the mode of every socket file ServeListening creates, and of the lock file beside it.
const socketMode = 0o600

// socketPathLimit is the longest socket path in bytes the kernel binds.
const socketPathLimit = len(syscall.RawSockaddrUnix{}.Path) - 1

// lockSuffix names the lock file beside each socket, the socket's path with .lock added.
const lockSuffix = ".lock"

// checkSocketPath refuses an abstract socket name and a path longer than the kernel binds.
func checkSocketPath(path string) error {
	if strings.HasPrefix(path, "@") {
		return complaint("must be a file path, not an abstract name", path)
	}
	if len(path) > socketPathLimit {
		return fmt.Errorf("must be at most %d bytes, got %d", socketPathLimit, len(path))
	}
	return nil
}

// listenSocket returns a listener on a new socket at path that only this account can open, holding its lock file.
func listenSocket(ctx context.Context, path string) (net.Listener, error) {
	socket := socketFile(path)
	lock, err := os.OpenFile(socket.lockPath(), os.O_RDWR|os.O_CREATE, socketMode)
	if err != nil {
		return nil, socket.lockFailed(err)
	}
	listener, err := socket.bindLocked(ctx, lock)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	return locked{Listener: listener, lock: lock}, nil
}

// exclusively takes the lock on file without waiting, reporting whether another open file holds it.
func exclusively(file *os.File) (bool, error) {
	err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	return errors.Is(err, syscall.EWOULDBLOCK), err
}

// privately sets the mode of the socket c holds to socketMode before it binds.
func privately(_, _ string, c syscall.RawConn) error {
	var fchmod error
	cerr := c.Control(func(fd uintptr) { fchmod = syscall.Fchmod(int(fd), socketMode) })
	return errors.Join(cerr, fchmod)
}

// socketFile is the path of a socket file, with its lock file beside it.
type socketFile string

// lockPath returns the path of the lock file beside the socket.
func (s socketFile) lockPath() string {
	return string(s) + lockSuffix
}

// bindLocked takes lock without waiting, then clears the socket's path and binds a private socket there.
func (s socketFile) bindLocked(ctx context.Context, lock *os.File) (net.Listener, error) {
	held, err := exclusively(lock)
	if held {
		return nil, s.refuseHeld(ctx)
	}
	if err != nil {
		return nil, s.lockFailed(err)
	}
	if err := s.clearStale(ctx); err != nil {
		return nil, err
	}
	return s.bindPrivately(ctx)
}

// lockFailed is the refusal of a start that could not take the lock beside the socket.
func (s socketFile) lockFailed(err error) error {
	return fmt.Errorf("lock the socket at %s: %w", s, err)
}

// refuseHeld names what holds the lock beside the socket, a server that answers on it, else one starting.
func (s socketFile) refuseHeld(ctx context.Context) error {
	if s.knock(ctx) == nil {
		return s.listensOn()
	}
	return fmt.Errorf("another server is starting on %s", s)
}

// listensOn is the refusal of a socket that a server answers on.
func (s socketFile) listensOn() error {
	return fmt.Errorf("a server already listens on %s", s)
}

// knock dials the socket and hangs up, whatever the end of ctx.
func (s socketFile) knock(ctx context.Context) error {
	var dialer net.Dialer
	conn, err := dialer.DialContext(context.WithoutCancel(ctx), "unix", string(s))
	if err != nil {
		return err
	}
	_ = conn.Close()
	return nil
}

// clearStale removes a socket at the path that nothing listens on, refusing whatever else it finds there.
func (s socketFile) clearStale(ctx context.Context) error {
	info, err := os.Lstat(string(s))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode().Type() != fs.ModeSocket {
		return fmt.Errorf("%s is not a socket, so it stays in place", s)
	}
	err = s.knock(ctx)
	if err == nil {
		return s.listensOn()
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		return fmt.Errorf("check the socket at %s: %w", s, err)
	}
	return os.Remove(string(s))
}

// bindPrivately binds a socket of mode socketMode at the path, removing it again when the umask took bits it needs.
func (s socketFile) bindPrivately(ctx context.Context) (net.Listener, error) {
	config := net.ListenConfig{Control: privately}
	listener, err := config.Listen(context.WithoutCancel(ctx), "unix", string(s))
	if err != nil {
		return nil, err
	}
	if !s.private() {
		_ = listener.Close()
		return nil, fmt.Errorf("the socket at %s is not mode %04o, check the umask", s, socketMode)
	}
	return listener, nil
}

// private reports whether the path holds a socket of mode socketMode.
func (s socketFile) private() bool {
	info, err := os.Lstat(string(s))
	return err == nil && info.Mode() == fs.ModeSocket|socketMode
}

// locked is a socket listener that frees its lock once its socket file is gone.
type locked struct {
	net.Listener
	lock *os.File
}

// Close removes the socket file, then frees the lock.
func (l locked) Close() error {
	err := l.Listener.Close()
	return errors.Join(err, l.lock.Close())
}

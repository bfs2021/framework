// SPDX-License-Identifier: Apache-2.0

//go:build linux

package gonsole_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/gopherium/framework/gonsole"
)

// lockFile returns the place of the lock file beside the socket at p.
func (p place) lockFile() place {
	return p + ".lock"
}

// tryLock takes the lock beside the socket at p from an open file of its own and frees it at once.
func (p place) tryLock() error {
	file, err := os.OpenFile(string(p.lockFile()), os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	return syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

// holdLock takes the lock beside the socket at p from an open file of its own, as another start would, until t ends.
func (p place) holdLock(t *testing.T) {
	t.Helper()
	file, err := os.OpenFile(string(p.lockFile()), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatalf("opening %s: %v", p.lockFile(), err)
	}
	t.Cleanup(func() { _ = file.Close() })
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatalf("locking %s: %v", p.lockFile(), err)
	}
}

// wantLockHeld fails t unless the lock file beside the socket at p is in place and another open file holds it.
func (p place) wantLockHeld(t *testing.T, holder string) {
	t.Helper()
	if err := p.tryLock(); !errors.Is(err, syscall.EWOULDBLOCK) {
		t.Errorf("locking %s once the start was refused = %v, want the lock of %s still held there",
			p.lockFile(), err, holder)
	}
}

// noForks keeps the process from starting a child until t ends.
func noForks(t *testing.T) {
	t.Helper()
	syscall.ForkLock.RLock()
	t.Cleanup(syscall.ForkLock.RUnlock)
}

// boundSocket leaves at p a socket that is bound and never listens, as a start between its bind and its listen does.
func (p place) boundSocket(t *testing.T) {
	t.Helper()
	fd, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("making a socket: %v", err)
	}
	t.Cleanup(func() { _ = syscall.Close(fd) })
	if err := syscall.Bind(fd, &syscall.SockaddrUnix{Name: string(p)}); err != nil {
		t.Fatalf("binding %s: %v", p, err)
	}
}

// attempt is one start of ServeListening that a test does not wait for.
type attempt struct {
	journal *journal
	done    chan error
	cancel  context.CancelFunc
}

// attemptWith starts ServeListening on srv and l under ctx and returns at once.
func attemptWith(ctx context.Context, srv *http.Server, l gonsole.Listening) attempt {
	ctx, cancel := context.WithCancel(ctx)
	a := attempt{journal: newJournal(), done: make(chan error, 1), cancel: cancel}
	go func() { a.done <- gonsole.ServeListening(ctx, srv, defaults, l, nil, a.journal.logger()) }()
	return a
}

// attemptOn starts ServeListening over the report names on l under ctx and returns at once.
func attemptOn(ctx context.Context, l gonsole.Listening) attempt {
	return attemptWith(ctx, gonsole.NewServer("", reportNames(), defaults), l)
}

// settle waits until the start listens or returns, and reports which with what it returned.
func (a attempt) settle() (bool, error) {
	select {
	case <-a.journal.listening:
		return true, nil
	case err := <-a.done:
		return false, err
	}
}

// end ends a start that listens and returns what it returned.
func (a attempt) end(t *testing.T) error {
	t.Helper()
	a.cancel()
	return ended(t, a.done, defaults)
}

func TestServeListeningRefusesAStartThatOverlapsAnotherOnAStaleSocket(t *testing.T) {
	t.Parallel()

	path := socketAt(t)
	path.deadSocket(t)
	var second attempt
	var listened bool
	var refused error
	overlap := sync.OnceFunc(func() {
		second = attemptOn(t.Context(), path.listening())
		listened, refused = second.settle()
	})
	traced := httptrace.WithClientTrace(t.Context(), &httptrace.ClientTrace{
		ConnectDone: func(string, string, error) { overlap() },
	})
	first := attemptOn(traced, path.listening())
	if serving, err := first.settle(); !serving {
		t.Fatalf("the first ServeListening() returned %v before listening", err)
	}

	body := path.get(t)

	path.wantLockHeld(t, "the first start")
	if listened {
		t.Errorf("the second start listened while the first checked the stale socket, want it refused")
		_ = second.end(t)
	}
	if want := "http server: another server is starting on " + string(path); !listened && errorText(refused) != want {
		t.Errorf("the second ServeListening() = %q, want %q", errorText(refused), want)
	}
	if body != "quarterly\nyearly\n" {
		t.Errorf("body = %q, want the first server answering the report names", body)
	}
	if err := first.end(t); err != nil {
		t.Errorf("the first ServeListening() = %v, want nil", err)
	}
}

func TestServeListeningRefusesAStartWhileAnotherHoldsTheLock(t *testing.T) {
	t.Parallel()

	path := socketAt(t)
	path.holdLock(t)
	path.boundSocket(t)
	before := path.describe()

	listenRefused(t, path.listening(), "http server: another server is starting on "+string(path))

	path.wantLockHeld(t, "the start that holds it")
	if after := path.describe(); after != before {
		t.Errorf("%s was %s and is now %s, want the socket of the start that holds the lock left in place",
			path, before, after)
	}
}

func TestServeListeningHoldsTheLockUntilTheSocketIsGone(t *testing.T) {
	t.Parallel()

	path := socketAt(t)
	noForks(t)
	run := serveListening(t, path.listening(), reportNames(), defaults)

	serving := path.tryLock()
	err := run.end()
	stopped := path.tryLock()

	if !errors.Is(serving, syscall.EWOULDBLOCK) {
		t.Errorf("locking %s while the server served = %v, want it held", path.lockFile(), serving)
	}
	if err != nil || stopped != nil {
		t.Errorf("ServeListening() = %v, locking once it returned = %v, want nil and the lock free", err, stopped)
	}
	if !path.gone() {
		t.Errorf("%s is still there once ServeListening returned, want it removed", path)
	}
	if got := path.lockFile().describe(); got != `-rw-------, 0 bytes, linked to ""` {
		t.Errorf("the lock file is %s, want it left in place, empty and of mode 0600", got)
	}
}

func TestServeListeningRefusesASocketAProgramWithoutTheLockServes(t *testing.T) {
	t.Parallel()

	path := socketAt(t)
	other, err := net.Listen("unix", string(path))
	if err != nil {
		t.Fatalf("listening on %s: %v", path, err)
	}
	defer func() { _ = other.Close() }()

	listenRefused(t, path.listening(), "http server: a server already listens on "+string(path))

	conn, err := net.Dial("unix", string(path))
	if err != nil {
		t.Fatalf("the other program's socket at %s stopped answering: %v", path, err)
	}
	_ = conn.Close()
}

// heldUntil returns a handler that tells arrived of its request, then answers once proceed closes.
func heldUntil(arrived chan<- struct{}, proceed <-chan struct{}) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(arrived)
		<-proceed
		_, _ = io.WriteString(w, "quarterly\n")
	})
}

func TestServeListeningLetsANewStartServeWhileTheOldOneDrains(t *testing.T) {
	t.Parallel()

	path := socketAt(t)
	noForks(t)
	arrived := make(chan struct{})
	proceed := make(chan struct{})
	release := sync.OnceFunc(func() { close(proceed) })
	defer release()
	srv := gonsole.NewServer("", heldUntil(arrived, proceed), defaults)
	unlistened := make(chan struct{})
	srv.RegisterOnShutdown(func() { close(unlistened) })
	old := attemptWith(t.Context(), srv, path.listening())
	if serving, err := old.settle(); !serving {
		t.Fatalf("the old ServeListening() returned %v before listening", err)
	}
	answered := make(chan int, 1)
	go func() {
		code, _ := path.status()
		answered <- code
	}()
	<-arrived
	old.cancel()
	<-unlistened

	fresh := serveListening(t, path.listening(), reportNames(), defaults)
	body := path.get(t)
	release()

	if code := <-answered; code != http.StatusOK {
		t.Errorf("answer %d, want the request the old run drained answered 200", code)
	}
	if err := ended(t, old.done, defaults); err != nil {
		t.Errorf("the old ServeListening() = %v, want nil", err)
	}
	if body != "quarterly\nyearly\n" {
		t.Errorf("body = %q, want the new start answering the report names while the old run drained", body)
	}
	if err := fresh.end(); err != nil {
		t.Errorf("the new ServeListening() = %v, want nil", err)
	}
}

func TestServeListeningStopsWhenTheSocketsFolderIsReadOnly(t *testing.T) {
	t.Parallel()

	folder := shortFolder(t)
	path := place(filepath.Join(folder, "myapp.sock"))
	if err := os.WriteFile(string(path.lockFile()), nil, 0o600); err != nil {
		t.Fatalf("writing %s: %v", path.lockFile(), err)
	}
	if err := os.Chmod(folder, 0o500); err != nil {
		t.Fatalf("taking the write bit off %s: %v", folder, err)
	}
	t.Cleanup(func() { _ = os.Chmod(folder, 0o700) })

	listenRefused(t, path.listening(), "http server: listen unix "+string(path)+": bind: permission denied")
}

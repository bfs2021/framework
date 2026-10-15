// SPDX-License-Identifier: Apache-2.0

package gonsole

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestVisitorAddrReadsOneAddress(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		values []string
		want   string
		ok     bool
	}{
		{"an IPv4 address", []string{"203.0.113.7"}, "203.0.113.7:0", true},
		{"an IPv4 address with its port", []string{"203.0.113.7:8443"}, "203.0.113.7:8443", true},
		{"an IPv6 address", []string{"2001:db8::1"}, "[2001:db8::1]:0", true},
		{"an IPv6 address with its port", []string{"[2001:db8::1]:443"}, "[2001:db8::1]:443", true},
		{"an IPv4-mapped address", []string{"::ffff:203.0.113.7"}, "203.0.113.7:0", true},
		{"an address with a zone", []string{"fe80::1%eth0"}, "[fe80::1]:0", true},
		{"no value", nil, "", false},
		{"an empty value", []string{""}, "", false},
		{"two values", []string{"203.0.113.7", "198.51.100.2"}, "", false},
		{"a comma list", []string{"203.0.113.7, 198.51.100.2"}, "", false},
		{"a comma list after a zone", []string{"fe80::1%eth0, 203.0.113.7"}, "", false},
		{"a comma list after a zone with a port", []string{"[fe80::1%eth0,203.0.113.7]:443"}, "", false},
		{"a word", []string{"unknown"}, "", false},
		{"a port above the highest", []string{"203.0.113.7:99999"}, "", false},
		{"an IPv6 address in brackets with no port", []string{"[2001:db8::1]"}, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, ok := visitorAddr(tc.values)

			if got != tc.want || ok != tc.ok {
				t.Errorf("visitorAddr(%q) = %q, %t, want %q, %t", tc.values, got, ok, tc.want, tc.ok)
			}
		})
	}
}

// pipeAddr is the address of every end of an in-memory pipe.
type pipeAddr struct{}

// Network names the pipe.
func (pipeAddr) Network() string { return "pipe" }

// String names the pipe.
func (pipeAddr) String() string { return "pipe" }

// pipeListener hands out the server ends of the in-memory pipes its dial makes.
type pipeListener struct {
	conns  chan net.Conn
	closed chan struct{}
	once   sync.Once
}

// newPipeListener returns a pipe listener that holds up to four connections nobody accepted yet.
func newPipeListener() *pipeListener {
	return &pipeListener{conns: make(chan net.Conn, 4), closed: make(chan struct{})}
}

// Accept returns the server end of the next pipe, or net.ErrClosed once the listener closed.
func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.conns:
		return conn, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

// Close makes every later Accept fail.
func (l *pipeListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

// Addr returns the pipe's address.
func (l *pipeListener) Addr() net.Addr {
	return pipeAddr{}
}

// dial returns the client end of a new pipe whose server end the listener hands out.
func (l *pipeListener) dial() net.Conn {
	client, server := net.Pipe()
	l.conns <- server
	return client
}

// idleWindow is the idle exit the idler tests run under, in the fake time of a synctest bubble.
const idleWindow = 10 * time.Second

// tick is the shortest step of fake time.
const tick = time.Nanosecond

// ledger is a log target that keeps every line, safe to read while another goroutine writes.
type ledger struct {
	mu   sync.Mutex
	text strings.Builder
}

// Write keeps p.
func (l *ledger) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.text.Write(p)
}

// String returns everything kept.
func (l *ledger) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.text.String()
}

// idleRun is an idler under test with the run it ends, how often it ended it and what it logged.
type idleRun struct {
	idle   *idler
	run    context.Context
	ends   *atomic.Int32
	logged *ledger
}

// startIdle returns an idler over a run of parent that ends once nothing has been open for idleWindow.
func startIdle(parent context.Context) idleRun {
	run, end := context.WithCancel(parent)
	ends := new(atomic.Int32)
	logged := new(ledger)
	counted := func() {
		ends.Add(1)
		end()
	}
	idle := newIdler(run, counted, idleWindow, slog.New(slog.NewTextHandler(logged, nil)))
	return idleRun{idle: idle, run: run, ends: ends, logged: logged}
}

// saidIdle reports whether the idler logged its idle line.
func (r idleRun) saidIdle() bool {
	return strings.Contains(r.logged.String(), `level=INFO msg="no connection for the idle exit" idle_exit=10s`)
}

// liveAfter lets d of fake time pass and reports whether run is still live.
func liveAfter(run context.Context, d time.Duration) bool {
	time.Sleep(d)
	synctest.Wait()
	return run.Err() == nil
}

// endsAWindowAfter fails t unless run, from now, is live a tick before a window passes and ended once it has.
func endsAWindowAfter(t *testing.T, run context.Context, event string) {
	t.Helper()
	closing := liveAfter(run, idleWindow-tick)
	closed := liveAfter(run, tick)
	if !closing {
		t.Errorf("the run ended a tick before a window passed since %s", event)
	}
	if closed {
		t.Errorf("the run is live a window after %s, want it ended", event)
	}
}

// connect returns the server end of a new pipe, accepted through a listener the idler counts.
func (r idleRun) connect(t *testing.T) net.Conn {
	t.Helper()
	pipes := newPipeListener()
	pipes.dial()
	conn, err := r.idle.counting(pipes).Accept()
	if err != nil {
		t.Fatalf("accepting a pipe: %v", err)
	}
	return conn
}

// windowed is a listener whose idle exit is idleWindow.
var windowed = Listening{Address: "127.0.0.1:0", IdleExit: idleWindow}

func TestIdlerEndsTheRunAtTheWindow(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		r := startIdle(t.Context())
		defer r.idle.stop()

		early := liveAfter(r.run, idleWindow-tick)
		late := liveAfter(r.run, tick)

		if !early {
			t.Errorf("the run ended a tick before the window passed")
		}
		if late {
			t.Errorf("the run is live once the window passed with nothing open, want it ended")
		}
		if !r.saidIdle() {
			t.Errorf("log = %q, want the idle line", r.logged.String())
		}
	})
}

func TestIdlerWaitsWhileAConnectionIsOpen(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		r := startIdle(t.Context())
		defer r.idle.stop()

		conn := r.connect(t)
		open := liveAfter(r.run, 2*idleWindow)
		_ = conn.Close()

		if !open {
			t.Errorf("the run ended while a connection was open")
		}
		endsAWindowAfter(t, r.run, "the close")
	})
}

func TestIdlerCountsAConnectionClosedTwiceOnce(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		r := startIdle(t.Context())
		defer r.idle.stop()
		first := r.connect(t)
		second := r.connect(t)

		_ = first.Close()
		_ = first.Close()
		open := liveAfter(r.run, 2*idleWindow)
		_ = second.Close()

		if !open {
			t.Errorf("the run ended while a second connection was open, want the first counted closed once")
		}
		endsAWindowAfter(t, r.run, "the last close")
	})
}

func TestIdlerStartsTheWindowAgainAfterAQuickConnection(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		r := startIdle(t.Context())
		defer r.idle.stop()
		time.Sleep(idleWindow / 2)

		_ = r.connect(t).Close()
		first := liveAfter(r.run, idleWindow/2)
		closing := liveAfter(r.run, idleWindow/2-tick)
		closed := liveAfter(r.run, tick)

		if !first {
			t.Errorf("the run ended when the first window passed, want the window started again at the close")
		}
		if !closing {
			t.Errorf("the run ended a tick before a window passed since the close")
		}
		if closed {
			t.Errorf("the run is live a window after the close, want it ended")
		}
	})
}

func TestIdlerWaitsAWindowAfterAStaleFire(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		r := startIdle(t.Context())
		defer r.idle.stop()
		conn := r.connect(t)
		time.Sleep(idleWindow)
		synctest.Wait()

		_ = conn.Close()
		r.idle.fire()

		if r.run.Err() != nil {
			t.Errorf("the run ended on a timer fire right after the close, want it to wait a window from the close")
		}
		endsAWindowAfter(t, r.run, "the close")
	})
}

func TestIdlerWaitsForARunningHandler(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		r := startIdle(t.Context())
		defer r.idle.stop()
		release := make(chan struct{})
		handler := r.idle.wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))

		conn := r.connect(t)
		go handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
		synctest.Wait()
		_ = conn.Close()
		running := liveAfter(r.run, 2*idleWindow)
		close(release)

		if !running {
			t.Errorf("the run ended while a handler ran after its connection closed")
		}
		endsAWindowAfter(t, r.run, "the handler returned")
	})
}

// takeOver is a handler that takes its connection over from net/http and hands it to taken.
func takeOver(t *testing.T, taken chan<- net.Conn) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Errorf("taking the connection over: %v", err)
			return
		}
		taken <- conn
	})
}

func TestIdlingCountsAConnectionAHandlerTakesOverUntilItCloses(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		taken := make(chan net.Conn, 1)
		srv := &http.Server{Handler: takeOver(t, taken)}
		pipes := newPipeListener()
		run, listener, stop := windowed.idling(t.Context(), srv, pipes, slog.New(slog.DiscardHandler))
		defer stop()
		served := make(chan error, 1)
		go func() { served <- srv.Serve(listener) }()
		client := pipes.dial()
		_, _ = io.WriteString(client, "GET /reports HTTP/1.1\r\nHost: reports.example\r\n\r\n")
		conn := <-taken
		synctest.Wait()

		open := liveAfter(run, 2*idleWindow)
		_ = conn.Close()

		if !open {
			t.Errorf("the run ended while a connection a handler took over was open after its handler returned")
		}
		endsAWindowAfter(t, run, "the close of the connection the handler took over")
		_ = client.Close()
		_ = srv.Close()
		<-served
	})
}

// halfCloser is a connection whose writing side closes alone.
type halfCloser interface {
	CloseWrite() error
}

// reportFile returns a file in a new folder of t that holds 136 KB of report names.
func reportFile(t *testing.T) (*os.File, []byte) {
	t.Helper()
	body := bytes.Repeat([]byte("quarterly\n"), 13600)
	path := filepath.Join(t.TempDir(), "reports.txt")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return file, body
}

// acceptTCP returns both ends of a new loopback TCP connection, the server's end through a listener r counts.
func (r idleRun) acceptTCP(t *testing.T) (client, counted net.Conn) {
	t.Helper()
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	t.Cleanup(func() { _ = tcp.Close() })
	client, err = net.Dial("tcp", tcp.Addr().String())
	if err != nil {
		t.Fatalf("dialling %s: %v", tcp.Addr(), err)
	}
	t.Cleanup(func() { _ = client.Close() })
	counted, err = r.idle.counting(tcp).Accept()
	if err != nil {
		t.Fatalf("accepting: %v", err)
	}
	t.Cleanup(func() { _ = counted.Close() })
	return client, counted
}

func TestCountedConnectionsKeepTheFastPathsOfTCP(t *testing.T) {
	t.Parallel()

	r := startIdle(t.Context())
	defer r.idle.stop()
	client, conn := r.acceptTCP(t)
	file, body := reportFile(t)
	half, halves := conn.(halfCloser)
	from, copies := conn.(io.ReaderFrom)
	if !halves || !copies {
		t.Fatalf("the counted TCP connection keeps CloseWrite %t and ReadFrom %t, want both kept", halves, copies)
	}
	received := make(chan []byte, 1)
	go func() {
		got, _ := io.ReadAll(client)
		received <- got
	}()

	sent, sendErr := from.ReadFrom(file)
	closeErr := half.CloseWrite()

	if got := <-received; !bytes.Equal(got, body) {
		t.Errorf("the client read %d bytes before the half close, want all %d", len(got), len(body))
	}
	if sent != int64(len(body)) || sendErr != nil {
		t.Errorf("ReadFrom() = %d, %v, want %d and nil", sent, sendErr, len(body))
	}
	if closeErr != nil {
		t.Errorf("CloseWrite() = %v, want nil", closeErr)
	}
}

func TestCountedConnectionsInventNoHalfCloseOnAPipe(t *testing.T) {
	t.Parallel()

	r := startIdle(t.Context())
	defer r.idle.stop()
	conn := r.connect(t)
	defer func() { _ = conn.Close() }()

	if _, halves := conn.(halfCloser); halves {
		t.Errorf("the counted pipe offers CloseWrite, want no half close the pipe lacks")
	}
}

// recovered serves one request through handler and returns what it panicked with.
func recovered(handler http.Handler) (panicked any) {
	defer func() { panicked = recover() }()
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	return nil
}

func TestIdlerReleasesAHandlerThatPanics(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		r := startIdle(t.Context())
		defer r.idle.stop()
		handler := r.idle.wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic(http.ErrAbortHandler) }))
		time.Sleep(idleWindow / 2)

		panicked := recovered(handler)

		if panicked != http.ErrAbortHandler {
			t.Errorf("the wrapped handler panicked with %v, want the handler's own panic passed on", panicked)
		}
		endsAWindowAfter(t, r.run, "the handler panicked")
	})
}

func TestIdlerStaysQuietAfterTheRunEnds(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		parent, endParent := context.WithCancel(t.Context())
		r := startIdle(parent)
		defer r.idle.stop()

		endParent()
		time.Sleep(idleWindow)
		synctest.Wait()

		if r.saidIdle() {
			t.Errorf("log = %q, want no idle line once the run ended", r.logged.String())
		}
		if ends := r.ends.Load(); ends != 0 {
			t.Errorf("the idler ended the run %d times after it ended, want none", ends)
		}
	})
}

// sameHook reports whether got is the hook want, the one function.
func sameHook(got, want func(net.Conn, http.ConnState)) bool {
	return reflect.ValueOf(got).Pointer() == reflect.ValueOf(want).Pointer()
}

func TestIdlingLeavesTheServersOwnConnState(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		own := func(net.Conn, http.ConnState) {}
		srv := &http.Server{ConnState: own}

		_, _, stop := windowed.idling(t.Context(), srv, newPipeListener(), slog.New(slog.DiscardHandler))
		stop()

		if !sameHook(srv.ConnState, own) {
			t.Errorf("ConnState changed, want the server's own hook left in place")
		}
	})
}

func TestIdlingCountsTheServersRunningRequests(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		srv := &http.Server{Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release })}
		run, _, stop := windowed.idling(t.Context(), srv, newPipeListener(), slog.New(slog.DiscardHandler))
		defer stop()

		go srv.Handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
		running := liveAfter(run, 2*idleWindow)
		close(release)
		returned := liveAfter(run, idleWindow)

		if !running {
			t.Errorf("the run ended while a request of the server ran, want the idler to count it")
		}
		if returned {
			t.Errorf("the run is live a window after the request returned, want it ended")
		}
	})
}

func TestServeListeningStopsItsIdlerWhenServingFails(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		timeouts := Timeouts{ReadHeader: time.Second, Read: time.Second, Idle: time.Minute, Grace: time.Second,
			CancelGrace: time.Second, StopGrace: time.Second}
		srv := NewServer("", http.NotFoundHandler(), timeouts)
		_ = srv.Close()
		logged := new(ledger)
		at := Listening{Address: "127.0.0.1:0", IdleExit: idleWindow}

		err := ServeListening(t.Context(), srv, timeouts, at, nil, slog.New(slog.NewTextHandler(logged, nil)))
		time.Sleep(idleWindow)
		synctest.Wait()

		if !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("ServeListening() = %v, want the serving failure", err)
		}
		if strings.Contains(logged.String(), "no connection for the idle exit") {
			t.Errorf("log = %q, want no idle line once ServeListening returned", logged.String())
		}
	})
}

// still is a handler that answers nothing, comparable with ==.
type still struct{}

// ServeHTTP answers nothing.
func (still) ServeHTTP(http.ResponseWriter, *http.Request) {}

func TestIdlingWithZeroLeavesTheServerAlone(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		own := func(net.Conn, http.ConnState) {}
		srv := &http.Server{ConnState: own, Handler: still{}}
		pipes := newPipeListener()
		never := Listening{Address: "127.0.0.1:0"}

		run, listener, stop := never.idling(t.Context(), srv, pipes, slog.New(slog.DiscardHandler))
		stop()

		if run != t.Context() {
			t.Errorf("idling() returned a new context, want the one it was given")
		}
		if listener != net.Listener(pipes) {
			t.Errorf("idling() returned the listener %T, want the one it was given", listener)
		}
		if !sameHook(srv.ConnState, own) {
			t.Errorf("ConnState changed, want the server's own hook left in place")
		}
		if srv.Handler != http.Handler(still{}) {
			t.Errorf("Handler = %T, want the server's own handler left in place", srv.Handler)
		}
	})
}

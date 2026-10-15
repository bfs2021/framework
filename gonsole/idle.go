// SPDX-License-Identifier: Apache-2.0

package gonsole

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

// idler counts what is open on one server and ends its run once nothing has been open for its window.
type idler struct {
	mu     sync.Mutex
	open   int
	quiet  time.Time
	window time.Duration
	run    context.Context
	end    context.CancelFunc
	logger *slog.Logger
	timer  *time.Timer
}

// newIdler returns an idler that calls end once nothing has been open for window, unless run already ended.
func newIdler(run context.Context, end context.CancelFunc, window time.Duration, logger *slog.Logger) *idler {
	idle := &idler{quiet: time.Now(), window: window, run: run, end: end, logger: logger}
	idle.timer = time.AfterFunc(window, idle.fire)
	return idle
}

// counting returns listener with each connection it accepts counted open until that connection's first close.
func (i *idler) counting(listener net.Listener) net.Listener {
	return countedListener{Listener: listener, idle: i}
}

// wrap returns next, counting each request while it runs.
func (i *idler) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i.hold()
		defer i.release()
		next.ServeHTTP(w, r)
	})
}

// hold counts one more open thing.
func (i *idler) hold() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.open++
}

// release counts one open thing fewer, starting the window again once nothing is open.
func (i *idler) release() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.open--
	if i.open == 0 {
		i.quiet = time.Now()
		i.timer.Reset(i.window)
	}
}

// fire ends the run once nothing has been open for a full window and the run is still live.
func (i *idler) fire() {
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.due() || i.run.Err() != nil {
		return
	}
	i.logger.Info("no connection for the idle exit", "idle_exit", i.window)
	i.end()
}

// due reports whether nothing is open and nothing has been for a full window.
func (i *idler) due() bool {
	return i.open == 0 && time.Since(i.quiet) >= i.window
}

// stop stops the timer and ends the run.
func (i *idler) stop() {
	i.timer.Stop()
	i.end()
}

// idling returns the run of srv under ctx and listener counted for l's idle exit, zero never, with its stop.
func (l Listening) idling(
	ctx context.Context, srv *http.Server, listener net.Listener, logger *slog.Logger,
) (context.Context, net.Listener, func()) {
	if l.IdleExit == 0 {
		return ctx, listener, func() {}
	}
	run, end := context.WithCancel(ctx)
	idle := newIdler(run, end, l.IdleExit, logger)
	srv.Handler = idle.wrap(srv.Handler)
	return run, idle.counting(listener), idle.stop
}

// countedListener is a listener whose idler counts each connection it accepts until that connection's first close.
type countedListener struct {
	net.Listener
	idle *idler
}

// Accept waits for the next connection and counts it open until its first close.
func (c countedListener) Accept() (net.Conn, error) {
	conn, err := c.Listener.Accept()
	if err != nil {
		return nil, err
	}
	c.idle.hold()
	return countedOnce(conn, sync.OnceFunc(c.idle.release)), nil
}

// closeWriter is a connection whose writing side closes alone.
type closeWriter interface {
	CloseWrite() error
}

// countedOnce returns conn calling closed when it closes, keeping its half close when it has one.
func countedOnce(conn net.Conn, closed func()) net.Conn {
	counted := countedConn{Conn: conn, closed: closed}
	if half, ok := conn.(closeWriter); ok {
		return halfClosingConn{countedConn: counted, half: half}
	}
	return counted
}

// countedConn is a connection that calls closed each time it closes.
type countedConn struct {
	net.Conn
	closed func()
}

// Close closes the connection and calls closed.
func (c countedConn) Close() error {
	defer c.closed()
	return c.Conn.Close()
}

// ReadFrom copies r into the connection as io.Copy does, keeping the connection's own sendfile path.
func (c countedConn) ReadFrom(r io.Reader) (int64, error) {
	return io.Copy(c.Conn, r)
}

// halfClosingConn is a counted connection that keeps the half close of the connection it counts.
type halfClosingConn struct {
	countedConn
	half closeWriter
}

// CloseWrite closes the writing side of the connection.
func (c halfClosingConn) CloseWrite() error {
	return c.half.CloseWrite()
}

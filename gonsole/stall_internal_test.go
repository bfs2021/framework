// SPDX-License-Identifier: Apache-2.0

package gonsole

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"testing/synctest"
	"time"
)

// stallBound is how much fake time a stalled body may hold a run before a test calls the run held.
const stallBound = time.Hour

// readsTheBody answers each request once it has read the whole body.
func readsTheBody() http.Handler {
	return http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
	})
}

// stallABody sends over a new pipe a request that promises 1000 bytes of body and sends one, then reads every answer.
func stallABody(pipes *pipeListener) {
	client := pipes.dial()
	go func() {
		_, _ = io.WriteString(client, "POST /reports HTTP/1.1\r\nHost: reports.example\r\nContent-Length: 1000\r\n\r\nq")
		_, _ = io.Copy(io.Discard, client)
		_ = client.Close()
	}()
}

// heldByAStall reports whether a stalled body holds a run of srv under timeouts for stallBound, idle after idleWindow.
func heldByAStall(t *testing.T, srv *http.Server, timeouts Timeouts) bool {
	t.Helper()
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	gone := make(chan struct{})
	srv.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			close(gone)
		}
	}
	pipes := newPipeListener()
	run, listener, stop := windowed.idling(parent, srv, pipes, slog.New(slog.DiscardHandler))
	defer stop()
	served := make(chan error, 1)
	go func() { served <- serveOn(run, srv, listener, timeouts, nil, nil) }()
	stallABody(pipes)

	time.Sleep(stallBound)
	synctest.Wait()
	held := len(served) == 0
	cancel()
	<-served
	<-gone
	return held
}

func TestEveryIdleExitServeListeningTakesEndsAStalledBody(t *testing.T) {
	t.Parallel()

	bounded := Timeouts{ReadHeader: 10 * time.Second, Read: 30 * time.Second, Idle: 120 * time.Second,
		Grace: time.Second, CancelGrace: time.Second, StopGrace: time.Second}
	noRead := bounded
	noRead.Read = 0
	noHeader := bounded
	noHeader.ReadHeader = 0
	noIdle := bounded
	noIdle.Idle = 0
	cases := []struct {
		name     string
		timeouts Timeouts
	}{
		{"the default timeouts", bounded},
		{"no read timeout", noRead},
		{"no header timeout", noHeader},
		{"no idle timeout", noIdle},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				srv := NewServer("", readsTheBody(), tc.timeouts)
				if refuseIdle(windowed, srv) != nil {
					return
				}
				if heldByAStall(t, srv, tc.timeouts) {
					t.Errorf("ServeListening takes this idle exit, yet a stalled body held the run open for %v", stallBound)
				}
			})
		})
	}
}

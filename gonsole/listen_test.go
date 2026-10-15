// SPDX-License-Identifier: Apache-2.0

package gonsole_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gopherium/framework/gonsole"
)

// local is the listener a program falls back to when its listener settings are empty.
var local = gonsole.Listening{Address: "localhost:8080"}

func TestEnvReadsWhereToListen(t *testing.T) {
	t.Parallel()

	idling := gonsole.Listening{Address: "localhost:8080", IdleExit: 10 * time.Minute}
	withHeader := gonsole.Listening{Address: "localhost:8080", ClientAddrHeader: "X-Client-Addr"}
	cases := []struct {
		name     string
		values   map[string]string
		fallback gonsole.Listening
		want     gonsole.Listening
		err      string
	}{
		{"no settings", nil, local, local, ""},
		{"an address", map[string]string{"MYAPP_ADDR": "127.0.0.1:9000"}, local,
			gonsole.Listening{Network: "tcp", Address: "127.0.0.1:9000"}, ""},
		{"a port alone", map[string]string{"PORT": "8080"}, local,
			gonsole.Listening{Network: "tcp", Address: ":8080"}, ""},
		{"an address beats a port", map[string]string{"MYAPP_ADDR": "127.0.0.1:9000", "PORT": "8080"}, local,
			gonsole.Listening{Network: "tcp", Address: "127.0.0.1:9000"}, ""},
		{"an address beside a port that is no number", map[string]string{"MYAPP_ADDR": "127.0.0.1:9000", "PORT": "abc"},
			local, gonsole.Listening{Network: "tcp", Address: "127.0.0.1:9000"}, ""},
		{"a zero port", map[string]string{"PORT": "0"}, local, gonsole.Listening{},
			`PORT: must stand above zero, got "0"`},
		{"a negative port", map[string]string{"PORT": "-1"}, local, gonsole.Listening{},
			`PORT: must stand above zero, got "-1"`},
		{"a port above the highest", map[string]string{"PORT": "65536"}, local, gonsole.Listening{},
			`PORT: must stand at or below 65535, got "65536"`},
		{"a port that is a word", map[string]string{"PORT": "abc"}, local, gonsole.Listening{},
			`PORT: must be a whole number, got "abc"`},
		{"a port that is a pipe path", map[string]string{"PORT": `\\.\pipe\myapp`}, local, gonsole.Listening{},
			`PORT: must be a whole number, got "\\\\.\\pipe\\myapp"`},
		{"a header", map[string]string{"MYAPP_CLIENT_ADDR_HEADER": "X-Client-Addr"}, local, withHeader, ""},
		{"a header from the fallback", nil, withHeader, withHeader, ""},
		{"a header setting over the fallback's", map[string]string{"MYAPP_CLIENT_ADDR_HEADER": "X-Real-IP"}, withHeader,
			gonsole.Listening{Address: "localhost:8080", ClientAddrHeader: "X-Real-IP"}, ""},
		{"an idle exit from the fallback", nil, idling, idling, ""},
		{"an idle exit", map[string]string{"MYAPP_IDLE_EXIT": "10m"}, local,
			gonsole.Listening{Address: "localhost:8080", IdleExit: 10 * time.Minute}, ""},
		{"an idle exit of zero over a fallback above it", map[string]string{"MYAPP_IDLE_EXIT": "0s"}, idling,
			local, ""},
		{"an idle exit in bare seconds", map[string]string{"MYAPP_IDLE_EXIT": "600"}, local, gonsole.Listening{},
			`MYAPP_IDLE_EXIT: must be a duration like 30s, got "600"`},
		{"a negative idle exit", map[string]string{"MYAPP_IDLE_EXIT": "-1s"}, local, gonsole.Listening{},
			`MYAPP_IDLE_EXIT: must not be negative, got "-1s"`},
		{"two failures", map[string]string{"PORT": "abc", "MYAPP_IDLE_EXIT": "600"}, local, gonsole.Listening{},
			"PORT: must be a whole number, got \"abc\"\nMYAPP_IDLE_EXIT: must be a duration like 30s, got \"600\""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := settings(tc.values).Listening(tc.fallback)

			if got != tc.want || errorText(err) != tc.err {
				t.Errorf("Listening() = %+v, %q, want %+v, %q", got, errorText(err), tc.want, tc.err)
			}
		})
	}
}

// loopback is a listener on a free loopback port.
var loopback = gonsole.Listening{Address: "127.0.0.1:0"}

// listeningRun is one ServeListening run a test started, with the address it listens on, its log and its stop calls.
type listeningRun struct {
	address string
	journal *journal
	stopped *stopper
	end     func() error
}

// serveListening starts ServeListening over handler on l under timeouts and returns the run once it listens.
func serveListening(t *testing.T, l gonsole.Listening, handler http.Handler, timeouts gonsole.Timeouts) listeningRun {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	j := newJournal()
	s := &stopper{}
	srv := gonsole.NewServer("", handler, timeouts)
	done := make(chan error, 1)
	go func() { done <- gonsole.ServeListening(ctx, srv, timeouts, l, s.stop, j.logger()) }()
	select {
	case address := <-j.listening:
		return listeningRun{address: address, journal: j, stopped: s, end: func() error {
			cancel()
			return ended(t, done, timeouts)
		}}
	case err := <-done:
		cancel()
		t.Fatalf("ServeListening() returned %v before listening", err)
	}
	return listeningRun{}
}

// start is one ServeListening call a test makes, with the server, the timeouts beside it and the listener.
type start struct {
	srv      *http.Server
	timeouts gonsole.Timeouts
	at       gonsole.Listening
}

// startOf returns the start of a server over the report names built from timeouts, on l.
func startOf(l gonsole.Listening, timeouts gonsole.Timeouts) start {
	return start{srv: gonsole.NewServer("", reportNames(), timeouts), timeouts: timeouts, at: l}
}

// startRefused fails t unless ServeListening on l under timeouts answers want, with nothing bound or stopped.
func startRefused(t *testing.T, l gonsole.Listening, timeouts gonsole.Timeouts, want string) {
	t.Helper()
	refusedOn(t, startOf(l, timeouts), want)
}

// refusedOn fails t unless the start s answers want, with nothing bound or stopped.
func refusedOn(t *testing.T, s start, want string) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var stopped stopper
	j := newJournal()

	err := gonsole.ServeListening(ctx, s.srv, s.timeouts, s.at, stopped.stop, j.logger())

	if errorText(err) != want {
		t.Errorf("ServeListening() = %q, want %q", errorText(err), want)
	}
	if j.said("msg=listening") || stopped.calls != 0 {
		t.Errorf("listened %t, stop calls %d, want nothing bound or stopped", j.said("msg=listening"), stopped.calls)
	}
}

func TestServeListeningAnswersWhereItIsTold(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		at   gonsole.Listening
	}{
		{"an address with no network", loopback},
		{"an address on tcp", gonsole.Listening{Network: "tcp", Address: "127.0.0.1:0"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			run := serveListening(t, tc.at, reportNames(), defaults)

			body := get(t, run.address)
			err := run.end()

			if host, _, _ := net.SplitHostPort(run.address); host != "127.0.0.1" {
				t.Errorf("listening on %q, want the host 127.0.0.1 that the listener names", run.address)
			}
			if body != "quarterly\nyearly\n" || err != nil {
				t.Errorf("body = %q, ServeListening() = %v, want the report names and nil", body, err)
			}
			if !run.stopped.ranOnceWithin(defaults.StopGrace) {
				t.Errorf("stop calls = %d, live %t, deadline in %v, want one live call within the stop grace",
					run.stopped.calls, run.stopped.live, run.stopped.deadline)
			}
		})
	}
}

// idleUnbounded is the refusal of an idle exit while a connection to the server can wait forever.
const idleUnbounded = "gonsole: Listening.IdleExit needs the server's ReadTimeout above zero, " +
	"and its IdleTimeout and ReadHeaderTimeout not negative"

func TestServeListeningRefusesBeforeItListens(t *testing.T) {
	t.Parallel()

	noGrace := defaults
	noGrace.Grace = 0
	noRead := defaults
	noRead.Read = 0
	negativeRead := defaults
	negativeRead.Read = -time.Second
	negativeIdle := defaults
	negativeIdle.Idle = -time.Second
	negativeHeader := defaults
	negativeHeader.ReadHeader = -time.Second
	idleExit := gonsole.Listening{Address: "127.0.0.1:0", IdleExit: time.Minute}
	udp := gonsole.Listening{Network: "udp", Address: "127.0.0.1:0"}
	cases := []struct {
		name     string
		timeouts gonsole.Timeouts
		at       gonsole.Listening
		err      string
	}{
		{"a zero grace", noGrace, loopback, "gonsole: Timeouts.Grace must stand above zero, got 0s"},
		{"a udp network", defaults, udp, `gonsole: Listening.Network must be unix, tcp or empty, got "udp"`},
		{"an empty address", defaults, gonsole.Listening{}, "gonsole: Listening.Address must not be empty"},
		{"a negative idle exit", defaults, gonsole.Listening{Address: "127.0.0.1:0", IdleExit: -time.Second},
			"gonsole: Listening.IdleExit must not be negative, got -1s"},
		{"an idle exit with no read timeout", noRead, idleExit, idleUnbounded},
		{"an idle exit beside a negative read timeout", negativeRead, idleExit, idleUnbounded},
		{"an idle exit beside a negative idle timeout", negativeIdle, idleExit, idleUnbounded},
		{"an idle exit beside a negative header timeout", negativeHeader, idleExit, idleUnbounded},
		{"a zero grace beside a udp network", noGrace, udp, "gonsole: Timeouts.Grace must stand above zero, got 0s\n" +
			`gonsole: Listening.Network must be unix, tcp or empty, got "udp"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			startRefused(t, tc.at, tc.timeouts, tc.err)
		})
	}
}

func TestServeListeningStopsWhenTheListenFails(t *testing.T) {
	t.Parallel()

	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("taking a port: %v", err)
	}
	defer func() { _ = taken.Close() }()
	s := stopper{fails: errors.New("the reports plugin did not stop")}
	at := gonsole.Listening{Address: taken.Addr().String()}

	err = gonsole.ServeListening(t.Context(), gonsole.NewServer("", reportNames(), defaults), defaults, at, s.stop, nil)

	if !strings.HasPrefix(errorText(err), "http server: listen tcp "+taken.Addr().String()) {
		t.Errorf("ServeListening() = %v, want the listen failure", err)
	}
	if !strings.HasSuffix(errorText(err), "\nthe reports plugin did not stop") {
		t.Errorf("ServeListening() = %v, want the stop error joined", err)
	}
	if !s.ranOnceWithin(defaults.StopGrace) {
		t.Errorf("stop calls = %d, live %t, deadline in %v, want one live call within the stop grace",
			s.calls, s.live, s.deadline)
	}
}

// finishesInsideTheGrace fails t unless a request ServeListening on l serves when the run ends gets its answer.
func finishesInsideTheGrace(t *testing.T, l gonsole.Listening, fetch func(string) (int, error)) {
	t.Helper()
	arrived := make(chan struct{})
	proceed := make(chan struct{})
	pending := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(arrived)
		<-proceed
		_, _ = io.WriteString(w, "quarterly\n")
	})
	run := serveListening(t, l, pending, defaults)
	answered := make(chan int, 1)
	go func() {
		code, _ := fetch(run.address)
		answered <- code
	}()
	<-arrived
	ended := make(chan error, 1)
	go func() { ended <- run.end() }()
	<-run.journal.down

	close(proceed)

	if code := <-answered; code != http.StatusOK {
		t.Errorf("answer %d, want the request that arrived before the shutdown answered 200", code)
	}
	if err := <-ended; err != nil {
		t.Errorf("ServeListening() = %v, want nil", err)
	}
}

// cancelledPastTheGrace fails t unless a request ServeListening on l serves past the grace ends with ErrGraceRanOut.
func cancelledPastTheGrace(t *testing.T, l gonsole.Listening, fetch func(string) (int, error)) {
	t.Helper()
	arrived := make(chan struct{})
	causes := make(chan error, 1)
	waiting := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(arrived)
		<-r.Context().Done()
		causes <- context.Cause(r.Context())
	})
	timeouts := defaults
	timeouts.Grace = 50 * time.Millisecond
	run := serveListening(t, l, waiting, timeouts)
	go func() { _, _ = fetch(run.address) }()
	<-arrived

	err := run.end()

	if err != nil {
		t.Errorf("ServeListening() = %v, want nil once the cancelled request ended", err)
	}
	select {
	case cause := <-causes:
		if !errors.Is(cause, gonsole.ErrGraceRanOut) {
			t.Errorf("cause = %v, want the grace to have run out", cause)
		}
	default:
		t.Errorf("the request was still running when ServeListening returned")
	}
}

func TestServeListeningDrainsAsServeDoes(t *testing.T) {
	t.Parallel()

	t.Run("a request inside the grace finishes", func(t *testing.T) {
		t.Parallel()

		finishesInsideTheGrace(t, loopback, status)
	})
	t.Run("a request past the grace ends with the grace run out", func(t *testing.T) {
		t.Parallel()

		cancelledPastTheGrace(t, loopback, status)
	})
}

// unlogged starts ServeListening over the report names on l with no logger or stop, returning its address and end.
func unlogged(t *testing.T, l gonsole.Listening) (string, func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	srv := gonsole.NewServer("", reportNames(), defaults)
	addresses := make(chan string, 1)
	srv.BaseContext = func(listener net.Listener) context.Context {
		addresses <- listener.Addr().String()
		return ctx
	}
	done := make(chan error, 1)
	go func() { done <- gonsole.ServeListening(ctx, srv, defaults, l, nil, nil) }()
	select {
	case address := <-addresses:
		return address, func() error {
			cancel()
			return ended(t, done, defaults)
		}
	case err := <-done:
		cancel()
		t.Fatalf("ServeListening() returned %v before listening", err)
	}
	return "", nil
}

func TestServeListeningRunsWithoutALogger(t *testing.T) {
	t.Parallel()

	t.Run("serving the report names", func(t *testing.T) {
		t.Parallel()

		address, end := unlogged(t, loopback)

		body := get(t, address)
		err := end()

		if body != "quarterly\nyearly\n" || err != nil {
			t.Errorf("body = %q, ServeListening() = %v, want the report names and nil", body, err)
		}
	})
	t.Run("ending an idle run", func(t *testing.T) {
		t.Parallel()

		at := gonsole.Listening{Address: "127.0.0.1:0", IdleExit: 50 * time.Millisecond}
		srv := gonsole.NewServer("", reportNames(), defaults)
		done := make(chan error, 1)
		go func() { done <- gonsole.ServeListening(t.Context(), srv, defaults, at, nil, nil) }()

		if err := ended(t, done, defaults); err != nil {
			t.Errorf("ServeListening() = %v, want nil once the idle exit passed", err)
		}
	})
}

// view is what a handler saw of one request.
type view struct {
	remote    string
	proto     string
	forwarded string
	host      string
	visitor   bool
	tls       bool
	socket    bool
}

// viewer answers the report names and hands what it saw of each request to views.
func viewer(views chan<- view) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, visitor := r.Header["X-Client-Addr"]
		views <- view{
			remote: r.RemoteAddr, proto: r.Header.Get("X-Forwarded-Proto"), forwarded: r.Header.Get("X-Forwarded-Host"),
			host: r.Host, visitor: visitor, tls: r.TLS != nil, socket: gonsole.OverSocket(r),
		}
		_, _ = io.WriteString(w, "quarterly\nyearly\n")
	})
}

// onlyView returns what the handler saw of the one request it served, stopping t when it served none.
func onlyView(t *testing.T, views <-chan view) view {
	t.Helper()
	select {
	case seen := <-views:
		return seen
	default:
		t.Fatalf("the handler served no request")
		return view{}
	}
}

// visit returns a GET of the root at address for the reports host, with forwarded headers and values in X-Client-Addr.
func visit(t *testing.T, address string, values []string) *http.Request {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+address+"/", nil)
	if err != nil {
		t.Fatalf("building the request: %v", err)
	}
	request.Host = "reports.example"
	request.Header.Set("X-Forwarded-Proto", "https")
	request.Header.Set("X-Forwarded-Host", "reports.example")
	request.Header["X-Client-Addr"] = values
	return request
}

// send sends request through client and returns the status code of its answer.
func send(t *testing.T, client *http.Client, request *http.Request) int {
	t.Helper()
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("GET %s: %v", request.URL, err)
	}
	_ = response.Body.Close()
	return response.StatusCode
}

// believing is a listener on a free loopback port that names X-Client-Addr as the visitor's header.
var believing = gonsole.Listening{Address: "127.0.0.1:0", ClientAddrHeader: "X-Client-Addr"}

func TestServeListeningDropsTheVisitorHeaderOverTCP(t *testing.T) {
	t.Parallel()

	views := make(chan view, 1)
	run := serveListening(t, believing, viewer(views), defaults)

	code := send(t, patient, visit(t, run.address, []string{"203.0.113.7"}))
	err := run.end()

	if code != http.StatusOK || err != nil {
		t.Errorf("answer %d, ServeListening() = %v, want 200 and nil", code, err)
	}
	seen := onlyView(t, views)
	if host, _, _ := net.SplitHostPort(seen.remote); host != "127.0.0.1" {
		t.Errorf("RemoteAddr = %q, want the loopback peer the header never replaces over TCP", seen.remote)
	}
	if seen.visitor {
		t.Errorf("the handler saw X-Client-Addr over TCP, want it dropped")
	}
	if seen.socket {
		t.Errorf("OverSocket() = true over TCP, want false")
	}
}

func TestOverSocketIsFalseWithoutAConnection(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest(http.MethodGet, "http://reports.example/", nil)

	if gonsole.OverSocket(request) {
		t.Errorf("OverSocket() = true for a request with no connection, want false")
	}
}

func TestServeListeningAnswersThroughTheDefaultMux(t *testing.T) {
	t.Parallel()

	run := serveListening(t, believing, nil, defaults)

	code, _ := status(run.address)
	err := run.end()

	if code != http.StatusNotFound || err != nil {
		t.Errorf("GET answered %d, ServeListening() = %v, want the default mux's 404 and nil", code, err)
	}
}

// lineOf returns the place of the first kept line holding text, -1 when none does.
func (j *journal) lineOf(text string) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return slices.IndexFunc(j.lines, func(line string) bool { return strings.Contains(line, text) })
}

// endsIdle fails t unless ServeListening on l under timeouts ends unprompted with nil, logging the idle exit first.
func endsIdle(t *testing.T, l gonsole.Listening, timeouts gonsole.Timeouts) {
	t.Helper()
	endsIdleOn(t, startOf(l, timeouts))
}

// endsIdleOn fails t unless the start s ends unprompted with nil, logging the idle exit first.
func endsIdleOn(t *testing.T, s start) {
	t.Helper()
	j := newJournal()
	var stopped stopper
	done := make(chan error, 1)
	go func() { done <- gonsole.ServeListening(t.Context(), s.srv, s.timeouts, s.at, stopped.stop, j.logger()) }()

	err := ended(t, done, s.timeouts)

	if err != nil {
		t.Errorf("ServeListening() = %v, want nil once the idle exit passed", err)
	}
	if !stopped.ranOnceWithin(s.timeouts.StopGrace) {
		t.Errorf("stop calls = %d, live %t, deadline in %v, want one live call within the stop grace",
			stopped.calls, stopped.live, stopped.deadline)
	}
	idle := j.lineOf(`level=INFO msg="no connection for the idle exit" idle_exit=` + s.at.IdleExit.String())
	if down := j.lineOf(`msg="shutting down"`); idle < 0 || down < idle {
		t.Errorf("log = %q, want the idle line before the shutting down line", j.lines)
	}
}

func TestServeListeningEndsAnIdleRunWithoutAnError(t *testing.T) {
	t.Parallel()

	noIdle := defaults
	noIdle.Idle = 0
	noHeader := defaults
	noHeader.ReadHeader = 0
	cases := []struct {
		name     string
		timeouts gonsole.Timeouts
	}{
		{"the default timeouts", defaults},
		{"no idle timeout", noIdle},
		{"no read header timeout", noHeader},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			endsIdle(t, gonsole.Listening{Address: "127.0.0.1:0", IdleExit: 50 * time.Millisecond}, tc.timeouts)
		})
	}
}

// boundedBy returns a server over the report names built from the default timeouts, then changed by change.
func boundedBy(change func(*http.Server)) *http.Server {
	srv := gonsole.NewServer("", reportNames(), defaults)
	change(srv)
	return srv
}

func TestServeListeningJudgesTheIdleExitOnTheServersTimeouts(t *testing.T) {
	t.Parallel()

	idleExit := gonsole.Listening{Address: "127.0.0.1:0", IdleExit: 50 * time.Millisecond}
	cases := []struct {
		name string
		srv  *http.Server
	}{
		{"a server built with no timeouts", &http.Server{Handler: reportNames()}},
		{"a server whose read timeout was cleared", boundedBy(func(srv *http.Server) { srv.ReadTimeout = 0 })},
		{"a server whose idle timeout is negative", boundedBy(func(srv *http.Server) { srv.IdleTimeout = -time.Second })},
		{"a server whose header timeout is negative",
			boundedBy(func(srv *http.Server) { srv.ReadHeaderTimeout = -time.Second })},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			refusedOn(t, start{srv: tc.srv, timeouts: defaults, at: idleExit}, idleUnbounded)
		})
	}
	t.Run("a bounded server beside timeouts that bound nothing", func(t *testing.T) {
		t.Parallel()

		unbounded := defaults
		unbounded.ReadHeader, unbounded.Read, unbounded.Idle = 0, 0, 0

		endsIdleOn(t, start{srv: gonsole.NewServer("", reportNames(), defaults), timeouts: unbounded, at: idleExit})
	})
}

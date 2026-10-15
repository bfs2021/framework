// SPDX-License-Identifier: Apache-2.0

//go:build linux

package gonsole_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gopherium/framework/gonsole"
)

// socketPath is a socket path the reader tests name, where nothing ever listens.
const socketPath = "/run/myapp/myapp.sock"

// pathOfLength returns a socket path of exactly n bytes.
func pathOfLength(n int) string {
	return "/tmp/" + strings.Repeat("s", n-len("/tmp/"))
}

func TestEnvReadsASocketPath(t *testing.T) {
	t.Parallel()

	header := "X-Client-Addr"
	socket := gonsole.Listening{Network: "unix", Address: socketPath}
	headed := gonsole.Listening{Network: "unix", Address: socketPath, ClientAddrHeader: header}
	cases := []struct {
		name     string
		values   map[string]string
		fallback gonsole.Listening
		want     gonsole.Listening
		err      string
	}{
		{"a socket beats an address and a port", map[string]string{
			"MYAPP_SOCKET": socketPath, "MYAPP_ADDR": "127.0.0.1:9000", "PORT": "8080", "MYAPP_CLIENT_ADDR_HEADER": header,
		}, local, gonsole.Listening{Network: "unix", Address: socketPath, ClientAddrHeader: header}, ""},
		{"a socket from the fallback", map[string]string{"MYAPP_CLIENT_ADDR_HEADER": header}, socket,
			gonsole.Listening{Network: "unix", Address: socketPath, ClientAddrHeader: header}, ""},
		{"a socket and its header from the fallback", nil, headed, headed, ""},
		{"a fallback socket that an address beats needs no header", map[string]string{"MYAPP_ADDR": "127.0.0.1:9000"},
			socket, gonsole.Listening{Network: "tcp", Address: "127.0.0.1:9000"}, ""},
		{"a fallback socket that only PORT beats", map[string]string{"PORT": "8080", "MYAPP_CLIENT_ADDR_HEADER": header},
			socket, gonsole.Listening{Network: "tcp", Address: ":8080", ClientAddrHeader: header}, ""},
		{"a socket without the header setting", map[string]string{"MYAPP_SOCKET": socketPath}, local,
			gonsole.Listening{}, "MYAPP_CLIENT_ADDR_HEADER is required to serve on a socket"},
		{"a path of 107 bytes", map[string]string{"MYAPP_SOCKET": pathOfLength(107), "MYAPP_CLIENT_ADDR_HEADER": header},
			local, gonsole.Listening{Network: "unix", Address: pathOfLength(107), ClientAddrHeader: header}, ""},
		{"a path of 108 bytes", map[string]string{"MYAPP_SOCKET": pathOfLength(108), "MYAPP_CLIENT_ADDR_HEADER": header},
			local, gonsole.Listening{}, "MYAPP_SOCKET: must be at most 107 bytes, got 108"},
		{"an abstract name", map[string]string{"MYAPP_SOCKET": "@myapp", "MYAPP_CLIENT_ADDR_HEADER": header}, local,
			gonsole.Listening{}, `MYAPP_SOCKET: must be a file path, not an abstract name, got "@myapp"`},
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

func TestServeListeningRefusesASocketBeforeItListens(t *testing.T) {
	t.Parallel()

	path := string(socketAt(t))
	cases := []struct {
		name string
		at   gonsole.Listening
		err  string
	}{
		{"a socket with no header", gonsole.Listening{Network: "unix", Address: path},
			"gonsole: Listening.ClientAddrHeader must name a header for a socket"},
		{"a socket path of 108 bytes",
			gonsole.Listening{Network: "unix", Address: pathOfLength(108), ClientAddrHeader: "X-Client-Addr"},
			"gonsole: Listening.Address: must be at most 107 bytes, got 108"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			startRefused(t, tc.at, defaults, tc.err)
		})
	}
}

// place is a path on disk that a socket test serves on or puts something at.
type place string

// shortFolder returns a new folder of a short name under the temporary folder, removed once t ends.
func shortFolder(t *testing.T) string {
	t.Helper()
	folder, err := os.MkdirTemp("", "gs")
	if err != nil {
		t.Fatalf("making a folder: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(folder); err != nil {
			t.Errorf("removing %s: %v", folder, err)
		}
	})
	return folder
}

// socketAt returns the place of a socket in a new short folder of t.
func socketAt(t *testing.T) place {
	t.Helper()
	return place(filepath.Join(shortFolder(t), "myapp.sock"))
}

// listening is a listener on the socket at p that reads the visitor's address from X-Client-Addr.
func (p place) listening() gonsole.Listening {
	return gonsole.Listening{Network: "unix", Address: string(p), ClientAddrHeader: "X-Client-Addr"}
}

// client returns a client whose every request dials the socket at p.
func (p place) client() *http.Client {
	var dialer net.Dialer
	return &http.Client{Timeout: patient.Timeout, Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", string(p))
		},
	}}
}

// answer fetches the root over the socket at p as the program in front does, naming the visitor 203.0.113.7.
func (p place) answer() (*http.Response, error) {
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://reports.example/", nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("X-Client-Addr", "203.0.113.7")
	return p.client().Do(request)
}

// get fetches the root over the socket at p and returns its body.
func (p place) get(t *testing.T) string {
	t.Helper()
	response, err := p.answer()
	if err != nil {
		t.Fatalf("GET over %s: %v", p, err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("reading the answer: %v", err)
	}
	return string(body)
}

// status fetches the root over the socket at p and returns the answer's status code, or the error when none came.
func (p place) status() (int, error) {
	response, err := p.answer()
	if err != nil {
		return 0, err
	}
	_ = response.Body.Close()
	return response.StatusCode, nil
}

// socketStatus is status over the socket at the address a server logged.
func socketStatus(address string) (int, error) {
	return place(address).status()
}

// deadSocket leaves at p a socket file that nothing listens on.
func (p place) deadSocket(t *testing.T) {
	t.Helper()
	syscall.ForkLock.RLock()
	defer syscall.ForkLock.RUnlock()
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: string(p), Net: "unix"})
	if err != nil {
		t.Fatalf("listening on %s: %v", p, err)
	}
	listener.SetUnlinkOnClose(false)
	_ = listener.Close()
}

// mode returns the type and mode of what is at p, without following a link.
func (p place) mode(t *testing.T) fs.FileMode {
	t.Helper()
	info, err := os.Lstat(string(p))
	if err != nil {
		t.Errorf("reading %s: %v", p, err)
		return 0
	}
	return info.Mode()
}

// gone reports whether nothing is at p.
func (p place) gone() bool {
	_, err := os.Lstat(string(p))
	return errors.Is(err, fs.ErrNotExist)
}

// describe returns the type, mode, size and link target of what is at p, or why nothing could be read there.
func (p place) describe() string {
	info, err := os.Lstat(string(p))
	if err != nil {
		return err.Error()
	}
	target, _ := os.Readlink(string(p))
	return fmt.Sprintf("%v, %d bytes, linked to %q", info.Mode(), info.Size(), target)
}

// listenRefused fails t unless ServeListening on l fails its listen with want, serves nothing and calls stop once.
func listenRefused(t *testing.T, l gonsole.Listening, want string) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var s stopper
	j := newJournal()

	err := gonsole.ServeListening(ctx, gonsole.NewServer("", reportNames(), defaults), defaults, l, s.stop, j.logger())

	if errorText(err) != want {
		t.Errorf("ServeListening() = %q, want %q", errorText(err), want)
	}
	if j.said("msg=listening") {
		t.Errorf("log = %q, want no listening line", j.lines)
	}
	if !s.ranOnceWithin(defaults.StopGrace) {
		t.Errorf("stop calls = %d, live %t, deadline in %v, want one live call within the stop grace",
			s.calls, s.live, s.deadline)
	}
}

// servesPrivately fails t unless ServeListening answers on a socket at path of mode 0600 that is gone once it returns.
func servesPrivately(t *testing.T, path place) {
	t.Helper()
	run := serveListening(t, path.listening(), reportNames(), defaults)

	body := path.get(t)
	mode := path.mode(t)
	err := run.end()

	if run.address != string(path) {
		t.Errorf("listening on %q, want the socket's path %q", run.address, path)
	}
	if body != "quarterly\nyearly\n" {
		t.Errorf("body = %q, want the report names", body)
	}
	if err != nil {
		t.Errorf("ServeListening() = %v, want nil", err)
	}
	if mode != fs.ModeSocket|0o600 {
		t.Errorf("mode while serving = %v, want a socket of mode 0600", mode)
	}
	if !path.gone() {
		t.Errorf("%s is still there once ServeListening returned, want it removed", path)
	}
}

// refusedForItsMode fails t unless ServeListening at path refuses the socket's mode and leaves no file behind.
func refusedForItsMode(t *testing.T, path place) {
	t.Helper()
	listenRefused(t, path.listening(), "http server: the socket at "+string(path)+" is not mode 0600, check the umask")
	if !path.gone() {
		t.Errorf("%s is still there, want the refused socket removed", path)
	}
}

func TestServeListeningServesOnAPrivateSocket(t *testing.T) {
	t.Parallel()

	servesPrivately(t, socketAt(t))
}

func TestServeListeningMakesTheSocketPrivateUnderAnyUmask(t *testing.T) {
	cases := []struct {
		name  string
		umask int
		check func(*testing.T, place)
	}{
		{"umask 000", 0o000, servesPrivately},
		{"umask 022", 0o022, servesPrivately},
		{"umask 0277", 0o277, refusedForItsMode},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := socketAt(t)
			restore := syscall.Umask(tc.umask)
			t.Cleanup(func() { syscall.Umask(restore) })

			tc.check(t, path)
		})
	}
}

func TestServeListeningReplacesAStaleSocket(t *testing.T) {
	t.Parallel()

	path := socketAt(t)
	path.deadSocket(t)
	run := serveListening(t, path.listening(), reportNames(), defaults)

	body := path.get(t)
	err := run.end()

	if body != "quarterly\nyearly\n" {
		t.Errorf("body = %q, want the report names from the server that replaced the stale socket", body)
	}
	if err != nil {
		t.Errorf("ServeListening() = %v, want nil", err)
	}
}

func TestServeListeningRefusesASocketAServerHolds(t *testing.T) {
	t.Parallel()

	path := socketAt(t)
	first := serveListening(t, path.listening(), reportNames(), defaults)

	listenRefused(t, path.listening(), "http server: a server already listens on "+string(path))

	body := path.get(t)
	if err := first.end(); err != nil {
		t.Errorf("the first ServeListening() = %v, want nil", err)
	}
	if body != "quarterly\nyearly\n" {
		t.Errorf("body = %q, want the first server still answering the report names", body)
	}
}

// writeFile puts a regular file at path.
func writeFile(t *testing.T, path place) {
	t.Helper()
	if err := os.WriteFile(string(path), []byte("quarterly\n"), 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
}

// makeFolder puts an empty folder at path.
func makeFolder(t *testing.T, path place) {
	t.Helper()
	if err := os.Mkdir(string(path), 0o700); err != nil {
		t.Fatalf("making %s: %v", path, err)
	}
}

// linkToADeadSocket puts at path a symbolic link to a dead socket in the same folder.
func linkToADeadSocket(t *testing.T, path place) {
	t.Helper()
	target := place(filepath.Join(filepath.Dir(string(path)), "dead.sock"))
	target.deadSocket(t)
	if err := os.Symlink(string(target), string(path)); err != nil {
		t.Fatalf("linking %s: %v", path, err)
	}
}

// lockedSocket puts at path a dead socket of mode 0o000.
func lockedSocket(t *testing.T, path place) {
	t.Helper()
	path.deadSocket(t)
	if err := os.Chmod(string(path), 0o000); err != nil {
		t.Fatalf("locking %s: %v", path, err)
	}
}

// fileAbove puts a regular file where the folder of path should be.
func fileAbove(t *testing.T, path place) {
	t.Helper()
	writeFile(t, place(filepath.Dir(string(path))))
}

func TestServeListeningLeavesWhatIsNotASocket(t *testing.T) {
	t.Parallel()

	notASocket := "%[1]s is not a socket, so it stays in place"
	cases := []struct {
		name string
		at   string
		put  func(*testing.T, place)
		err  string
	}{
		{"a regular file", "myapp.sock", writeFile, notASocket},
		{"a folder", "myapp.sock", makeFolder, notASocket},
		{"a symbolic link to a dead socket", "myapp.sock", linkToADeadSocket, notASocket},
		{"a socket this account cannot connect to", "myapp.sock", lockedSocket,
			"check the socket at %[1]s: dial unix %[1]s: connect: permission denied"},
		{"a path under a regular file", "notes/myapp.sock", fileAbove,
			"lock the socket at %[1]s: open %[1]s.lock: not a directory"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			path := place(filepath.Join(shortFolder(t), tc.at))
			tc.put(t, path)
			before := path.describe()

			listenRefused(t, path.listening(), "http server: "+fmt.Sprintf(tc.err, path))

			if after := path.describe(); after != before {
				t.Errorf("%s was %s and is now %s, want it left as it was", path, before, after)
			}
		})
	}
}

func TestServeListeningStopsWhenTheSocketsFolderIsMissing(t *testing.T) {
	t.Parallel()

	path := place(filepath.Join(shortFolder(t), "missing", "myapp.sock"))

	listenRefused(t, path.listening(),
		"http server: lock the socket at "+string(path)+": open "+string(path)+".lock: no such file or directory")
}

func TestServeListeningDrainsOverASocket(t *testing.T) {
	t.Parallel()

	t.Run("a request inside the grace finishes", func(t *testing.T) {
		t.Parallel()

		finishesInsideTheGrace(t, socketAt(t).listening(), socketStatus)
	})
	t.Run("a request past the grace ends with the grace run out", func(t *testing.T) {
		t.Parallel()

		cancelledPastTheGrace(t, socketAt(t).listening(), socketStatus)
	})
}

func TestServeListeningTakesTheVisitorFromTheHeader(t *testing.T) {
	t.Parallel()

	path := socketAt(t)
	views := make(chan view, 1)
	run := serveListening(t, path.listening(), viewer(views), defaults)

	code := send(t, path.client(), visit(t, "reports.example", []string{"203.0.113.7"}))
	err := run.end()

	if code != http.StatusOK || err != nil {
		t.Errorf("answer %d, ServeListening() = %v, want 200 and nil", code, err)
	}
	want := view{
		remote: "203.0.113.7:0", proto: "https", forwarded: "reports.example", host: "reports.example", socket: true,
	}
	if seen := onlyView(t, views); seen != want {
		t.Errorf("the handler saw %+v, want %+v", seen, want)
	}
}

func TestServeListeningRefusesASocketRequestWithoutOneVisitor(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		values []string
	}{
		{"no header", nil},
		{"an empty header", []string{""}},
		{"two header lines", []string{"203.0.113.7", "198.51.100.2"}},
		{"a comma list", []string{"fe80::1%eth0, 203.0.113.7"}},
		{"a value that is no address", []string{"unknown"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			path := socketAt(t)
			views := make(chan view, 1)
			run := serveListening(t, path.listening(), viewer(views), defaults)

			code := send(t, path.client(), visit(t, "reports.example", tc.values))
			err := run.end()

			if code != http.StatusBadRequest || err != nil {
				t.Errorf("answer %d, ServeListening() = %v, want 400 and nil", code, err)
			}
			if len(views) != 0 {
				t.Errorf("the handler served the request, want no handler run")
			}
			if !run.journal.said(`level=WARN msg="refused a socket request without one visitor address" ` +
				"header=X-Client-Addr") {
				t.Errorf("log = %q, want the warning naming the header", run.journal.lines)
			}
		})
	}
}

func TestServeListeningRefusesASocketRequestWithoutOneVisitorAndNoLogger(t *testing.T) {
	t.Parallel()

	path := socketAt(t)
	_, end := unlogged(t, path.listening())

	code := send(t, path.client(), visit(t, "reports.example", nil))
	err := end()

	if code != http.StatusBadRequest || err != nil {
		t.Errorf("answer %d, ServeListening() = %v, want 400 and nil", code, err)
	}
}

func TestServeListeningEndsAnIdleRunOnASocket(t *testing.T) {
	t.Parallel()

	path := socketAt(t)
	at := path.listening()
	at.IdleExit = 50 * time.Millisecond

	endsIdle(t, at, defaults)

	if !path.gone() {
		t.Errorf("%s is still there once the idle run ended, want it removed", path)
	}
}

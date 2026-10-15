// SPDX-License-Identifier: Apache-2.0

//go:build linux

package gonsole_test

import (
	"syscall"
	"testing"

	"github.com/gopherium/framework/gonsole"
)

func TestMainServesOnASocket(t *testing.T) {
	t.Parallel()

	path := socketAt(t)
	variables := []string{"MYAPP_SOCKET=" + string(path), "MYAPP_CLIENT_ADDR_HEADER=X-Client-Addr"}
	cmd := exampleCommand(t, variables, "serve")
	lines := launch(t, cmd)
	if address := listeningAddress(t, lines); address != string(path) {
		t.Fatalf("listening on %q, want the socket's path %q", address, path)
	}

	body := path.get(t)
	signalled(t, cmd, lines, syscall.SIGTERM)
	err := cmd.Wait()

	if err != nil {
		t.Errorf("exit = %v, want 0", err)
	}
	if body != "quarterly\nyearly\n" {
		t.Errorf("body = %q, want the report names over the socket", body)
	}
	if !path.gone() {
		t.Errorf("%s is still there once the program exited, want it removed", path)
	}
}

func TestMainRefusesASocketWithoutTheHeaderSetting(t *testing.T) {
	t.Parallel()

	got := runExample(t, "", []string{"MYAPP_SOCKET=" + string(socketAt(t))}, "serve")

	if got.code != gonsole.ExitFailed {
		t.Errorf("code = %d, want 1, stderr %q", got.code, got.stderr)
	}
	if want := "myapp: MYAPP_CLIENT_ADDR_HEADER is required to serve on a socket\n"; got.stderr != want {
		t.Errorf("stderr = %q, want %q", got.stderr, want)
	}
}

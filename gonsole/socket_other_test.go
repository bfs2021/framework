// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package gonsole_test

import (
	"testing"

	"github.com/gopherium/framework/gonsole"
)

func TestEnvRefusesASocketOffLinux(t *testing.T) {
	t.Parallel()

	env := settings(map[string]string{
		"MYAPP_SOCKET": "/run/myapp/myapp.sock", "MYAPP_CLIENT_ADDR_HEADER": "X-Client-Addr",
	})

	got, err := env.Listening(local)

	if got != (gonsole.Listening{}) || errorText(err) != "MYAPP_SOCKET: a socket listener needs Linux" {
		t.Errorf("Listening() = %+v, %q, want nothing and the refusal off Linux", got, errorText(err))
	}
}

func TestServeListeningRefusesASocketOffLinux(t *testing.T) {
	t.Parallel()

	at := gonsole.Listening{Network: "unix", Address: "/run/myapp/myapp.sock", ClientAddrHeader: "X-Client-Addr"}

	startRefused(t, at, defaults, "gonsole: Listening.Address: a socket listener needs Linux")
}

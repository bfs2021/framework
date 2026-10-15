// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package gonsole

import (
	"context"
	"errors"
	"net"
)

// errNeedsLinux is the refusal of a socket listener off Linux.
var errNeedsLinux = errors.New("a socket listener needs Linux")

// checkSocketPath refuses every socket path.
func checkSocketPath(string) error {
	return errNeedsLinux
}

// listenSocket refuses every socket listener.
func listenSocket(context.Context, string) (net.Listener, error) {
	return nil, errNeedsLinux
}

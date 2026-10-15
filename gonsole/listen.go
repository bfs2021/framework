// SPDX-License-Identifier: Apache-2.0

package gonsole

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strconv"
	"time"
)

// highestPort is the highest port TCP names.
const highestPort = 65535

// errIdleUnbounded is the refusal of an idle exit while a connection to the server can wait forever.
var errIdleUnbounded = errors.New(
	"gonsole: Listening.IdleExit needs the server's ReadTimeout above zero, " +
		"and its IdleTimeout and ReadHeaderTimeout not negative")

// Listening is where a server listens, whose visitor address it believes and when it stops for want of work.
type Listening struct {
	// Network is unix for a socket file or tcp for an address, empty meaning tcp.
	Network string
	// Address is the socket's path, the SOCKET setting, or the TCP host and port, from ADDR or PORT.
	Address string
	// ClientAddrHeader names the header holding the visitor's address on a socket, the CLIENT_ADDR_HEADER setting.
	ClientAddrHeader string
	// IdleExit is how long the server waits with nothing open before the run ends, zero never, the IDLE_EXIT setting.
	IdleExit time.Duration
}

// Listening returns the listener settings, SOCKET before ADDR before PORT before fallback.
func (e Env) Listening(fallback Listening) (Listening, error) {
	var read Listening
	var failed [3]error
	read.Network, read.Address, failed[0] = e.where(fallback)
	read.ClientAddrHeader = cmp.Or(e.Value("CLIENT_ADDR_HEADER"), fallback.ClientAddrHeader)
	failed[1] = e.needsHeader(read)
	read.IdleExit, failed[2] = e.Duration("IDLE_EXIT", fallback.IdleExit, AllowZero())
	if err := errors.Join(failed[:]...); err != nil {
		return Listening{}, err
	}
	return read, nil
}

// where returns the network and address the first set of SOCKET, ADDR and PORT names, else the fallback's.
func (e Env) where(fallback Listening) (string, string, error) {
	if path := e.Value("SOCKET"); path != "" {
		return e.socket(path)
	}
	if address := e.Value("ADDR"); address != "" {
		return "tcp", address, nil
	}
	port, err := Env{Getenv: e.Getenv}.Count("PORT", 0, AtMost(highestPort))
	if err != nil {
		return "", "", err
	}
	if port > 0 {
		return "tcp", ":" + strconv.Itoa(port), nil
	}
	return fallback.Network, fallback.Address, nil
}

// socket returns the socket at path, an error naming the SOCKET setting when this system cannot bind it.
func (e Env) socket(path string) (string, string, error) {
	if err := checkSocketPath(path); err != nil {
		return "", "", fmt.Errorf("%s: %w", e.Key("SOCKET"), err)
	}
	return "unix", path, nil
}

// needsHeader returns an error naming the CLIENT_ADDR_HEADER setting when l serves on a socket without a header.
func (e Env) needsHeader(l Listening) error {
	if l.headless() {
		return fmt.Errorf("%s is required to serve on a socket", e.Key("CLIENT_ADDR_HEADER"))
	}
	return nil
}

// ServeListening is Serve on the listener l names in place of srv.Addr, ending the run once l's idle exit passes.
func ServeListening(
	ctx context.Context, srv *http.Server, t Timeouts, l Listening, stop func(context.Context) error,
	logger *slog.Logger,
) error {
	if err := errors.Join(refuseGraces(t), refuseListening(l), refuseIdle(l, srv)); err != nil {
		return err
	}
	listener, err := listenOn(ctx, l)
	if err != nil {
		return errors.Join(fmt.Errorf("http server: %w", err), stopWithin(ctx, t, stop))
	}
	logger = cmp.Or(logger, slog.New(slog.DiscardHandler))
	srv.Handler = visitors(l.ClientAddrHeader, cmp.Or[http.Handler](srv.Handler, http.DefaultServeMux), logger)
	run, counted, stopIdling := l.idling(ctx, srv, listener, logger)
	defer stopIdling()
	return serveOn(run, srv, counted, t, stop, logger)
}

// refuseListening returns an error naming each field of l that ServeListening cannot serve on.
func refuseListening(l Listening) error {
	var refused []error
	if !slices.Contains([]string{"", "tcp", "unix"}, l.Network) {
		refused = append(refused, fmt.Errorf("gonsole: Listening.Network must be unix, tcp or empty, got %q", l.Network))
	}
	if l.Address == "" {
		refused = append(refused, errors.New("gonsole: Listening.Address must not be empty"))
	}
	if l.headless() {
		refused = append(refused, errors.New("gonsole: Listening.ClientAddrHeader must name a header for a socket"))
	}
	if err := l.socketPath(); err != nil {
		refused = append(refused, fmt.Errorf("gonsole: Listening.Address: %w", err))
	}
	return errors.Join(refused...)
}

// refuseIdle refuses an idle exit of l below zero, or above zero while a connection to srv can wait forever.
func refuseIdle(l Listening, srv *http.Server) error {
	switch {
	case l.IdleExit < 0:
		return fmt.Errorf("gonsole: Listening.IdleExit must not be negative, got %v", l.IdleExit)
	case l.IdleExit > 0 && waitsForever(srv):
		return errIdleUnbounded
	}
	return nil
}

// waitsForever reports whether no timeout of srv bounds a connection's wait for its next request, header or body.
func waitsForever(srv *http.Server) bool {
	return srv.ReadTimeout <= 0 || min(srv.IdleTimeout, srv.ReadHeaderTimeout) < 0
}

// listenOn returns a listener on the network and address l names.
func listenOn(ctx context.Context, l Listening) (net.Listener, error) {
	if l.Network == "unix" {
		return listenSocket(ctx, l.Address)
	}
	return net.Listen("tcp", l.Address)
}

// headless reports whether l serves on a socket with no header naming the visitor's address.
func (l Listening) headless() bool {
	return l.Network == "unix" && l.ClientAddrHeader == ""
}

// socketPath refuses the path of a socket listener l when this system cannot bind it, nothing for TCP.
func (l Listening) socketPath() error {
	if l.Network != "unix" {
		return nil
	}
	return checkSocketPath(l.Address)
}

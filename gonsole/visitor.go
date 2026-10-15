// SPDX-License-Identifier: Apache-2.0

package gonsole

import (
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"strings"
)

// OverSocket reports whether r came over a Unix socket connection.
func OverSocket(r *http.Request) bool {
	local, ok := r.Context().Value(http.LocalAddrContextKey).(net.Addr)
	return ok && local.Network() == "unix"
}

// visitors returns next with header dropped from every request and its address put in RemoteAddr on a socket.
func visitors(header string, next http.Handler, logger *slog.Logger) http.Handler {
	if header == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		values := r.Header.Values(header)
		visitor := new(http.Request)
		*visitor = *r
		visitor.Header = r.Header.Clone()
		visitor.Header.Del(header)
		if !OverSocket(r) {
			next.ServeHTTP(w, visitor)
			return
		}
		address, ok := visitorAddr(values)
		if !ok {
			logger.Warn("refused a socket request without one visitor address", "header", header)
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}
		visitor.RemoteAddr = address
		next.ServeHTTP(w, visitor)
	})
}

// visitorAddr returns the one address values hold as host and port, port 0 when it names none.
func visitorAddr(values []string) (string, bool) {
	if len(values) != 1 || strings.Contains(values[0], ",") {
		return "", false
	}
	visitor, err := parseVisitor(values[0])
	if err != nil {
		return "", false
	}
	return netip.AddrPortFrom(visitor.Addr().Unmap().WithZone(""), visitor.Port()).String(), true
}

// parseVisitor returns the address and port value names, port 0 for a bare address.
func parseVisitor(value string) (netip.AddrPort, error) {
	if visitor, err := netip.ParseAddrPort(value); err == nil {
		return visitor, nil
	}
	addr, err := netip.ParseAddr(value)
	return netip.AddrPortFrom(addr, 0), err
}

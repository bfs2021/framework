// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql/driver"
	"errors"
	"log/slog"
	"sync"
	"time"

	modernc "modernc.org/sqlite"

	"github.com/gopherium/framework/dbkit"
	"github.com/gopherium/framework/dbkit/sqlite/internal/seam"
)

const (
	// lowerBusyTimeout sets a connection's busy timeout to zero.
	lowerBusyTimeout = "PRAGMA busy_timeout=0"
	// optimizeStatement is the optimize every new connection runs.
	optimizeStatement = "PRAGMA optimize=0x10002"
	// optimizeSkipped is the note a busy optimize leaves.
	optimizeSkipped = "dbkit: PRAGMA optimize skipped on a new connection while the database is busy"
)

// connector opens the connections of one SQLite file through a private driver value.
type connector struct {
	// driver is the private driver value the handle reports.
	driver *modernc.Driver
	// open opens one connection by the driver's name of the file.
	open func(name string) (driver.Conn, error)
	// opening lets one call of open run at a time.
	opening sync.Mutex
	// name is the driver's name of the file with every rule.
	name string
	// path is the absolute path of the file.
	path string
	// busyTimeout is how long a busy open or a connection waiting for a lock keeps trying.
	busyTimeout time.Duration
	// busyPoll is how often a busy open or a connection waiting for a lock tries again.
	busyPoll time.Duration
	// wait makes the waits of a new connection end with their context.
	wait func(conn driver.Conn) (driver.Conn, error)
	// logger receives the skipped optimize notes.
	logger *slog.Logger
}

// newConnector returns the connector of the file at path, opened through drv with the rules of opts.
func newConnector(drv *modernc.Driver, path string, opts Options) *connector {
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	c := &connector{
		driver:      drv,
		open:        drv.Open,
		name:        connectionString(path, opts),
		path:        path,
		busyTimeout: opts.BusyTimeout,
		busyPoll:    opts.BusyPoll,
		logger:      logger,
	}
	c.wait = c.waitOn
	return c
}

// Connect opens one connection, waiting while busy, optimizes it, ends its waits with ctx and passes it to the seam.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := c.openWaiting(ctx)
	if err != nil {
		return nil, err
	}
	if err := c.optimize(ctx, conn.(driver.ExecerContext)); err != nil {
		return nil, errors.Join(err, conn.Close())
	}
	waiting, err := c.wait(conn)
	if err != nil {
		return nil, errors.Join(err, conn.Close())
	}
	return seam.Wrap(c.path, waiting), nil
}

// openWaiting opens one connection, trying again every busy poll while busy, until the busy timeout passes or ctx ends.
func (c *connector) openWaiting(ctx context.Context) (driver.Conn, error) {
	deadline := time.NewTimer(c.busyTimeout)
	defer deadline.Stop()
	poll := time.NewTicker(c.busyPoll)
	defer poll.Stop()
	for {
		conn, err := c.openOnce()
		if err == nil {
			return conn, nil
		}
		if !errors.Is(Classify(err), dbkit.ErrBusy) {
			return nil, err
		}
		poll.Reset(c.busyPoll)
		select {
		case <-ctx.Done():
			return nil, ended(ctx, err)
		case <-deadline.C:
			return nil, err
		case <-poll.C:
		}
	}
}

// openOnce opens one connection while it holds the opening mutex.
func (c *connector) openOnce() (driver.Conn, error) {
	c.opening.Lock()
	defer c.opening.Unlock()
	return c.open(c.name)
}

// Driver returns the private driver value of the handle.
func (c *connector) Driver() driver.Driver {
	return c.driver
}

// optimize lowers the busy timeout of conn, runs PRAGMA optimize and skips a busy answer with a note.
func (c *connector) optimize(ctx context.Context, conn driver.ExecerContext) error {
	_, err := conn.ExecContext(ctx, lowerBusyTimeout, nil)
	if err == nil {
		_, err = conn.ExecContext(ctx, optimizeStatement, nil)
	}
	if errors.Is(Classify(err), dbkit.ErrBusy) {
		c.logger.InfoContext(ctx, optimizeSkipped, slog.String("path", c.path), slog.Any("error", err))
		return nil
	}
	return err
}

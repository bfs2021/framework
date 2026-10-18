// SPDX-License-Identifier: Apache-2.0

package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/gopherium/framework/dbkit"
	"github.com/gopherium/framework/dbkit/sqlite"
)

const (
	// busyPoll is how often a call behind a held writer tries the lock again.
	busyPoll = 5 * time.Millisecond
	// busyDeadline is the deadline, and the cancel, of a call behind a held writer.
	busyDeadline = 100 * time.Millisecond
	// busyBound is the longest a call behind a held writer may run, a tenth of the busy timeout.
	busyBound = optionsBusyTimeout / 10
	// busyLateHold is how long a holder keeps the write lock after the deadline of the write it blocks.
	busyLateHold = 400 * time.Millisecond
	// busyHold is how long a holder keeps the write lock while a write with no deadline waits for it.
	busyHold = 50 * time.Millisecond
	// busySchema creates the table the busy tests write to.
	busySchema = "CREATE TABLE busy_rows (a INTEGER NOT NULL)"
	// busyInsert is the write a held writer makes wait.
	busyInsert = "INSERT INTO busy_rows VALUES (1)"
	// busyOtherInsert is a write that is not the one under test.
	busyOtherInsert = "INSERT INTO busy_rows VALUES (0)"
	// busyWritten counts the rows busyInsert wrote.
	busyWritten = "SELECT count(*) FROM busy_rows WHERE a = 1"
	// busyLateSchema is the schema change that leaves a connection's cached schema stale.
	busyLateSchema = "CREATE TABLE late_schema (a INTEGER)"
)

// busyShape is one call that waits for the write lock.
type busyShape struct {
	// name names the call.
	name string
	// run makes the call on db under ctx, with prepared holding busyInsert prepared before the writer took the lock.
	run func(ctx context.Context, db *sql.DB, prepared *sql.Stmt) error
	// busy reports that the call's error also matches dbkit.ErrBusy through Classify.
	busy bool
	// longPoll reports that the call also runs on a handle whose busy poll is the busy timeout.
	longPoll bool
}

// busyShapes are the calls a held writer makes wait.
var busyShapes = []busyShape{
	{"an autocommit insert", func(ctx context.Context, db *sql.DB, _ *sql.Stmt) error {
		_, err := db.ExecContext(ctx, busyInsert)
		return err
	}, false, true},
	{"an insert returning its row", func(ctx context.Context, db *sql.DB, _ *sql.Stmt) error {
		var a int64
		return db.QueryRowContext(ctx, busyInsert+" RETURNING a").Scan(&a)
	}, false, true},
	{"BeginTx", func(ctx context.Context, db *sql.DB, _ *sql.Stmt) error {
		tx, err := db.BeginTx(ctx, nil)
		if err == nil {
			_ = tx.Rollback()
		}
		return err
	}, true, true},
	{"InTx", func(ctx context.Context, db *sql.DB, _ *sql.Stmt) error {
		return dbkit.InTx(ctx, db, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, busyInsert)
			return err
		})
	}, true, false},
	{"a statement prepared before the writer", func(ctx context.Context, _ *sql.DB, prepared *sql.Stmt) error {
		_, err := prepared.ExecContext(ctx)
		return err
	}, false, false},
}

// busyCase is one shape run on a handle with one busy poll.
type busyCase struct {
	// name names the shape and its poll.
	name string
	// shape is the call.
	shape busyShape
	// poll is the busy poll of the handle.
	poll time.Duration
}

// busyCases returns every shape at busyPoll, and the long poll shapes again at a busy poll of the busy timeout.
func busyCases() []busyCase {
	var cases []busyCase
	for _, shape := range busyShapes {
		cases = append(cases, busyCase{name: shape.name, shape: shape, poll: busyPoll})
		if shape.longPoll {
			cases = append(cases, busyCase{name: shape.name + " at a busy poll of the busy timeout", shape: shape,
				poll: optionsBusyTimeout})
		}
	}
	return cases
}

// busyOptions returns testOptions with the busy poll given.
func busyOptions(poll time.Duration) sqlite.Options {
	opts := testOptions()
	opts.BusyPoll = poll
	return opts
}

// busyBehindAWriter returns a handle with poll, busyInsert prepared on it and a writer holding its lock until cleanup.
func busyBehindAWriter(t *testing.T, poll time.Duration) (*sql.DB, *sql.Stmt) {
	t.Helper()
	db := rulesOpen(t, busyOptions(poll))
	rulesExec(t, db, busySchema)
	busyWarmUp(t, db)
	prepared, err := db.PrepareContext(t.Context(), busyInsert)
	if err != nil {
		t.Fatalf("PrepareContext(%q) error = %v, want nil", busyInsert, err)
	}
	t.Cleanup(func() { _ = prepared.Close() })
	rulesHoldWriter(t, db, busyOtherInsert)
	return db, prepared
}

// busyWarmUp opens two connections of db at once and returns both to the pool, so a call behind the writer opens none.
func busyWarmUp(t *testing.T, db *sql.DB) {
	t.Helper()
	first, second := busyPin(t, db), busyPin(t, db)
	if err := errors.Join(first.Close(), second.Close()); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
}

// busyEnd is how a call behind a held writer ended.
type busyEnd struct {
	// err is the error of the call.
	err error
	// waited is how long the call ran.
	waited time.Duration
}

// busyRun makes the call of shape on db under ctx and returns how it ended.
func busyRun(ctx context.Context, shape busyShape, db *sql.DB, prepared *sql.Stmt) busyEnd {
	began := time.Now()
	err := shape.run(ctx, db, prepared)
	return busyEnd{err: err, waited: time.Since(began)}
}

// busyMustEnd fails the test unless the call of shape ended with want within busyBound, busy as shape says.
func busyMustEnd(t *testing.T, shape busyShape, end busyEnd, want error) {
	t.Helper()
	if !errors.Is(end.err, want) {
		t.Errorf("%s error = %v, want one matching %v", shape.name, end.err, want)
	}
	if busy := errors.Is(sqlite.Classify(end.err), dbkit.ErrBusy); busy != shape.busy {
		t.Errorf("%s error %v matches dbkit.ErrBusy = %v, want %v", shape.name, end.err, busy, shape.busy)
	}
	if end.waited > busyBound {
		t.Errorf("%s answered after %v, want within a tenth of the busy timeout %v", shape.name, end.waited,
			optionsBusyTimeout)
	}
}

// busyMustEndEvery runs every case behind a held writer under the context end makes and expects want.
func busyMustEndEvery(t *testing.T, end func(context.Context) (context.Context, context.CancelFunc), want error) {
	t.Helper()
	for _, c := range busyCases() {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			db, prepared := busyBehindAWriter(t, c.poll)
			ctx, cancel := end(t.Context())
			defer cancel()

			got := busyRun(ctx, c.shape, db, prepared)

			busyMustEnd(t, c.shape, got, want)
		})
	}
}

// busyDeadlineOf returns ctx with busyDeadline as its deadline.
func busyDeadlineOf(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, busyDeadline)
}

// busyCancelOf returns ctx cancelled once busyDeadline has passed, and what stops the cancel and cancels it.
func busyCancelOf(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	later := time.AfterFunc(busyDeadline, cancel)
	return ctx, func() {
		later.Stop()
		cancel()
	}
}

func TestABusyWaitEndsAtItsDeadline(t *testing.T) {
	t.Parallel()

	busyMustEndEvery(t, busyDeadlineOf, context.DeadlineExceeded)
}

func TestABusyWaitEndsWhenCancelled(t *testing.T) {
	t.Parallel()

	busyMustEndEvery(t, busyCancelOf, context.Canceled)
}

// busyPin returns a connection of db pinned for the rest of the test.
func busyPin(t *testing.T, db *sql.DB) *sql.Conn {
	t.Helper()
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatalf("Conn() error = %v, want nil", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// busyPinExec runs query on conn and fails the test when it cannot.
func busyPinExec(t *testing.T, conn *sql.Conn, query string) {
	t.Helper()
	if _, err := conn.ExecContext(t.Context(), query); err != nil {
		t.Fatalf("ExecContext(%q) error = %v, want nil", query, err)
	}
}

// busyBegin takes the write lock on db, released by the test or at cleanup.
func busyBegin(t *testing.T, db *sql.DB) *sql.Tx {
	t.Helper()
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("BeginTx() error = %v, want nil", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return tx
}

// busyMustReceive returns what ch receives and fails the test when nothing arrives within busyBound.
func busyMustReceive(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(busyBound):
		t.Fatalf("%s still runs after %v, want it ended", what, busyBound)
		return nil
	}
}

// busyRows returns how many rows busyInsert wrote to db.
func busyRows(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRowContext(t.Context(), busyWritten).Scan(&n); err != nil {
		t.Fatalf("%s error = %v, want nil", busyWritten, err)
	}
	return n
}

func TestALateLockLeavesACancelledWriteUnwritten(t *testing.T) {
	t.Parallel()

	db := rulesOpen(t, busyOptions(busyPoll))
	rulesExec(t, db, busySchema)
	pinned, other := busyPin(t, db), busyPin(t, db)
	busyPinExec(t, pinned, busyOtherInsert)
	busyPinExec(t, other, busyLateSchema)
	holder, err := other.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("BeginTx() error = %v, want nil", err)
	}
	t.Cleanup(func() { _ = holder.Rollback() })
	ctx, cancel := context.WithTimeout(t.Context(), busyDeadline)
	defer cancel()
	wrote := make(chan error, 1)

	go func() {
		_, err := pinned.ExecContext(ctx, busyInsert)
		wrote <- err
	}()
	<-ctx.Done()
	<-time.After(busyLateHold)
	if err := holder.Rollback(); err != nil {
		t.Fatalf("Rollback() error = %v, want nil", err)
	}

	if err := busyMustReceive(t, wrote, "the cancelled write"); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("the write error = %v, want one matching context.DeadlineExceeded", err)
	}
	if n := busyRows(t, db); n != 0 {
		t.Errorf("rows written = %d once the late lock ended, want 0 from a write whose deadline came first", n)
	}
}

// busyPrepareThenEndAWait returns busyInsert prepared on pinned once it and a write of pinned ended at their deadlines.
func busyPrepareThenEndAWait(t *testing.T, db *sql.DB, pinned *sql.Conn) *sql.Stmt {
	t.Helper()
	prepared, err := pinned.PrepareContext(t.Context(), busyInsert)
	if err != nil {
		t.Fatalf("PrepareContext(%q) error = %v, want nil", busyInsert, err)
	}
	t.Cleanup(func() { _ = prepared.Close() })
	first := busyBegin(t, db)
	ctx, cancel := context.WithTimeout(t.Context(), busyDeadline)
	defer cancel()
	if _, err := pinned.ExecContext(ctx, busyOtherInsert); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the write behind the first holder error = %v, want one matching context.DeadlineExceeded", err)
	}
	again, stop := context.WithTimeout(t.Context(), busyDeadline)
	defer stop()
	if _, err := prepared.ExecContext(again); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the prepared insert behind the first holder error = %v, want one matching context.DeadlineExceeded", err)
	}
	if err := first.Rollback(); err != nil {
		t.Fatalf("Rollback() error = %v, want nil", err)
	}
	return prepared
}

// busyHoldFor takes the write lock on db for hold and returns the channel that receives the error of its release.
func busyHoldFor(t *testing.T, db *sql.DB, hold time.Duration) <-chan error {
	t.Helper()
	holder := busyBegin(t, db)
	released := make(chan error, 1)
	time.AfterFunc(hold, func() { released <- holder.Rollback() })
	return released
}

func TestAConnectionWritesAgainAfterItsWaitEnded(t *testing.T) {
	t.Parallel()

	db := rulesOpen(t, busyOptions(busyPoll))
	rulesExec(t, db, busySchema)
	prepared := busyPrepareThenEndAWait(t, db, busyPin(t, db))
	began := time.Now()
	released := busyHoldFor(t, db, busyHold)

	_, err := prepared.ExecContext(t.Context())

	waited := time.Since(began)
	if err != nil {
		t.Errorf("the prepared insert error = %v, want it written once the second holder let go", err)
	}
	if waited < busyHold || waited > busyBound {
		t.Errorf("the prepared insert answered after %v, want after the hold %v and within %v", waited, busyHold,
			busyBound)
	}
	if err := busyMustReceive(t, released, "the second holder"); err != nil {
		t.Errorf("Rollback() error = %v, want nil", err)
	}
	if n := busyRows(t, db); n != 1 {
		t.Errorf("rows written = %d, want 1 from the prepared insert", n)
	}
}

func TestEveryConnectionEndsItsWaitAtItsDeadline(t *testing.T) {
	t.Parallel()

	db := rulesHeldElsewhere(t, testOptions())

	for i, conn := range rulesConns(t, db) {
		ctx, cancel := context.WithTimeout(t.Context(), busyDeadline)
		waited, err := rulesBegin(ctx, conn)
		cancel()

		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("BeginTx() on connection %d error = %v, want one matching context.DeadlineExceeded", i, err)
		}
		if waited > busyBound {
			t.Errorf("BeginTx() on connection %d answered after %v, want within a tenth of the busy timeout %v", i,
				waited, optionsBusyTimeout)
		}
	}
}

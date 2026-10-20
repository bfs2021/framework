// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql/driver"
	"testing"
)

const (
	// internalInputs is the count of placeholders the fake statement answers.
	internalInputs = 3
	// internalAffected is the count of rows the fake statement's run answers.
	internalAffected = 4
)

// internalSpanConn is a driver connection that records the Done channel the wait's span holds during each call.
type internalSpanConn struct {
	// driverConn is left nil.
	driverConn
	// wait is the wait whose span each call reads.
	wait *busyWait
	// seen maps each call to the Done channel of the span it ran in, nil for a call with no span.
	seen map[string]<-chan struct{}
	// rows are the rows every query of the connection answers.
	rows *internalSpanRows
	// stmtRows are the rows every query of a statement the connection prepared answers.
	stmtRows *internalSpanRows
}

// record notes the Done channel of the span the call name runs in.
func (c *internalSpanConn) record(name string) {
	c.seen[name] = internalSpanDone(c.wait)
}

// BeginTx records its span and starts no transaction.
func (c *internalSpanConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	c.record("BeginTx")
	return nil, errStepFailed
}

// ExecContext records its span and runs nothing.
func (c *internalSpanConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	c.record("ExecContext")
	return nil, errStepFailed
}

// QueryContext records its span and answers the rows of the connection.
func (c *internalSpanConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	c.record("QueryContext")
	return c.rows, nil
}

// PrepareContext records its span and answers a statement that records the span of each run.
func (c *internalSpanConn) PrepareContext(context.Context, string) (driver.Stmt, error) {
	c.record("PrepareContext")
	return &internalSpanStmt{conn: c}, nil
}

// Ping records its span.
func (c *internalSpanConn) Ping(context.Context) error {
	c.record("Ping")
	return nil
}

// internalSpanStmt is a driver statement that records the span of each run on the connection that prepared it.
type internalSpanStmt struct {
	// Stmt is left nil.
	driver.Stmt
	// conn is the connection that prepared the statement.
	conn *internalSpanConn
}

// ExecContext records its span and runs nothing.
func (s *internalSpanStmt) ExecContext(context.Context, []driver.NamedValue) (driver.Result, error) {
	s.conn.record("Stmt.ExecContext")
	return nil, errStepFailed
}

// QueryContext records its span and answers the statement rows of the connection.
func (s *internalSpanStmt) QueryContext(context.Context, []driver.NamedValue) (driver.Rows, error) {
	s.conn.record("Stmt.QueryContext")
	return s.conn.stmtRows, nil
}

// internalContexts returns a live context for each name, each with a Done channel of its own.
func internalContexts(t *testing.T, names ...string) map[string]context.Context {
	t.Helper()
	ctxs := make(map[string]context.Context, len(names))
	for _, name := range names {
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		ctxs[name] = ctx
	}
	return ctxs
}

// internalReadAndClose reads one row of rows and closes them when the query answered no error.
func internalReadAndClose(rows driver.Rows, err error) {
	if err == nil {
		_ = rows.Next(nil)
		_ = rows.Close()
	}
}

// internalWrappedCall is one call through a wrapped connection, made under the context of its name.
type internalWrappedCall struct {
	// name names the call.
	name string
	// run makes the call under ctx.
	run func(ctx context.Context)
}

// internalCallsOf returns every call of conn and of the statement it prepares, in the order the test makes them.
func internalCallsOf(conn *busyConn) []internalWrappedCall {
	var stmt driver.Stmt
	return []internalWrappedCall{
		{"BeginTx", func(ctx context.Context) { _, _ = conn.BeginTx(ctx, driver.TxOptions{}) }},
		{"ExecContext", func(ctx context.Context) { _, _ = conn.ExecContext(ctx, internalOptimize, nil) }},
		{"QueryContext", func(ctx context.Context) { internalReadAndClose(conn.QueryContext(ctx, internalOptimize, nil)) }},
		{"PrepareContext", func(ctx context.Context) { stmt, _ = conn.PrepareContext(ctx, internalOptimize) }},
		{"Stmt.ExecContext", func(ctx context.Context) { _, _ = stmt.(driver.StmtExecContext).ExecContext(ctx, nil) }},
		{"Stmt.QueryContext", func(ctx context.Context) {
			internalReadAndClose(stmt.(driver.StmtQueryContext).QueryContext(ctx, nil))
		}},
		{"Ping", func(ctx context.Context) { _ = conn.Ping(ctx) }},
	}
}

// internalRunCalls makes each call under its context in ctxs and fails the test when a span stays after a call.
func internalRunCalls(t *testing.T, wait *busyWait, calls []internalWrappedCall, ctxs map[string]context.Context) {
	t.Helper()
	for _, call := range calls {
		call.run(ctxs[call.name])
		if s := wait.span.Load(); s != nil {
			t.Errorf("the span after %s = %+v, want none between calls", call.name, s)
		}
	}
}

// internalMustReadUnder fails the test unless each Next and Close of rows ran under the Done channel of ctx.
func internalMustReadUnder(t *testing.T, name string, rows *internalSpanRows, ctx context.Context) {
	t.Helper()
	if rows.nextUnder != ctx.Done() || rows.closedUnder != ctx.Done() {
		t.Errorf("the span during Next and Close of the rows of %s holds %v and %v, want the Done channel %v of %s",
			name, rows.nextUnder, rows.closedUnder, ctx.Done(), name)
	}
}

func TestEveryCallHandsItsDoneChannelToTheWait(t *testing.T) {
	t.Parallel()

	wait := &busyWait{}
	fake := &internalSpanConn{wait: wait, seen: map[string]<-chan struct{}{}, rows: &internalSpanRows{wait: wait},
		stmtRows: &internalSpanRows{wait: wait}}
	conn := &busyConn{driverConn: fake, wait: wait}
	ctxs := internalContexts(t, "BeginTx", "ExecContext", "QueryContext", "PrepareContext", "Ping",
		"Stmt.ExecContext", "Stmt.QueryContext")

	internalRunCalls(t, wait, internalCallsOf(conn), ctxs)

	for name, ctx := range ctxs {
		if done, ok := fake.seen[name]; !ok || done != ctx.Done() {
			t.Errorf("the span during %s holds %v, want the Done channel %v of the call's own context", name, done,
				ctx.Done())
		}
	}
	internalMustReadUnder(t, "QueryContext", fake.rows, ctxs["QueryContext"])
	internalMustReadUnder(t, "Stmt.QueryContext", fake.stmtRows, ctxs["Stmt.QueryContext"])
}

// internalStmtConn is a driver connection whose every prepare answers its statement.
type internalStmtConn struct {
	// driverConn is left nil.
	driverConn
	// stmt is the statement every prepare answers.
	stmt driver.Stmt
}

// PrepareContext answers the statement of the connection.
func (c *internalStmtConn) PrepareContext(context.Context, string) (driver.Stmt, error) {
	return c.stmt, nil
}

// internalStmt is a driver statement whose every call answers a value of its own and whose Close is recorded.
type internalStmt struct {
	// Stmt is left nil.
	driver.Stmt
	// closed reports that Close ran.
	closed bool
}

// NumInput answers internalInputs.
func (*internalStmt) NumInput() int {
	return internalInputs
}

// Close records that it ran and fails with errCloseFailed.
func (s *internalStmt) Close() error {
	s.closed = true
	return errCloseFailed
}

// ExecContext answers internalAffected rows affected.
func (*internalStmt) ExecContext(context.Context, []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(internalAffected), nil
}

// QueryContext answers rows that answer every column type question.
func (*internalStmt) QueryContext(context.Context, []driver.NamedValue) (driver.Rows, error) {
	return &internalTypedRows{}, nil
}

// internalPlainStmt is a driver statement with only the methods of driver.Stmt.
type internalPlainStmt struct {
	// Stmt is left nil.
	driver.Stmt
}

// internalPrepare prepares a statement through a wrapper whose driver answers stmt, and returns what it answered.
func internalPrepare(t *testing.T, stmt driver.Stmt) driver.Stmt {
	t.Helper()
	conn := &busyConn{driverConn: &internalStmtConn{stmt: stmt}, wait: &busyWait{}}
	got, err := conn.PrepareContext(t.Context(), internalOptimize)
	if err != nil {
		t.Fatalf("PrepareContext() error = %v, want nil", err)
	}
	return got
}

// internalMustRunTheStatement fails the test unless stmt runs and answers the results of internalStmt.
func internalMustRunTheStatement(t *testing.T, stmt driverStmt) {
	t.Helper()
	result, err := stmt.ExecContext(t.Context(), nil)
	if err != nil {
		t.Fatalf("ExecContext() error = %v, want nil", err)
	}
	if n, err := result.RowsAffected(); n != internalAffected || err != nil {
		t.Errorf("RowsAffected() = %d, %v, want the driver's %d", n, err, internalAffected)
	}
	rows, err := stmt.QueryContext(t.Context(), nil)
	if err != nil {
		t.Fatalf("QueryContext() error = %v, want nil", err)
	}
	for _, p := range internalColumnProbes {
		if !p.answers(rows) {
			t.Errorf("%s on the rows of a prepared query is missing or answers otherwise, want the driver's", p.name)
		}
	}
}

func TestAPreparedStatementKeepsEveryInterfaceOfTheDriversStatement(t *testing.T) {
	t.Parallel()

	fake := &internalStmt{}
	stmt, ok := internalPrepare(t, fake).(driverStmt)
	if !ok {
		t.Fatalf("PrepareContext() answers %T, want a statement with every interface of driverStmt", stmt)
	}

	internalMustRunTheStatement(t, stmt)

	if n := stmt.NumInput(); n != internalInputs {
		t.Errorf("NumInput() = %d, want the driver's %d", n, internalInputs)
	}
	if err := stmt.Close(); err != errCloseFailed || !fake.closed {
		t.Errorf("Close() error = %v and closed = %v, want the driver's statement closed and its error as it is", err,
			fake.closed)
	}
}

func TestAStatementLackingAnInterfaceIsPreparedAsItIs(t *testing.T) {
	t.Parallel()

	plain := &internalPlainStmt{}

	if got := internalPrepare(t, plain); got != driver.Stmt(plain) {
		t.Errorf("PrepareContext() = %T, want the driver's statement %T as it is", got, plain)
	}
}

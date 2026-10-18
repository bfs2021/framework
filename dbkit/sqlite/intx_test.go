// SPDX-License-Identifier: Apache-2.0

package sqlite_test

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/gopherium/framework/dbkit"
	"github.com/gopherium/framework/dbkit/sqlite/sqlitetest"
)

// intxInsert is a transaction body that writes one row to intx_rows.
func intxInsert(tx *sql.Tx) error {
	_, err := tx.Exec("INSERT INTO intx_rows VALUES (1)")
	return err
}

// intxCount returns how many rows intx_rows holds on db.
func intxCount(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM intx_rows").Scan(&n); err != nil {
		t.Fatalf("count error = %v, want nil", err)
	}
	return n
}

func TestInTxJoinsAFailedRollbackToTheBodysError(t *testing.T) {
	t.Parallel()

	errBody := errors.New("intx test: the transaction body failed on purpose")
	errRollback := errors.New("intx test: the rollback failed on purpose")
	faults := &sqlitetest.Faults{}
	db := sqlitetest.OpenWithFaults(t, testOptions(), faults)
	if _, err := db.ExecContext(t.Context(), "CREATE TABLE intx_rows (a INTEGER NOT NULL)"); err != nil {
		t.Fatalf("CREATE TABLE error = %v, want nil", err)
	}
	faults.FailNextRollback(errRollback)

	err := dbkit.InTx(t.Context(), db, func(tx *sql.Tx) error {
		if err := intxInsert(tx); err != nil {
			return err
		}
		return errBody
	})

	if !errors.Is(err, errBody) {
		t.Errorf("InTx() error = %v, want it to match the body error", err)
	}
	if !errors.Is(err, errRollback) {
		t.Errorf("InTx() error = %v, want it to match the rollback error", err)
	}
	if n := intxCount(t, db); n != 0 {
		t.Errorf("rows after the failed InTx() = %d, want 0", n)
	}
	if err := dbkit.InTx(t.Context(), db, intxInsert); err != nil {
		t.Errorf("a later InTx() error = %v, want the transaction ended and the write lock free", err)
	}
	if n := intxCount(t, db); n != 1 {
		t.Errorf("rows after the later InTx() = %d, want 1", n)
	}
}

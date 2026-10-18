// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin

package sqlite

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	modernc "modernc.org/sqlite"
)

const (
	// newFileHolderPath names the switch that makes the test binary hold the write lock of the file at its value.
	newFileHolderPath = "DBKIT_TEST_NEW_FILE_HOLDER"
	// newFileMigratorPath names the environment switch that makes the test binary migrate the file at its value.
	newFileMigratorPath = "DBKIT_TEST_NEW_FILE_MIGRATOR"
	// newFileHeld is the line a holder prints once it holds the write lock.
	newFileHeld = "dbkit test: the write lock of the new file is held"
	// newFileStarted is the line a migrator prints right before it migrates.
	newFileStarted = "dbkit test: the migrator starts"
	// newFileMigrators is how many processes migrate one new file at once.
	newFileMigrators = 8
	// newFileRounds is how many new files the test migrates, one after another.
	newFileRounds = 3
	// newFileHold is how long the holder keeps the write lock after the last migrator started.
	newFileHold = 300 * time.Millisecond
	// newFileBusyTimeout is a busy timeout no migrator waits out.
	newFileBusyTimeout = time.Minute
	// newFileBusyPoll is how often the first open of a migrator tries again.
	newFileBusyPoll = 5 * time.Millisecond
	// newFileTable is the version table of the migrators.
	newFileTable = "new_file_versions"
)

// newFileOutput is what a helper process printed, which reports when the process prints its line.
type newFileOutput struct {
	// mu guards text.
	mu sync.Mutex
	// text is everything the process printed.
	text strings.Builder
	// line is the line the process prints once it is ready.
	line string
	// printed is closed once text holds line.
	printed chan struct{}
	// once closes printed once.
	once sync.Once
}

// Write keeps p and closes printed once the output holds the line.
func (o *newFileOutput) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.text.Write(p)
	if strings.Contains(o.text.String(), o.line+"\n") {
		o.once.Do(func() { close(o.printed) })
	}
	return len(p), nil
}

// String returns everything the process printed.
func (o *newFileOutput) String() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.text.String()
}

// newFileKind is one kind of helper process.
type newFileKind struct {
	// test is the test function the process runs.
	test string
	// env is the environment switch that hands the process the path of the file.
	env string
	// line is the line the process prints once it is ready.
	line string
}

var (
	// newFileHolder holds the write lock of a new file until its standard input closes.
	newFileHolder = newFileKind{test: "TestNewFileHolderProcess", env: newFileHolderPath, line: newFileHeld}
	// newFileMigrator migrates a new file.
	newFileMigrator = newFileKind{test: "TestNewFileMigratorProcess", env: newFileMigratorPath, line: newFileStarted}
)

// newFileProcess is a run of the test binary as a helper process.
type newFileProcess struct {
	// cmd is the running test binary.
	cmd *exec.Cmd
	// stdin is the standard input of the process.
	stdin io.WriteCloser
	// output is what the process printed.
	output *newFileOutput
	// ended is closed once the process was reaped.
	ended chan struct{}
	// err is how the process ended, read once ended is closed.
	err error
}

// start runs the helper process on the file at path, which the end of the test kills.
func (k newFileKind) start(t *testing.T, path string) *newFileProcess {
	t.Helper()
	output := &newFileOutput{line: k.line, printed: make(chan struct{})}
	cmd := exec.Command(os.Args[0], "-test.run=^"+k.test+"$")
	cmd.Env = append(os.Environ(), k.env+"="+path)
	cmd.Stdout, cmd.Stderr = output, output
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("StdinPipe() error = %v, want nil", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}
	p := &newFileProcess{cmd: cmd, stdin: stdin, output: output, ended: make(chan struct{})}
	go func() {
		p.err = cmd.Wait()
		close(p.ended)
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-p.ended
	})
	return p
}

// mustPrint fails the test when the process ends before it prints its line.
func (p *newFileProcess) mustPrint(t *testing.T) {
	t.Helper()
	select {
	case <-p.output.printed:
	case <-p.ended:
		t.Fatalf("the helper process ended with %v before it printed %q\n%s", p.err, p.output.line, p.output)
	}
}

// wait returns how the process ended once it was reaped.
func (p *newFileProcess) wait() error {
	<-p.ended
	return p.err
}

// release closes the standard input of the process and returns how the process ended.
func (p *newFileProcess) release() error {
	_ = p.stdin.Close()
	return p.wait()
}

// newFileOptions returns the options of a migrator.
func newFileOptions() Options {
	opts := internalOptions()
	opts.BusyTimeout, opts.BusyPoll = newFileBusyTimeout, newFileBusyPoll
	return opts
}

// newFileMigrations returns the one SQL migration of a migrator.
func newFileMigrations() Migrations {
	up := &fstest.MapFile{Data: []byte("-- +goose Up\nCREATE TABLE new_file_rows (a INTEGER);\n")}
	return Migrations{Table: newFileTable, FS: fstest.MapFS{"00001_new_file_rows.sql": up},
		LockWait: lockLongWait, LockPoll: lockPoll}
}

func TestNewFileHolderProcess(t *testing.T) {
	path := os.Getenv(newFileHolderPath)
	if path == "" {
		return
	}
	if err := ofdOnce(); err != nil {
		t.Fatalf("ofdOnce() in the holder error = %v, want nil", err)
	}
	conn, err := (&modernc.Driver{}).Open(path)
	if err != nil {
		t.Fatalf("Open() in the holder error = %v, want nil", err)
	}
	defer func() { _ = conn.Close() }()
	plain := conn.(driver.ExecerContext)
	if _, err := plain.ExecContext(context.Background(), "BEGIN IMMEDIATE", nil); err != nil {
		t.Fatalf("BEGIN IMMEDIATE in the holder error = %v, want nil", err)
	}
	fmt.Println(newFileHeld)
	if _, err := io.Copy(io.Discard, os.Stdin); err != nil {
		t.Fatalf("read the standard input of the holder: %v", err)
	}
	if _, err := plain.ExecContext(context.Background(), "ROLLBACK", nil); err != nil {
		t.Fatalf("ROLLBACK in the holder error = %v, want nil", err)
	}
}

func TestNewFileMigratorProcess(t *testing.T) {
	path := os.Getenv(newFileMigratorPath)
	if path == "" {
		return
	}
	db, err := Open("sqlite:"+path, newFileOptions())
	if err != nil {
		t.Fatalf("Open() in a migrator error = %v, want nil", err)
	}
	defer func() { _ = db.Close() }()
	fmt.Println(newFileStarted)

	if err := Migrate(context.Background(), db, newFileMigrations()); err != nil {
		t.Fatalf("Migrate() in a migrator error = %v, want nil", err)
	}
}

// newFileStartMigrators starts newFileMigrators migrators on the file at path and returns once each one started.
func newFileStartMigrators(t *testing.T, path string) []*newFileProcess {
	t.Helper()
	migrators := make([]*newFileProcess, newFileMigrators)
	for i := range migrators {
		migrators[i] = newFileMigrator.start(t, path)
	}
	for _, m := range migrators {
		m.mustPrint(t)
	}
	return migrators
}

// newFileMustApplyOnce fails the test unless the file at path records its one migration as applied once.
func newFileMustApplyOnce(t *testing.T, path string) {
	t.Helper()
	db, err := Open("sqlite:"+path, internalOptions())
	if err != nil {
		t.Fatalf("Open() error = %v, want nil", err)
	}
	defer func() { _ = db.Close() }()
	var n int64
	query := "SELECT count(*) FROM " + newFileTable + " WHERE version_id = 1 AND is_applied"
	if err := db.QueryRowContext(t.Context(), query).Scan(&n); err != nil || n != 1 {
		t.Errorf("%s records version 1 %d times, %v, want once", newFileTable, n, err)
	}
}

// newFileRound fails the test unless every migrator of a new file ends cleanly while a holder keeps its write lock.
func newFileRound(t *testing.T, round int) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "site.db")
	holder := newFileHolder.start(t, path)
	holder.mustPrint(t)
	migrators := newFileStartMigrators(t, path)
	time.Sleep(newFileHold)

	if err := holder.release(); err != nil {
		t.Fatalf("round %d: the holder ended with %v, want a clean end\n%s", round, err, holder.output)
	}
	for i, m := range migrators {
		if err := m.wait(); err != nil {
			t.Errorf("round %d: migrator %d ended with %v, want a clean end\n%s", round, i, err, m.output)
		}
	}
	newFileMustApplyOnce(t, path)
}

func TestMigrateRunsOnANewFileWhileAProcessHoldsTheWriteLock(t *testing.T) {
	t.Parallel()

	for round := range newFileRounds {
		newFileRound(t, round)
	}
}

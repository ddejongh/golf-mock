// Package pgtest gives tests an isolated, migrated Postgres database.
//
// If VP_TEST_DATABASE_URL is set, tests create throwaway databases on that
// server (it needs CREATEDB). Otherwise an embedded Postgres 17 is started
// for the test binary; its binaries are downloaded once and cached in
// ~/.embedded-postgres-go. Set VP_TEST_SKIP_DB=1 to skip database tests.
//
// Packages using New must call pgtest.Main from TestMain.
package pgtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"sync"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5"

	"github.com/ddejongh/venuepulse/backend-go/internal/store"
)

var (
	startOnce  sync.Once
	baseURL    string
	startErr   error
	embedded   *embeddedpostgres.EmbeddedPostgres
	runtimeDir string
)

// Main runs the package's tests and stops the embedded server afterwards.
func Main(m *testing.M) {
	code := m.Run()
	if embedded != nil {
		if err := embedded.Stop(); err != nil {
			fmt.Fprintf(os.Stderr, "pgtest: stop embedded postgres: %v\n", err)
		}
		_ = os.RemoveAll(runtimeDir)
	}
	os.Exit(code)
}

// New creates a fresh database, applies all migrations, and returns an open
// Store and the database URL. Both are cleaned up when t finishes.
func New(t testing.TB) (*store.Store, string) {
	t.Helper()
	if os.Getenv("VP_TEST_SKIP_DB") != "" {
		t.Skip("VP_TEST_SKIP_DB is set")
	}
	startOnce.Do(start)
	if startErr != nil {
		t.Fatalf("pgtest: %v", startErr)
	}
	ctx := context.Background()

	name := "vptest_" + randomHex(t)
	admin, err := pgx.Connect(ctx, baseURL)
	if err != nil {
		t.Fatalf("pgtest: connect: %v", err)
	}
	defer func() { _ = admin.Close(ctx) }()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("pgtest: create database: %v", err)
	}

	dbURL := withDatabase(t, baseURL, name)
	st, err := store.Open(ctx, dbURL)
	if err != nil {
		t.Fatalf("pgtest: %v", err)
	}
	t.Cleanup(func() {
		st.Close()
		conn, err := pgx.Connect(ctx, baseURL)
		if err != nil {
			t.Logf("pgtest: cleanup connect: %v", err)
			return
		}
		defer func() { _ = conn.Close(ctx) }()
		if _, err := conn.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			t.Logf("pgtest: drop database: %v", err)
		}
	})

	m, err := st.Migrator()
	if err != nil {
		t.Fatalf("pgtest: %v", err)
	}
	defer func() { _ = m.Close() }()
	if _, err := m.Up(ctx); err != nil {
		t.Fatalf("pgtest: %v", err)
	}
	return st, dbURL
}

func start() {
	if u := os.Getenv("VP_TEST_DATABASE_URL"); u != "" {
		baseURL = u
		return
	}
	port, err := freePort()
	if err != nil {
		startErr = err
		return
	}
	runtimeDir, err = os.MkdirTemp("", "pgtest-")
	if err != nil {
		startErr = fmt.Errorf("runtime dir: %w", err)
		return
	}
	cfg := embeddedpostgres.DefaultConfig().
		Version(embeddedpostgres.V17).
		Port(port).
		RuntimePath(runtimeDir).
		Logger(io.Discard)
	embedded = embeddedpostgres.NewDatabase(cfg)
	if err := embedded.Start(); err != nil {
		startErr = fmt.Errorf("start embedded postgres (set VP_TEST_DATABASE_URL to use an existing server): %w", err)
		embedded = nil
		return
	}
	baseURL = cfg.GetConnectionURL() + "?sslmode=disable"
}

func freePort() (uint32, error) {
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("find free port: %w", err)
	}
	defer func() { _ = l.Close() }()
	return uint32(l.Addr().(*net.TCPAddr).Port), nil //nolint:gosec // ports fit in uint32
}

func withDatabase(t testing.TB, base, name string) string {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("pgtest: parse url: %v", err)
	}
	u.Path = "/" + name
	return u.String()
}

func randomHex(t testing.TB) string {
	t.Helper()
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b[:])
}

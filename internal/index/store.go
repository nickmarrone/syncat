package index

import (
	"context"
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite" // registers the "sqlite" driver; pure Go, no cgo (SPEC.md §12)
)

// Store is the durable index: one SQLite database per node, at
// <datadir>/db/index.db (SPEC.md §3, §5). It holds this node's own view of
// each share's files (the `files` table), the latest IndexUpdate mirrored
// from each peer (`peer_files`), and resumable-transfer bookkeeping
// (`pending_transfers`, schema only in this phase — Phase 5 populates it).
//
// Concurrency model: a single *sql.DB in WAL mode (see openPragmas), shared
// by the scanner, the watcher's rescan trigger, and (in later phases) the
// sync engine and the REST API. WAL allows any number of concurrent
// readers alongside a single writer without blocking reads, which is the
// access pattern this package has (frequent reads for diffing a scan
// against the index; writes only when applying a scan result or a peer
// update). We do not hand out a *sql.Tx across goroutines or hold one open
// across an fs walk; every write path opens, uses, and commits/rolls back
// its transaction within one method call, so two writers racing just
// serialize through SQLite's single-writer rule (surfaced by Go's
// connection pool blocking, not a corrupt interleave) rather than
// deadlocking. database/sql's *DB is documented safe for concurrent use by
// multiple goroutines, which is what we rely on here.
type Store struct {
	db *sql.DB
}

// currentSchemaVersion is bumped whenever migrations is extended. Stored
// in SQLite's own PRAGMA user_version (an integer baked into the database
// file header) rather than a table we'd have to query separately — it's
// exactly what PRAGMA user_version exists for, and it's readable/writable
// in the same connection setup step as the other pragmas below.
const currentSchemaVersion = 1

// migrations[i] upgrades a database from schema version i to i+1.
// Migrations run inside a single transaction per step (see migrate) so a
// failure partway through a step never leaves the schema half-applied.
var migrations = []func(ctx context.Context, tx *sql.Tx) error{
	migrateV1,
}

// Open opens (creating if necessary) the SQLite database at path and
// migrates it to currentSchemaVersion. It is safe to call repeatedly
// against the same path (e.g. across daemon restarts) — migration is
// idempotent and a no-op once the schema is current.
func Open(ctx context.Context, path string) (*Store, error) {
	// modernc.org/sqlite accepts the same DSN query-parameter pragmas as
	// mattn's driver. _pragma=busy_timeout sets SQLite's own internal
	// retry loop (belt-and-suspenders alongside WAL, which is the main
	// reason concurrent access doesn't produce SQLITE_BUSY in practice
	// here) so a writer that does briefly block waits up to 5s instead of
	// failing immediately. _pragma=foreign_keys(1) turns on FK
	// enforcement; this schema doesn't declare any FKs yet (share_id is
	// just a string, not a reference to a shares table — shares live in
	// config.toml, not this database), but it's set from the start so
	// enforcement is never silently off for whichever later migration
	// adds one (e.g. pending_transfers referencing files).
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("index: open %s: %w", path, err)
	}

	if err := configurePool(ctx, db); err != nil {
		db.Close()
		return nil, err
	}

	s := &Store{db: db}
	if err := s.migrate(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// configurePool sets WAL mode and the connection-pool shape. SQLite's WAL
// mode allows one writer concurrent with many readers (vs. the default
// rollback journal, which locks the whole database for the duration of a
// write); we cap the pool at a modest size since SQLite still serializes
// actual writes internally regardless of how many *sql.Conns Go hands out.
func configurePool(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `PRAGMA journal_mode=WAL`); err != nil {
		return fmt.Errorf("index: set WAL mode: %w", err)
	}
	if _, err := db.ExecContext(ctx, `PRAGMA synchronous=NORMAL`); err != nil {
		return fmt.Errorf("index: set synchronous mode: %w", err)
	}
	// WAL mode tolerates multiple connections, but modernc's driver (like
	// most SQLite drivers) still serializes writes at the database level;
	// keep the pool small so we're not fanning out more concurrent
	// connections than SQLite can usefully use, while still allowing
	// concurrent readers.
	db.SetMaxOpenConns(8)
	return nil
}

// migrate runs every migration step between the database's current
// PRAGMA user_version and currentSchemaVersion, each in its own
// transaction.
func (s *Store) migrate(ctx context.Context) error {
	var version int
	if err := s.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("index: read schema version: %w", err)
	}
	if version > currentSchemaVersion {
		return fmt.Errorf("index: database schema version %d is newer than this binary supports (%d)", version, currentSchemaVersion)
	}

	for version < currentSchemaVersion {
		step := migrations[version] // migrations[i]: version i -> i+1
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("index: begin migration tx: %w", err)
		}
		if err := step(ctx, tx); err != nil {
			tx.Rollback()
			return fmt.Errorf("index: migrate schema %d -> %d: %w", version, version+1, err)
		}
		version++
		// PRAGMA user_version doesn't accept bound parameters; version is
		// our own int, never user input.
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`PRAGMA user_version = %d`, version)); err != nil {
			tx.Rollback()
			return fmt.Errorf("index: set schema version %d: %w", version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("index: commit migration to schema %d: %w", version, err)
		}
	}
	return nil
}

func migrateV1(ctx context.Context, tx *sql.Tx) error {
	stmts := []string{
		`CREATE TABLE files (
			share_id     TEXT    NOT NULL,
			relpath      TEXT    NOT NULL,
			type         TEXT    NOT NULL,
			size         INTEGER NOT NULL,
			mtime_ns     INTEGER NOT NULL,
			mode         INTEGER NOT NULL,
			sha256       BLOB,
			version_json TEXT    NOT NULL,
			deleted      INTEGER NOT NULL DEFAULT 0,
			updated_at   INTEGER NOT NULL,
			PRIMARY KEY (share_id, relpath)
		)`,
		`CREATE INDEX files_share_deleted ON files (share_id, deleted)`,
		`CREATE TABLE peer_files (
			peer_key     TEXT    NOT NULL,
			share_id     TEXT    NOT NULL,
			relpath      TEXT    NOT NULL,
			type         TEXT    NOT NULL,
			size         INTEGER NOT NULL,
			mtime_ns     INTEGER NOT NULL,
			mode         INTEGER NOT NULL,
			sha256       BLOB,
			version_json TEXT    NOT NULL,
			deleted      INTEGER NOT NULL DEFAULT 0,
			updated_at   INTEGER NOT NULL,
			PRIMARY KEY (peer_key, share_id, relpath)
		)`,
		`CREATE INDEX peer_files_share ON peer_files (peer_key, share_id, deleted)`,
		// pending_transfers: created now so Phase 5's resume support has a
		// stable schema to migrate onto later, per the phase brief. Not
		// used by this phase. One row per (share_id, relpath, peer_key,
		// direction) in-flight transfer; offset is the resume point
		// (SPEC.md §4's FileRequest.offset).
		`CREATE TABLE pending_transfers (
			share_id     TEXT    NOT NULL,
			relpath      TEXT    NOT NULL,
			peer_key     TEXT    NOT NULL,
			direction    TEXT    NOT NULL,
			version_json TEXT    NOT NULL,
			offset       INTEGER NOT NULL DEFAULT 0,
			updated_at   INTEGER NOT NULL,
			PRIMARY KEY (share_id, relpath, peer_key, direction)
		)`,
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec %q: %w", stmt, err)
		}
	}
	return nil
}

// Close closes the underlying database.
func (s *Store) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("index: close database: %w", err)
	}
	return nil
}

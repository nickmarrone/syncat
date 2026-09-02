// Package index keeps this node's view of what is on disk (SPEC.md §5):
// durable storage in SQLite ([Store], store.go), change detection and
// ignore matching ([Scanner], [Matcher], scanner.go), and a debounced
// fsnotify [Watcher] (watcher.go).
//
// This package is one of the gomobile-safe leaves called out in SPEC.md
// §12: it imports no UI/CLI/HTTP packages, builds with CGO_ENABLED=0 (the
// SQLite driver is modernc.org/sqlite, a pure-Go implementation), and the
// scanner reads share contents through an fs.FS rather than calling os.*
// directly, so a future mobile port can supply its own sandboxed
// filesystem.
//
// Version vectors are opaque here: this package copies a row's
// protocol.VersionVector through unchanged and never compares or bumps
// one. That algebra lives in internal/sync.
package index

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nickmarrone/syncat/internal/protocol"
	_ "modernc.org/sqlite" // registers the "sqlite" driver; pure Go, no cgo (SPEC.md §12)
)

// --- the database: opening and migrating -------------------------------

// Store is the durable index: one SQLite database per node, at
// <datadir>/db/index.db (SPEC.md §3, §5). It holds this node's own view of
// each share's files (the `files` table), the latest IndexUpdate mirrored
// from each peer (`peer_files`), and resumable-transfer bookkeeping
// (`pending_transfers`, schema only — nothing writes it yet; see
// migrateV1).
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
		// pending_transfers: nothing reads or writes this table yet. It
		// exists so resumable transfers have a stable schema to migrate
		// onto rather than needing a new migration. One row per (share_id,
		// relpath, peer_key, direction) in-flight transfer; offset is the
		// resume point (SPEC.md §4's FileRequest.offset, currently always 0).
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

// --- FileRow: the index's row shape ------------------------------------

// FileRow is the index's on-disk representation of one file/dir/symlink
// entry within a share: protocol.FileInfo (the wire shape) plus the local
// bookkeeping columns SPEC.md §5 assigns to the `files` table (share_id,
// deleted, updated_at). Callers that need the wire shape use [FileRow.Info];
// callers building a row from a wire message use [FileRowFromInfo].
//
// Keeping this as an explicit, richer type — rather than reusing
// protocol.FileInfo directly as the row shape — means a schema change here
// (e.g. adding a local-only column) never risks leaking onto the wire, and
// vice versa; the conversion between the two is small and tested (see
// store_test.go).
type FileRow struct {
	ShareID   string
	RelPath   string
	Type      protocol.FileType
	Size      int64
	MTimeNS   int64
	Mode      uint32
	SHA256    []byte
	Version   protocol.VersionVector
	Deleted   bool
	UpdatedAt time.Time
}

// Info converts the row to its wire shape.
func (r FileRow) Info() protocol.FileInfo {
	return protocol.FileInfo{
		RelPath: r.RelPath,
		Type:    r.Type,
		Size:    r.Size,
		MTimeNS: r.MTimeNS,
		Mode:    r.Mode,
		SHA256:  r.SHA256,
		Version: r.Version,
		Deleted: r.Deleted,
	}
}

// FileRowFromInfo builds a FileRow from a wire FileInfo plus the local
// columns the wire message doesn't carry.
func FileRowFromInfo(shareID string, info protocol.FileInfo, updatedAt time.Time) FileRow {
	return FileRow{
		ShareID:   shareID,
		RelPath:   info.RelPath,
		Type:      info.Type,
		Size:      info.Size,
		MTimeNS:   info.MTimeNS,
		Mode:      info.Mode,
		SHA256:    info.SHA256,
		Version:   info.Version,
		Deleted:   info.Deleted,
		UpdatedAt: updatedAt,
	}
}

// cloneVersion returns a copy of v so callers can hand out a row's version
// vector without letting the recipient mutate the row's own map.
func cloneVersion(v protocol.VersionVector) protocol.VersionVector {
	if v == nil {
		return nil
	}
	out := make(protocol.VersionVector, len(v))
	for k, val := range v {
		out[k] = val
	}
	return out
}

// ErrNotFound is returned by lookups (GetFile) when no row matches.
var ErrNotFound = errors.New("index: not found")

// --- the files table: this node's own view -----------------------------

// GetFile returns the row for one (shareID, relpath), including tombstones
// (rows with Deleted=true).
func (s *Store) GetFile(ctx context.Context, shareID, relpath string) (FileRow, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT relpath, type, size, mtime_ns, mode, sha256, version_json, deleted, updated_at
		FROM files WHERE share_id = ? AND relpath = ?`, shareID, relpath)
	fr, err := scanFileRow(row, shareID)
	if errors.Is(err, sql.ErrNoRows) {
		return FileRow{}, fmt.Errorf("index: get %s/%s: %w", shareID, relpath, ErrNotFound)
	}
	if err != nil {
		return FileRow{}, fmt.Errorf("index: get %s/%s: %w", shareID, relpath, err)
	}
	return fr, nil
}

// PutFile upserts one row into the files table.
func (s *Store) PutFile(ctx context.Context, row FileRow) error {
	vj, err := versionJSON(row.Version)
	if err != nil {
		return fmt.Errorf("index: put %s/%s: %w", row.ShareID, row.RelPath, err)
	}
	if _, err := s.db.ExecContext(ctx, putFileSQL,
		row.ShareID, row.RelPath, string(row.Type), row.Size, row.MTimeNS, row.Mode,
		row.SHA256, vj, boolInt(row.Deleted), timeNS(row.UpdatedAt),
	); err != nil {
		return fmt.Errorf("index: put %s/%s: %w", row.ShareID, row.RelPath, err)
	}
	return nil
}

// ListShare returns every row for a share, in relpath order.
// includeDeleted controls whether tombstones are included.
func (s *Store) ListShare(ctx context.Context, shareID string, includeDeleted bool) ([]FileRow, error) {
	query := `SELECT relpath, type, size, mtime_ns, mode, sha256, version_json, deleted, updated_at
		FROM files WHERE share_id = ?`
	if !includeDeleted {
		query += ` AND deleted = 0`
	}
	query += ` ORDER BY relpath`
	rows, err := s.db.QueryContext(ctx, query, shareID)
	if err != nil {
		return nil, fmt.Errorf("index: list share %s: %w", shareID, err)
	}
	defer rows.Close()
	return collectFileRows(rows, shareID)
}

// ListShareMap is a convenience wrapper around ListShare for the common
// case of diffing a scan against the index: a relpath -> FileRow lookup,
// including tombstones (the scanner needs to see them, to distinguish a
// brand-new file from one being resurrected — see scanner.go).
func (s *Store) ListShareMap(ctx context.Context, shareID string) (map[string]FileRow, error) {
	rows, err := s.ListShare(ctx, shareID, true)
	if err != nil {
		return nil, err
	}
	out := make(map[string]FileRow, len(rows))
	for _, r := range rows {
		out[r.RelPath] = r
	}
	return out, nil
}

// ApplyScanResult persists a Scanner diff in one transaction: every added,
// content-changed, and metadata-only row is upserted, and every deleted
// row is written back as a tombstone (Deleted=true, other columns
// unchanged from what the scanner reported). Applying is a separate,
// explicit step from scanning (see scanner.go's doc comment) so a caller
// can inspect or filter a ScanResult — e.g. skip files ignored mid-flight,
// or hand it to internal/sync's reconciler first — before it becomes durable.
func (s *Store) ApplyScanResult(ctx context.Context, result *ScanResult) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("index: apply scan result: begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit

	stmt, err := tx.PrepareContext(ctx, putFileSQL)
	if err != nil {
		return fmt.Errorf("index: apply scan result: prepare: %w", err)
	}
	defer stmt.Close()

	apply := func(rows []FileRow) error {
		for _, row := range rows {
			vj, err := versionJSON(row.Version)
			if err != nil {
				return fmt.Errorf("row %s/%s: %w", row.ShareID, row.RelPath, err)
			}
			if _, err := stmt.ExecContext(ctx,
				row.ShareID, row.RelPath, string(row.Type), row.Size, row.MTimeNS, row.Mode,
				row.SHA256, vj, boolInt(row.Deleted), timeNS(row.UpdatedAt),
			); err != nil {
				return fmt.Errorf("row %s/%s: %w", row.ShareID, row.RelPath, err)
			}
		}
		return nil
	}
	for _, rows := range [][]FileRow{result.Added, result.ContentChanged, result.MetadataOnly, result.Deleted} {
		if err := apply(rows); err != nil {
			return fmt.Errorf("index: apply scan result: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("index: apply scan result: commit: %w", err)
	}
	return nil
}

const putFileSQL = `
	INSERT INTO files (share_id, relpath, type, size, mtime_ns, mode, sha256, version_json, deleted, updated_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT (share_id, relpath) DO UPDATE SET
		type = excluded.type,
		size = excluded.size,
		mtime_ns = excluded.mtime_ns,
		mode = excluded.mode,
		sha256 = excluded.sha256,
		version_json = excluded.version_json,
		deleted = excluded.deleted,
		updated_at = excluded.updated_at`

// rowScanner is satisfied by both *sql.Row and *sql.Rows, letting
// scanFileRow serve GetFile (single row) and the List* methods (many rows)
// with one implementation.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanFileRow(row rowScanner, shareID string) (FileRow, error) {
	var (
		relpath, typ, versionJSONStr string
		size, mtimeNS                int64
		mode                         uint32
		sha256                       []byte
		deleted                      int
		updatedAtNS                  int64
	)
	if err := row.Scan(&relpath, &typ, &size, &mtimeNS, &mode, &sha256, &versionJSONStr, &deleted, &updatedAtNS); err != nil {
		return FileRow{}, err
	}
	version, err := unmarshalVersion(versionJSONStr)
	if err != nil {
		return FileRow{}, fmt.Errorf("decode version for %s/%s: %w", shareID, relpath, err)
	}
	return FileRow{
		ShareID:   shareID,
		RelPath:   relpath,
		Type:      protocol.FileType(typ),
		Size:      size,
		MTimeNS:   mtimeNS,
		Mode:      mode,
		SHA256:    sha256,
		Version:   version,
		Deleted:   deleted != 0,
		UpdatedAt: time.Unix(0, updatedAtNS).UTC(),
	}, nil
}

func collectFileRows(rows *sql.Rows, shareID string) ([]FileRow, error) {
	var out []FileRow
	for rows.Next() {
		fr, err := scanFileRow(rows, shareID)
		if err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		out = append(out, fr)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}
	return out, nil
}

// versionJSON encodes a version vector for storage. Marshaling
// map[string]uint64 cannot practically fail, but we still propagate the
// error rather than panicking (no panics in library code).
func versionJSON(v protocol.VersionVector) (string, error) {
	if v == nil {
		return "{}", nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("marshal version vector: %w", err)
	}
	return string(b), nil
}

func unmarshalVersion(s string) (protocol.VersionVector, error) {
	if s == "" || s == "{}" {
		return nil, nil
	}
	var v protocol.VersionVector
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return nil, err
	}
	return v, nil
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func timeNS(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

// --- the peer_files table: each peer's mirrored index ------------------

// UpsertPeerFiles replaces this peer's known state for the given rows, in
// one transaction. Each row's ShareID/RelPath identifies which file it
// describes; PeerKey is passed once for the whole batch since an
// IndexUpdate always arrives from a single peer connection.
//
// This mirrors ApplyScanResult's shape (bulk, transactional) but for the
// peer_files side of the schema. internal/sync's Session calls it on
// every IndexUpdate it receives.
func (s *Store) UpsertPeerFiles(ctx context.Context, peerKey string, rows []FileRow) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("index: upsert peer files: begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO peer_files (peer_key, share_id, relpath, type, size, mtime_ns, mode, sha256, version_json, deleted, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (peer_key, share_id, relpath) DO UPDATE SET
			type = excluded.type,
			size = excluded.size,
			mtime_ns = excluded.mtime_ns,
			mode = excluded.mode,
			sha256 = excluded.sha256,
			version_json = excluded.version_json,
			deleted = excluded.deleted,
			updated_at = excluded.updated_at`)
	if err != nil {
		return fmt.Errorf("index: upsert peer files: prepare: %w", err)
	}
	defer stmt.Close()

	for _, row := range rows {
		vj, err := versionJSON(row.Version)
		if err != nil {
			return fmt.Errorf("index: upsert peer files: row %s/%s: %w", row.ShareID, row.RelPath, err)
		}
		if _, err := stmt.ExecContext(ctx,
			peerKey, row.ShareID, row.RelPath, string(row.Type), row.Size, row.MTimeNS, row.Mode,
			row.SHA256, vj, boolInt(row.Deleted), timeNS(row.UpdatedAt),
		); err != nil {
			return fmt.Errorf("index: upsert peer files: row %s/%s: %w", row.ShareID, row.RelPath, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("index: upsert peer files: commit: %w", err)
	}
	return nil
}

// ListPeerFiles returns everything known about one peer's view of a share,
// in relpath order, including tombstones.
func (s *Store) ListPeerFiles(ctx context.Context, peerKey, shareID string) ([]FileRow, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT relpath, type, size, mtime_ns, mode, sha256, version_json, deleted, updated_at
		FROM peer_files WHERE peer_key = ? AND share_id = ? ORDER BY relpath`, peerKey, shareID)
	if err != nil {
		return nil, fmt.Errorf("index: list peer files %s/%s: %w", peerKey, shareID, err)
	}
	defer rows.Close()
	return collectFileRows(rows, shareID)
}

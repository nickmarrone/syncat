package index

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nickmarrone/syncat/internal/protocol"
)

// ErrNotFound is returned by lookups (GetFile) when no row matches.
var ErrNotFound = errors.New("index: not found")

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
// or hand it to Phase 5's reconciler first — before it becomes durable.
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

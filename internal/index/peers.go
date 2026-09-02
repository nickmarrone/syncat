package index

import (
	"context"
	"fmt"
)

// UpsertPeerFiles replaces this peer's known state for the given rows, in
// one transaction. Each row's ShareID/RelPath identifies which file it
// describes; PeerKey is passed once for the whole batch since an
// IndexUpdate always arrives from a single peer connection.
//
// This mirrors ApplyScanResult's shape (bulk, transactional) but for the
// peer_files side of the schema. Phase 5's reconciler is the intended
// caller, on every IndexUpdate; this phase only exposes the storage
// primitive.
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

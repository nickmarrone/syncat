package index

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/nickmarrone/syncat/internal/protocol"
	"time"
)

type Cursor struct {
	Epoch         string
	AppliedSeq    uint64
	SnapshotID    string
	SnapshotBatch uint64
}
type JournalRow struct {
	Seq uint64
	Row FileRow
}

func (s *Store) ShareState(ctx context.Context, share string) (string, uint64, error) {
	_, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO share_state(share_id,epoch) VALUES(?,lower(hex(randomblob(16))))`, share)
	if err != nil {
		return "", 0, err
	}
	var e string
	var next uint64
	err = s.db.QueryRowContext(ctx, `SELECT epoch,next_seq FROM share_state WHERE share_id=?`, share).Scan(&e, &next)
	if next > 0 {
		next--
	}
	return e, next, err
}
func (s *Store) Cursor(ctx context.Context, peer, share, dir string) (Cursor, error) {
	var c Cursor
	err := s.db.QueryRowContext(ctx, `SELECT epoch,applied_seq,snapshot_id,snapshot_batch FROM peer_cursors WHERE peer_key=? AND share_id=? AND direction=?`, peer, share, dir).Scan(&c.Epoch, &c.AppliedSeq, &c.SnapshotID, &c.SnapshotBatch)
	if errors.Is(err, sql.ErrNoRows) {
		return Cursor{}, nil
	}
	return c, err
}
func (s *Store) SetCursor(ctx context.Context, peer, share, dir string, c Cursor) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO peer_cursors(peer_key,share_id,direction,epoch,applied_seq,snapshot_id,snapshot_batch) VALUES(?,?,?,?,?,?,?) ON CONFLICT(peer_key,share_id,direction) DO UPDATE SET epoch=excluded.epoch,applied_seq=excluded.applied_seq,snapshot_id=excluded.snapshot_id,snapshot_batch=excluded.snapshot_batch`, peer, share, dir, c.Epoch, c.AppliedSeq, c.SnapshotID, c.SnapshotBatch)
	return err
}
func (s *Store) JournalSince(ctx context.Context, share, epoch string, seq uint64, limit int) ([]JournalRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT seq,relpath,type,size,mtime_ns,mode,sha256,version_json,deleted,created_at FROM change_journal WHERE share_id=? AND epoch=? AND seq>? ORDER BY seq LIMIT ?`, share, epoch, seq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []JournalRow
	for rows.Next() {
		var j JournalRow
		var typ, vj string
		var del int
		var ns int64
		if err := rows.Scan(&j.Seq, &j.Row.RelPath, &typ, &j.Row.Size, &j.Row.MTimeNS, &j.Row.Mode, &j.Row.SHA256, &vj, &del, &ns); err != nil {
			return nil, err
		}
		j.Row.ShareID = share
		j.Row.Type = protocolFileType(typ)
		j.Row.Deleted = del != 0
		j.Row.UpdatedAt = time.Unix(0, ns)
		j.Row.Version, err = unmarshalVersion(vj)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}
func protocolFileType(s string) protocol.FileType { return protocol.FileType(s) }
func (s *Store) OldestJournalSeq(ctx context.Context, share, epoch string) (uint64, error) {
	var n sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT MIN(seq) FROM change_journal WHERE share_id=? AND epoch=?`, share, epoch).Scan(&n)
	if !n.Valid {
		return 0, err
	}
	return uint64(n.Int64), err
}
func (s *Store) PruneJournal(ctx context.Context, before time.Time) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM change_journal WHERE created_at<?`, before.UnixNano())
	return err
}

func (s *Store) StageSnapshotBatch(ctx context.Context, peer, share, id string, batch uint64, files []FileRow) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if batch == 0 {
		if _, err = tx.ExecContext(ctx, `DELETE FROM snapshot_staging WHERE peer_key=? AND share_id=? AND snapshot_id<>?`, peer, share, id); err != nil {
			return err
		}
	}
	for _, r := range files {
		vj, e := versionJSON(r.Version)
		if e != nil {
			return e
		}
		_, e = tx.ExecContext(ctx, `INSERT INTO snapshot_staging(peer_key,share_id,snapshot_id,batch,relpath,type,size,mtime_ns,mode,sha256,version_json,deleted) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(peer_key,share_id,snapshot_id,relpath) DO UPDATE SET batch=excluded.batch,type=excluded.type,size=excluded.size,mtime_ns=excluded.mtime_ns,mode=excluded.mode,sha256=excluded.sha256,version_json=excluded.version_json,deleted=excluded.deleted`, peer, share, id, batch, r.RelPath, string(r.Type), r.Size, r.MTimeNS, r.Mode, r.SHA256, vj, boolInt(r.Deleted))
		if e != nil {
			return e
		}
	}
	return tx.Commit()
}
func (s *Store) CommitSnapshot(ctx context.Context, peer, share, id, epoch string, seq uint64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `DELETE FROM peer_files WHERE peer_key=? AND share_id=?`, peer, share); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO peer_files(peer_key,share_id,relpath,type,size,mtime_ns,mode,sha256,version_json,deleted,updated_at) SELECT peer_key,share_id,relpath,type,size,mtime_ns,mode,sha256,version_json,deleted,? FROM snapshot_staging WHERE peer_key=? AND share_id=? AND snapshot_id=?`, time.Now().UnixNano(), peer, share, id)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO dirty_paths(peer_key,share_id,relpath,updated_at) SELECT ?,?,relpath,? FROM peer_files WHERE peer_key=? AND share_id=?`, peer, share, time.Now().UnixNano(), peer, share)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM snapshot_staging WHERE peer_key=? AND share_id=?`, peer, share)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO peer_cursors(peer_key,share_id,direction,epoch,applied_seq) VALUES(?,?,'incoming',?,?) ON CONFLICT(peer_key,share_id,direction) DO UPDATE SET epoch=excluded.epoch,applied_seq=excluded.applied_seq,snapshot_id='',snapshot_batch=0`, peer, share, epoch, seq)
	if err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) ApplyPeerDelta(ctx context.Context, peer, share, epoch string, from, to uint64, rows []FileRow) error {
	c, err := s.Cursor(ctx, peer, share, "incoming")
	if err != nil {
		return err
	}
	if c.Epoch != epoch || from != c.AppliedSeq+1 {
		return fmt.Errorf("index: delta gap: have %s/%d got %s/%d", c.Epoch, c.AppliedSeq, epoch, from)
	}
	if uint64(len(rows)) != to-from+1 {
		return fmt.Errorf("index: invalid delta range")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, r := range rows {
		vj, e := versionJSON(r.Version)
		if e != nil {
			return e
		}
		_, e = tx.ExecContext(ctx, putPeerFileSQL, peer, share, r.RelPath, string(r.Type), r.Size, r.MTimeNS, r.Mode, r.SHA256, vj, boolInt(r.Deleted), time.Now().UnixNano())
		if e != nil {
			return e
		}
		_, e = tx.ExecContext(ctx, `INSERT INTO dirty_paths(peer_key,share_id,relpath,updated_at) VALUES(?,?,?,?) ON CONFLICT(peer_key,share_id,relpath) DO UPDATE SET updated_at=excluded.updated_at`, peer, share, r.RelPath, time.Now().UnixNano())
		if e != nil {
			return e
		}
	}
	_, err = tx.ExecContext(ctx, `UPDATE peer_cursors SET applied_seq=? WHERE peer_key=? AND share_id=? AND direction='incoming'`, to, peer, share)
	if err != nil {
		return err
	}
	return tx.Commit()
}

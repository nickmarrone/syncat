package index

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// --- holds: changes stopped because they looked like a disaster ---------

// Hold records that a share, or one peer's link to it, is on hold: a change
// looked like a disaster (see GLOSSARY.md), so it was neither recorded nor
// exchanged. internal/core decides when to place, confirm, and clear one;
// this package only makes it survive a restart, because a hold that a
// restart quietly forgot would let the very change it stopped go through on
// the next scan.
type Hold struct {
	ShareID string

	// PeerKey is empty for a hold on this node's own scans of the share, and
	// otherwise the hex public key of the peer whose incoming change is held.
	PeerKey string

	Reason  string
	Deletes int // files the held change would delete
	Live    int // live files in the share when the hold was placed
	Since   time.Time

	// Confirmed means the user has said to let the held change through. The
	// next pass that would have been held applies instead, and clears it.
	Confirmed bool
}

// migrateV4 adds holds. One row per held (share, peer) link; peer_key is
// the empty string for a hold on this node's own scans.
func migrateV4(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `CREATE TABLE holds (
		share_id  TEXT    NOT NULL,
		peer_key  TEXT    NOT NULL,
		reason    TEXT    NOT NULL,
		deletes   INTEGER NOT NULL,
		live      INTEGER NOT NULL,
		since     INTEGER NOT NULL,
		confirmed INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY (share_id, peer_key))`); err != nil {
		return fmt.Errorf("create holds: %w", err)
	}
	return nil
}

// PutHold inserts or replaces the hold for (h.ShareID, h.PeerKey).
func (s *Store) PutHold(ctx context.Context, h Hold) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO holds (share_id, peer_key, reason, deletes, live, since, confirmed)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (share_id, peer_key) DO UPDATE SET
			reason = excluded.reason, deletes = excluded.deletes, live = excluded.live,
			since = excluded.since, confirmed = excluded.confirmed`,
		h.ShareID, h.PeerKey, h.Reason, h.Deletes, h.Live, timeNS(h.Since), boolInt(h.Confirmed))
	if err != nil {
		return fmt.Errorf("index: put hold %s/%s: %w", h.ShareID, h.PeerKey, err)
	}
	return nil
}

// DeleteHold removes the hold for (shareID, peerKey), if any.
func (s *Store) DeleteHold(ctx context.Context, shareID, peerKey string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM holds WHERE share_id = ? AND peer_key = ?`, shareID, peerKey); err != nil {
		return fmt.Errorf("index: delete hold %s/%s: %w", shareID, peerKey, err)
	}
	return nil
}

// ListHolds returns every hold, oldest first.
func (s *Store) ListHolds(ctx context.Context) ([]Hold, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT share_id, peer_key, reason, deletes, live, since, confirmed
		FROM holds ORDER BY since, share_id, peer_key`)
	if err != nil {
		return nil, fmt.Errorf("index: list holds: %w", err)
	}
	defer rows.Close()
	var out []Hold
	for rows.Next() {
		var h Hold
		var since int64
		var confirmed int
		if err := rows.Scan(&h.ShareID, &h.PeerKey, &h.Reason, &h.Deletes, &h.Live, &since, &confirmed); err != nil {
			return nil, fmt.Errorf("index: list holds: %w", err)
		}
		h.Since = time.Unix(0, since).UTC()
		h.Confirmed = confirmed != 0
		out = append(out, h)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("index: list holds: %w", err)
	}
	return out, nil
}

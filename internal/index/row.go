package index

import (
	"time"

	"github.com/nickmarrone/syncat/internal/protocol"
)

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
// row_test.go).
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

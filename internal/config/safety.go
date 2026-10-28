package config

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Validate checks that every set field of s is in range. Zero fields mean
// "use the default" and are always valid.
func (s Safety) Validate() error {
	var errs []error
	if s.HoldDeletePercent < 0 || s.HoldDeletePercent > 100 {
		errs = append(errs, fmt.Errorf("hold_delete_percent %d must be between 1 and 100", s.HoldDeletePercent))
	}
	if s.HoldDeleteMin < 0 {
		errs = append(errs, fmt.Errorf("hold_delete_min %d must be at least 1", s.HoldDeleteMin))
	}
	if s.TrashRetentionDays < 0 {
		errs = append(errs, fmt.Errorf("trash_retention_days %d must be at least 1", s.TrashRetentionDays))
	}
	if s.TrashMaxSize != "" {
		if _, err := ParseTrashMaxSize(s.TrashMaxSize); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// SafetyFor returns the Safety settings in force for shareID, a share this
// node offers or one it subscribes to, with every unset field filled in
// from its default (the trash retention from the node-wide
// TrashRetentionDays). An unknown shareID gets the defaults.
func (c *Config) SafetyFor(shareID string) Safety {
	var s Safety
	for _, sh := range c.Shares {
		if sh.ID == shareID {
			s = sh.Safety
		}
	}
	for _, sub := range c.Subscriptions {
		if sub.ShareID == shareID {
			s = sub.Safety
		}
	}
	if s.HoldDeletePercent == 0 {
		s.HoldDeletePercent = DefaultHoldDeletePercent
	}
	if s.HoldDeleteMin == 0 {
		s.HoldDeleteMin = DefaultHoldDeleteMin
	}
	if s.TrashRetentionDays == 0 {
		s.TrashRetentionDays = c.TrashRetentionDays
	}
	if s.TrashRetentionDays == 0 {
		s.TrashRetentionDays = DefaultTrashRetentionDays
	}
	if s.TrashMaxSize == "" {
		s.TrashMaxSize = DefaultTrashMaxSize
	}
	return s
}

// TrashMaxSize is a parsed Safety.TrashMaxSize: either a percentage of the
// share's live size or an absolute number of bytes.
type TrashMaxSize struct {
	Percent int   // set for "25%"
	Bytes   int64 // set for "2GiB"
}

// Limit returns the limit in bytes for a share whose live files total
// liveBytes.
func (m TrashMaxSize) Limit(liveBytes int64) int64 {
	if m.Percent > 0 {
		return liveBytes * int64(m.Percent) / 100
	}
	return m.Bytes
}

// sizeUnits are the suffixes ParseTrashMaxSize accepts, longest first so
// "GiB" is tried before "B".
var sizeUnits = []struct {
	suffix string
	mult   int64
}{
	{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10},
	{"TB", 1e12}, {"GB", 1e9}, {"MB", 1e6}, {"KB", 1e3},
	{"B", 1},
}

// ParseTrashMaxSize parses a trash size limit: "N%" (1 to 100) of the
// share's live size, or a whole number of bytes with an optional unit
// (B, KB, MB, GB, TB, KiB, MiB, GiB, TiB).
func ParseTrashMaxSize(s string) (TrashMaxSize, error) {
	bad := fmt.Errorf("trash_max_size %q must be a percentage like 25%% or a size like 2GiB", s)
	s = strings.TrimSpace(s)
	if p, ok := strings.CutSuffix(s, "%"); ok {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil || n < 1 || n > 100 {
			return TrashMaxSize{}, bad
		}
		return TrashMaxSize{Percent: n}, nil
	}
	mult := int64(1)
	for _, u := range sizeUnits {
		if num, ok := strings.CutSuffix(s, u.suffix); ok {
			s, mult = strings.TrimSpace(num), u.mult
			break
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 1 || n > (1<<62)/mult {
		return TrashMaxSize{}, bad
	}
	return TrashMaxSize{Bytes: n * mult}, nil
}

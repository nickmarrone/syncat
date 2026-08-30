package main

import "strings"

// splitLeadingPositional splits args into up to n leading tokens that
// don't look like a flag (i.e. don't start with "-"), followed by
// whatever remains. SPEC.md §8's CLI usage puts positional arguments
// before flags for several subcommands (`share add PATH --name N`,
// `subscribe PEER SHARE LOCALPATH --mode M`, ...), but the stdlib flag
// package's FlagSet.Parse only recognizes flags that appear before the
// first positional argument — it stops at the first non-flag token and
// treats everything after it (flags included) as positional. Callers
// pre-split with this helper, then hand only rest to fs.Parse.
//
// A path that itself starts with "-" defeats this (it would be
// misidentified as the start of the flag section); that's an accepted,
// documented limitation rather than something worth a full flag/arg
// interleaving parser for this CLI's needs.
func splitLeadingPositional(args []string, n int) (positional, rest []string) {
	for len(positional) < n && len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		positional = append(positional, args[0])
		args = args[1:]
	}
	return positional, args
}

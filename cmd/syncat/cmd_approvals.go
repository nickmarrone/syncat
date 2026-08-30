package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/nickmarrone/syncat/internal/config"
)

// cmdApprovals implements `syncat approvals [grant|deny ID]` (SPEC.md
// §8). The approval queue itself (SPEC.md §2.3/§6) is deferred past this
// build — see internal/core's package doc comment and internal/api's
// routes — so both the list and grant/deny paths simply surface the
// server's 501 "not implemented" response rather than duplicating that
// decision here.
func cmdApprovals(paths *config.Paths, args []string) error {
	c, err := newAPIClient(paths)
	if err != nil {
		return err
	}
	if len(args) == 0 {
		return c.do(context.Background(), http.MethodGet, "/api/approvals", nil, nil)
	}
	if len(args) != 2 || (args[0] != "grant" && args[0] != "deny") {
		return fmt.Errorf("usage: syncat approvals [grant|deny ID]")
	}
	decision, id := args[0], args[1]
	return c.do(context.Background(), http.MethodPost, "/api/approvals/"+url.PathEscape(id), map[string]string{"decision": decision}, nil)
}

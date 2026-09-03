// Package webui embeds the static single-page web UI assets via go:embed
// (SPEC.md §9).
package webui

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

// embedded holds internal/webui/static/ (SPEC.md §9's hand-written,
// build-step-free single-page UI: index.html, app.js, style.css,
// favicon.svg), still rooted at "static/". Use Assets instead.
//
//go:embed static
var embedded embed.FS

// Assets is the embedded tree rooted at static/ itself, so "index.html"
// (not "static/index.html") is a top-level entry — what Handler serves
// at "/".
var Assets fs.FS = mustSub(embedded, "static")

func mustSub(f embed.FS, dir string) fs.FS {
	sub, err := fs.Sub(f, dir)
	if err != nil {
		// Only reachable if the go:embed directive above and this dir
		// name disagree, which a build would already have failed on.
		panic(err)
	}
	return sub
}

// staticContentTypes pins the Content-Type this handler sends for each
// extension the UI ships, rather than trusting the host's mime.types
// file — mime.TypeByExtension merges in OS-provided mappings that vary
// by distro/container image, and a wrong or missing mapping for .js
// (served as application/octet-stream, say) silently breaks ES module
// loading in some browsers. http.ServeContent (which http.FileServer
// calls into) only does its own extension/sniff-based detection when the
// Content-Type header isn't already set, so setting it ourselves first
// is authoritative.
var staticContentTypes = map[string]string{
	".html": "text/html; charset=utf-8",
	".js":   "text/javascript; charset=utf-8",
	".css":  "text/css; charset=utf-8",
	".svg":  "image/svg+xml",
}

// Handler serves the embedded static UI at "/", defaulting "/" (and any
// directory path) to index.html, with fixed Content-Types for the
// extensions above.
//
// Path traversal: fs.FS implementations (embed.FS, and the fs.Sub view
// over it) reject any path containing a ".." element per the fs.ValidPath
// contract — Open returns fs.ErrInvalid rather than resolving outside the
// tree — and http.FileServer additionally path.Cleans the incoming
// request URL before mapping it to a filesystem path, collapsing
// "/../../etc/passwd" down to "/etc/passwd" (still inside the FS view,
// still just a 404) long before either check would matter. There is no
// way to reach a file outside Assets through this handler.
//
// Auth: intentionally unauthenticated (mounted outside the api package's
// auth() wrapper) — a browser has no X-Syncat-Token yet when it first
// loads the page; see GET /ui-token in internal/api/server.go for how
// the page bootstraps one afterward. The daemon's loopback-only listener
// (ListenLoopback) is what keeps this reachable only from the local
// machine.
func Handler() http.Handler {
	fileServer := http.FileServer(http.FS(Assets))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clean := path.Clean(r.URL.Path)
		ext := path.Ext(clean)
		if ext == "" && (clean == "/" || strings.HasSuffix(r.URL.Path, "/")) {
			// http.FileServer serves index.html for a directory request
			// (including "/" itself); match its Content-Type to the file
			// it actually sends rather than the (extension-less) request
			// path.
			ext = ".html"
		}
		if ct, ok := staticContentTypes[ext]; ok {
			w.Header().Set("Content-Type", ct)
		}
		fileServer.ServeHTTP(w, r)
	})
}

package webui

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// expectedFiles is the SPEC.md §9 static asset set: index.html, app.js,
// style.css, and a favicon.
var expectedFiles = []string{"index.html", "app.js", "style.css", "favicon.svg"}

func TestAssetsContainExpectedFiles(t *testing.T) {
	for _, name := range expectedFiles {
		data, err := fs.ReadFile(Assets, name)
		if err != nil {
			t.Errorf("Assets missing %q: %v", name, err)
			continue
		}
		if len(data) == 0 {
			t.Errorf("Assets %q is empty", name)
		}
	}
}

func TestHandlerServesIndexAtRoot(t *testing.T) {
	h := Handler()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GET / status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
	ct := w.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "text/html") {
		t.Errorf("GET / Content-Type = %q, want text/html prefix", ct)
	}
	if !strings.Contains(w.Body.String(), "<title>") {
		t.Errorf("GET / body doesn't look like index.html: %s", w.Body.String())
	}
}

func TestHandlerMimeTypes(t *testing.T) {
	// "/index.html" is deliberately not a case here: http.FileServer
	// canonicalizes a request for the index file itself into a 301
	// redirect to "/" (standard net/http behavior, nothing to do with
	// this package's own logic) — TestHandlerServesIndexAtRoot already
	// covers "/" serving index.html with the right Content-Type.
	cases := []struct {
		path   string
		prefix string
	}{
		{"/app.js", "text/javascript"},
		{"/style.css", "text/css"},
		{"/favicon.svg", "image/svg+xml"},
	}
	h := Handler()
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("GET %s status = %d, want 200", tc.path, w.Code)
			continue
		}
		ct := w.Header().Get("Content-Type")
		if !strings.HasPrefix(ct, tc.prefix) {
			t.Errorf("GET %s Content-Type = %q, want prefix %q", tc.path, ct, tc.prefix)
		}
	}
}

// TestHandlerRejectsPathTraversal exercises a handful of ways a request
// might try to escape the embedded static/ tree. None should ever
// succeed: fs.FS (embed.FS included) rejects any path containing ".."
// per the fs.ValidPath contract, and http.FileServer additionally
// path.Cleans the request URL before resolving it against the FS, so a
// traversal attempt collapses to an in-tree (or nonexistent) path long
// before either defense matters in practice. Every case here must come
// back 404, and — the actually security-relevant assertion — must never
// come back 200 with content from outside internal/webui/static/.
func TestHandlerRejectsPathTraversal(t *testing.T) {
	paths := []string{
		"/../embed.go",
		"/../../go.mod",
		"/%2e%2e/embed.go",
		"/static/../../embed.go",
		"/..%2f..%2fembed.go",
	}
	h := Handler()
	for _, p := range paths {
		req := httptest.NewRequest(http.MethodGet, p, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code == http.StatusOK {
			t.Errorf("GET %s = 200, want traversal to be blocked (body: %s)", p, w.Body.String())
		}
		if strings.Contains(w.Body.String(), "package webui") || strings.Contains(w.Body.String(), "module github.com") {
			t.Errorf("GET %s leaked source outside static/: %s", p, w.Body.String())
		}
	}
}

package uitest

// The browser side: one headless Chrome per test, with helpers that act on
// the page and wait on real DOM conditions.
//
// Two rules keep these tests from flaking against a UI that rebuilds its
// whole view every 2s:
//
//   - Every action finds its element and acts on it inside one JavaScript
//     evaluation. The page's event loop cannot run a poll re-render between
//     the lookup and the click, so a test never clicks a node that has just
//     been replaced. Evaluations run as a user gesture, so what needs one
//     (the clipboard) behaves as it does for a real click.
//
//   - Nothing waits by sleeping. Every wait polls a condition in the page
//     until it holds or a timeout passes, and the timeout message says which
//     condition it was. "A poll has happened" is itself a condition: the
//     helper script counts the UI's renders (see helperJS), so a test that
//     needs a state to survive a refresh waits for renders to tick over.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/emulation"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/runtime"
	"github.com/chromedp/chromedp"
)

// uiTimeout bounds a wait on something the UI does by itself: a render, a
// toast, the next 2s poll. It is generous because a loaded machine can
// stretch a poll, and a wait returns the moment its condition holds.
const uiTimeout = 15 * time.Second

// helperJS is injected into every document before the page's own scripts.
// window.__ut.renders counts the times the UI replaced #app's children,
// which it does on every render (app.js render()), so a test can wait for
// "two more polls have re-rendered the view" rather than sleeping 4s and
// hoping. The finders only return visible elements, so "the Remove button
// is reachable" means a user could actually see it.
const helperJS = `(() => {
  if (window.__ut) return;
  const ut = window.__ut = {
    renders: 0,
    visible(el) { return !!el && el.isConnected && el.getClientRects().length > 0; },
    all(sel, text, root) {
      return Array.from((root || document).querySelectorAll(sel))
        .filter((el) => ut.visible(el) && (text == null || el.textContent.includes(text)));
    },
    find(sel, text, root) { return ut.all(sel, text, root)[0] || null; },
    // The first visible sel (optionally containing text) inside the first
    // visible outer element containing outerText.
    within(outerSel, outerText, sel, text) {
      const outer = ut.all(outerSel, outerText);
      for (const o of outer) { const el = ut.find(sel, text, o); if (el) return el; }
      return null;
    },
    appText() { const a = document.getElementById('app'); return a ? a.innerText : ''; },
  };
  const watch = () => {
    const app = document.getElementById('app');
    if (app) new MutationObserver(() => { ut.renders++; }).observe(app, { childList: true });
  };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', watch);
  else watch();
})();`

type tab struct {
	t    *testing.T
	ctx  context.Context
	name string

	mu         sync.Mutex
	exceptions []string
	console    []string
}

// newBrowser starts a headless Chrome for this test and returns its first
// tab. On failure the cleanup saves a screenshot, the page's text and the
// browser console of every tab into the run's artifacts dir.
func newBrowser(t *testing.T) *tab {
	t.Helper()
	e := requireEnv(t)
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.ExecPath(e.chrome),
		chromedp.WindowSize(1280, 900),
	)
	allocCtx, cancelAlloc := chromedp.NewExecAllocator(context.Background(), opts...)
	t.Cleanup(cancelAlloc)
	ctx, cancel := chromedp.NewContext(allocCtx)
	t.Cleanup(cancel)
	return setupTab(t, ctx, "tab1")
}

// newTab opens a second tab in the same browser window, as a person would
// with Ctrl+T, rather than in a new window. Tests bring the tab they act on
// to the front first, as a person clicks on it: Chrome may treat a
// background tab as hidden, and the UI does not poll from a hidden tab.
func (tb *tab) newTab(name string) *tab {
	tb.t.Helper()
	ctx, cancel := chromedp.NewContext(tb.ctx, chromedp.WithNewWindow(false))
	tb.t.Cleanup(cancel)
	return setupTab(tb.t, ctx, name)
}

func setupTab(t *testing.T, ctx context.Context, name string) *tab {
	t.Helper()
	tb := &tab{t: t, ctx: ctx, name: name}
	err := chromedp.Listen(ctx, func(ev any) {
		switch ev := ev.(type) {
		case *runtime.EventExceptionThrown:
			d := ev.ExceptionDetails
			msg := d.Text
			if d.Exception != nil && d.Exception.Description != "" {
				msg += " " + d.Exception.Description
			}
			tb.mu.Lock()
			tb.exceptions = append(tb.exceptions, fmt.Sprintf("%s (%s:%d)", msg, d.URL, d.LineNumber+1))
			tb.mu.Unlock()
		case *runtime.EventConsoleAPICalled:
			var parts []string
			for _, a := range ev.Args {
				if a.Value != nil {
					parts = append(parts, string(a.Value))
				} else {
					parts = append(parts, a.Description)
				}
			}
			tb.mu.Lock()
			tb.console = append(tb.console, string(ev.Type)+": "+strings.Join(parts, " "))
			tb.mu.Unlock()
		}
	})
	if err != nil {
		t.Fatalf("start the browser (%s): %v", testEnv.chrome, err)
	}
	if _, err := chromedp.Call(ctx, page.AddScriptToEvaluateOnNewDocument, page.AddScriptToEvaluateOnNewDocumentParams{Source: helperJS}); err != nil {
		t.Fatalf("inject the helper script: %v", err)
	}
	// Registered after the context's cancel, so it runs before it (cleanups
	// are LIFO) while the tab can still take a screenshot.
	t.Cleanup(tb.finish)
	return tb
}

var unsafeChars = regexp.MustCompile(`[^A-Za-z0-9_.-]+`)

// finish runs at the end of the test. An uncaught exception in the page
// fails the test whatever else happened: the UI catching its own errors is
// part of what "degrades cleanly" means. On failure it keeps the evidence.
func (tb *tab) finish() {
	tb.mu.Lock()
	exceptions := append([]string(nil), tb.exceptions...)
	console := append([]string(nil), tb.console...)
	tb.mu.Unlock()
	for _, ex := range exceptions {
		tb.t.Errorf("%s: uncaught exception in the page: %s", tb.name, ex)
	}
	if !tb.t.Failed() {
		return
	}
	base := filepath.Join(testEnv.artifacts, unsafeChars.ReplaceAllString(tb.t.Name(), "_")+"-"+tb.name)
	ctx, cancel := context.WithTimeout(tb.ctx, 10*time.Second)
	defer cancel()
	if png, err := chromedp.Run(ctx, chromedp.FullScreenshot(100)); err == nil {
		if os.WriteFile(base+".png", png, 0o644) == nil {
			tb.t.Logf("%s: screenshot saved to %s.png", tb.name, base)
		}
	} else {
		tb.t.Logf("%s: no screenshot: %v", tb.name, err)
	}
	if txt, err := chromedp.Run(ctx, chromedp.Evaluate[string](`location.href + "\n\n" + document.body.innerText`)); err == nil {
		os.WriteFile(base+".txt", []byte(txt), 0o644)
	}
	if len(console) > 0 || len(exceptions) > 0 {
		var b strings.Builder
		for _, ex := range exceptions {
			b.WriteString("exception: " + ex + "\n")
		}
		for _, c := range console {
			b.WriteString(c + "\n")
		}
		os.WriteFile(base+".console.log", []byte(b.String()), 0o644)
	}
}

// --- page access -----------------------------------------------------------------

// eval evaluates expr as a user gesture and decodes its result.
func evalIn[T any](tb *tab, timeout time.Duration, expr string) (T, error) {
	ctx, cancel := context.WithTimeout(tb.ctx, timeout)
	defer cancel()
	return chromedp.Run(ctx, chromedp.Evaluate[T](expr, func(p *runtime.EvaluateParams) {
		p.UserGesture = ptr(true)
	}))
}

func ptr[T any](v T) *T { return &v }

// open navigates to url and waits for the UI's first real render (one built
// from /api/status, which sets #app's data-route).
func (tb *tab) open(url string) {
	tb.t.Helper()
	ctx, cancel := context.WithTimeout(tb.ctx, uiTimeout)
	defer cancel()
	if err := chromedp.Do(ctx, chromedp.Navigate(url)); err != nil {
		tb.t.Fatalf("%s: navigate to %s: %v", tb.name, url, err)
	}
	tb.waitRendered()
}

// reload is the browser's reload button.
func (tb *tab) reload() {
	tb.t.Helper()
	ctx, cancel := context.WithTimeout(tb.ctx, uiTimeout)
	defer cancel()
	if err := chromedp.Do(ctx, chromedp.Reload()); err != nil {
		tb.t.Fatalf("%s: reload: %v", tb.name, err)
	}
	tb.waitRendered()
}

func (tb *tab) waitRendered() {
	tb.t.Helper()
	tb.wait("the UI to render from the daemon's status", `!!document.getElementById('app').dataset.route && !!window.__ut`)
}

// route switches the UI's hash route, as the nav tabs do, and waits for the
// view to follow.
func (tb *tab) route(r string) {
	tb.t.Helper()
	tb.do("go to #"+r, `(() => { location.hash = '#`+r+`'; return true; })()`)
	tb.wait("the "+r+" view", `document.getElementById('app').dataset.route === '`+r+`'`)
}

// front brings the tab to the front, as clicking on it would.
func (tb *tab) front() {
	tb.t.Helper()
	if _, err := chromedp.Call(tb.ctx, page.BringToFront, cdp.Empty{}); err != nil {
		tb.t.Fatalf("%s: bring to front: %v", tb.name, err)
	}
}

// waitFor polls a boolean JavaScript expression until it is true. Errors
// while polling (a navigation tearing down the context, say) count as
// "not yet". On timeout it fails with desc and what the view showed.
func (tb *tab) waitFor(timeout time.Duration, desc, expr string) {
	tb.t.Helper()
	start := time.Now()
	var lastErr error
	for {
		ok, err := evalIn[bool](tb, 5*time.Second, expr)
		if err == nil && ok {
			tb.t.Logf("%s: ok: %s (%.1fs)", tb.name, desc, time.Since(start).Seconds())
			return
		}
		lastErr = err
		if time.Since(start) > timeout {
			view, _ := evalIn[string](tb, 5*time.Second, `document.body ? document.body.innerText : ''`)
			if lastErr == nil {
				lastErr = errors.New("condition stayed false")
			}
			tb.t.Fatalf("%s: timed out after %s waiting for: %s (%v)\n--- page text ---\n%s", tb.name, timeout, desc, lastErr, view)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (tb *tab) wait(desc, expr string) {
	tb.t.Helper()
	tb.waitFor(uiTimeout, desc, expr)
}

// do runs a JavaScript action that returns true once it has acted, retrying
// while it returns false (its element is not on the page yet).
func (tb *tab) do(desc, expr string) {
	tb.t.Helper()
	tb.waitFor(uiTimeout, desc, expr)
}

// click clicks the element elemExpr evaluates to, once it exists and is
// enabled. It focuses it first, as a real click does, which also moves focus
// out of any input the test just filled (render() holds off re-rendering
// while an input has focus).
func (tb *tab) click(desc, elemExpr string) {
	tb.t.Helper()
	tb.do("click "+desc, `(() => { const el = `+elemExpr+`; if (!el || el.disabled) return false; el.focus(); el.click(); return true; })()`)
}

// allowClipboard grants the page at origin clipboard access and makes the
// page behave as focused, which the async clipboard API requires.
func (tb *tab) allowClipboard(origin string) {
	tb.t.Helper()
	origin = strings.TrimSuffix(origin, "/")
	for _, name := range []string{"clipboard-read", "clipboard-write"} {
		_, err := chromedp.CallBrowser(tb.ctx, browser.SetPermission, browser.SetPermissionParams{
			Permission: &browser.PermissionDescriptor{Name: name, AllowWithoutSanitization: true},
			Setting:    browser.PermissionSettingGranted,
			Origin:     origin,
		})
		if err != nil {
			tb.t.Fatalf("%s: grant %s: %v", tb.name, name, err)
		}
	}
	if _, err := chromedp.Call(tb.ctx, emulation.SetFocusEmulationEnabled, emulation.SetFocusEmulationEnabledParams{Enabled: true}); err != nil {
		tb.t.Fatalf("%s: emulate focus: %v", tb.name, err)
	}
}

// clipboard reads the system clipboard through the page.
func (tb *tab) clipboard() string {
	tb.t.Helper()
	ctx, cancel := context.WithTimeout(tb.ctx, 5*time.Second)
	defer cancel()
	v, err := chromedp.Run(ctx, chromedp.Evaluate[string](`navigator.clipboard.readText()`, chromedp.EvalAwaitPromise, func(p *runtime.EvaluateParams) {
		p.UserGesture = ptr(true)
	}))
	if err != nil {
		tb.t.Fatalf("%s: read the clipboard: %v", tb.name, err)
	}
	return v
}

// viewport resizes the page's viewport, as a narrow window or phone would.
func (tb *tab) viewport(w, h int64) {
	tb.t.Helper()
	ctx, cancel := context.WithTimeout(tb.ctx, uiTimeout)
	defer cancel()
	if err := chromedp.Do(ctx, chromedp.EmulateViewport(w, h)); err != nil {
		tb.t.Fatalf("%s: set the viewport to %dx%d: %v", tb.name, w, h, err)
	}
}

// fill types value into the input elemExpr evaluates to, the way the
// browser reports typing to the page: focus, value, and an input event.
// The focus also holds off the UI's poll re-render (render() skips while an
// input in the view has focus), exactly as it would for a person typing.
func (tb *tab) fill(desc, elemExpr, value string) {
	tb.t.Helper()
	tb.do("fill "+desc, `(() => { const el = `+elemExpr+`; if (!el) return false; el.focus(); el.value = `+jsString(value)+`;
	  el.dispatchEvent(new Event('input', { bubbles: true })); el.dispatchEvent(new Event('change', { bubbles: true })); return true; })()`)
}

// value reads a JavaScript expression's value now, failing the test if the
// page cannot be evaluated at all.
func value[T any](tb *tab, expr string) T {
	tb.t.Helper()
	v, err := evalIn[T](tb, 5*time.Second, expr)
	if err != nil {
		tb.t.Fatalf("%s: evaluate %s: %v", tb.name, expr, err)
	}
	return v
}

// renders is how many times the UI has re-rendered since the page loaded.
func (tb *tab) renders() int { return value[int](tb, `window.__ut.renders`) }

// waitPolls waits until the UI has re-rendered n more times. Called right
// after an action, every render it counts comes from a poll, so it is the
// way to say "this state has survived n refreshes".
func (tb *tab) waitPolls(n int) {
	tb.t.Helper()
	from := tb.renders()
	tb.waitFor(time.Duration(n)*2*time.Second+uiTimeout, strconv.Itoa(n)+" poll re-renders", fmt.Sprintf(`window.__ut.renders >= %d`, from+n))
}

// toast waits for a toast containing text.
func (tb *tab) toast(text string) {
	tb.t.Helper()
	tb.wait("a toast saying "+strconv.Quote(text), `!!__ut.find('#toasts .toast', `+jsString(text)+`)`)
}

// jsString quotes s as a JavaScript string literal (JSON is a subset).
func jsString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

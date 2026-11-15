package crawler

import (
	"fmt"
	"net/url"
	"sync"

	"github.com/shurco/goClone/pkg/netutil"
)

// linkRewriter maps same-site resource URLs to their deterministic local mirror paths.
// The mapping is a pure function of the URL (asset folder by extension, flat filename
// via ReplaceSlashWithDash), so a URL always rewrites to the same path; a per-instance
// map additionally verifies that one page never maps the same URL to two different
// local paths. Each parsed HTML page gets its own instance; the package-level helpers
// keep a guarded default so the existing function signatures stay unchanged.
type linkRewriter struct {
	mu       sync.Mutex
	urlToLoc map[string]string
}

func newLinkRewriter() *linkRewriter {
	return &linkRewriter{urlToLoc: make(map[string]string)}
}

// localPath returns the mirror-absolute local path ("/assets/...") for a same-site
// asset URL. It returns "" when the URL is malformed or points at another host.
func (rw *linkRewriter) localPath(absLink string) string {
	parsedURL, err := url.Parse(absLink)
	if err != nil {
		return ""
	}
	if parsedURL.Host != projectURL.Host && parsedURL.Host != "" {
		return ""
	}
	return "/" + netutil.Folders[kindForURL(absLink)] + "/" + netutil.ReplaceSlashWithDash(parsedURL.Path)
}

// rewrite records the url -> local path mapping and returns it. If the URL was already
// mapped on this page, the previously recorded path is returned and the two must agree;
// a mismatch is reported as an error so URL rewriting and dedup never disagree.
func (rw *linkRewriter) rewrite(absLink string) (string, error) {
	local := rw.localPath(absLink)
	if local == "" {
		return "", fmt.Errorf("rewrite: %q is not a same-site asset", absLink)
	}

	rw.mu.Lock()
	defer rw.mu.Unlock()
	if prev, ok := rw.urlToLoc[absLink]; ok && prev != local {
		return "", fmt.Errorf("rewrite: asset %q mapped to two local paths: %q vs %q", absLink, prev, local)
	}
	rw.urlToLoc[absLink] = local
	return local, nil
}

// kindForURL resolves the asset kind ("css", "js", "img", "font") from the URL's file
// extension, defaulting to "js" to match the historical preload fallback behavior.
func kindForURL(absLink string) string {
	switch netutil.GetAssetDir(urlPathOnly(absLink)) {
	case netutil.Folders["css"]:
		return "css"
	case netutil.Folders["font"]:
		return "font"
	case netutil.Folders["img"]:
		return "img"
	default:
		return "js"
	}
}

func urlPathOnly(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	return u.Path
}

// defaultRewriter backs the package-level saveAsset/readCSS/processSrcset helpers used
// by the existing tests; production page parsing constructs per-page instances.
var (
	defaultRewriterMu sync.Mutex
	defaultRewriter   = newLinkRewriter()
)

// currentRewriter returns the package-default rewriter under its guard so test resets
// cannot race with production reads.
func currentRewriter() *linkRewriter {
	defaultRewriterMu.Lock()
	defer defaultRewriterMu.Unlock()
	return defaultRewriter
}

func resetDefaultRewriter() {
	defaultRewriterMu.Lock()
	defer defaultRewriterMu.Unlock()
	defaultRewriter = newLinkRewriter()
}

package crawler

import (
	"fmt"
	"net/url"
	"os"
	"strings"
)

// saveAsset records a newly discovered asset of the given kind ("js", "img", "font"),
// schedules its download through the manifest dedup gate, and rewrites original in body
// to the local mirror path. If the link belongs to a different host it is left unchanged.
// It uses the package-default rewriter; page parsing should call saveAssetWith with a
// per-page linkRewriter instead.
func saveAsset(kind, absLink, original, body string) string {
	return saveAssetWith(defaultRewriter, kind, absLink, original, body)
}

// saveAssetWith is the per-page variant: it enforces that one page never maps a URL to
// two different local paths, skips assets already successful on disk (their references
// are still rewritten so the page links at the existing local file), and only schedules
// a download when the manifest claim is granted.
func saveAssetWith(rw *linkRewriter, kind, absLink, original, body string) string {
	parsedURL, err := url.Parse(absLink)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error parsing URL:", err)
		return body
	}
	if parsedURL.Host != projectURL.Host && parsedURL.Host != "" {
		return body
	}

	if addAsset(kind, absLink) {
		if scheduleAsset(kind, absLink) {
			fmt.Fprintf(os.Stderr, "%s found --> %s\n", kind, absLink)
		} else {
			fmt.Fprintf(os.Stderr, "%s skipped (already complete) --> %s\n", kind, absLink)
		}
	}

	newLink, err := rw.rewrite(absLink)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return body
	}
	return strings.ReplaceAll(body, original, newLink)
}

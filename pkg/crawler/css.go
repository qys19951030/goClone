package crawler

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/geziyor/geziyor"
	"github.com/geziyor/geziyor/client"
	"github.com/shurco/goClone/pkg/fsutil"
	"github.com/shurco/goClone/pkg/manifest"
	"github.com/shurco/goClone/pkg/netutil"
)

// cssURLRegexp matches url(...) references inside CSS content.
var cssURLRegexp = regexp.MustCompile(`url\((.*?)\)`)

func parseCSS(_ *geziyor.Geziyor, r *client.Response) {
	body := string(r.Body)
	cssURL := r.Request.URL.String()
	// Use ReplaceSlashWithDash so the filename matches what quotesParse rewrites in HTML.
	name := netutil.ReplaceSlashWithDash(r.Request.URL.Path)
	cssDir := netutil.Folders["css"]
	relPath := filepath.ToSlash(filepath.Join(cssDir, name))

	index, err := fsutil.OpenFile(filepath.Join(projectPath, cssDir, name), fsutil.FsCWTFlags, 0o666)
	if err != nil {
		log.Printf("parseCSS: open %s: %v", name, err)
		recordCSS(cssURL, relPath, manifest.StatusFailed)
		return
	}
	if _, err := fsutil.WriteOSFile(index, readCSSWith(defaultRewriter, body, body, r.Request.URL)); err != nil {
		log.Printf("parseCSS: write %s: %v", name, err)
		recordCSS(cssURL, relPath, manifest.StatusFailed)
		return
	}
	recordCSS(cssURL, relPath, manifest.StatusSuccess)
}

func recordCSS(link, relPath, status string) {
	if man == nil {
		return
	}
	if err := man.Put(manifest.Record{
		URL:    link,
		Type:   manifest.TypeCSS,
		Path:   relPath,
		Status: status,
	}); err != nil {
		log.Printf("manifest record lost for %s: %v", link, err)
		return
	}
	if status == manifest.StatusFailed {
		runStats.assetFailed("css")
	} else {
		runStats.assetNew("css")
	}
}

func readCSS(data, body string, base *url.URL) string {
	return readCSSWith(defaultRewriter, data, body, base)
}

// readCSSWith rewrites same-site url(...) references, schedules font/img downloads via
// the manifest gate, and checks every rewrite through the page's linkRewriter so the
// same URL can never resolve to two local paths.
func readCSSWith(rw *linkRewriter, data, body string, base *url.URL) string {
	for rawLine := range strings.SplitSeq(data, "\n") {
		line := strings.TrimSpace(rawLine)
		for _, match := range cssURLRegexp.FindAllStringSubmatch(line, -1) {
			original := match[1]
			clean := strings.ReplaceAll(strings.ReplaceAll(original, `'`, ""), `"`, "")
			parsedURL, err := url.Parse(clean)
			if err != nil {
				fmt.Fprintln(os.Stderr, "Error parsing URL:", err)
				continue
			}

			resolved := base.ResolveReference(parsedURL)
			if resolved.Scheme == "data" || resolved.Scheme == "blob" {
				continue
			}
			if resolved.Host != projectURL.Host && resolved.Host != "" {
				continue
			}

			folder := netutil.GetAssetDir(resolved.Path)
			if folder == "" {
				continue
			}
			link := resolved.String()
			kind := kindForURL(link)

			switch folder {
			case netutil.Folders["font"], netutil.Folders["img"]:
				if addAsset(kind, link) {
					if scheduleAsset(kind, link) {
						fmt.Fprintf(os.Stderr, "%s found --> %s\n", kind, link)
					} else {
						fmt.Fprintf(os.Stderr, "%s skipped (already complete) --> %s\n", kind, link)
					}
				}
			}

			newLink, err := rw.rewrite(link)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				continue
			}
			body = strings.ReplaceAll(body, original, newLink)
		}
	}
	return body
}

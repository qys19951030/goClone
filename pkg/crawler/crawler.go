// Package crawler mirrors websites for offline use: it crawls HTML pages, downloads
// linked CSS, JavaScript, images, and fonts, rewrites URLs to local paths, and writes
// a directory tree suitable for opening in a browser.
package crawler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/geziyor/geziyor"
	"github.com/geziyor/geziyor/client"

	"github.com/shurco/goClone/pkg/fsutil"
	"github.com/shurco/goClone/pkg/manifest"
	"github.com/shurco/goClone/pkg/netutil"
)

// ManifestFileName is the JSON Lines manifest stored inside every mirror directory.
const ManifestFileName = "manifest.jsonl"

// CloneSite downloads the site at args[0] (a valid absolute http or https URL) into
// ./<host>/ relative to the process working directory. flag configures proxy, User-Agent,
// cookies, robots.txt handling, optional headless Chrome rendering, asset root, download
// limits, logging, and whether to open the mirror or serve it over HTTP.
//
// Before any request is made, the mirror's manifest is opened and read back: pages and
// assets already recorded as successful are skipped, while failed and unknown items are
// processed. A manifest that cannot be opened or written fails the run up front — the
// crawler never silently degrades to a manifest-less run.
//
// The crawl itself does not honour ctx cancellation (Geziyor limitation); ctx is used
// for asset downloads and, when Serve is true, for graceful HTTP server shutdown.
func CloneSite(ctx context.Context, args []string, flag Flags) error {
	return cloneSite(ctx, args, flag, os.Stdout)
}

func cloneSite(ctx context.Context, args []string, flag Flags, summary *os.File) error {
	if len(args) < 1 {
		return fmt.Errorf("url is required")
	}

	if strings.TrimSpace(flag.AssetsRoot) != "" {
		netutil.SetAssetRoot(flag.AssetsRoot)
	}
	netutil.SetDefaultUserAgent(flag.UserAgent)
	if flag.HTTPTimeoutSeconds > 0 {
		netutil.SetHTTPTimeout(time.Duration(flag.HTTPTimeoutSeconds) * time.Second)
	}
	if flag.MaxDownloadMB > 0 {
		netutil.SetMaxDownloadBytes(int64(flag.MaxDownloadMB) * 1024 * 1024)
	}
	if flag.MaxConcurrentWorkers > 0 {
		SetDownloadConcurrency(flag.MaxConcurrentWorkers)
	}

	u, err := url.ParseRequestURI(args[0])
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("%q is not a valid URL", args[0])
	}
	crawlCtx = ctx
	domain = fmt.Sprintf("%s://%s", u.Scheme, u.Host)
	projectURL = u
	projectPath = filepath.Join(fsutil.Workdir(), projectURL.Host)
	files = filesBase{}
	runStats = newStats()
	resetDefaultRewriter()

	// Fail before crawling when the mirror directory or manifest are unusable.
	if fi, statErr := os.Stat(projectPath); statErr == nil && !fi.IsDir() {
		return fmt.Errorf("mirror path %s is not a directory", projectPath)
	}
	manifestPath := filepath.Join(projectPath, ManifestFileName)
	man, err = manifest.Open(ctx, manifestPath)
	if err != nil {
		return err
	}
	defer func() { man = nil }()
	// A fatal manifest write cancels this derived context, stopping in-flight downloads
	// instead of letting them keep producing records that can no longer be persisted.
	crawlCtx = man.Context()

	// Seed resume-run accounting from disk: every success record counts as a skip up
	// front; items processed again below are added on top, so the two never overlap.
	resumePages := 0
	for _, r := range man.Snapshot() {
		if r.Status != manifest.StatusSuccess {
			continue
		}
		switch r.Type {
		case manifest.TypePage:
			resumePages++
		case manifest.TypeCSS, manifest.TypeJS, manifest.TypeImg, manifest.TypeFont:
			runStats.assetSkippedURL(r.Type, r.URL)
		}
	}
	for range resumePages {
		runStats.pageSkipped()
	}

	geziyorOptions := &geziyor.Options{
		AllowedDomains:    []string{projectURL.Host},
		StartURLs:         []string{u.String()},
		ParseFunc:         quotesParse,
		UserAgent:         flag.UserAgent,
		CookiesDisabled:   flag.Cookies,
		RobotsTxtDisabled: flag.Robots,
		LogDisabled:       !flag.Verbose,
	}
	if flag.ProxyString != "" {
		geziyorOptions.ProxyFunc = client.RoundRobinProxy(flag.ProxyString)
	}
	if flag.BrowserEndpoint != "" {
		geziyorOptions.BrowserEndpoint = flag.BrowserEndpoint
	}
	// Enumerate every page known to the manifest, not just the user's start URL:
	// successful ones are counted as skips at the gate, failed/unknown pages re-enter
	// the crawl so resume works even when the start URL was already complete.
	geziyorOptions.StartRequestsFunc = func(g *geziyor.Geziyor) {
		for _, pageURL := range startPageURLs(u.String()) {
			getPageIfNeeded(g, pageURL, g.Opt.ParseFunc)
		}
		retryFailedAssets(g)
	}

	geziyor.NewGeziyor(geziyorOptions).Start()

	// Block until every background asset download goroutine has finished.
	downloadWg.Wait()

	// Auto-hint: if browser rendering was enabled but nothing was captured.
	if flag.BrowserEndpoint != "" {
		filesMu.Lock()
		empty := len(files.pages) == 0 && len(files.css) == 0 && len(files.js) == 0 && len(files.img) == 0 && len(files.font) == 0
		filesMu.Unlock()
		if empty {
			fmt.Fprintln(os.Stderr, "Hint: JS rendering returned no content. If you run Chrome inside Docker on Linux and see ERR_CONNECTION_REFUSED or ERR_NAME_NOT_RESOLVED:")
			fmt.Fprintln(os.Stderr, " - Run Chrome with host networking: docker run --net=host --rm -d --name headless chromedp/headless-shell:stable")
			fmt.Fprintln(os.Stderr, " - Or serve a URL reachable from the container (use host IP or a shared Docker network)")
			fmt.Fprintln(os.Stderr, " - Ensure you pass the full DevTools WS URL from /json/version when needed")
		}
	}

	printSummary(summary)

	// A manifest write failure means records were lost: the run must end as failed.
	// Close is idempotent, so the deferred reset above stays safe.
	if manifestErr := man.Err(); manifestErr != nil {
		if closeErr := man.Close(); closeErr != nil && !errors.Is(closeErr, manifestErr) {
			return fmt.Errorf("manifest failure: %w (close: %v)", manifestErr, closeErr)
		}
		return fmt.Errorf("manifest failure: %w", manifestErr)
	}
	if closeErr := man.Close(); closeErr != nil {
		return fmt.Errorf("manifest close: %w", closeErr)
	}

	if flag.Open {
		openURL := projectPath + "/index.html"
		if flag.Serve {
			openURL = fmt.Sprintf("http://localhost:%d/index.html", flag.ServePort)
		}
		cmd := open(openURL)
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("%v: %w", cmd.Args, err)
		}
	}

	if flag.Serve {
		srv := &http.Server{
			Addr:    fmt.Sprintf(":%d", flag.ServePort),
			Handler: http.FileServer(http.Dir(projectPath)),
		}
		go func() {
			<-ctx.Done()
			_ = srv.Shutdown(context.Background())
		}()
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve: %w", err)
		}
	}

	return nil
}

// getPageIfNeeded enqueues a page only when the manifest does not already hold a
// success record for it, so resumed runs never re-request completed pages.
func getPageIfNeeded(g *geziyor.Geziyor, pageURL string, callback func(*geziyor.Geziyor, *client.Response)) {
	if man != nil && man.IsSuccess(pageURL) {
		fmt.Fprintf(os.Stderr, "page skipped (already complete) --> %s\n", pageURL)
		return
	}
	g.Get(pageURL, callback)
}

// startPageURLs returns the pages to feed into the crawl: every page record already
// known to the manifest (so resume re-attempts previously failed pages), with the
// user-supplied start URL guaranteed first even when no manifest exists yet.
func startPageURLs(startURL string) []string {
	seen := map[string]bool{startURL: true}
	pages := []string{startURL}
	if man != nil {
		for _, r := range man.Snapshot() {
			if r.Type != manifest.TypePage || seen[r.URL] {
				continue
			}
			seen[r.URL] = true
			pages = append(pages, r.URL)
		}
	}
	return pages
}

// retryFailedAssets re-queues every asset whose latest manifest record is a failure:
// stylesheets re-enter the geziyor parse pipeline, the other kinds go straight back
// through the download gate. Without this, skipped pages would never re-discover them.
func retryFailedAssets(g *geziyor.Geziyor) {
	if man == nil {
		return
	}
	for _, r := range man.Snapshot() {
		if r.Status != manifest.StatusFailed {
			continue
		}
		switch r.Type {
		case manifest.TypeCSS:
			scheduleCSS(g, r.URL)
		case manifest.TypeJS, manifest.TypeImg, manifest.TypeFont:
			scheduleAsset(r.Type, r.URL)
		}
	}
}

// printSummary writes the run summary to out (stdout in production) and nowhere near
// the progress chatter, which lives on stderr.
func printSummary(out *os.File) {
	pagesNew, pagesSkipped, _, newN, skippedN, failedN, failedByType := runStats.snapshot()

	fmt.Fprintln(out, "Crawl summary:")
	fmt.Fprintf(out, "  pages: new=%d skipped=%d\n", pagesNew, pagesSkipped)
	fmt.Fprintf(out, "  assets: new=%d skipped=%d failed=%d\n", newN, skippedN, failedN)
	fmt.Fprintf(out, "  manifest entries (cumulative): %d\n", man.Count())
	if len(failedByType) == 0 {
		fmt.Fprintln(out, "  failures by type: none")
		return
	}
	fmt.Fprintln(out, "  failures by type:")
	for _, kind := range []string{manifest.TypePage, manifest.TypeCSS, manifest.TypeJS, manifest.TypeImg, manifest.TypeFont} {
		if n := failedByType[kind]; n > 0 {
			fmt.Fprintf(out, "    %s: %d\n", kind, n)
		}
	}
}

// processSrcset rewrites all URLs inside a srcset-like attribute value and triggers
// downloads through the manifest gate.
func processSrcset(rw *linkRewriter, attr string, body string, join func(string) string) string {
	if strings.TrimSpace(attr) == "" {
		return body
	}
	for part := range strings.SplitSeq(attr, ",") {
		p := strings.TrimSpace(part)
		if p == "" {
			continue
		}
		origURL, _, _ := strings.Cut(p, " ")
		abs := join(origURL)
		body = saveAssetWith(rw, "img", abs, origURL, body)
	}
	return body
}

func quotesParse(g *geziyor.Geziyor, r *client.Response) {
	body := string(r.Body)
	pageURL := r.Request.URL.String()
	urlPath := r.Response.Request.URL.Path
	fmt.Fprintf(os.Stderr, "page: %s://%s%s\n", projectURL.Scheme, projectURL.Host, urlPath)

	rw := newLinkRewriter()

	// CSS files
	r.HTMLDoc.Find("link[rel='stylesheet']").Each(func(i int, s *goquery.Selection) {
		data, exists := s.Attr("href")
		if !exists {
			return
		}
		abs := r.JoinURL(data)
		parsedURL, err := url.Parse(abs)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error parsing URL:", err)
			return
		}
		if parsedURL.Host == projectURL.Host || parsedURL.Host == "" {
			scheduleCSS(g, abs)
			newLink, err := rw.rewrite(abs)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return
			}
			body = strings.ReplaceAll(body, data, newLink)
		}
	})

	// JS files
	r.HTMLDoc.Find("script[src],script[data-rocket-src]").Each(func(i int, s *goquery.Selection) {
		if data, exists := s.Attr("src"); exists {
			body = saveAssetWith(rw, "js", r.JoinURL(data), data, body)
		}
		if data1, exists1 := s.Attr("data-rocket-src"); exists1 {
			body = saveAssetWith(rw, "js", r.JoinURL(data1), data1, body)
		}
	})

	// Preload assets (fonts, images, scripts, styles)
	r.HTMLDoc.Find("link[rel='preload']").Each(func(i int, s *goquery.Selection) {
		data, ok := s.Attr("href")
		if !ok {
			return
		}
		as, _ := s.Attr("as")
		abs := r.JoinURL(data)
		switch as {
		case "font":
			body = saveAssetWith(rw, "font", abs, data, body)
		case "image":
			body = saveAssetWith(rw, "img", abs, data, body)
		case "script":
			body = saveAssetWith(rw, "js", abs, data, body)
		case "style":
			parsedURL, err := url.Parse(abs)
			if err == nil && (parsedURL.Host == projectURL.Host || parsedURL.Host == "") {
				scheduleCSS(g, abs)
				newLink, err := rw.rewrite(abs)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					return
				}
				body = strings.ReplaceAll(body, data, newLink)
			}
		default:
			body = saveAssetWith(rw, "js", abs, data, body)
		}
	})

	// Images and lazy variants
	r.HTMLDoc.Find("img[src],img[data-lazy-src],img[data-lazy-srcset],img[srcset]").Each(func(i int, s *goquery.Selection) {
		if v, ok := s.Attr("data-lazy-srcset"); ok {
			body = processSrcset(rw, v, body, r.JoinURL)
		}
		if v, ok := s.Attr("data-lazy-src"); ok {
			body = saveAssetWith(rw, "img", r.JoinURL(v), v, body)
		}
		if v, ok := s.Attr("srcset"); ok {
			body = processSrcset(rw, v, body, r.JoinURL)
		}
		if v, ok := s.Attr("src"); ok {
			parsed, err := url.Parse(v)
			if err != nil {
				fmt.Fprintln(os.Stderr, "Error parsing URL:", err)
				return
			}
			if parsed.Scheme == "data" || parsed.Scheme == "blob" {
				return
			}
			body = saveAssetWith(rw, "img", r.JoinURL(v), v, body)
		}
	})

	// picture <source srcset>
	r.HTMLDoc.Find("picture source[srcset]").Each(func(i int, s *goquery.Selection) {
		if v, ok := s.Attr("srcset"); ok {
			body = processSrcset(rw, v, body, r.JoinURL)
		}
	})

	// <noscript> fallback content. Go's HTML parser keeps the contents of a
	// <noscript> element as raw text (scripting is treated as enabled), so any
	// <img>/<source> assets inside are invisible to the selectors above. Many
	// sites (especially JS-rendered ones) place the real <img src> fallbacks
	// there, so re-parse the inner HTML and extract those assets too.
	r.HTMLDoc.Find("noscript").Each(func(i int, s *goquery.Selection) {
		inner, err := goquery.NewDocumentFromReader(strings.NewReader(s.Text()))
		if err != nil {
			return
		}
		inner.Find("img[src],img[srcset],source[srcset]").Each(func(j int, im *goquery.Selection) {
			if v, ok := im.Attr("srcset"); ok {
				body = processSrcset(rw, v, body, r.JoinURL)
			}
			if v, ok := im.Attr("src"); ok {
				parsed, err := url.Parse(v)
				if err != nil {
					fmt.Fprintln(os.Stderr, "Error parsing URL:", err)
					return
				}
				if parsed.Scheme == "data" || parsed.Scheme == "blob" {
					return
				}
				body = saveAssetWith(rw, "img", r.JoinURL(v), v, body)
			}
		})
	})

	// Inline CSS blocks
	r.HTMLDoc.Find("style").Each(func(i int, s *goquery.Selection) {
		data := s.Text()
		body = readCSSWith(rw, data, body, r.Response.Request.URL)
	})

	// Links to other pages — schedule immediately when the manifest allows the claim,
	// so resumed runs do not re-request pages already completed on disk.
	r.HTMLDoc.Find("a").Each(func(i int, s *goquery.Selection) {
		data, exists := s.Attr("href")
		if !exists {
			return
		}
		parsedURL, err := url.Parse(data)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error parsing URL:", err)
			return
		}
		if (parsedURL.Host == projectURL.Host || parsedURL.Host == "") && parsedURL.Path != "/" {
			nextURL := r.JoinURL(data)
			if addAsset("pages", nextURL) {
				getPageIfNeeded(g, nextURL, quotesParse)
			}
		}
	})

	if urlPath == "" {
		addAsset("pages", pageURL)
	}

	// Write page to disk
	var filePath string
	cleanURLPath := strings.TrimPrefix(urlPath, "/")
	if cleanURLPath == "" {
		filePath = filepath.Join(projectPath, "index.html")
	} else {
		filePath = filepath.Join(projectPath, cleanURLPath, "index.html")
	}
	relPath := pageRelPath(filePath)

	index, err := fsutil.OpenFile(filePath, fsutil.FsCWTFlags, 0o666)
	if err != nil {
		log.Printf("quotesParse: open %s: %v", filePath, err)
		recordPage(pageURL, relPath, manifest.StatusFailed)
		return
	}
	body = strings.ReplaceAll(body, domain, "")
	if _, err := fsutil.WriteOSFile(index, body); err != nil {
		log.Printf("quotesParse: write %s: %v", filePath, err)
		recordPage(pageURL, relPath, manifest.StatusFailed)
		return
	}
	recordPage(pageURL, relPath, manifest.StatusSuccess)
}

// scheduleCSS fetches a stylesheet through the manifest gate: a CSS already successful
// on disk is neither re-requested nor re-counted as new.
func scheduleCSS(g *geziyor.Geziyor, abs string) {
	if !addAsset("css", abs) {
		return
	}
	if man != nil {
		if !man.Claim(abs) {
			if man.IsSuccess(abs) {
				runStats.assetSkippedURL("css", abs)
				fmt.Fprintf(os.Stderr, "css skipped (already complete) --> %s\n", abs)
			}
			return
		}
	}
	fmt.Fprintf(os.Stderr, "css found --> %s\n", abs)
	g.Get(abs, parseCSS)
}

// recordPage stores the page outcome in the manifest and counts it once. Pages already
// read back as successful from disk are never parsed again, so every success record
// written here is a new or retried page of this run.
func recordPage(pageURL, relPath, status string) {
	if man == nil {
		return
	}
	if err := man.Put(manifest.Record{
		URL:    pageURL,
		Type:   manifest.TypePage,
		Path:   relPath,
		Status: status,
	}); err != nil {
		log.Printf("manifest record lost for %s: %v", pageURL, err)
	}
	if status == manifest.StatusSuccess {
		runStats.pageNew()
	} else {
		runStats.pageFailed()
	}
}

// pageRelPath returns the mirror-relative path of an absolute page file path.
func pageRelPath(abs string) string {
	rel, err := filepath.Rel(projectPath, abs)
	if err != nil {
		return abs
	}
	return filepath.ToSlash(rel)
}

// open opens the specified URL in the default browser of the user.
func open(url string) *exec.Cmd {
	var cmd string
	var args []string

	switch runtime.GOOS {
	case "windows":
		cmd = "cmd"
		args = []string{"/c", "start"}
	case "darwin":
		cmd = "open"
	default: // "linux", "freebsd", "openbsd", "netbsd"
		cmd = "xdg-open"
	}
	args = append(args, url)
	return exec.Command(cmd, args...)
}

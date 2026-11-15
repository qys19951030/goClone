package crawler

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/shurco/goClone/pkg/manifest"
)

// testSite builds a small local mirror target on a stable listener: callers may Stop
// and Restart it (e.g. to flip the flaky-image behavior) while keeping the same URL,
// which manifest resume tests require because records store absolute URLs.
func testSite(t *testing.T, flakyImageOK *atomic.Bool) *testServer {
	t.Helper()
	hits := map[string]*atomic.Int32{
		"/":               {},
		"/about":          {},
		"/img/a.png":      {},
		"/js/app.js":      {},
		"/css/site.css":   {},
		"/font/f.woff2":   {},
		"/img/broken.png": {},
	}
	mux := http.NewServeMux()

	indexPage := func() string {
		return `<html><head>
<link rel="stylesheet" href="/css/site.css">
<script src="/js/app.js"></script>
<link rel="preload" as="font" href="/font/f.woff2">
</head><body>
<img src="/img/a.png">
<img src="/img/broken.png">
<a href="/about">about</a>
<style>body{background:url('/img/a.png')}</style>
</body></html>`
	}
	aboutPage := `<html><body><a href="/">home</a><img src="/img/a.png"></body></html>`

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if c, ok := hits[r.URL.Path]; ok {
			c.Add(1)
		}
		switch r.URL.Path {
		case "/":
			_, _ = w.Write([]byte(indexPage()))
		case "/about":
			_, _ = w.Write([]byte(aboutPage))
		case "/img/a.png":
			_, _ = w.Write([]byte("PNG-DATA"))
		case "/js/app.js":
			_, _ = w.Write([]byte("console.log(1)"))
		case "/css/site.css":
			_, _ = w.Write([]byte("body{background:url('/img/a.png')}"))
		case "/font/f.woff2":
			_, _ = w.Write([]byte("WOFF2-DATA"))
		case "/img/broken.png":
			if flakyImageOK != nil && flakyImageOK.Load() {
				_, _ = w.Write([]byte("PNG-OK-NOW"))
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &testServer{
		t:    t,
		ln:   ln,
		hits: hits,
		mux:  mux,
	}
	srv.start()
	t.Cleanup(srv.stop)
	return srv
}

// testServer wraps an httptest.Server on a fixed listener so Restart keeps the same URL.
type testServer struct {
	t    *testing.T
	ln   net.Listener
	hits map[string]*atomic.Int32
	mux  *http.ServeMux
	hs   *httptest.Server
}

func (s *testServer) start() {
	hs := httptest.NewUnstartedServer(s.mux)
	hs.Listener = s.ln
	hs.Start()
	s.hs = hs
}

func (s *testServer) stop() {
	if s.hs != nil {
		s.hs.Close()
		s.hs = nil
	}
}

// restart stops and re-serves on the same listener, preserving the server URL.
func (s *testServer) restart() {
	s.stop()
	s.start()
}

func (s *testServer) URL() string { return s.hs.URL }

func (s *testServer) hitsFor(path string) int32 {
	return s.hits[path].Load()
}

// runCloneIn executes a full CloneSite run inside workdir, capturing stdout (summary)
// and stderr (progress) separately.
func runCloneIn(t *testing.T, workdir, rawURL string) (stdout, stderr string) {
	t.Helper()
	prev, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(workdir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(prev) })

	oldOut, oldErr := os.Stdout, os.Stderr
	rOut, wOut, _ := os.Pipe()
	rErr, wErr, _ := os.Pipe()
	os.Stdout, os.Stderr = wOut, wErr

	var wg sync.WaitGroup
	var outBuf, errBuf bytes.Buffer
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = bufio.NewReader(rOut).WriteTo(&outBuf) }()
	go func() { defer wg.Done(); _, _ = bufio.NewReader(rErr).WriteTo(&errBuf) }()

	runErr := CloneSite(context.Background(), []string{rawURL}, Flags{})

	_ = wOut.Close()
	_ = wErr.Close()
	os.Stdout, os.Stderr = oldOut, oldErr
	wg.Wait()

	if runErr != nil {
		t.Fatalf("CloneSite failed: %v", runErr)
	}
	return outBuf.String(), errBuf.String()
}

func manifestPathFor(t *testing.T, workdir, rawURL string) string {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(workdir, u.Host, ManifestFileName)
}

func readManifestRecords(t *testing.T, path string) []manifest.Record {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open manifest: %v", err)
	}
	defer f.Close()
	var out []manifest.Record
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var r manifest.Record
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("manifest line unparseable: %v: %q", err, line)
		}
		out = append(out, r)
	}
	return out
}

func recordByURL(t *testing.T, path, wantURL string) manifest.Record {
	t.Helper()
	// The manifest is append-only, so a URL may have several history lines; the last
	// one is the current truth.
	var found manifest.Record
	matched := false
	for _, r := range readManifestRecords(t, path) {
		if r.URL == wantURL {
			found = r
			matched = true
		}
	}
	if !matched {
		t.Fatalf("no manifest record for %s", wantURL)
	}
	return found
}

// TestE2E_FirstRunWritesEverything covers the empty-manifest first run: every page and
// asset is fetched, the manifest records successes, and the summary counts them.
func TestE2E_FirstRunWritesEverything(t *testing.T) {
	var flaky atomic.Bool // stays false: broken.png always 500s
	ts := testSite(t, &flaky)
	workdir := t.TempDir()

	stdout, stderr := runCloneIn(t, workdir, ts.URL()+"/")
	mp := manifestPathFor(t, workdir, ts.URL()+"/")

	// Pages and healthy assets were really requested.
	if ts.hitsFor("/") == 0 || ts.hitsFor("/about") == 0 {
		t.Fatalf("pages not crawled: root=%d about=%d", ts.hitsFor("/"), ts.hitsFor("/about"))
	}
	if ts.hitsFor("/img/a.png") == 0 || ts.hitsFor("/js/app.js") == 0 ||
		ts.hitsFor("/css/site.css") == 0 || ts.hitsFor("/font/f.woff2") == 0 {
		t.Fatalf("assets not downloaded")
	}

	records := readManifestRecords(t, mp)
	if len(records) == 0 {
		t.Fatal("manifest must contain records after first run")
	}
	// Every physical line complete and independently parseable (checked in helper).
	root := recordByURL(t, mp, ts.URL()+"/")
	if root.Type != manifest.TypePage || root.Status != manifest.StatusSuccess || root.Path != "index.html" {
		t.Fatalf("unexpected root record: %+v", root)
	}
	img := recordByURL(t, mp, ts.URL()+"/img/a.png")
	if img.Status != manifest.StatusSuccess || img.Path != "assets/img/img-a.png" {
		t.Fatalf("unexpected img record: %+v", img)
	}
	// Failed asset is recorded as failed rather than silently dropped.
	broken := recordByURL(t, mp, ts.URL()+"/img/broken.png")
	if broken.Status != manifest.StatusFailed {
		t.Fatalf("broken image must be recorded failed, got %+v", broken)
	}

	// Summary on stdout, progress chatter on stderr only.
	if !strings.Contains(stdout, "Crawl summary:") {
		t.Fatalf("summary missing from stdout:\n%s", stdout)
	}
	if strings.Contains(stdout, "page:") || strings.Contains(stdout, "found -->") {
		t.Fatalf("progress must not appear on stdout:\n%s", stdout)
	}
	if !strings.Contains(stderr, "page:") {
		t.Fatalf("progress should appear on stderr:\n%s", stderr)
	}
	for _, want := range []string{"pages: new=2 skipped=0", "assets: new=4", "failed=1",
		"failures by type:", "img: 1", "manifest entries (cumulative):"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout)
		}
	}

	// Files really exist at the recorded paths.
	for _, p := range []string{
		filepath.Join(workdir, urlHost(t, ts.URL()), "index.html"),
		filepath.Join(workdir, urlHost(t, ts.URL()), "about", "index.html"),
		filepath.Join(workdir, urlHost(t, ts.URL()), "assets", "img", "img-a.png"),
		filepath.Join(workdir, urlHost(t, ts.URL()), "assets", "css", "css-site.css"),
	} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("expected mirror file %s: %v", p, err)
		}
	}
}

// TestE2E_SecondRunSkipsCompleted verifies that success items on disk are neither
// re-requested nor re-counted as new: memory and disk dedup agree.
func TestE2E_SecondRunSkipsCompleted(t *testing.T) {
	var flaky atomic.Bool
	ts := testSite(t, &flaky)
	workdir := t.TempDir()

	runCloneIn(t, workdir, ts.URL()+"/")
	firstRootHits := ts.hitsFor("/")
	firstImgHits := ts.hitsFor("/img/a.png")
	firstCSSHits := ts.hitsFor("/css/site.css")
	if firstRootHits == 0 || firstImgHits == 0 || firstCSSHits == 0 {
		t.Fatalf("first run did not fetch expected items")
	}

	stdout, _ := runCloneIn(t, workdir, ts.URL()+"/")

	// No repeat HTTP requests for anything that succeeded on disk.
	if ts.hitsFor("/") != firstRootHits {
		t.Fatalf("root page re-requested on resume: %d -> %d", firstRootHits, ts.hitsFor("/"))
	}
	if ts.hitsFor("/about") != 1 {
		t.Fatalf("about page re-requested: %d", ts.hitsFor("/about"))
	}
	if ts.hitsFor("/img/a.png") != firstImgHits || ts.hitsFor("/css/site.css") != firstCSSHits {
		t.Fatalf("successful assets re-requested on resume")
	}
	// The persistently failing asset is retried (not skipped).
	if ts.hitsFor("/img/broken.png") < 2 {
		t.Fatalf("failed asset must be retried on resume, hits=%d", ts.hitsFor("/img/broken.png"))
	}

	if !strings.Contains(stdout, "pages: new=0 skipped=2") {
		t.Fatalf("summary must show both pages skipped:\n%s", stdout)
	}
	if !strings.Contains(stdout, "skipped=") {
		t.Fatalf("summary must count skipped assets:\n%s", stdout)
	}
}

// TestE2E_FailedItemRecovers covers reprocessing a previously failed item: once the
// server starts answering, the manifest record flips to success and no failure remains.
func TestE2E_FailedItemRecovers(t *testing.T) {
	var flaky atomic.Bool
	ts := testSite(t, &flaky)
	workdir := t.TempDir()
	mp := manifestPathFor(t, workdir, ts.URL()+"/")

	runCloneIn(t, workdir, ts.URL()+"/")
	if got := recordByURL(t, mp, ts.URL()+"/img/broken.png"); got.Status != manifest.StatusFailed {
		t.Fatalf("expected failed first, got %+v", got)
	}

	flaky.Store(true)
	stdout, _ := runCloneIn(t, workdir, ts.URL()+"/")

	got := recordByURL(t, mp, ts.URL()+"/img/broken.png")
	if got.Status != manifest.StatusSuccess || got.Path != "assets/img/img-broken.png" {
		t.Fatalf("failed item must recover to success, got %+v", got)
	}
	if !strings.Contains(stdout, "assets: new=1") {
		t.Fatalf("only the recovered asset should be new:\n%s", stdout)
	}
	if !strings.Contains(stdout, "failed=0") {
		t.Fatalf("no failures should remain after recovery:\n%s", stdout)
	}
	// Cumulative count never shrinks: broken.png stays one distinct URL entry.
	records := readManifestRecords(t, mp)
	urls := map[string]bool{}
	for _, r := range records {
		urls[r.URL] = true
	}
	if len(urls) < 7 {
		t.Fatalf("cumulative distinct entries shrank: %d urls", len(urls))
	}
}

// TestE2E_MemoryAndDiskDedupAgree proves the in-memory gate blocks duplicate downloads
// within one run and the on-disk success view blocks them across runs.
func TestE2E_MemoryAndDiskDedupAgree(t *testing.T) {
	var flaky atomic.Bool
	ts := testSite(t, &flaky)
	workdir := t.TempDir()

	runCloneIn(t, workdir, ts.URL()+"/")
	// a.png is referenced from index HTML, inline CSS, the CSS file and /about:
	// in-memory dedup must collapse all of them to one download per run.
	if got := ts.hitsFor("/img/a.png"); got != 1 {
		t.Fatalf("in-memory dedup failed: /img/a.png requested %d times in one run", got)
	}

	runCloneIn(t, workdir, ts.URL()+"/")
	if got := ts.hitsFor("/img/a.png"); got != 1 {
		t.Fatalf("disk dedup failed: /img/a.png requested %d times across runs", got)
	}
}

// TestE2E_SameURLHasUniqueLocalPath ensures one resource URL never resolves to two
// different local paths inside the same page, even with many references.
func TestE2E_SameURLHasUniqueLocalPath(t *testing.T) {
	var flaky atomic.Bool
	ts := testSite(t, &flaky)
	workdir := t.TempDir()

	runCloneIn(t, workdir, ts.URL()+"/")
	indexBytes, err := os.ReadFile(filepath.Join(workdir, urlHost(t, ts.URL()), "index.html"))
	if err != nil {
		t.Fatal(err)
	}
	body := string(indexBytes)
	// Every rewritten occurrence of a.png must point at the single flat local path.
	if strings.Contains(body, "/img/a.png") {
		t.Fatalf("raw URL still present in page:\n%s", body)
	}
	want := "/assets/img/img-a.png"
	if !strings.Contains(body, want) {
		t.Fatalf("expected local path %s in page:\n%s", want, body)
	}
	// Collect distinct local paths that references to the a.png URL resolve to. The
	// deterministic rewrite allows exactly one; broken.png is a different resource and
	// therefore excluded.
	localPathsForA := map[string]bool{}
	for _, token := range strings.FieldsFunc(body, func(r rune) bool {
		return r == '"' || r == '\'' || r == '(' || r == ')' || r == ' ' || r == '\n' || r == '\t'
	}) {
		if strings.Contains(token, "img-a.png") {
			localPathsForA[token] = true
		}
	}
	if len(localPathsForA) != 1 {
		t.Fatalf("expected exactly one local path for a.png, got %d: %v", len(localPathsForA), localPathsForA)
	}
	if !localPathsForA[want] {
		t.Fatalf("unique path mismatch: %v want %s", localPathsForA, want)
	}
}

// TestE2E_UnwritableManifestFailsBeforeCrawl: a manifest path that cannot be a writable
// file must abort the run before any HTTP work, with no silent fallback.
func TestE2E_UnwritableManifestFailsBeforeCrawl(t *testing.T) {
	var flaky atomic.Bool
	ts := testSite(t, &flaky)
	workdir := t.TempDir()
	host := urlHost(t, ts.URL())
	mirror := filepath.Join(workdir, host)
	// A directory occupying the manifest path can never be opened as a writable file,
	// regardless of uid (permission bits are bypassed by root, a type mismatch is not).
	if err := os.MkdirAll(filepath.Join(mirror, ManifestFileName), 0o775); err != nil {
		t.Fatal(err)
	}

	prev, _ := os.Getwd()
	if err := os.Chdir(workdir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(prev) })

	err := CloneSite(context.Background(), []string{ts.URL() + "/"}, Flags{})
	if err == nil {
		t.Fatal("expected CloneSite to fail when manifest cannot be opened")
	}
	if !strings.Contains(err.Error(), "manifest") {
		t.Fatalf("error should mention manifest: %v", err)
	}
	if ts.hitsFor("/") != 0 {
		t.Fatalf("no crawl traffic allowed after manifest preflight failure, root hits=%d", ts.hitsFor("/"))
	}
}

// TestE2E_ManifestCumulativeCountOnlyGrows checks appending never truncates history and
// the reported cumulative count matches the distinct-URL memory view.
func TestE2E_ManifestCumulativeCountOnlyGrows(t *testing.T) {
	var flaky atomic.Bool
	ts := testSite(t, &flaky)
	workdir := t.TempDir()
	mp := manifestPathFor(t, workdir, ts.URL()+"/")

	stdout1, _ := runCloneIn(t, workdir, ts.URL()+"/")
	first := readManifestRecords(t, mp)
	firstLines := len(first)

	stdout2, _ := runCloneIn(t, workdir, ts.URL()+"/")
	second := readManifestRecords(t, mp)
	if len(second) < firstLines {
		t.Fatalf("manifest history truncated: %d -> %d lines", firstLines, len(second))
	}
	// The broken asset gets fresh failed lines on every run; distinct URL count is stable.
	distinct := map[string]bool{}
	for _, r := range second {
		distinct[r.URL] = true
	}
	if !strings.Contains(stdout1, "manifest entries (cumulative):") {
		t.Fatalf("first summary missing cumulative count:\n%s", stdout1)
	}
	if !strings.Contains(stdout2, "manifest entries (cumulative):") {
		t.Fatalf("second summary missing cumulative count:\n%s", stdout2)
	}
	if len(distinct) == 0 {
		t.Fatal("no distinct manifest URLs")
	}
}

// TestE2E_SkipCountedOncePerURL proves resume seeding and page-parse skips never
// double-count the same resource: the second-run skip total equals the distinct number
// of successful assets on disk, not the number of HTML references to them.
func TestE2E_SkipCountedOncePerURL(t *testing.T) {
	var flaky atomic.Bool
	ts := testSite(t, &flaky)
	workdir := t.TempDir()

	runCloneIn(t, workdir, ts.URL()+"/")
	// /img/a.png is referenced three times in the mirror (index img, index inline CSS,
	// the CSS file) but is exactly one distinct asset URL.
	stdout, _ := runCloneIn(t, workdir, ts.URL()+"/")

	// Distinct successful assets on disk: css, js, img/a.png, font = 4.
	if !strings.Contains(stdout, "assets: new=0 skipped=4 failed=") {
		t.Fatalf("each distinct asset skipped exactly once, got:\n%s", stdout)
	}
}

// TestE2E_ManifestWriteFailureEndsRunFailed verifies that when the single writer can no
// longer persist records, the failure surfaces as a non-nil run error rather than being
// swallowed.
func TestE2E_ManifestWriteFailureEndsRunFailed(t *testing.T) {
	var flaky atomic.Bool
	ts := testSite(t, &flaky)
	workdir := t.TempDir()

	prev, _ := os.Getwd()
	if err := os.Chdir(workdir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(prev) })

	// First run establishes a healthy manifest.
	if err := CloneSite(context.Background(), []string{ts.URL() + "/"}, Flags{}); err != nil {
		t.Fatalf("first run: %v", err)
	}

	// Sabotage the manifest file so appends fail at write time.
	mp := filepath.Join(workdir, urlHost(t, ts.URL()), ManifestFileName)
	if err := os.Chmod(mp, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(mp, 0o666) })

	// Root may bypass permission bits; in that case the directory-type preflight test
	// already covers fail-closed behavior, so skip rather than assert a false negative.
	if os.Geteuid() == 0 {
		t.Skip("root bypasses file permission bits; covered by the directory-path preflight test")
	}
	err := CloneSite(context.Background(), []string{ts.URL() + "/"}, Flags{})
	if err == nil {
		t.Fatal("a manifest write failure must end the run with an error")
	}
	if !strings.Contains(err.Error(), "manifest") {
		t.Fatalf("error should identify the manifest: %v", err)
	}
}

func urlHost(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u.Host
}

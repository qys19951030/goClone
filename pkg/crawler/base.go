package crawler

import (
	"context"
	"net/url"
	"path/filepath"
	"slices"
	"sync"

	"github.com/shurco/goClone/pkg/manifest"
	"github.com/shurco/goClone/pkg/netutil"
)

// Flags holds CLI-equivalent options for [CloneSite].
type Flags struct {
	// Open launches the mirrored index in the system default browser after the crawl.
	Open bool
	// Serve starts a static file HTTP server on ServePort and blocks until ctx is cancelled.
	Serve bool
	// ServePort is the TCP port for the optional static server (default in CLI: 8088).
	ServePort int
	// UserAgent is sent with HTTP requests (Geziyor and asset downloads).
	UserAgent string
	// ProxyString is a proxy list/URL accepted by Geziyor's RoundRobinProxy; empty disables.
	ProxyString string
	// Cookies, when true, disables sending cookies (Geziyor CookiesDisabled).
	Cookies bool
	// Robots, when true, disables robots.txt checks (Geziyor RobotsTxtDisabled).
	Robots bool
	// BrowserEndpoint is a Chrome DevTools WebSocket URL for rendered pages; empty uses HTTP only.
	BrowserEndpoint string
	// AssetsRoot is the top-level folder name for css/js/img/font under the mirror (e.g. "assets").
	AssetsRoot string
	// MaxConcurrentWorkers limits parallel asset downloads (semaphore size).
	MaxConcurrentWorkers int
	// MaxDownloadMB is the per-asset size cap in megabytes for downloads.
	MaxDownloadMB int
	// HTTPTimeoutSeconds sets the HTTP client timeout for asset fetches.
	HTTPTimeoutSeconds int
	// Verbose enables Geziyor logging when true.
	Verbose bool
}

var (
	files   filesBase
	filesMu sync.Mutex

	projectURL  *url.URL
	projectPath string
	domain      string

	// crawlCtx is the context from the current CloneSite call; used by download goroutines
	// to respect cancellation. Defaults to Background for safety in tests.
	crawlCtx = context.Background()

	// man is the run manifest: the single source of truth for dedup (memory and disk).
	// It stays nil in unit tests that exercise the old helpers directly.
	man *manifest.Manifest

	// runStats accumulates the end-of-run summary counters.
	runStats stats

	// downloadAsset schedules an asset download asynchronously. It is set by init() in
	// init_download.go and may be replaced by tests. The function is goroutine-safe.
	downloadAsset func(kind, link, projectPath string)
)

// stats holds the summary counters printed at the end of a run.
type stats struct {
	mu sync.Mutex

	pagesNew     int
	pagesSkipped int
	pagesFailed  int

	assetsNew     map[string]int // kind -> newly downloaded
	assetsSkipped map[string]int // kind -> skipped (success on disk or claimed in memory)
	assetsFailed  map[string]int // kind -> failed
}

func newStats() stats {
	return stats{
		assetsNew:     make(map[string]int),
		assetsSkipped: make(map[string]int),
		assetsFailed:  make(map[string]int),
	}
}

func (s *stats) pageNew() {
	s.mu.Lock()
	s.pagesNew++
	s.mu.Unlock()
}

func (s *stats) pageSkipped() {
	s.mu.Lock()
	s.pagesSkipped++
	s.mu.Unlock()
}

func (s *stats) pageFailed() {
	s.mu.Lock()
	s.pagesFailed++
	s.mu.Unlock()
}

func (s *stats) assetNew(kind string) {
	s.mu.Lock()
	s.assetsNew[kind]++
	s.mu.Unlock()
}

func (s *stats) assetSkipped(kind string) {
	s.mu.Lock()
	s.assetsSkipped[kind]++
	s.mu.Unlock()
}

func (s *stats) assetFailed(kind string) {
	s.mu.Lock()
	s.assetsFailed[kind]++
	s.mu.Unlock()
}

// snapshot returns consistent copies of every counter.
func (s *stats) snapshot() (pagesNew, pagesSkipped, pagesFailed int, newN, skippedN, failedN int, failedByType map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pagesNew, pagesSkipped, pagesFailed = s.pagesNew, s.pagesSkipped, s.pagesFailed
	failedByType = make(map[string]int, len(s.assetsFailed))
	if s.pagesFailed > 0 {
		failedByType[manifest.TypePage] = s.pagesFailed
	}
	for _, n := range s.assetsNew {
		newN += n
	}
	for _, n := range s.assetsSkipped {
		skippedN += n
	}
	for kind, n := range s.assetsFailed {
		failedN += n
		failedByType[kind] = n
	}
	return
}

type filesBase struct {
	pages []string
	css   []string
	js    []string
	img   []string
	font  []string
}

// addAsset appends link to the slice identified by kind under filesMu and returns true
// when the link was newly added, false if already known.
// Valid kinds: "css", "js", "img", "font", "pages".
func addAsset(kind, link string) bool {
	filesMu.Lock()
	defer filesMu.Unlock()

	var list *[]string
	switch kind {
	case "css":
		list = &files.css
	case "js":
		list = &files.js
	case "img":
		list = &files.img
	case "font":
		list = &files.font
	case "pages":
		list = &files.pages
	default:
		return false
	}

	if slices.Contains(*list, link) {
		return false
	}
	*list = append(*list, link)
	return true
}

// manifestKind maps crawler asset kind names to manifest record types.
func manifestKind(kind string) string {
	switch kind {
	case "css":
		return manifest.TypeCSS
	case "js":
		return manifest.TypeJS
	case "img":
		return manifest.TypeImg
	case "font":
		return manifest.TypeFont
	default:
		return kind
	}
}

// localRelPath computes the mirror-relative path of an asset URL, matching the layout
// written by netutil.Extractor (e.g. "assets/img/img-a.png"). Returns "" for URLs whose
// extension has no asset directory mapping.
func localRelPath(kind, absLink string) string {
	u, err := url.Parse(absLink)
	if err != nil {
		return ""
	}
	dir := netutil.Folders[kind]
	if dir == "" {
		return ""
	}
	return filepath.ToSlash(filepath.Join(dir, netutil.ReplaceSlashWithDash(u.Path)))
}

package crawler

import (
	"log"
	"sync"
	"time"

	"github.com/shurco/goClone/pkg/manifest"
	"github.com/shurco/goClone/pkg/netutil"
)

var (
	maxConcurrentDownloads = 8
	downloadSemaphore      = make(chan struct{}, maxConcurrentDownloads)
	semMu                  sync.Mutex
	// downloadWg tracks in-flight background downloads; call downloadWg.Wait() to
	// block until all assets scheduled via downloadAsset have finished.
	downloadWg sync.WaitGroup
)

// SetDownloadConcurrency replaces the global download semaphore capacity so at most n
// asset downloads run concurrently. Values n <= 0 are ignored.
func SetDownloadConcurrency(n int) {
	if n <= 0 {
		return
	}
	semMu.Lock()
	defer semMu.Unlock()
	maxConcurrentDownloads = n
	downloadSemaphore = make(chan struct{}, maxConcurrentDownloads)
}

// scheduleAsset is the manifest-aware gate in front of every asset download. It returns
// true when the caller may assume the asset is handled (downloaded now, or already
// successful on disk) and false when scheduling was refused. The manifest Claim is the
// single dedup authority: a success record from a previous run and an in-memory claim
// from this run both block the download.
func scheduleAsset(kind, link string) bool {
	if man == nil { // unit-test mode without a manifest
		if downloadAsset != nil {
			downloadAsset(kind, link, projectPath)
		}
		return true
	}
	if !man.Claim(link) {
		if man.IsSuccess(link) {
			runStats.assetSkippedURL(kind, link)
		}
		return false
	}
	downloadAsset(kind, link, projectPath)
	return true
}

func init() {
	downloadAsset = func(kind, link, path string) {
		downloadWg.Add(1)
		go func() {
			defer downloadWg.Done()
			downloadSemaphore <- struct{}{}
			defer func() { <-downloadSemaphore }()

			relPath := localRelPath(kind, link)
			const maxAttempts = 3
			var lastErr error
			for attempt := 1; attempt <= maxAttempts; attempt++ {
				if crawlCtx.Err() != nil {
					lastErr = crawlCtx.Err()
					break // context cancelled — stop retrying
				}
				if err := netutil.Extractor(crawlCtx, link, path); err == nil {
					recordAsset(kind, link, relPath, manifest.StatusSuccess, nil)
					return
				} else {
					lastErr = err
				}
				time.Sleep(time.Duration(attempt) * 300 * time.Millisecond)
			}
			log.Printf("download failed after retries: %s: %v", link, lastErr)
			recordAsset(kind, link, relPath, manifest.StatusFailed, lastErr)
		}()
	}
}

// recordAsset persists the outcome, updates run stats and surfaces a dead manifest as a
// run failure instead of silently dropping the record.
func recordAsset(kind, link, relPath, status string, cause error) {
	if man != nil {
		if err := man.Put(manifest.Record{
			URL:    link,
			Type:   manifestKind(kind),
			Path:   relPath,
			Status: status,
		}); err != nil {
			log.Printf("manifest record lost for %s: %v", link, err)
		}
	}
	switch status {
	case manifest.StatusSuccess:
		runStats.assetNew(kind)
	case manifest.StatusFailed:
		runStats.assetFailed(kind)
		if cause != nil {
			log.Printf("asset failed: %s: %v", link, cause)
		}
	}
}

// Package manifest implements the on-disk crawl manifest that makes goclone runs
// resumable. The manifest is a JSON Lines file (one JSON object per line) stored in
// the mirror directory. Records are append-only: the same URL may appear on several
// lines as its status changes, and the last record for a URL is the current truth.
//
// Only a single writer goroutine ever touches the file handle; producers (page crawl
// goroutines and asset download goroutines) hand records to it over a channel, so
// concurrent writes can never interleave on disk. Reads never load the whole file:
// read-back streams line by line and keeps only the latest record per URL in memory.
package manifest

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Record types.
const (
	TypePage = "page"
	TypeCSS  = "css"
	TypeJS   = "js"
	TypeImg  = "img"
	TypeFont = "font"
)

// Record statuses.
const (
	StatusSuccess = "success"
	StatusFailed  = "failed"
)

const (
	// flushBatchSize is the maximum number of records accumulated before the writer
	// appends a batch to disk.
	flushBatchSize = 64
	// flushInterval bounds how long records wait in the batch when traffic is sparse.
	flushInterval = 200 * time.Millisecond
	// readBufferSize covers long manifest lines without falling back to whole-file reads.
	readBufferSize = 1 << 20
)

// Record is one manifest line: the processing outcome of a URL.
type Record struct {
	// URL is the absolute address that was fetched or downloaded.
	URL string `json:"url"`
	// Type is one of TypePage/TypeCSS/TypeJS/TypeImg/TypeFont.
	Type string `json:"type"`
	// Path is the mirror-relative local path ("" when nothing was written).
	Path string `json:"path"`
	// Time is the RFC3339Nano UTC timestamp of the record.
	Time string `json:"time"`
	// Status is one of StatusSuccess/StatusFailed.
	Status string `json:"status"`
}

func (r Record) valid() error {
	if r.URL == "" {
		return errors.New("manifest: record url is empty")
	}
	switch r.Type {
	case TypePage, TypeCSS, TypeJS, TypeImg, TypeFont:
	default:
		return fmt.Errorf("manifest: unknown record type %q", r.Type)
	}
	switch r.Status {
	case StatusSuccess, StatusFailed:
	default:
		return fmt.Errorf("manifest: unknown record status %q", r.Status)
	}
	return nil
}

// Manifest is an append-only JSON Lines manifest with a synchronized in-memory view.
// All methods are goroutine-safe.
type Manifest struct {
	path string
	file *os.File
	now  func() time.Time

	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	records map[string]Record // latest record per URL; the synchronized memory view
	claimed map[string]struct{}
	fatal   error // set when the single writer can no longer persist records

	recCh  chan Record
	doneCh chan struct{}

	// prefix is written once before the first flush (newline separator when the
	// pre-existing file ended with a dropped fragment).
	prefix []byte

	closeMu sync.Mutex
	closed  bool
}

// Open prepares the manifest at path for a new run. The parent directory is created
// when missing; opening fails (rather than degrading to a manifest-less run) when the
// parent is not a directory, the path itself is a directory, or the file is not
// writable. An existing manifest is streamed back: the latest record per URL seeds the
// in-memory view, and a trailing partial line left by an interrupted process is
// discarded, with the number of dropped lines reported on stderr.
//
// Cancelling ctx (or a fatal write error inside the writer) stops in-flight work; the
// returned Manifest owns a derived context available via [Manifest.Context].
func Open(ctx context.Context, path string) (*Manifest, error) {
	dir := filepath.Dir(path)
	if fi, err := os.Stat(dir); err == nil && !fi.IsDir() {
		return nil, fmt.Errorf("manifest: directory %s is not a directory", dir)
	}
	if err := os.MkdirAll(dir, 0o775); err != nil {
		return nil, fmt.Errorf("manifest: create directory %s: %w", dir, err)
	}
	if fi, err := os.Stat(path); err == nil && fi.IsDir() {
		return nil, fmt.Errorf("manifest: %s is a directory, not a file", path)
	}

	records, dropped, unterminated, err := readBack(path)
	if err != nil {
		return nil, err
	}
	if dropped > 0 {
		fmt.Fprintf(os.Stderr, "manifest: dropped %d incomplete/unparseable line(s) from %s\n", dropped, path)
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o666)
	if err != nil {
		return nil, fmt.Errorf("manifest: open %s for append: %w", path, err)
	}

	mctx, cancel := context.WithCancel(ctx)
	m := &Manifest{
		path:    path,
		file:    file,
		now:     time.Now,
		ctx:     mctx,
		cancel:  cancel,
		records: records,
		claimed: make(map[string]struct{}),
		recCh:   make(chan Record, 2*flushBatchSize),
		doneCh:  make(chan struct{}),
	}
	// A trailing fragment from an interrupted run has no newline; the writer must start
	// on a fresh line so its first record never fuses with the unparseable fragment.
	if unterminated {
		m.prefix = []byte{'\n'}
	}
	go m.writer()
	return m, nil
}

// readBack streams the manifest line by line without loading the whole file. A final
// fragment without a trailing newline (interrupted write) or a line that fails to parse
// is counted as dropped and skipped. unterminated is true when the file ends with bytes
// that were not terminated by '\n', so the writer knows to separate new appends.
func readBack(path string) (records map[string]Record, dropped int, unterminated bool, err error) {
	records = make(map[string]Record)

	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return records, 0, false, nil
		}
		return nil, 0, false, fmt.Errorf("manifest: read back %s: %w", path, err)
	}
	defer f.Close()

	reader := bufio.NewReaderSize(f, readBufferSize)
	dropped = 0
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) > 0 {
			trimmed := bytes.TrimSpace(line)
			if readErr == io.EOF && !bytes.HasSuffix(line, []byte{'\n'}) {
				// Trailing fragment of an interrupted append: never trustworthy.
				dropped++
				unterminated = true
			} else if len(trimmed) > 0 {
				var r Record
				if jsonErr := json.Unmarshal(trimmed, &r); jsonErr != nil || r.valid() != nil {
					dropped++
				} else {
					records[r.URL] = r // later lines win: last record is current truth
				}
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, 0, false, fmt.Errorf("manifest: read back %s: %w", path, readErr)
		}
	}
	return records, dropped, unterminated, nil
}

// writer is the single owner of the file handle. It appends complete batches only, so
// any interruption leaves whole, parseable lines behind (a partially written final line
// is discarded on the next read-back).
func (m *Manifest) writer() {
	defer close(m.doneCh)

	batch := make([]Record, 0, flushBatchSize)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		var buf bytes.Buffer
		if len(m.prefix) > 0 {
			buf.Write(m.prefix)
			m.prefix = nil
		}
		for _, r := range batch {
			encoded, err := json.Marshal(r)
			if err != nil {
				m.fail(fmt.Errorf("manifest: encode record: %w", err))
				batch = batch[:0]
				return
			}
			buf.Write(encoded)
			buf.WriteByte('\n')
		}
		if _, err := m.file.Write(buf.Bytes()); err != nil {
			m.fail(fmt.Errorf("manifest: append %s: %w", m.path, err))
		} else if err := m.file.Sync(); err != nil {
			m.fail(fmt.Errorf("manifest: sync %s: %w", m.path, err))
		}
		batch = batch[:0]
	}

	for {
		select {
		case r, ok := <-m.recCh:
			if !ok {
				flush()
				return
			}
			batch = append(batch, r)
			if len(batch) >= flushBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// fail records the first writer error and cancels the run context so producers stop.
func (m *Manifest) fail(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fatal == nil {
		m.fatal = err
		m.cancel()
	}
}

// Context returns the context cancelled when the writer suffers a fatal error.
func (m *Manifest) Context() context.Context { return m.ctx }

// Err returns the fatal writer error, if any.
func (m *Manifest) Err() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.fatal
}

// IsSuccess reports whether url has a current success record in the memory view.
func (m *Manifest) IsSuccess(url string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.records[url]
	return ok && r.Status == StatusSuccess
}

// Claim reserves url for processing in this run. It returns true only when url has no
// success record on disk (from a previous run) and has not been claimed in memory
// during this run; failed and previously-unknown URLs are claimable. A manifest whose
// writer has failed rejects every claim so the run winds down instead of queuing work
// that cannot be persisted.
func (m *Manifest) Claim(url string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fatal != nil {
		return false
	}
	if r, ok := m.records[url]; ok && r.Status == StatusSuccess {
		return false
	}
	if _, ok := m.claimed[url]; ok {
		return false
	}
	m.claimed[url] = struct{}{}
	return true
}

// Put updates the in-memory view (latest status wins for a repeated URL) and enqueues
// the record for the single writer. It returns the fatal error once the manifest is
// unwritable so callers cannot lose records silently.
func (m *Manifest) Put(r Record) error {
	if err := r.valid(); err != nil {
		return err
	}
	if r.Time == "" {
		r.Time = m.now().UTC().Format(time.RFC3339Nano)
	}

	m.mu.Lock()
	if m.fatal != nil {
		err := m.fatal
		m.mu.Unlock()
		return err
	}
	m.records[r.URL] = r // count only grows: keys are added, never deleted
	m.mu.Unlock()

	select {
	case m.recCh <- r:
	case <-m.ctx.Done():
		m.mu.Lock()
		err := m.fatal
		m.mu.Unlock()
		if err != nil {
			return err
		}
		return m.ctx.Err()
	}
	return nil
}

// Count is the cumulative number of distinct URLs ever recorded; it only grows.
func (m *Manifest) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.records)
}

// Get returns the current record for url.
func (m *Manifest) Get(url string) (Record, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.records[url]
	return r, ok
}

// Close flushes every queued record, stops the writer and closes the file. The first
// persistent writer error, if any, is returned so the caller can end the run as failed.
// Close is idempotent: repeated calls return the same outcome.
func (m *Manifest) Close() error {
	m.closeMu.Lock()
	if m.closed {
		m.closeMu.Unlock()
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.fatal
	}
	m.closed = true
	m.closeMu.Unlock()

	close(m.recCh)
	<-m.doneCh
	closeErr := m.file.Close()

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fatal != nil {
		return m.fatal
	}
	return closeErr
}
